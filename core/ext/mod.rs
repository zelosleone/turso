#[cfg(feature = "fs")]
mod dynamic;
mod vtab_xconnect;
use crate::schema::{Schema, Table};
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use crate::UringIO;
use crate::{function::ExternalFunc, Connection, Database, LimboError, IO};
use crate::{vtab::VirtualTable, SymbolTable};
#[cfg(feature = "fs")]
pub use dynamic::{add_builtin_vfs_extensions, add_vfs_module, list_vfs_modules, VfsMod};
use parking_lot::Mutex;
use std::{
    ffi::{c_char, c_void, CStr, CString},
    rc::Rc,
    sync::Arc,
};
use turso_ext::{
    ExtensionApi, InitAggFunction, ResultCode, ScalarFunction, VTabKind, VTabModuleImpl,
};
pub use turso_ext::{FinalizeFunction, StepFunction, Value as ExtValue, ValueType as ExtValueType};
pub use vtab_xconnect::{close, execute, prepare_stmt};

/// The context passed to extensions to register with Core
/// along with the function pointers
#[repr(C)]
pub struct ExtensionCtx {
    syms: *mut SymbolTable,
    schema_data: *mut c_void,
    schema_handler: SchemaHandler,
}

type SchemaHandler = unsafe extern "C" fn(
    schema_data: *mut c_void,
    table_name: *const c_char,
    table: *mut c_void,
) -> ResultCode;

/// Handler for our Connection that has direct Arc<Schema> access
/// to register a table from an extension.
unsafe extern "C" fn handle_schema_insert_connection(
    schema_data: *mut c_void,
    table_name: *const c_char,
    table: *mut c_void,
) -> ResultCode {
    let schema = &mut *(schema_data as *mut Schema);
    let c_str = CStr::from_ptr(table_name);
    let name = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    let table = Box::from_raw(table as *mut Arc<Table>);
    schema.tables.insert(name, *table);
    ResultCode::OK
}

/// Handler for Database with Mutex<Arc<Schema>> access to
/// register a table from an extension.
unsafe extern "C" fn handle_schema_insert_database(
    schema_data: *mut c_void,
    table_name: *const c_char,
    table: *mut c_void,
) -> ResultCode {
    let mutex = &*(schema_data as *mut Mutex<Arc<Schema>>);
    let mut guard = mutex.lock();
    let schema = Arc::make_mut(&mut *guard);

    let c_str = CStr::from_ptr(table_name);
    let name = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    let table = Box::from_raw(table as *mut Arc<Table>);
    schema.tables.insert(name, *table);
    ResultCode::OK
}

pub(crate) unsafe extern "C" fn register_vtab_module(
    ctx: *mut c_void,
    name: *const c_char,
    module: VTabModuleImpl,
    kind: VTabKind,
) -> ResultCode {
    if name.is_null() || ctx.is_null() {
        return ResultCode::Error;
    }

    let c_str = unsafe { CString::from_raw(name as *mut c_char) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::Error,
    };

    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    let module = Rc::new(module);
    let vmodule = VTabImpl {
        module_kind: kind,
        implementation: module,
    };

    unsafe {
        let syms = &mut *ext_ctx.syms;
        syms.vtab_modules.insert(name_str.clone(), vmodule.into());

        if kind == VTabKind::TableValuedFunction {
            if let Ok(vtab) = VirtualTable::function(&name_str, syms) {
                // Use the schema handler to insert the table
                let table = Box::into_raw(Box::new(Arc::new(Table::Virtual(vtab))));
                let c_name = match CString::new(name_str) {
                    Ok(s) => s,
                    Err(_) => return ResultCode::Error,
                };

                let result = (ext_ctx.schema_handler)(
                    ext_ctx.schema_data,
                    c_name.as_ptr(),
                    table as *mut c_void,
                );
                if result != ResultCode::OK {
                    let _ = Box::from_raw(table);
                    return result;
                }
            } else {
                return ResultCode::Error;
            }
        }
    }
    ResultCode::OK
}

#[derive(Clone)]
pub struct VTabImpl {
    pub module_kind: VTabKind,
    pub implementation: Rc<VTabModuleImpl>,
}

pub(crate) unsafe extern "C" fn register_scalar_function(
    ctx: *mut c_void,
    name: *const c_char,
    func: ScalarFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        (*ext_ctx.syms).functions.insert(
            name_str.clone(),
            Rc::new(ExternalFunc::new_scalar(name_str, func)),
        );
    }
    ResultCode::OK
}

pub(crate) unsafe extern "C" fn register_aggregate_function(
    ctx: *mut c_void,
    name: *const c_char,
    args: i32,
    init_func: InitAggFunction,
    step_func: StepFunction,
    finalize_func: FinalizeFunction,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(name) };
    let name_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return ResultCode::InvalidArgs,
    };
    if ctx.is_null() {
        return ResultCode::Error;
    }
    let ext_ctx = unsafe { &mut *(ctx as *mut ExtensionCtx) };
    unsafe {
        (*ext_ctx.syms).functions.insert(
            name_str.clone(),
            Rc::new(ExternalFunc::new_aggregate(
                name_str,
                args,
                (init_func, step_func, finalize_func),
            )),
        );
    }
    ResultCode::OK
}

impl Database {
    #[cfg(feature = "fs")]
    #[allow(clippy::arc_with_non_send_sync, dead_code)]
    pub fn open_with_vfs(
        &self,
        path: &str,
        vfs: &str,
    ) -> crate::Result<(Arc<dyn IO>, Arc<Database>)> {
        use crate::{MemoryIO, SyscallIO};
        use dynamic::get_vfs_modules;

        let io: Arc<dyn IO> = match vfs {
            "memory" => Arc::new(MemoryIO::new()),
            "syscall" => Arc::new(SyscallIO::new()?),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            "io_uring" => Arc::new(UringIO::new()?),
            other => match get_vfs_modules().iter().find(|v| v.0 == vfs) {
                Some((_, vfs)) => vfs.clone(),
                None => {
                    return Err(LimboError::InvalidArgument(format!("no such VFS: {other}")));
                }
            },
        };
        let db = Self::open_file(io.clone(), path, false, false)?;
        Ok((io, db))
    }

    /// Register any built-in extensions that can be stored on the Database so we do not have
    /// to register these once-per-connection, and the connection can just extend its symbol table
    pub fn register_global_builtin_extensions(&self) -> Result<(), String> {
        let syms = self.builtin_syms.as_ptr();
        // Pass the mutex pointer and the appropriate handler
        let schema_mutex_ptr = &self.schema as *const Mutex<Arc<Schema>> as *mut Mutex<Arc<Schema>>;
        let ctx = Box::into_raw(Box::new(ExtensionCtx {
            syms,
            schema_data: schema_mutex_ptr as *mut c_void,
            schema_handler: handle_schema_insert_database,
        }));
        let mut ext_api = ExtensionApi {
            ctx: ctx as *mut c_void,
            register_scalar_function,
            register_aggregate_function,
            register_vtab_module,
            #[cfg(feature = "fs")]
            vfs_interface: turso_ext::VfsInterface {
                register_vfs: dynamic::register_vfs,
                builtin_vfs: std::ptr::null_mut(),
                builtin_vfs_count: 0,
            },
        };

        #[cfg(feature = "uuid")]
        crate::uuid::register_extension(&mut ext_api);
        #[cfg(feature = "series")]
        crate::series::register_extension(&mut ext_api);
        #[cfg(feature = "fs")]
        {
            let vfslist = add_builtin_vfs_extensions(Some(ext_api)).map_err(|e| e.to_string())?;
            for (name, vfs) in vfslist {
                add_vfs_module(name, vfs);
            }
        }
        let _ = unsafe { Box::from_raw(ctx) };
        Ok(())
    }
}

impl Connection {
    /// # Safety:
    /// Only to be used when registering a staticly linked extension manually.
    /// you probably want to use `Connection::load_extension(path)`.
    /// Do not call if you have multiple connection open.
    pub fn _build_turso_ext(&self) -> ExtensionApi {
        let schema_ptr = self.schema.as_ptr();
        let schema_direct = unsafe { Arc::as_ptr(&*schema_ptr) as *mut Schema };
        let ctx = ExtensionCtx {
            syms: self.syms.as_ptr(),
            schema_data: schema_direct as *mut c_void,
            schema_handler: handle_schema_insert_connection,
        };

        let ctx = Box::into_raw(Box::new(ctx)) as *mut c_void;
        ExtensionApi {
            ctx,
            register_scalar_function,
            register_aggregate_function,
            register_vtab_module,
            #[cfg(feature = "fs")]
            vfs_interface: turso_ext::VfsInterface {
                register_vfs: dynamic::register_vfs,
                builtin_vfs: std::ptr::null_mut(),
                builtin_vfs_count: 0,
            },
        }
    }

    /// # Safety:
    /// Only to be used if you have previously called Connection::build_turso_ext
    /// before registering an extension manually.
    pub unsafe fn _free_extension_ctx(&self, api: ExtensionApi) {
        if api.ctx.is_null() {
            return;
        }
        let _ = unsafe { Box::from_raw(api.ctx as *mut ExtensionCtx) };
    }
}
