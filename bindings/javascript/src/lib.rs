#![deny(clippy::all)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use limbo_core::{maybe_init_database_file, LimboError};
use napi::{bindgen_prelude::ObjectFinalize, Env, JsUnknown, Result as NapiResult};
use napi_derive::napi;

#[napi(object)]
pub struct OpenDatabaseOptions {
    pub readonly: bool,
    pub file_must_exist: bool,
    pub timeout: u32,
    // verbose => Callback,
}

#[napi(custom_finalize)]
pub struct Database {
    #[napi(writable = false)]
    pub memory: bool,

    // TODO: implement each property
    // #[napi(writable = false)]
    // pub readonly: bool,
    // #[napi(writable = false)]
    // pub in_transaction: bool,
    // #[napi(writable = false)]
    // pub open: bool,
    #[napi(writable = false)]
    pub name: String,
    _db: Arc<limbo_core::Database>,
    conn: Rc<limbo_core::Connection>,
}

impl ObjectFinalize for Database {
    // TODO: check if something more is required
    fn finalize(self, _env: Env) -> napi::Result<()> {
        self.conn.close().map_err(into_napi_error)?;
        Ok(())
    }
}

#[napi]
impl Database {
    #[napi(constructor)]
    pub fn new(path: String, _options: Option<OpenDatabaseOptions>) -> napi::Result<Self> {
        let memory = path == ":memory:";
        let io: Arc<dyn limbo_core::IO> = if memory {
            Arc::new(limbo_core::MemoryIO::new())
        } else {
            Arc::new(limbo_core::PlatformIO::new().map_err(into_napi_error)?)
        };
        let file = io
            .open_file(&path, limbo_core::OpenFlags::Create, false)
            .map_err(into_napi_error)?;
        maybe_init_database_file(&file, &io).map_err(into_napi_error)?;
        let db_file = Arc::new(DatabaseFile::new(file));
        let db = limbo_core::Database::open(io, &path, db_file, false).map_err(into_napi_error)?;
        let conn = db.connect().map_err(into_napi_error)?;

        Ok(Self {
            memory,
            _db: db,
            conn,
            name: path,
        })
    }

    #[napi]
    pub fn prepare(&self, sql: String) -> napi::Result<Statement> {
        let stmt = self.conn.prepare(&sql).map_err(into_napi_error)?;
        Ok(Statement::new(RefCell::new(stmt)))
    }

    #[napi]
    pub fn transaction(&self) {
        todo!()
    }


    #[napi]
    pub fn pragma(&self) {
        todo!()
    }


    #[napi]
    pub fn backup(&self) {
        todo!()
    }


    #[napi]
    pub fn serialize(&self) {
        todo!()
    }


    #[napi]
    pub fn function(&self) {
        todo!()
    }


    #[napi]
    pub fn aggregate(&self) {
        todo!()
    }


    #[napi]
    pub fn table(&self) {
        todo!()
    }


    #[napi]
    pub fn load_extension(&self) {
        todo!()
    }


    #[napi]
    pub fn exec(&self) {
        todo!()
    }


    #[napi]
    pub fn close(&self) {
        todo!()
    }
}

// TODO: Add the (parent) 'database' property
#[napi]
pub struct Statement {
    // TODO: implement each property when core supports it
    // #[napi(writable = false)]
    // pub reader: bool,
    // #[napi(writable = false)]
    // pub readonly: bool,
    // #[napi(writable = false)]
    // pub busy: bool,
    inner: RefCell<limbo_core::Statement>,
}

#[napi]
impl Statement {
    pub fn new(inner: RefCell<limbo_core::Statement>) -> Self {
        Self { inner }
    }

    #[napi]
    pub fn get(&self, env: Env) -> napi::Result<JsUnknown> {
        let mut stmt = self.inner.borrow_mut();
        stmt.reset();
        let step = stmt.step().map_err(into_napi_error)?;
        match step {
            limbo_core::StepResult::Row => {
                let row = stmt.row().unwrap();
                let mut obj = env.create_object()?;
                for (idx, value) in row.get_values().enumerate() {
                    let key = stmt.get_column_name(idx);
                    let js_value = to_js_value(&env, value);
                    obj.set_named_property(&key, js_value)?;
                }
                Ok(obj.into_unknown())
            }
            limbo_core::StepResult::Done => Ok(env.get_undefined()?.into_unknown()),
            limbo_core::StepResult::IO => todo!(),
            limbo_core::StepResult::Interrupt | limbo_core::StepResult::Busy => Err(
                napi::Error::new(napi::Status::GenericFailure, format!("{:?}", step)),
            ),
        }
    }

    #[napi]
    pub fn run(&self, _env: Env, _args: Vec<JsUnknown>) {
        todo!()
    }

    #[napi]
    pub fn iterate() {
        todo!()
    }

    #[napi]
    pub fn pluck() {
        todo!()
    }
    #[napi]
    pub fn expand() {
        todo!()
    }
    #[napi]
    pub fn raw() {
        todo!()
    }
    #[napi]
    pub fn columns() {
        todo!()
    }
    #[napi]
    pub fn bind() {
        todo!()
    }
}

fn to_js_value(env: &napi::Env, value: &limbo_core::OwnedValue) -> napi::Result<JsUnknown> {
    match value {
        limbo_core::OwnedValue::Null => Ok(env.get_null()?.into_unknown()),
        limbo_core::OwnedValue::Integer(i) => Ok(env.create_int64(*i)?.into_unknown()),
        limbo_core::OwnedValue::Float(f) => Ok(env.create_double(*f)?.into_unknown()),
        limbo_core::OwnedValue::Text(s) => Ok(env.create_string(s.as_str())?.into_unknown()),
        limbo_core::OwnedValue::Blob(b) => Ok(env.create_buffer_copy(b.as_slice())?.into_unknown()),
    }
}

struct DatabaseFile {
    file: Arc<dyn limbo_core::File>,
}

unsafe impl Send for DatabaseFile {}
unsafe impl Sync for DatabaseFile {}

impl DatabaseFile {
    pub fn new(file: Arc<dyn limbo_core::File>) -> Self {
        Self { file }
    }
}

impl limbo_core::DatabaseStorage for DatabaseFile {
    fn read_page(&self, page_idx: usize, c: limbo_core::Completion) -> limbo_core::Result<()> {
        let r = match c {
            limbo_core::Completion::Read(ref r) => r,
            _ => unreachable!(),
        };
        let size = r.buf().len();
        assert!(page_idx > 0);
        if !(512..=65536).contains(&size) || size & (size - 1) != 0 {
            return Err(limbo_core::LimboError::NotADB);
        }
        let pos = (page_idx - 1) * size;
        self.file.pread(pos, c)?;
        Ok(())
    }

    fn write_page(
        &self,
        page_idx: usize,
        buffer: Arc<std::cell::RefCell<limbo_core::Buffer>>,
        c: limbo_core::Completion,
    ) -> limbo_core::Result<()> {
        let size = buffer.borrow().len();
        let pos = (page_idx - 1) * size;
        self.file.pwrite(pos, buffer, c)?;
        Ok(())
    }

    fn sync(&self, c: limbo_core::Completion) -> limbo_core::Result<()> {
        self.file.sync(c)
    }
}

#[inline]
pub fn into_napi_error(limbo_error: LimboError) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, format!("{limbo_error}"))
}
