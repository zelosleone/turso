use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};

pub fn derive_vtab_module(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &ast.ident;

    let register_fn_name = format_ident!("register_{}", struct_name);
    let create_schema_fn_name = format_ident!("create_schema_{}", struct_name);
    let open_fn_name = format_ident!("open_{}", struct_name);
    let close_fn_name = format_ident!("close_{}", struct_name);
    let filter_fn_name = format_ident!("filter_{}", struct_name);
    let column_fn_name = format_ident!("column_{}", struct_name);
    let next_fn_name = format_ident!("next_{}", struct_name);
    let eof_fn_name = format_ident!("eof_{}", struct_name);
    let update_fn_name = format_ident!("update_{}", struct_name);
    let rowid_fn_name = format_ident!("rowid_{}", struct_name);
    let destroy_fn_name = format_ident!("destroy_{}", struct_name);
    let best_idx_fn_name = format_ident!("best_idx_{}", struct_name);

    let expanded = quote! {
        impl #struct_name {
            #[no_mangle]
            unsafe extern "C" fn #create_schema_fn_name(
                argv: *const ::limbo_ext::Value, argc: i32
            ) -> *mut ::std::ffi::c_char {
                let args = if argv.is_null() {
                    &Vec::new()
                } else {
                    ::std::slice::from_raw_parts(argv, argc as usize)
                };
                let sql = <#struct_name as ::limbo_ext::VTabModule>::create_schema(&args);
                ::std::ffi::CString::new(sql).unwrap().into_raw()
            }

            #[no_mangle]
            unsafe extern "C" fn #open_fn_name(ctx: *const ::std::ffi::c_void) -> *const ::std::ffi::c_void {
                if ctx.is_null() {
                    return ::std::ptr::null();
                }
                let ctx  = ctx as *const #struct_name;
                let ctx: &#struct_name = &*ctx;
                if let Ok(cursor) = <#struct_name as ::limbo_ext::VTabModule>::open(ctx) {
                    return ::std::boxed::Box::into_raw(::std::boxed::Box::new(cursor)) as *const ::std::ffi::c_void;
                } else {
                    return ::std::ptr::null();
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #close_fn_name(
                cursor: *const ::std::ffi::c_void
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let boxed_cursor = ::std::boxed::Box::from_raw(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                boxed_cursor.close()
            }

            #[no_mangle]
            unsafe extern "C" fn #filter_fn_name(
                cursor: *const ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
                idx_str: *const ::std::ffi::c_char,
                idx_num: i32,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                let args = ::std::slice::from_raw_parts(argv, argc as usize);
                let idx_str = if idx_str.is_null() {
                    None
                } else {
                    Some((unsafe { ::std::ffi::CStr::from_ptr(idx_str).to_str().unwrap() }, idx_num))
                };
                <#struct_name as ::limbo_ext::VTabModule>::filter(cursor, args, idx_str)
            }

            #[no_mangle]
            unsafe extern "C" fn #column_fn_name(
                cursor: *const ::std::ffi::c_void,
                idx: u32,
            ) -> ::limbo_ext::Value {
                if cursor.is_null() {
                    return ::limbo_ext::Value::error(::limbo_ext::ResultCode::Error);
                }
                let cursor = unsafe { &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor) };
                match <#struct_name as ::limbo_ext::VTabModule>::column(cursor, idx) {
                    Ok(val) => val,
                    Err(e) => ::limbo_ext::Value::error_with_message(e.to_string())
                }
            }

            #[no_mangle]
            unsafe extern "C" fn #next_fn_name(
                cursor: *const ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if cursor.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let cursor = &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                <#struct_name as ::limbo_ext::VTabModule>::next(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #eof_fn_name(
                cursor: *const ::std::ffi::c_void,
            ) -> bool {
                if cursor.is_null() {
                    return true;
                }
                let cursor = &mut *(cursor as *mut <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                <#struct_name as ::limbo_ext::VTabModule>::eof(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #update_fn_name(
                vtab: *const ::std::ffi::c_void,
                argc: i32,
                argv: *const ::limbo_ext::Value,
                p_out_rowid: *mut i64,
            ) -> ::limbo_ext::ResultCode {
                if vtab.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let vtab = &mut *(vtab as *mut #struct_name);
                let args = ::std::slice::from_raw_parts(argv, argc as usize);

                let old_rowid = match args.get(0).map(|v| v.value_type()) {
                    Some(::limbo_ext::ValueType::Integer) => args.get(0).unwrap().to_integer(),
                    _ => None,
                };
                let new_rowid = match args.get(1).map(|v| v.value_type()) {
                    Some(::limbo_ext::ValueType::Integer) => args.get(1).unwrap().to_integer(),
                    _ => None,
                };
                let columns = &args[2..];
                match (old_rowid, new_rowid) {
                    // DELETE: old_rowid provided, no new_rowid
                    (Some(old), None) => {
                     if <#struct_name as VTabModule>::delete(vtab, old).is_err() {
                            return ::limbo_ext::ResultCode::Error;
                      }
                            return ::limbo_ext::ResultCode::OK;
                    }
                    // UPDATE: old_rowid provided and new_rowid may exist
                    (Some(old), Some(new)) => {
                        if <#struct_name as VTabModule>::update(vtab, old, &columns).is_err() {
                            return ::limbo_ext::ResultCode::Error;
                        }
                        return ::limbo_ext::ResultCode::OK;
                    }
                    // INSERT: no old_rowid (old_rowid = None)
                    (None, _) => {
                        if let Ok(rowid) = <#struct_name as VTabModule>::insert(vtab, &columns) {
                            if !p_out_rowid.is_null() {
                                *p_out_rowid = rowid;
                                 return ::limbo_ext::ResultCode::RowID;
                            }
                            return ::limbo_ext::ResultCode::OK;
                        }
                    }
                }
                return ::limbo_ext::ResultCode::Error;
            }

            #[no_mangle]
            pub unsafe extern "C" fn #rowid_fn_name(ctx: *const ::std::ffi::c_void) -> i64 {
                if ctx.is_null() {
                    return -1;
                }
                let cursor = &*(ctx as *const <#struct_name as ::limbo_ext::VTabModule>::VCursor);
                <<#struct_name as ::limbo_ext::VTabModule>::VCursor as ::limbo_ext::VTabCursor>::rowid(cursor)
            }

            #[no_mangle]
            unsafe extern "C" fn #destroy_fn_name(
                vtab: *const ::std::ffi::c_void,
            ) -> ::limbo_ext::ResultCode {
                if vtab.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }

                let vtab = &mut *(vtab as *mut #struct_name);
                if <#struct_name as VTabModule>::destroy(vtab).is_err() {
                    return ::limbo_ext::ResultCode::Error;
                }

                return ::limbo_ext::ResultCode::OK;
            }

            #[no_mangle]
            pub unsafe extern "C" fn #best_idx_fn_name(
                constraints: *const ::limbo_ext::ConstraintInfo,
                n_constraints: i32,
                order_by: *const ::limbo_ext::OrderByInfo,
                n_order_by: i32,
            ) -> ::limbo_ext::ExtIndexInfo {
                let constraints = if n_constraints > 0 { std::slice::from_raw_parts(constraints, n_constraints as usize) } else { &[] };
                let order_by = if n_order_by > 0 { std::slice::from_raw_parts(order_by, n_order_by as usize) } else { &[] };
                <#struct_name as ::limbo_ext::VTabModule>::best_index(constraints, order_by).to_ffi()
            }

            #[no_mangle]
            pub unsafe extern "C" fn #register_fn_name(
                api: *const ::limbo_ext::ExtensionApi
            ) -> ::limbo_ext::ResultCode {
                if api.is_null() {
                    return ::limbo_ext::ResultCode::Error;
                }
                let api = &*api;
                let name = <#struct_name as ::limbo_ext::VTabModule>::NAME;
                let name_c = ::std::ffi::CString::new(name).unwrap().into_raw() as *const ::std::ffi::c_char;
                let table_instance = ::std::boxed::Box::into_raw(::std::boxed::Box::new(#struct_name::default()));
                let module = ::limbo_ext::VTabModuleImpl {
                    ctx: table_instance as *const ::std::ffi::c_void,
                    name: name_c,
                    create_schema: Self::#create_schema_fn_name,
                    open: Self::#open_fn_name,
                    close: Self::#close_fn_name,
                    filter: Self::#filter_fn_name,
                    column: Self::#column_fn_name,
                    next: Self::#next_fn_name,
                    eof: Self::#eof_fn_name,
                    update: Self::#update_fn_name,
                    rowid: Self::#rowid_fn_name,
                    destroy: Self::#destroy_fn_name,
                    best_idx: Self::#best_idx_fn_name,
                };
                (api.register_vtab_module)(api.ctx, name_c, module, <#struct_name as ::limbo_ext::VTabModule>::VTAB_KIND)
            }
        }
    };

    TokenStream::from(expanded)
}
