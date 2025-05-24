use crate::{types::Value, Connection, Statement, StepResult};
use limbo_ext::{Conn as ExtConn, ResultCode, Stmt, Value as ExtValue};
use std::{
    boxed::Box,
    ffi::{c_char, c_void, CStr, CString},
    num::NonZeroUsize,
    ptr,
    rc::Weak,
};

pub unsafe extern "C" fn close(ctx: *mut c_void) {
    if ctx.is_null() {
        return;
    }
    // only free the memory for the boxed connection, we don't upgrade
    // and actually close the core connection.
    let _ = Box::from_raw(ctx as *mut Weak<Connection>);
}

/// This function takes ownership of the optional limbo_ext::Value array if provided
pub unsafe extern "C" fn execute(
    ctx: *mut ExtConn,
    sql: *const c_char,
    args: *mut ExtValue,
    arg_count: i32,
    last_insert_rowid: *mut i64,
) -> ResultCode {
    let c_str = unsafe { CStr::from_ptr(sql as *mut c_char) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            tracing::error!("query: failed to convert sql to string");
            return ResultCode::Error;
        }
    };
    let Ok(extcon) = ExtConn::from_ptr(ctx) else {
        tracing::error!("query: null connection");
        return ResultCode::Error;
    };
    let weak_ptr = extcon._ctx as *const Weak<Connection>;
    let weak = &*weak_ptr;
    let Some(conn) = weak.upgrade() else {
        tracing::error!("prepare_stmt: failed to upgrade weak pointer in prepare stmt");
        return ResultCode::Error;
    };
    match conn.query(&sql_str) {
        Ok(Some(mut stmt)) => {
            if arg_count > 0 {
                let args_slice = &mut std::slice::from_raw_parts_mut(args, arg_count as usize);
                for (i, val) in args_slice.iter_mut().enumerate() {
                    stmt.bind_at(
                        NonZeroUsize::new(i + 1).unwrap(),
                        Value::from_ffi(std::mem::take(val)).unwrap_or(Value::Null),
                    );
                }
            }
            loop {
                match stmt.step() {
                    Ok(StepResult::Row) => {
                        tracing::error!("execute used for query returning a row");
                        return ResultCode::Error;
                    }
                    Ok(StepResult::Done) => {
                        *last_insert_rowid = conn.last_insert_rowid() as i64;
                        return ResultCode::OK;
                    }
                    Ok(StepResult::IO) => {
                        let _ = conn.pager.io.run_once();
                        continue;
                    }
                    Ok(StepResult::Interrupt) => return ResultCode::Interrupt,
                    Ok(StepResult::Busy) => return ResultCode::Busy,
                    Err(e) => {
                        tracing::error!("execute: failed to execute query: {:?}", e);
                        return ResultCode::Error;
                    }
                }
            }
        }
        Ok(None) => tracing::error!("query: no statement returned"),
        Err(e) => tracing::error!("query: failed to execute query: {:?}", e),
    };
    ResultCode::Error
}

pub unsafe extern "C" fn prepare_stmt(ctx: *mut ExtConn, sql: *const c_char) -> *const Stmt {
    let c_str = unsafe { CStr::from_ptr(sql as *mut c_char) };
    let sql_str = match c_str.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            tracing::error!("prepare_stmt: failed to convert sql to string");
            return ptr::null_mut();
        }
    };
    let Ok(extcon) = ExtConn::from_ptr(ctx) else {
        tracing::error!("prepare_stmt: null connection");
        return ptr::null_mut();
    };
    let weak_ptr = extcon._ctx as *const Weak<Connection>;
    let weak = &*weak_ptr;
    let Some(conn) = weak.upgrade() else {
        tracing::error!("prepare_stmt: failed to upgrade weak pointer in prepare stmt");
        return ptr::null_mut();
    };
    match conn.prepare(&sql_str) {
        Ok(stmt) => {
            let raw_stmt = Box::into_raw(Box::new(stmt)) as *mut c_void;
            Box::into_raw(Box::new(Stmt::new(
                extcon._ctx,
                raw_stmt,
                stmt_bind_args_fn,
                stmt_step,
                stmt_get_row,
                stmt_get_column_names,
                stmt_free_current_row,
                stmt_close,
            ))) as *const Stmt
        }
        Err(_) => ptr::null_mut(),
    }
}

/// expected 1 based indexing
pub unsafe extern "C" fn stmt_bind_args_fn(
    ctx: *mut Stmt,
    idx: i32,
    arg: *const ExtValue,
) -> ResultCode {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        tracing::error!("prepare_stmt: null stmt pointer");
        return ResultCode::Error;
    };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let Ok(owned_val) = Value::from_ffi_ptr(arg) else {
        tracing::error!("stmt_bind_args_fn: failed to convert arg to Value");
        return ResultCode::Error;
    };
    let Some(idx) = NonZeroUsize::new(idx as usize) else {
        tracing::error!("stmt_bind_args_fn: invalid index");
        return ResultCode::Error;
    };
    stmt_ctx.bind_at(idx, owned_val);
    ResultCode::OK
}

pub unsafe extern "C" fn stmt_step(stmt: *mut Stmt) -> ResultCode {
    let Ok(stmt) = Stmt::from_ptr(stmt) else {
        tracing::error!("stmt_step: failed to convert stmt to Stmt");
        return ResultCode::Error;
    };
    if stmt._conn.is_null() || stmt._ctx.is_null() {
        tracing::error!("stmt_step: null connection or context");
        return ResultCode::Error;
    }
    let conn: &Connection = unsafe { &*(stmt._conn as *const Connection) };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    while let Ok(res) = stmt_ctx.step() {
        match res {
            StepResult::Row => return ResultCode::Row,
            StepResult::Done => return ResultCode::EOF,
            StepResult::IO => {
                // always handle IO step result internally.
                let _ = conn.pager.io.run_once();
                continue;
            }
            StepResult::Interrupt => return ResultCode::Interrupt,
            StepResult::Busy => return ResultCode::Busy,
        }
    }
    ResultCode::Error
}

pub unsafe extern "C" fn stmt_get_row(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        tracing::error!("stmt_get_row: failed to convert stmt to Stmt");
        return;
    };
    if !stmt.current_row.is_null() {
        stmt.free_current_row();
    }
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    if let Some(row) = stmt_ctx.row() {
        let values = row.get_values();
        let mut owned_values = Vec::with_capacity(row.len());
        for value in values {
            owned_values.push(Value::to_ffi(value));
        }
        stmt.current_row = Box::into_raw(owned_values.into_boxed_slice()) as *mut ExtValue;
        stmt.current_row_len = row.len() as i32;
    } else {
        stmt.current_row_len = 0;
    }
}

pub unsafe extern "C" fn stmt_free_current_row(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        return;
    };
    if !stmt.current_row.is_null() {
        let values: &mut [ExtValue] =
            std::slice::from_raw_parts_mut(stmt.current_row, stmt.current_row_len as usize);
        for value in values.iter_mut() {
            let owned_value = std::mem::take(value);
            owned_value.__free_internal_type();
        }
        let _ = Box::from_raw(stmt.current_row);
    }
}

pub unsafe extern "C" fn stmt_get_column_names(
    ctx: *mut Stmt,
    count: *mut i32,
) -> *mut *mut c_char {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        *count = 0;
        return ptr::null_mut();
    };
    let stmt_ctx: &mut Statement = unsafe { &mut *(stmt._ctx as *mut Statement) };
    let num_cols = stmt_ctx.num_columns();
    if num_cols == 0 {
        *count = 0;
        return ptr::null_mut();
    }
    let mut c_names: Vec<*mut c_char> = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let name = stmt_ctx.get_column_name(i);
        let c_str = CString::new(name.as_bytes()).unwrap();
        c_names.push(c_str.into_raw());
    }

    *count = c_names.len() as i32;
    let names_array = c_names.into_boxed_slice();
    Box::into_raw(names_array) as *mut *mut c_char
}

pub unsafe extern "C" fn stmt_close(ctx: *mut Stmt) {
    let Ok(stmt) = Stmt::from_ptr(ctx) else {
        return;
    };
    if !stmt.current_row.is_null() {
        stmt.free_current_row();
    }
    // take ownership of internal statement
    let wrapper = Box::from_raw(stmt as *mut Stmt);
    if !wrapper._ctx.is_null() {
        let mut _stmt: Box<Statement> = Box::from_raw(wrapper._ctx as *mut Statement);
        _stmt.reset()
    }
}
