use std::{
    rc::{Rc, Weak},
    sync::Arc,
};

use limbo_sqlite3_parser::ast;

use crate::{
    fast_lock::SpinLock,
    schema::Schema,
    storage::sqlite3_ondisk::DatabaseHeader,
    vdbe::{builder::ProgramBuilder, insn::Insn},
    Pager,
};

/// Maximum number of errors to report with integrity check. If we exceed this number we will short
/// circuit the procedure and return early to not waste time.
const MAX_INTEGRITY_CHECK_ERRORS: usize = 10;

pub fn translate_integrity_check(
    schema: &Schema,
    program: &mut ProgramBuilder,
) -> crate::Result<()> {
    let mut root_pages = Vec::with_capacity(schema.tables.len() + schema.indexes.len());
    // Collect root pages to run integrity check on
    for (name, table) in &schema.tables {
        match table.as_ref() {
            crate::schema::Table::BTree(table) => {
                root_pages.push(table.root_page);
            }
            _ => {}
        };
    }
    let message_register = program.alloc_register();
    program.emit_insn(Insn::IntegrityCk {
        max_errors: MAX_INTEGRITY_CHECK_ERRORS,
        roots: root_pages,
        message_register,
    });
    program.emit_insn(Insn::ResultRow {
        start_reg: message_register,
        count: 1,
    });
    Ok(())
}
