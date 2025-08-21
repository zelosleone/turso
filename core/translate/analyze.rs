use turso_parser::ast;

use crate::{
    bail_parse_error,
    schema::Schema,
    util::normalize_ident,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{Insn, RegisterOrLiteral::*},
    },
    Result,
};

pub fn translate_analyze(
    target_opt: Option<ast::QualifiedName>,
    schema: &Schema,
    mut program: ProgramBuilder,
) -> Result<ProgramBuilder> {
    let Some(target) = target_opt else {
        bail_parse_error!("ANALYZE with no target is not supported");
    };
    let normalized = normalize_ident(target.name.as_str());
    let Some(target_schema) = schema.get_table(&normalized) else {
        bail_parse_error!("ANALYZE <schema_name> is not supported");
    };
    let Some(target_btree) = target_schema.btree() else {
        bail_parse_error!("ANALYZE on index is not supported");
    };

    // This is emitted early because SQLite does, and thus generated VDBE matches a bit closer.
    let null_reg = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: null_reg,
        dest_end: None,
    });

    if let Some(sqlite_stat1) = schema.get_btree_table("sqlite_stat1") {
        // sqlite_stat1 already exists, so we need to remove the row
        // corresponding to the stats for the table which we're about to
        // ANALYZE. SQLite implements this as a full table scan over
        // sqlite_stat1 deleting any rows where the first column (table_name)
        // is the targeted table.
        let cursor_id = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_stat1.clone()));
        program.emit_insn(Insn::OpenWrite {
            cursor_id,
            root_page: Literal(sqlite_stat1.root_page),
            db: 0,
        });
        let after_loop = program.allocate_label();
        program.emit_insn(Insn::Rewind {
            cursor_id,
            pc_if_empty: after_loop,
        });
        let loophead = program.allocate_label();
        program.preassign_label_to_next_insn(loophead);
        let column_reg = program.alloc_register();
        program.emit_insn(Insn::Column {
            cursor_id,
            column: 0,
            dest: column_reg,
            default: None,
        });
        let tablename_reg = program.alloc_register();
        program.emit_insn(Insn::String8 {
            value: target_schema.get_name().to_string(),
            dest: tablename_reg,
        });
        program.mark_last_insn_constant();
        // FIXME: The SQLite instruction says p4=BINARY-8 and p5=81.  Neither are currently supported in Turso.
        program.emit_insn(Insn::Ne {
            lhs: column_reg,
            rhs: tablename_reg,
            target_pc: after_loop,
            flags: Default::default(),
            collation: None,
        });
        let rowid_reg = program.alloc_register();
        program.emit_insn(Insn::RowId {
            cursor_id,
            dest: rowid_reg,
        });
        program.emit_insn(Insn::Delete {
            cursor_id,
            table_name: "sqlite_stat1".to_string(),
        });
        program.emit_insn(Insn::Next {
            cursor_id,
            pc_if_next: loophead,
        });
        program.preassign_label_to_next_insn(after_loop);
    } else {
        bail_parse_error!("ANALYZE without an existing sqlite_stat1 is not supported");
    };

    if target_schema.columns().iter().any(|c| c.primary_key) {
        bail_parse_error!("ANALYZE on tables with primary key is not supported");
    }
    if !target_btree.has_rowid {
        bail_parse_error!("ANALYZE on tables without rowid is not supported");
    }

    // Count the number of rows in the target table, and insert it into sqlite_stat1.
    let sqlite_stat1 = schema
        .get_btree_table("sqlite_stat1")
        .expect("sqlite_stat1 either pre-existed or was just created");
    let stat_cursor = program.alloc_cursor_id(CursorType::BTreeTable(sqlite_stat1.clone()));
    program.emit_insn(Insn::OpenWrite {
        cursor_id: stat_cursor,
        root_page: Literal(sqlite_stat1.root_page),
        db: 0,
    });
    let target_cursor = program.alloc_cursor_id(CursorType::BTreeTable(target_btree.clone()));
    program.emit_insn(Insn::OpenRead {
        cursor_id: target_cursor,
        root_page: target_btree.root_page,
        db: 0,
    });
    let rowid_reg = program.alloc_register();
    let record_reg = program.alloc_register();
    let tablename_reg = program.alloc_register();
    let indexname_reg = program.alloc_register();
    let count_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: target_schema.get_name().to_string(),
        dest: tablename_reg,
    });
    program.emit_insn(Insn::Count {
        cursor_id: target_cursor,
        target_reg: count_reg,
        exact: true,
    });
    let after_insert = program.allocate_label();
    program.emit_insn(Insn::IfNot {
        reg: count_reg,
        target_pc: after_insert,
        jump_if_null: false,
    });
    program.emit_insn(Insn::Null {
        dest: indexname_reg,
        dest_end: None,
    });
    program.emit_insn(Insn::MakeRecord {
        start_reg: tablename_reg,
        count: 3,
        dest_reg: record_reg,
        index_name: None,
    });
    program.emit_insn(Insn::NewRowid {
        cursor: stat_cursor,
        rowid_reg,
        prev_largest_reg: 0,
    });
    // FIXME: SQLite sets OPFLAG_APPEND on the insert, but that's not supported in turso right now.
    // SQLite doesn't emit the table name, but like... why not?
    program.emit_insn(Insn::Insert {
        cursor: stat_cursor,
        key_reg: rowid_reg,
        record_reg,
        flag: Default::default(),
        table_name: "sqlite_stat1".to_string(),
    });
    program.preassign_label_to_next_insn(after_insert);
    // FIXME: Emit LoadAnalysis
    // FIXME: Emit Expire
    Ok(program)
}
