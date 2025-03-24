use crate::ast;
use crate::schema::Schema;
use crate::translate::ProgramBuilder;
use crate::translate::ProgramBuilderOpts;
use crate::translate::QueryMode;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::{CmpInsFlags, Insn};
use crate::{bail_parse_error, Result};

pub fn translate_drop_table(
    query_mode: QueryMode,
    tbl_name: ast::QualifiedName,
    if_exists: bool,
    schema: &Schema,
) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode,
        num_cursors: 1,
        approx_num_insns: 30,
        approx_num_labels: 1,
    });
    let table = schema.get_btree_table(tbl_name.name.0.as_str());
    if table.is_none() {
        if if_exists {
            let init_label = program.emit_init();
            let start_offset = program.offset();
            program.emit_halt();
            program.resolve_label(init_label, program.offset());
            program.emit_transaction(true);
            program.emit_constant_insns();
            program.emit_goto(start_offset);

            return Ok(program);
        }
        bail_parse_error!("No such table: {}", tbl_name.name.0.as_str());
    }
    let table = table.unwrap(); // safe since we just checked for None

    let init_label = program.emit_init();
    let start_offset = program.offset();

    let null_reg = program.alloc_register(); //  r1
    program.emit_null(null_reg, None);
    let tbl_name_reg = program.alloc_register(); //  r2
    let table_reg = program.emit_string8_new_reg(tbl_name.name.0.clone()); //  r3
    program.mark_last_insn_constant();
    let table_type = program.emit_string8_new_reg("trigger".to_string()); //  r4
    program.mark_last_insn_constant();
    let row_id_reg = program.alloc_register(); //  r5

    let table_name = "sqlite_schema";
    let schema_table = schema.get_btree_table(&table_name).unwrap();
    let sqlite_schema_cursor_id = program.alloc_cursor_id(
        Some(table_name.to_string()),
        CursorType::BTreeTable(schema_table.clone()),
    );
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    //  1. Remove all entries from the schema table related to the table we are dropping, except for triggers
    //  loop to beginning of schema table
    program.emit_insn(Insn::RewindAsync {
        cursor_id: sqlite_schema_cursor_id,
    });
    let end_metadata_label = program.allocate_label();
    program.emit_insn(Insn::RewindAwait {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_empty: end_metadata_label,
    });

    //  start loop on schema table
    let metadata_loop = program.allocate_label();
    program.resolve_label(metadata_loop, program.offset());
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 2,
        dest: tbl_name_reg,
    });
    let next_label = program.allocate_label();
    program.emit_insn(Insn::Ne {
        lhs: tbl_name_reg,
        rhs: table_reg,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
    });
    program.emit_insn(Insn::Column {
        cursor_id: sqlite_schema_cursor_id,
        column: 0,
        dest: tbl_name_reg,
    });
    program.emit_insn(Insn::Eq {
        lhs: tbl_name_reg,
        rhs: table_type,
        target_pc: next_label,
        flags: CmpInsFlags::default(),
    });
    program.emit_insn(Insn::RowId {
        cursor_id: sqlite_schema_cursor_id,
        dest: row_id_reg,
    });
    program.emit_insn(Insn::DeleteAsync {
        cursor_id: sqlite_schema_cursor_id,
    });
    program.emit_insn(Insn::DeleteAwait {
        cursor_id: sqlite_schema_cursor_id,
    });

    program.resolve_label(next_label, program.offset());
    program.emit_insn(Insn::NextAsync {
        cursor_id: sqlite_schema_cursor_id,
    });
    program.emit_insn(Insn::NextAwait {
        cursor_id: sqlite_schema_cursor_id,
        pc_if_next: metadata_loop,
    });
    program.resolve_label(end_metadata_label, program.offset());
    //  end of loop on schema table

    //  2. Destroy the indices within a loop
    let indices = schema.get_indices(&tbl_name.name.0);
    for index in indices {
        program.emit_insn(Insn::Destroy {
            root: index.root_page,
            former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
            is_temp: 0,
        });
        let null_reg_1 = program.alloc_register();
        let null_reg_2 = program.alloc_register();
        program.emit_null(null_reg_1, Some(null_reg_2));

        //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
        //  Requires support via https://github.com/tursodatabase/limbo/pull/768

        //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
        //  Requires support via https://github.com/tursodatabase/limbo/pull/768
    }

    //  3. Destroy the table structure
    program.emit_insn(Insn::Destroy {
        root: table.root_page,
        former_root_reg: 0, //  no autovacuum (https://www.sqlite.org/opcode.html#Destroy)
        is_temp: 0,
    });

    let r6 = program.alloc_register();
    let r7 = program.alloc_register();
    program.emit_null(r6, Some(r7));

    //  3. TODO: Open an ephemeral table, and read over triggers from schema table into ephemeral table
    //  Requires support via https://github.com/tursodatabase/limbo/pull/768

    //  4. TODO: Open a write cursor to the schema table and re-insert all triggers into the sqlite schema table from the ephemeral table and delete old trigger
    //  Requires support via https://github.com/tursodatabase/limbo/pull/768

    //  Drop the in-memory structures for the table
    program.emit_insn(Insn::DropTable {
        db: 0,
        _p2: 0,
        _p3: 0,
        table_name: tbl_name.name.0,
    });

    //  end of the program
    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_transaction(true);
    program.emit_constant_insns();

    program.emit_goto(start_offset);

    Ok(program)
}
