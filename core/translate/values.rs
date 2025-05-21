use crate::translate::emitter::Resolver;
use crate::translate::expr::{translate_expr_no_constant_opt, NoConstantOptReason};
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::insn::Insn;
use crate::vdbe::BranchOffset;
use limbo_sqlite3_parser::ast::Expr;

pub fn emit_values(
    program: &mut ProgramBuilder,
    values: &Vec<Vec<Expr>>,
    resolver: &Resolver,
) -> crate::Result<()> {
    if values.is_empty() {
        return Ok(());
    }

    if values.len() == 1 {
        let value = &values[0];
        let value_size = value.len();
        let start_reg = program.alloc_registers(value_size);
        for (i, v) in value.iter().enumerate() {
            translate_expr_no_constant_opt(
                program,
                None,
                &v,
                start_reg + i,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }
        program.emit_insn(Insn::ResultRow {
            start_reg,
            count: value_size,
        });
        return Ok(());
    }

    let yield_reg = program.alloc_register();
    let definition_label = program.allocate_label();
    let start_offset_label = program.allocate_label();
    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: definition_label,
        start_offset: start_offset_label,
    });

    program.preassign_label_to_next_insn(start_offset_label);
    let value_size = values[0].len();
    let start_reg = program.alloc_registers(value_size);
    for value in values {
        for (i, v) in value.iter().enumerate() {
            translate_expr_no_constant_opt(
                program,
                None,
                &v,
                start_reg + i,
                resolver,
                NoConstantOptReason::RegisterReuse,
            )?;
        }
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: BranchOffset::Offset(0),
        });
    }
    program.emit_insn(Insn::EndCoroutine { yield_reg });
    program.preassign_label_to_next_insn(definition_label);

    program.emit_insn(Insn::InitCoroutine {
        yield_reg,
        jump_on_definition: BranchOffset::Offset(0),
        start_offset: start_offset_label,
    });
    let end_label = program.allocate_label();
    let goto_label = program.allocate_label();
    program.preassign_label_to_next_insn(goto_label);
    program.emit_insn(Insn::Yield {
        yield_reg,
        end_offset: end_label,
    });
    let copy_reg = program.alloc_registers(value_size);
    for i in 0..value_size {
        program.emit_insn(Insn::Copy {
            src_reg: start_reg + i,
            dst_reg: copy_reg + i,
            amount: 0,
        });
    }
    program.emit_insn(Insn::ResultRow {
        start_reg: copy_reg,
        count: value_size,
    });
    program.emit_goto(goto_label);
    program.preassign_label_to_next_insn(end_label);

    Ok(())
}
