pub use create::Create;
pub use create_index::CreateIndex;
pub use delete::Delete;
pub use drop::Drop;
pub use insert::Insert;
pub use select::Select;
use turso_parser::ast::fmt::ToSqlContext;

pub mod create;
pub mod create_index;
pub mod delete;
pub mod drop;
pub mod insert;
pub mod predicate;
pub mod select;
pub mod transaction;
pub mod update;

/// Used to print sql strings that already have all the context it needs
pub struct EmptyContext;

impl ToSqlContext for EmptyContext {
    fn get_column_name(
        &self,
        _table_id: turso_parser::ast::TableInternalId,
        _col_idx: usize,
    ) -> String {
        unreachable!()
    }

    fn get_table_name(&self, _id: turso_parser::ast::TableInternalId) -> &str {
        unreachable!()
    }
}
