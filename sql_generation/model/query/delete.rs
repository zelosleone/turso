use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delete {
    pub(crate) table: String,
    pub(crate) predicate: Predicate,
}

impl Display for Delete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE FROM {} WHERE {}", self.table, self.predicate)
    }
}
