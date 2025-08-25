use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::model::table::SimValue;

use super::predicate::Predicate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Update {
    pub(crate) table: String,
    pub(crate) set_values: Vec<(String, SimValue)>, // Pair of value for set expressions => SET name=value
    pub(crate) predicate: Predicate,
}

impl Update {
    pub fn table(&self) -> &str {
        &self.table
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UPDATE {} SET ", self.table)?;
        for (i, (name, value)) in self.set_values.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{name} = {value}")?;
        }
        write!(f, " WHERE {}", self.predicate)?;
        Ok(())
    }
}
