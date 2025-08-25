use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Begin {
    pub immediate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rollback;

impl Display for Begin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BEGIN {}", if self.immediate { "IMMEDIATE" } else { "" })
    }
}

impl Display for Commit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "COMMIT")
    }
}

impl Display for Rollback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROLLBACK")
    }
}
