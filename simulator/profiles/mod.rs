use crate::profiles::{io::IOProfile, query::QueryProfile};

mod io;
mod query;

#[derive(Debug, Clone)]
pub struct Profile {
    /// Experimental MVCC feature
    pub experimental_mvcc: bool,
    pub io: IOProfile,
    pub query: QueryProfile,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            experimental_mvcc: false,
            io: Default::default(),
            query: Default::default(),
        }
    }
}
