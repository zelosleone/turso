use std::fmt::Display;

use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::profiles::{io::IOProfile, query::QueryProfile};

mod io;
mod query;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
pub struct Profile {
    #[garde(skip)]
    /// Experimental MVCC feature
    pub experimental_mvcc: bool,
    #[garde(dive)]
    pub io: IOProfile,
    #[garde(dive)]
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

/// Minimum value of field is dependent on another field in the struct
fn min_dependent<T: PartialOrd + Display>(min: &T) -> impl FnOnce(&T, &()) -> garde::Result + '_ {
    move |value, _| {
        if value < min {
            return Err(garde::Error::new(format!(
                "`{value}` is smaller than `{min}`"
            )));
        }
        Ok(())
    }
}

/// Maximum value of field is dependent on another field in the struct
fn max_dependent<T: PartialOrd + Display>(max: &T) -> impl FnOnce(&T, &()) -> garde::Result + '_ {
    move |value, _| {
        if value > max {
            return Err(garde::Error::new(format!(
                "`{value}` is bigger than `{max}`"
            )));
        }
        Ok(())
    }
}
