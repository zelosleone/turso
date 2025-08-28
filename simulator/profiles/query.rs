use garde::Validate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sql_generation::generation::Opts;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, Validate)]
#[serde(deny_unknown_fields, default)]
pub struct QueryProfile {
    #[garde(dive)]
    pub gen_opts: Opts,
    #[garde(skip)]
    pub create_table: bool,
    #[garde(skip)]
    pub create_index: bool,
    #[garde(skip)]
    pub insert: bool,
    #[garde(skip)]
    pub update: bool,
    #[garde(skip)]
    pub delete: bool,
    #[garde(skip)]
    pub drop_table: bool,
}

impl Default for QueryProfile {
    fn default() -> Self {
        Self {
            gen_opts: Opts::default(),
            create_table: true,
            create_index: true,
            insert: true,
            update: true,
            delete: true,
            drop_table: true,
        }
    }
}
