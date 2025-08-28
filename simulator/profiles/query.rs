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
    /// Effectively the weight of how many select operations we want
    pub read_weight: u32,
    #[garde(skip)]
    pub write_weight: u32,
    // All weights below are only going to be sampled when we determine we are doing a write operation,
    // meaning we first sample between `read_weight` and `write_weight`, and if we a write_weight we will then sample the weights below
    #[garde(skip)]
    pub create_table_weight: u32,
    #[garde(skip)]
    pub create_index_weight: u32,
    #[garde(skip)]
    pub insert_weight: u32,
    #[garde(skip)]
    pub update_weight: u32,
    #[garde(skip)]
    pub delete_weight: u32,
    #[garde(skip)]
    pub drop_table_weight: u32,
}

impl Default for QueryProfile {
    fn default() -> Self {
        Self {
            gen_opts: Opts::default(),
            read_weight: 60,
            write_weight: 50,
            create_table_weight: 15,
            create_index_weight: 5,
            insert_weight: 30,
            update_weight: 20,
            delete_weight: 20,
            drop_table_weight: 2,
        }
    }
}

#[derive(Debug, Clone, strum::VariantArray)]
pub enum QueryTypes {
    CreateTable,
    CreateIndex,
    Insert,
    Update,
    Delete,
    DropTable,
}
