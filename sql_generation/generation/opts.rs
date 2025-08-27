use std::ops::Range;

use crate::model::table::Table;

#[derive(Debug, Clone)]
pub struct Opts {
    /// Indexes enabled
    pub indexes: bool,
    pub table: TableOpts,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            indexes: true,
            table: Default::default(),
        }
    }
}

/// Trait used to provide context to generation functions
pub trait GenerationContext {
    fn tables(&self) -> &Vec<Table>;
    fn opts(&self) -> &Opts;
}

#[derive(Debug, Clone)]
pub struct TableOpts {
    pub large_table: LargeTableOpts,
    /// Range of numbers of columns to generate
    pub column_range: Range<u32>,
}

impl Default for TableOpts {
    fn default() -> Self {
        Self {
            large_table: Default::default(),
            // Up to 10 columns
            column_range: 1..11,
        }
    }
}

/// Options for generating large tables
#[derive(Debug, Clone)]
pub struct LargeTableOpts {
    pub enable: bool,
    pub large_table_prob: f32,
    /// Range of numbers of columns to generate
    pub column_range: Range<u32>,
}

impl Default for LargeTableOpts {
    fn default() -> Self {
        Self {
            enable: true,
            large_table_prob: 0.1,
            // todo: make this higher (128+)
            column_range: 64..125,
        }
    }
}
