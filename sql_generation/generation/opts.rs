use std::{
    num::{NonZero, NonZeroU32},
    ops::Range,
};

use rand::distr::weighted::WeightedIndex;

use crate::model::table::Table;

/// Trait used to provide context to generation functions
pub trait GenerationContext {
    fn tables(&self) -> &Vec<Table>;
    fn opts(&self) -> &Opts;
}

#[derive(Debug, Clone)]
pub struct Opts {
    /// Indexes enabled
    pub indexes: bool,
    pub table: TableOpts,
    pub query: QueryOpts,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            indexes: true,
            table: Default::default(),
            query: Default::default(),
        }
    }
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
    pub large_table_prob: f64,
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

#[derive(Debug, Default, Clone)]
pub struct QueryOpts {
    pub select: SelectOpts,
    pub from_clause: FromClauseOpts,
    pub insert: InsertOpts,
}

#[derive(Debug, Clone)]
pub struct SelectOpts {
    pub order_by_prob: f64,
    pub compound_selects: Vec<CompoundSelectWeight>,
}

impl Default for SelectOpts {
    fn default() -> Self {
        Self {
            order_by_prob: 0.3,
            compound_selects: vec![
                CompoundSelectWeight {
                    num_compound_selects: 0,
                    weight: 95,
                },
                CompoundSelectWeight {
                    num_compound_selects: 1,
                    weight: 4,
                },
                CompoundSelectWeight {
                    num_compound_selects: 2,
                    weight: 1,
                },
            ],
        }
    }
}

impl SelectOpts {
    pub fn compound_select_weighted_index(&self) -> WeightedIndex<u32> {
        WeightedIndex::new(self.compound_selects.iter().map(|weight| weight.weight)).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct CompoundSelectWeight {
    pub num_compound_selects: u32,
    pub weight: u32,
}

#[derive(Debug, Clone)]
pub struct FromClauseOpts {
    pub joins: Vec<JoinWeight>,
}

impl Default for FromClauseOpts {
    fn default() -> Self {
        Self {
            joins: vec![
                JoinWeight {
                    num_joins: 0,
                    weight: 90,
                },
                JoinWeight {
                    num_joins: 1,
                    weight: 7,
                },
                JoinWeight {
                    num_joins: 2,
                    weight: 3,
                },
            ],
        }
    }
}

impl FromClauseOpts {
    pub fn as_weighted_index(&self) -> WeightedIndex<u32> {
        WeightedIndex::new(self.joins.iter().map(|weight| weight.weight)).unwrap()
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct JoinWeight {
    pub num_joins: u32,
    pub weight: u32,
}

#[derive(Debug, Clone)]
pub struct InsertOpts {
    pub min_rows: NonZeroU32,
    pub max_rows: NonZeroU32,
}

impl Default for InsertOpts {
    fn default() -> Self {
        Self {
            min_rows: NonZero::new(1).unwrap(),
            max_rows: NonZero::new(10).unwrap(),
        }
    }
}
