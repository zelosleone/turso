mod opts;

use anarchist_readable_name_generator_lib::readable_name_custom;
use antithesis_sdk::random::{get_random, AntithesisRng};
use antithesis_sdk::*;
use clap::Parser;
use limbo::{Builder, Value};
use opts::Opts;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;

/// Represents a column in a SQLite table
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<Constraint>,
}

/// Represents SQLite data types
#[derive(Debug, Clone)]
pub enum DataType {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

/// Represents column constraints
#[derive(Debug, Clone)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
}

/// Represents a table in a SQLite schema
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// Represents a complete SQLite schema
#[derive(Debug, Clone)]
pub struct ArbitrarySchema {
    pub tables: Vec<Table>,
}

// Helper functions for generating random data
fn generate_random_identifier() -> String {
    readable_name_custom("_", AntithesisRng).replace('-', "_")
}

fn generate_random_data_type() -> DataType {
    match get_random() % 5 {
        0 => DataType::Integer,
        1 => DataType::Real,
        2 => DataType::Text,
        3 => DataType::Blob,
        _ => DataType::Numeric,
    }
}

fn generate_random_constraint() -> Constraint {
    match get_random() % 2 {
        0 => Constraint::NotNull,
        _ => Constraint::Unique,
    }
}

fn generate_random_column() -> Column {
    let name = generate_random_identifier();
    let data_type = generate_random_data_type();

    let constraint_count = (get_random() % 3) as usize;
    let mut constraints = Vec::with_capacity(constraint_count);

    for _ in 0..constraint_count {
        constraints.push(generate_random_constraint());
    }

    Column {
        name,
        data_type,
        constraints,
    }
}

fn generate_random_table() -> Table {
    let name = generate_random_identifier();
    let column_count = (get_random() % 10 + 1) as usize;
    let mut columns = Vec::with_capacity(column_count);
    let mut column_names = HashSet::new();

    // First, generate all columns without primary keys
    for _ in 0..column_count {
        let mut column = generate_random_column();

        // Ensure column names are unique within the table
        while column_names.contains(&column.name) {
            column.name = generate_random_identifier();
        }

        column_names.insert(column.name.clone());
        columns.push(column);
    }

    // Then, randomly select one column to be the primary key
    let pk_index = (get_random() % column_count as u64) as usize;
    columns[pk_index].constraints.push(Constraint::PrimaryKey);

    Table { name, columns }
}

pub fn gen_schema() -> ArbitrarySchema {
    let table_count = (get_random() % 10 + 1) as usize;
    let mut tables = Vec::with_capacity(table_count);
    let mut table_names = HashSet::new();

    for _ in 0..table_count {
        let mut table = generate_random_table();

        // Ensure table names are unique
        while table_names.contains(&table.name) {
            table.name = generate_random_identifier();
        }

        table_names.insert(table.name.clone());
        tables.push(table);
    }

    ArbitrarySchema { tables }
}

impl ArbitrarySchema {
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();

        for table in &self.tables {
            sql.push_str(&format!("CREATE TABLE {} (\n", table.name));

            for (i, column) in table.columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(",\n");
                }

                sql.push_str(&format!(
                    "  {} {}",
                    column.name,
                    data_type_to_sql(&column.data_type)
                ));

                for constraint in &column.constraints {
                    sql.push_str(&format!(" {}", constraint_to_sql(constraint)));
                }
            }

            sql.push_str("\n);\n\n");
        }

        sql
    }
}

fn data_type_to_sql(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Integer => "INTEGER",
        DataType::Real => "REAL",
        DataType::Text => "TEXT",
        DataType::Blob => "BLOB",
        DataType::Numeric => "NUMERIC",
    }
}

fn constraint_to_sql(constraint: &Constraint) -> String {
    match constraint {
        Constraint::PrimaryKey => "PRIMARY KEY".to_string(),
        Constraint::NotNull => "NOT NULL".to_string(),
        Constraint::Unique => "UNIQUE".to_string(),
    }
}

#[tokio::main]
async fn main() {
    let (num_nodes, main_id) = (1, "n-001");
    let startup_data = json!({
        "num_nodes": num_nodes,
        "main_node_id": main_id,
    });
    lifecycle::setup_complete(&startup_data);
    antithesis_init();

    let schema = gen_schema();

    let schema_sql = schema.to_sql();

    println!("{}", schema_sql);
    let opts = Opts::parse();
    let mut handles = Vec::new();

    for _ in 0..opts.nr_threads {
        // TODO: share the database between threads
        let db = Arc::new(Builder::new_local(":memory:").build().await.unwrap());
        let conn = db.connect().unwrap();
        conn.execute(&schema_sql, ()).await.unwrap();
        let nr_iterations = opts.nr_iterations;
        let db = db.clone();
        let handle = tokio::spawn(async move {
            let conn = db.connect().unwrap();

            for _ in 0..nr_iterations {
                let mut rows = conn.query("select 1", ()).await.unwrap();
                let row = rows.next().await.unwrap().unwrap();
                let value = row.get_value(0).unwrap();
                assert_always!(matches!(value, Value::Integer(1)), "value is incorrect");
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
    println!("Done.");
}
