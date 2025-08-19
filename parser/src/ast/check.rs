//! Check for additional syntax error
use crate::{ast::*, error::Error};

impl Cmd {
    /// Statement accessor
    pub fn stmt(&self) -> &Stmt {
        match self {
            Self::Explain(stmt) => stmt,
            Self::ExplainQueryPlan(stmt) => stmt,
            Self::Stmt(stmt) => stmt,
        }
    }
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Explain(_) => ColumnCount::Fixed(8),
            Self::ExplainQueryPlan(_) => ColumnCount::Fixed(4),
            Self::Stmt(stmt) => stmt.column_count(),
        }
    }
    /// Like `sqlite3_stmt_isexplain`
    pub fn is_explain(&self) -> bool {
        matches!(self, Self::Explain(_) | Self::ExplainQueryPlan(_))
    }
    /// Like `sqlite3_stmt_readonly`
    pub fn readonly(&self) -> bool {
        self.stmt().readonly()
    }
    /// check for extra rules
    pub fn check(&self) -> Result<(), Error> {
        self.stmt().check()
    }
}

/// Column count
pub enum ColumnCount {
    /// With `SELECT *` / PRAGMA
    Dynamic,
    /// Constant count
    Fixed(usize),
    /// No column
    None,
}

impl ColumnCount {
    fn incr(&mut self) {
        if let Self::Fixed(n) = self {
            *n += 1;
        }
    }
}

impl Stmt {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Delete { returning, .. } => column_count(returning),
            Self::Insert { returning, .. } => column_count(returning),
            Self::Pragma { .. } => ColumnCount::Dynamic,
            Self::Select(s) => s.column_count(),
            Self::Update(update) => {
                let Update { returning, .. } = &update;
                column_count(returning)
            }
            _ => ColumnCount::None,
        }
    }

    /// Like `sqlite3_stmt_readonly`
    pub fn readonly(&self) -> bool {
        match self {
            Self::Attach { .. } => true,
            Self::Begin { .. } => true,
            Self::Commit { .. } => true,
            Self::Detach { .. } => true,
            Self::Pragma { .. } => true, // TODO check all
            Self::Reindex { .. } => true,
            Self::Release { .. } => true,
            Self::Rollback { .. } => true,
            Self::Savepoint { .. } => true,
            Self::Select(..) => true,
            _ => false,
        }
    }

    /// check for extra rules
    pub fn check(&self) -> Result<(), Error> {
        match self {
            Self::AlterTable(alter_table) => {
                if let AlterTableBody::AddColumn(cd) = &alter_table.body {
                    for NamedColumnConstraint { constraint: c, .. } in &cd.constraints {
                        if let ColumnConstraint::PrimaryKey { .. } = c {
                            return Err(Error::Custom(
                                "Cannot add a PRIMARY KEY column".to_owned(),
                            ));
                        }
                        if let ColumnConstraint::Unique(..) = c {
                            return Err(Error::Custom("Cannot add a UNIQUE column".to_owned()));
                        }
                    }
                }
                Ok(())
            }
            Self::CreateTable {
                temporary,
                tbl_name,
                body,
                ..
            } => {
                if *temporary {
                    if let Some(ref db_name) = tbl_name.db_name {
                        if !db_name.as_str().eq_ignore_ascii_case("TEMP") {
                            return Err(Error::Custom(
                                "temporary table name must be unqualified".to_owned(),
                            ));
                        }
                    }
                }
                body.check(tbl_name)
            }
            Self::CreateView {
                view_name,
                columns,
                select,
                ..
            } => {
                // SQLite3 engine renames duplicates:
                for (i, c) in columns.iter().enumerate() {
                    for o in &columns[i + 1..] {
                        if c.col_name == o.col_name {
                            return Err(Error::Custom(format!(
                                "duplicate column name: {}",
                                c.col_name
                            )));
                        }
                    }
                }

                // SQLite3 engine raises this error later (not while parsing):
                match (select.column_count(), columns.is_empty()) {
                    (ColumnCount::Fixed(n), false) if n != columns.len() => {
                        Err(Error::Custom(format!(
                            "expected {} columns for {} but got {}",
                            columns.len(),
                            view_name,
                            n
                        )))
                    }
                    _ => Ok(()),
                }
            }
            Self::Delete {
                order_by, limit, ..
            } => {
                if !order_by.is_empty() && limit.is_none() {
                    return Err(Error::Custom("ORDER BY without LIMIT on DELETE".to_owned()));
                }
                Ok(())
            }
            Self::Insert { columns, body, .. } => {
                if columns.is_empty() {
                    return Ok(());
                }
                match body {
                    InsertBody::Select(select, ..) => {
                        match select.body.select.column_count() {
                            ColumnCount::Fixed(n) if n != columns.len() => Err(Error::Custom(
                                format!("{} values for {} columns", n, columns.len()),
                            )),
                            _ => Ok(()),
                        }
                    }
                    InsertBody::DefaultValues => Err(Error::Custom(format!(
                        "0 values for {} columns",
                        columns.len()
                    ))),
                }
            }
            Self::Update(update) => {
                let Update {
                    order_by, limit, ..
                } = update;
                if !order_by.is_empty() && limit.is_none() {
                    return Err(Error::Custom("ORDER BY without LIMIT on UPDATE".to_owned()));
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl CreateTableBody {
    /// check for extra rules
    pub fn check(&self, tbl_name: &QualifiedName) -> Result<(), Error> {
        if let Self::ColumnsAndConstraints {
            columns,
            constraints: _,
            options,
        } = self
        {
            let mut generated_count = 0;
            for c in columns {
                if c.col_name.as_str().eq_ignore_ascii_case("rowid") {
                    return Err(Error::Custom("cannot use reserved word: ROWID".to_owned()));
                }
                for cs in &c.constraints {
                    if let ColumnConstraint::Generated { .. } = cs.constraint {
                        generated_count += 1;
                    }
                }
            }
            if generated_count == columns.len() {
                return Err(Error::Custom(
                    "must have at least one non-generated column".to_owned(),
                ));
            }

            if options.contains(TableOptions::STRICT) {
                for c in columns {
                    match &c.col_type {
                        Some(Type { name, .. }) => {
                            // The datatype must be one of following: INT INTEGER REAL TEXT BLOB ANY
                            if !(name.eq_ignore_ascii_case("INT")
                                || name.eq_ignore_ascii_case("INTEGER")
                                || name.eq_ignore_ascii_case("REAL")
                                || name.eq_ignore_ascii_case("TEXT")
                                || name.eq_ignore_ascii_case("BLOB")
                                || name.eq_ignore_ascii_case("ANY"))
                            {
                                return Err(Error::Custom(format!(
                                    "unknown datatype for {}.{}: \"{}\"",
                                    tbl_name, c.col_name, name
                                )));
                            }
                        }
                        _ => {
                            // Every column definition must specify a datatype for that column. The freedom to specify a column without a datatype is removed.
                            return Err(Error::Custom(format!(
                                "missing datatype for {}.{}",
                                tbl_name, c.col_name
                            )));
                        }
                    }
                }
            }
            if options.contains(TableOptions::WITHOUT_ROWID) && !self.has_primary_key() {
                return Err(Error::Custom(format!(
                    "PRIMARY KEY missing on table {tbl_name}"
                )));
            }
        }
        Ok(())
    }

    /// explicit primary key constraint ?
    pub fn has_primary_key(&self) -> bool {
        if let Self::ColumnsAndConstraints {
            columns,
            constraints,
            ..
        } = self
        {
            for col in columns {
                for c in col {
                    if let ColumnConstraint::PrimaryKey { .. } = c {
                        return true;
                    }
                }
            }
            for c in constraints {
                if let TableConstraint::PrimaryKey { .. } = c.constraint {
                    return true;
                }
            }
        }
        false
    }
}

impl<'a> IntoIterator for &'a ColumnDefinition {
    type Item = &'a ColumnConstraint;
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, NamedColumnConstraint>,
        fn(&'a NamedColumnConstraint) -> &'a ColumnConstraint,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.constraints.iter().map(|nc| &nc.constraint)
    }
}

impl Select {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        self.body.select.column_count()
    }
}

impl OneSelect {
    /// Like `sqlite3_column_count` but more limited
    pub fn column_count(&self) -> ColumnCount {
        match self {
            Self::Select { columns, .. } => column_count(columns),
            Self::Values(values) => {
                assert!(!values.is_empty()); // TODO Validate
                ColumnCount::Fixed(values[0].len())
            }
        }
    }
    /// Check all VALUES have the same number of terms
    pub fn push(values: &mut Vec<Vec<Expr>>, v: Vec<Expr>) -> Result<(), Error> {
        if values[0].len() != v.len() {
            return Err(Error::Custom(
                "all VALUES must have the same number of terms".to_owned(),
            ));
        }
        values.push(v);
        Ok(())
    }
}

impl ResultColumn {
    fn column_count(&self) -> ColumnCount {
        match self {
            Self::Expr(..) => ColumnCount::Fixed(1),
            _ => ColumnCount::Dynamic,
        }
    }
}
fn column_count(cols: &[ResultColumn]) -> ColumnCount {
    assert!(!cols.is_empty());
    let mut count = ColumnCount::Fixed(0);
    for col in cols {
        match col.column_count() {
            ColumnCount::Fixed(_) => count.incr(),
            _ => return ColumnCount::Dynamic,
        }
    }
    count
}
