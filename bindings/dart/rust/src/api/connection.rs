use std::sync::Arc;

use flutter_rust_bridge::frb;
pub use turso_core::{Connection, Database};

use crate::{
    api::statement::RustStatement,
    helpers::{
        params::Params,
        result::{ExecuteResult, QueryResult},
        wrapper::Wrapper,
    },
};

#[frb(opaque)]
pub struct RustConnection {
    inner: Wrapper<Arc<Connection>>,
    database: Wrapper<Arc<Database>>,
}

impl RustConnection {
    pub fn new(
        connection: Wrapper<Arc<Connection>>,
        database: Wrapper<Arc<Database>>,
    ) -> RustConnection {
        RustConnection {
            inner: connection,
            database: database,
        }
    }

    pub async fn query(&self, sql: String, params: Params) -> QueryResult {
        self.prepare(sql).await.query(params).await
    }

    pub async fn execute(&self, sql: String, params: Params) -> ExecuteResult {
        self.prepare(sql).await.execute(params).await
    }

    pub async fn prepare(&self, sql: String) -> RustStatement {
        let statement = self.inner.prepare(&sql).unwrap();
        RustStatement::new(
            Wrapper { inner: statement },
            Wrapper {
                inner: self.inner.to_owned(),
            },
        )
    }
}
