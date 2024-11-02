use sqlx::{Row, Sqlite, SqlitePool};
use tokio::runtime::Runtime;

use super::TsoStore;
use crate::TsoResult;

pub struct SqliteStore {
    conn_pool: SqlitePool,
    runtime: Runtime,
}

impl TsoStore for SqliteStore {
    fn load_timestamp(&self, path: &str) -> TsoResult<u64> {
        self.runtime.block_on(async {
            let records = sqlx::query::<Sqlite>(
                r#"
SELECT TSO_TS FROM TSO_TIMESTAMP WHERE TSO_PATH = $1
"#,
            )
            .bind(path)
            .map(|row| row.get::<String, _>("TSO_TS"))
            .fetch_optional(&self.conn_pool)
            .await;
            match records {
                Ok(Some(rec)) => rec.parse::<u64>().map_err(|e| e.into()),
                Ok(None) => Ok(0),
                Err(e) => anyhow::bail!(e),
            }
        })
    }

    fn save_timestamp(&self, path: &str, ts: u64, node_id: &str) -> TsoResult<()> {
        self.runtime.block_on(async {
            let affected = sqlx::query::<Sqlite>(
                r#"
UPDATE TSO_TIMESTAMP
SET TSO_TS = $1, UPDATE_NODE = $2
WHERE TSO_PATH = $3
"#,
            )
            .bind(ts.to_string())
            .bind(node_id)
            .bind(path)
            .execute(&self.conn_pool)
            .await?
            .rows_affected();

            if affected == 0 {
                sqlx::query::<Sqlite>(
                    r#"
INSERT INTO TSO_TIMESTAMP (TSO_PATH, TSO_TS, UPDATE_NODE)
VALUES ($1, $2, $3)
ON CONFLICT(TSO_PATH) DO NOTHING;
"#,
                )
                .bind(path)
                .bind(ts.to_string())
                .bind(node_id)
                .execute(&self.conn_pool)
                .await?;
            }

            Ok(())
        })
    }
}

impl SqliteStore {
    pub fn new(url: &str) -> Self {
        let runtime = Runtime::new().unwrap();
        let conn_pool = runtime.block_on(async { SqlitePool::connect_lazy(url).unwrap() });
        SqliteStore { conn_pool, runtime }
    }
}
