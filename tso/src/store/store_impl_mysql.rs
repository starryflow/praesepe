use sqlx::{MySql, MySqlPool, Row};
use tokio::runtime::Runtime;

use crate::TsoResult;

use super::TsoStore;

pub struct MySQLStore {
    conn_pool: MySqlPool,
    runtime: Runtime,
}

impl TsoStore for MySQLStore {
    fn load_timestamp(&self, path: &str) -> TsoResult<u64> {
        self.runtime.block_on(async {
            let records = sqlx::query::<MySql>(
                r#"
SELECT TSO_TS FROM TSO_TIMESTAMP WHERE TSO_PATH = ?;
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
            let affected = sqlx::query::<MySql>(
                r#"
UPDATE TSO_TIMESTAMP
SET TSO_TS = ?, UPDATE_NODE = ?
WHERE TSO_PATH = ?;
"#,
            )
            .bind(ts.to_string())
            .bind(node_id)
            .bind(path)
            .execute(&self.conn_pool)
            .await?
            .rows_affected();

            if affected == 0 {
                sqlx::query::<MySql>(
                    r#"
INSERT IGNORE INTO TSO_TIMESTAMP (TSO_PATH, TSO_TS, UPDATE_NODE)
VALUES (?, ?, ?);
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

impl MySQLStore {
    pub fn new(url: &str) -> Self {
        let runtime = Runtime::new().unwrap();
        let conn_pool = runtime.block_on(async { MySqlPool::connect_lazy(url).unwrap() });
        MySQLStore { conn_pool, runtime }
    }
}
