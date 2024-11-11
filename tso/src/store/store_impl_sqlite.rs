use sqlx::{Executor, Row, Sqlite, SqlitePool};
use tokio::runtime::Runtime;

use super::TsoStore;
use crate::TsoResult;

pub struct SqliteStore {
    conn_pool: SqlitePool,
    rt: Runtime,
}

impl TsoStore for SqliteStore {
    fn load_timestamp(&self, path: &str) -> TsoResult<u64> {
        self.rt.block_on(async {
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
        self.rt.block_on(async {
            let affected = sqlx::query::<Sqlite>(
                r#"
UPDATE TSO_TIMESTAMP
SET TSO_TS = $1, TSO_NODE = $2, UPDATED = CURRENT_TIMESTAMP
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
INSERT INTO TSO_TIMESTAMP (TSO_PATH, TSO_TS, TSO_NODE)
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
        let rt = Runtime::new().unwrap();
        let conn_pool = rt.block_on(async {
            let conn = SqlitePool::connect(url).await.unwrap();

            let _ = conn
                .execute(
                    "CREATE TABLE IF NOT EXISTS TSO_TIMESTAMP (
    ID INTEGER PRIMARY KEY AUTOINCREMENT, -- 主键
    TSO_PATH TEXT NOT NULL, -- TSO 的标识
    TSO_TS TEXT NOT NULL, -- TSO 时间戳，全局唯一且自增，无符号 64 位数字
    TSO_NODE TEXT NOT NULL, -- 最近更新的节点信息
    CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 新增时间
    UPDATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 最近更新时间
    UNIQUE (TSO_PATH)
);",
                )
                .await;

            conn
        });
        SqliteStore { conn_pool, rt }
    }
}
