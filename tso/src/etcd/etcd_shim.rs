use std::fmt::Debug;

use sqlx::SqlitePool;
use tokio::runtime::Runtime;

pub struct EtcdShim {
    conn_pool: SqlitePool,
    rt: Runtime,
}

impl Debug for EtcdShim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("EtcdShim use client: {:?}", &self.conn_pool,))
    }
}

impl EtcdShim {
    pub fn new(url: &str) -> Self {
        let rt = Runtime::new().unwrap();
        let conn_pool = rt.block_on(async { SqlitePool::connect_lazy(url).unwrap() });
        EtcdShim { conn_pool, rt }
    }
}
