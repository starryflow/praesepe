use crate::TsoResult;

use super::{store_impl_mysql::MySQLStore, store_impl_sqlite::SqliteStore};

pub trait TsoStore: Send + Sync {
    fn load_timestamp(&self, path: &str) -> TsoResult<u64>;

    fn save_timestamp(&self, path: &str, ts: u64, node_id: &str) -> TsoResult<()>;
}

pub enum TsoStoreKind {
    MySQL(String),
    /// `sqlite::memory:` or `/path/to/your/database.db`
    Sqlite(String),
}

pub struct TsoStoreFactory;

impl TsoStoreFactory {
    pub fn get_instance(kind: &TsoStoreKind) -> Box<dyn TsoStore> {
        match kind {
            TsoStoreKind::MySQL(url) => Box::new(MySQLStore::new(url)),
            TsoStoreKind::Sqlite(url) => Box::new(SqliteStore::new(url)),
        }
    }
}
