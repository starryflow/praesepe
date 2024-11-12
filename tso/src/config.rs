use crate::{TsoEtcdKind, TsoStoreKind};

pub struct Config {
    pub leadership_worker_size: usize,
    pub allocator_worker_size: usize,

    pub etcd_kind: TsoEtcdKind,
    pub store_kind: TsoStoreKind,
    /// List of URLs for ETCD Server
    pub etcd_server_urls: String,

    /// unique in cluster
    pub name: String,

    /// defines the time within which a TSO primary/leader must update its TTL in etcd, otherwise etcd will expire the leader key and other servers can campaign the primary/leader again. Etcd only supports seconds TTL, so here is second too
    pub leader_lease_millis: u64,
    // leader_tick_interval is the interval to check leader
    pub leader_tick_interval_millis: u64,

    /// interval to save timestamp
    pub save_interval_millis: u64,
    /// The interval to update physical part of timestamp. Usually, this config should not be set.
    /// At most 1<<18 (262144) TSOs can be generated in the interval. The smaller the value, the more TSOs provided, and at the same time consuming more CPU time.
    /// This config is only valid in 1ms to 10s. If it's configured too long or too short, it will be automatically clamped to the range
    pub update_physical_interval_millis: u64,
    /// the max gap to reset the TSO
    pub max_reset_ts_gap_millis: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            leadership_worker_size: 40,
            allocator_worker_size: 4,

            etcd_kind: TsoEtcdKind::Shim,
            store_kind: TsoStoreKind::Sqlite("sqlite::memory:".into()),
            etcd_server_urls: "".into(),

            name: "TSO-Server-1".into(),
            leader_lease_millis: 3000,
            leader_tick_interval_millis: 50,
            save_interval_millis: 3000,
            update_physical_interval_millis: 50,
            max_reset_ts_gap_millis: 24 * 60 * 60 * 1000,
        }
    }
}
