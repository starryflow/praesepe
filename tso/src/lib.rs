mod allocator;
mod bootstrap;
mod cluster;
mod config;
mod error;
mod etcd;
mod metric;
mod store;
mod util;

pub use allocator::{AllocatorManager, Timestamp};
pub use bootstrap::{Bootstrap, ExitSignal};
pub use config::Config;
pub use etcd::TsoEtcdKind;
pub use store::TsoStoreKind;

pub type TsoResult<T> = anyhow::Result<T>;
