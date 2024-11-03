mod allocator;
mod bootstrap;
mod cluster;
mod config;
mod error;
mod metric;
mod store;
mod util;

pub use allocator::{AllocatorManager, Timestamp};
use bootstrap::ExitSignal;
pub use cluster::TsoLeadershipKind;
pub use store::TsoStoreKind;

pub type TsoResult<T> = anyhow::Result<T>;

pub fn example() {
    let config = crate::config::Config::default();

    let (exit_sender, exit_receiver) = tokio::sync::broadcast::channel(1);
    let exit_signal = ExitSignal::new(exit_receiver);

    let etcd_client =
        crate::bootstrap::Bootstrap::start_etcd(&config.etcd_server_urls, exit_signal.clone());

    let _ = crate::bootstrap::Bootstrap::start_server(config, etcd_client, exit_signal.clone());

    // exit
    let _ = exit_sender.send(());
}

mod test {
    #[test]
    pub fn test001() {
        let config = crate::config::Config::default();

        let (exit_sender, exit_receiver) = tokio::sync::broadcast::channel(1);
        let exit_signal = crate::bootstrap::ExitSignal::new(exit_receiver);

        let etcd_client =
            crate::bootstrap::Bootstrap::start_etcd(&config.etcd_server_urls, exit_signal.clone());

        let _ = crate::bootstrap::Bootstrap::start_server(config, etcd_client, exit_signal.clone());

        // exit
        let _ = exit_sender.send(());
    }

    #[test]
    pub fn test002() {
        // let mut config = crate::AllocatorConfig::default();
        // config.store_kind = crate::TsoStoreKind::MySQL("url".into());
        // let alloc = crate::AllocatorManager::new_and_setup(1, 1, config);

        // alloc.start_global_allocator_loop();
    }
}
