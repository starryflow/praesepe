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

#[cfg(test)]
mod test {

    #[test]
    pub fn test001() {
        env_logger::init();

        let config = crate::config::Config::default();

        let (_, exit_receiver) = tokio::sync::broadcast::channel(1);
        let exit_signal = crate::bootstrap::ExitSignal::new(exit_receiver);

        let etcd_client = crate::bootstrap::Bootstrap::create_etcd(
            &config,
            &config.etcd_server_urls,
            exit_signal.clone(),
        );

        let alloc =
            crate::bootstrap::Bootstrap::start_server(config, etcd_client, exit_signal.clone())
                .unwrap();

        log::info!("alloc loop will begin...");

        loop {
            let ts = alloc.handle_request(1).unwrap();
            log::info!("alloc new ts: {}", ts);

            std::thread::sleep(std::time::Duration::from_secs(3));
        }
    }
}
