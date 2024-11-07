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
pub use etcd::TsoEtcdKind;
pub use store::TsoStoreKind;

pub type TsoResult<T> = anyhow::Result<T>;

pub fn example() {
    let config = crate::config::Config::default();

    let (exit_sender, exit_receiver) = tokio::sync::broadcast::channel(1);
    let mut exit_signal = crate::bootstrap::ExitSignal::new(exit_receiver);

    let etcd_client = crate::bootstrap::Bootstrap::create_etcd(
        &config,
        &config.etcd_server_urls,
        exit_signal.clone(),
    );

    let _ = crate::bootstrap::Bootstrap::start_server(config, etcd_client, exit_signal.clone());

    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(100));

        // exit
        let _ = exit_sender.send(());
    });

    exit_signal.wait_exit();

    log::info!("exit")
}

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

        loop {
            let ts = alloc.handle_request(1).unwrap();
            log::info!("alloc new ts: {}", ts);

            std::thread::sleep(std::time::Duration::from_secs(3));
        }
    }

    #[test]
    pub fn test002() {
        // let mut config = crate::AllocatorConfig::default();
        // config.store_kind = crate::TsoStoreKind::MySQL("url".into());
        // let alloc = crate::AllocatorManager::new_and_setup(1, 1, config);

        // alloc.start_global_allocator_loop();
    }
}
