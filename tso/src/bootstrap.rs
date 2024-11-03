use std::time::Duration;

use crate::{
    cluster::{Cluster, EtcdClient, Participant},
    config::Config,
    store::TsoStoreFactory,
    util::constant::Constant,
    AllocatorManager, TsoResult,
};

pub struct Bootstrap;

impl Bootstrap {
    pub fn start_etcd(advertise_client_urls: &str, exit_signal: ExitSignal) -> EtcdClient {
        // Start the etcd and HTTP clients, then init the member
        EtcdClient::new(advertise_client_urls, exit_signal)

        // TODO: init_health_checker
    }

    pub fn start_server(
        config: Config,
        etcd_client: EtcdClient,
        exit_signal: ExitSignal,
    ) -> TsoResult<()> {
        let cluster_id = Cluster::init_cluster_id(&etcd_client, Constant::CLUSTER_ID_PATH)
            .inspect_err(|e| log::error!("failed to init cluster id, cause: {}", e))?;

        log::info!("init cluster id, cluster-id: {}", cluster_id);

        let root_path = Cluster::root_path(cluster_id);
        let member = Participant::new_and_start(&config, root_path, etcd_client);

        let store = TsoStoreFactory::get_instance(&config.store_kind);

        let mut alloc = AllocatorManager::new_and_start(config, member, store, exit_signal.clone());

        alloc.start_global_allocator_loop(exit_signal.clone());

        Self::start_server_loop(exit_signal);
        Ok(())
    }

    pub fn start_server_loop(mut exit_signal: ExitSignal) {
        // TODO: To make sure the etcd leader and TSO leader are on the same server
        // go s.leaderLoop()

        // TODO: checks whether there is another participant has higher priority and resign it as the leader if so
        // go s.etcdLeaderLoop()

        // TODO: set metric
        // go s.serverMetricsLoop()

        // TODO: watch Key change
        // go s.encryptionKeyManagerLoop()

        // TODO: watch TSO-Primary / Scheduling-Primary change, maintain `service_primary_map` for api forward
        // if s.IsAPIServiceMode() {
        //     s.initTSOPrimaryWatcher()
        //     s.initSchedulingPrimaryWatcher()
        // }

        loop {
            if exit_signal.wait_exit() {
                break;
            }
        }
    }
}

pub struct ExitSignal(tokio::sync::broadcast::Receiver<()>);

impl Clone for ExitSignal {
    fn clone(&self) -> Self {
        Self(self.0.resubscribe())
    }
}

impl ExitSignal {
    pub fn new(receiver: tokio::sync::broadcast::Receiver<()>) -> Self {
        Self(receiver)
    }

    pub fn try_exit(&mut self) -> bool {
        self.0.try_recv().is_ok()
    }

    pub fn wait_exit(&mut self) -> bool {
        loop {
            if self.try_exit() {
                return true;
            } else {
                std::thread::sleep(Duration::from_millis(Constant::LOOP_MIN_INTERVAL_MILLIS));
                continue;
            }
        }
    }

    pub async fn recv(&mut self) -> TsoResult<()> {
        self.0.recv().await.map_err(|e| anyhow::anyhow!(e))
    }
}
