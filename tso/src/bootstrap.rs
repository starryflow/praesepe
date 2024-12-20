use std::{sync::Arc, time::Duration};

use crate::{
    cluster::{Cluster, Participant},
    config::Config,
    etcd::EtcdFacade,
    store::TsoStoreFactory,
    util::constant::Constant,
    AllocatorManager, TsoResult,
};

pub struct Bootstrap;

impl Bootstrap {
    pub fn create_etcd(
        config: &Config,
        advertise_client_urls: &str,
        exit_signal: ExitSignal,
    ) -> EtcdFacade {
        // Start the etcd and HTTP clients, then init the member
        EtcdFacade::new(config.etcd_kind, advertise_client_urls, exit_signal)

        // TODO: init_health_checker
    }

    pub fn start_server(
        config: Config,
        etcd_client: EtcdFacade,
        exit_signal: ExitSignal,
    ) -> TsoResult<Arc<AllocatorManager>> {
        let cluster_id = Cluster::init_cluster_id(&etcd_client, Constant::CLUSTER_ID_PATH)
            .inspect_err(|e| log::error!("failed to init cluster id, cause: {}", e))?;

        log::info!("init cluster id, cluster-id: {}", cluster_id);

        let root_path = Cluster::root_path(cluster_id);
        let member = Participant::new_and_start(&config, root_path, etcd_client);

        let store = TsoStoreFactory::get(&config.store_kind);

        let alloc = AllocatorManager::new_and_start(config, member, store, exit_signal.clone())?;

        Self::start_server_loop(alloc.clone(), exit_signal)?;

        Ok(alloc)
    }

    pub fn start_server_loop(
        alloc: Arc<AllocatorManager>,
        exit_signal: ExitSignal,
    ) -> TsoResult<()> {
        alloc
            .clone()
            .start_global_allocator_loop(exit_signal.clone())?;

        // self.leader_loop(alloc);
        // std::thread::Builder::new()
        //     .name("TsoLeaderLoopWorker".into())
        //     .spawn(move || {})?;

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

        Ok(())
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
        match self.0.try_recv() {
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => false,
            _ => true,
        }
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

    pub async fn recv(&mut self) {
        // if `sender` is closed, will get a Err, but don't throw this Err
        let _ = self.0.recv().await;
    }
}
