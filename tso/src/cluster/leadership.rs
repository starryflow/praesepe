use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::Duration,
};

use coarsetime::Clock;
use etcd_client::EventType;
use parking_lot::Mutex;
use tokio::runtime::{Builder, Runtime};

use crate::{
    bootstrap::ExitSignal,
    etcd::{EtcdFacade, Watcher},
    util::constant::Constant,
    TsoResult,
};

use super::{lease::Lease, ParticipantInfo};

pub struct TsoLeadership {
    purpose: String,

    /// The lease which is used to get this leadership
    lease: Mutex<Option<Arc<Lease>>>,
    keep_alive_exit_signal: Mutex<Option<tokio::sync::broadcast::Sender<()>>>,
    etcd_client: Arc<EtcdFacade>,
    /// leader_key and leader_value are key-value pair in etcd
    leader_key: String,
    leader_value: Mutex<String>,
    /// primary_watch is for the primary watch only,
    /// which is used to reuse `Watch` interface in `Leadership`.
    primary_watch: AtomicBool,

    rt: Runtime,
}

impl TsoLeadership {
    pub fn campaign(&self, lease_timeout: u64, leader_data: &str) -> TsoResult<()> {
        *self.leader_value.lock() = leader_data.to_owned();

        // Create a new lease to campaign
        let mut new_lease = Lease::new(&self.purpose);
        new_lease.grant(lease_timeout, &self.etcd_client)?;

        // The leader key must not exist, so the CreateRevision is 0
        self.etcd_client
            .compare_and_set_str(&self.leader_key, leader_data, 0, new_lease.get_lease_id())
            .inspect_err(|_| new_lease.close(&self.etcd_client))?;

        log::info!(
            "write leaderData to leaderPath ok, leader-key: {}, purpose: {}",
            self.leader_key,
            self.purpose
        );

        self.lease.lock().replace(Arc::from(new_lease));
        Ok(())
    }

    pub fn keep(&self) {
        if let Some(lease) = self.lease.lock().as_ref() {
            let (sender, receiver) = tokio::sync::broadcast::channel(1);
            self.keep_alive_exit_signal.lock().replace(sender);
            let _ = self.rt.spawn(
                lease
                    .clone()
                    .keep_alive(self.etcd_client.clone(), ExitSignal::new(receiver)),
            );
        }
    }

    pub fn check(&self) -> bool {
        self.lease
            .lock()
            .as_ref()
            .map(|x| !x.is_expired())
            .unwrap_or(false)
    }

    pub fn watch(&self, revision: i64, exit_signal: ExitSignal) {
        self.rt.block_on(self.async_watch(revision, exit_signal));
    }
    async fn async_watch(&self, mut revision: i64, mut exit_signal: ExitSignal) {
        let mut watcher: Option<Watcher> = None;

        let mut ticker = tokio::time::interval(
            Duration::from_millis(Constant::REQUEST_PROGRESS_INTERVAL_MILLIS).into(),
        );
        let mut last_received_response_time = Clock::now_since_epoch().as_millis();

        'new_watcher: loop {
            // When etcd is not available, the watcher.Watch will block, so we check the etcd availability first
            if !self.etcd_client.is_healthy() {
                if Clock::now_since_epoch().as_millis() - last_received_response_time
                    > Constant::WATCH_LOOP_UNHEALTHY_TIMEOUT_MILLIS
                {
                    log::error!("the connection is unhealthy for a while, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}", revision, self.leader_key, self.purpose);
                    return;
                }
                log::warn!("the connection maybe unhealthy, retry to watch later, revision: {}, leader-key: {}, purpose: {}", revision, self.leader_key, self.purpose);

                tokio::select! {
                    biased;
                    _ = exit_signal.recv() => {
                        log::info!("server is closed, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}",revision, self.leader_key, self.purpose);
                        return;
                    }
                    _ = ticker.tick() => {
                        continue;  // continue to check the etcd availability
                    }
                }
            }

            if let Some(watcher) = &mut watcher {
                let _ = watcher.cancel().await;
            }

            // In order to prevent a watch stream being stuck in a partitioned node, make sure to wrap context with "WithRequireLeader"
            // TODO: the etcd-client crate can not support `WithRequireLeader` feature

            let (new_watcher, mut watch_stream) = match self.etcd_client.try_watch(
                &self.leader_key,
                Some(
                    etcd_client::WatchOptions::new()
                        .with_start_revision(revision)
                        .with_progress_notify()
                        .into(),
                ),
                Constant::REQUEST_PROGRESS_INTERVAL_MILLIS,
            ) {
                Ok((watcher, watch_stream)) => (watcher, watch_stream),
                Err(e) => {
                    log::warn!("error occurred while creating watch channel and retry it later in watch loop, cause: {}, revision: {}, leader-key: {}, purpose: {}", e,revision, self.leader_key, self.purpose);

                    tokio::select! {
                        biased;
                        _ = exit_signal.recv() => {
                            log::info!("server is closed, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}",revision, self.leader_key, self.purpose);
                            return;
                        }
                        _ = ticker.tick() => {
                            continue;
                        }
                    }
                }
            };
            watcher = Some(new_watcher);
            last_received_response_time = Clock::now_since_epoch().as_millis();
            log::info!(
                "watch channel is created, revision: {}, leader-key: {}, purpose: {}",
                revision,
                self.leader_key,
                self.purpose
            );

            'watch_chan_loop: loop {
                tokio::select! {
                    biased;
                    _ = exit_signal.recv() => {
                        log::info!("server is closed, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}",revision, self.leader_key, self.purpose);
                        return;
                    }
                    _ = ticker.tick() => {
                        // When etcd is not available, the watcher.RequestProgress will block, so we check the etcd availability first
                        if !self.etcd_client.is_healthy(){
                            log::warn!("the connection maybe unhealthy, retry to watch later, leader-key: {}, purpose: {}", self.leader_key, self.purpose);
                            continue 'new_watcher;
                        }

                        // We need to request progress to etcd to prevent etcd hold the watchChan
                        if let Err(e) = self.etcd_client.try_request_progress(watcher.as_mut().expect("watcher already create"), Constant::DEFAULT_REQUEST_TIMEOUT_MILLIS) {
                            log::warn!("failed to request progress in leader watch loop, cause: {}, revision: {}, leader-key: {}, purpose: {}", e, revision, self.leader_key, self.purpose);
                        }

                        // If no message comes from an etcd watchChan for WatchChTimeoutDuration, create a new one and need not to reset lastReceivedResponseTime
                        let cost = Clock::now_since_epoch().as_millis() - last_received_response_time;
                        if cost >= Constant::DEFAULT_REQUEST_TIMEOUT_MILLIS{
                            log::warn!("watch channel is blocked for a long time, recreating a new one, timeout: {}, leader-key: {}, purpose: {}", cost, self.leader_key,self.purpose);
                            continue 'new_watcher;
                        }

                        // avoid creating a new watcher
                    }
                    resp = watch_stream.message() =>{
                        last_received_response_time = Clock::now_since_epoch().as_millis();

                        match resp {
                            Ok(Some(resp)) => {
                                if resp.compact_revision() != 0 {
                                    log::warn!("required revision has been compacted, use the compact revision: {}, compact-revision: {}, leader-key: {}, purpose: {}",revision, resp.compact_revision(), self.leader_key, self.purpose);
                                    revision = resp.compact_revision();
                                    continue 'new_watcher;
                                } else if resp.events().is_empty() { // means IsProgressNotify
                                    log::debug!("watcher receives progress notify in watch loop, revision: {}, leader-key: {}, purpose: {}", revision, self.leader_key, self.purpose);
                                    // avoid creating a new watcher
                                    continue  'watch_chan_loop;
                                }

                                for e in resp.events() {
                                    match e.event_type() {
                                        EventType::Delete => {
                                            log::info!("current leadership is deleted, revision: {}, leader-key: {}, purpose: {}", resp.header().expect("watch resp header is empty").revision(), self.leader_key, self.purpose);
                                            return;
                                        }
                                        EventType::Put => {
                                            // ONLY `{service}/primary/transfer` API update primary will meet this condition
                                            if self.is_primary() {
                                                log::info!("current leadership is updated, revision: {}, leader_key: {}, cur-value: {}, purpose: {}", resp.header().expect("watch resp header is empty").revision(), self.leader_key,  e.kv().expect("watch resp kv is empty").value_str().unwrap_or_default(), self.purpose);
                                                return;
                                            }
                                        }
                                    }
                                }
                                revision = resp.header().expect("watch resp header is empty").revision() + 1;

                                // avoid creating a new watcher
                            }
                            Ok(None) => {
                                // avoid creating a new watchChan
                            }
                            Err(e) => {
                                log::error!("leadership watcher is canceled with {}, revision: {}, leader-key: {}, purpose: {}",e,revision,self.leader_key,self.purpose);
                                return;
                            }
                        }
                    }
                }

                continue 'watch_chan_loop;
            }
        }
    }

    pub fn reset(&self) {
        if let Some(lease) = self.lease.lock().as_ref() {
            if let Some(exit_signal) = self.keep_alive_exit_signal.lock().take() {
                let _ = exit_signal.send(());
            }
            lease.close(&self.etcd_client);
            self.primary_watch
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn get_leader(&self) -> TsoResult<(Option<ParticipantInfo>, i64)> {
        if let Some((info, mod_rev)) = self
            .etcd_client
            .get_part_info_with_mod_rev(&self.leader_key)?
        {
            Ok((Some(info), mod_rev))
        } else {
            Ok((None, 0))
        }
    }

    pub fn delete_leader_key(&self) -> TsoResult<()> {
        self.etcd_client.delete(&self.leader_key)?;

        // Reset the lease as soon as possible
        self.reset();
        log::info!(
            "delete the leader key ok, leader-key: {}, purpose: {}",
            self.leader_key,
            self.purpose
        );
        Ok(())
    }
}

impl Debug for TsoLeadership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "TsoLeadership use client: {:?}, for purpose: {}, ",
            &self.etcd_client, &self.purpose
        ))
    }
}

impl TsoLeadership {
    pub fn new(
        leader_key: &str,
        purpose: &str,
        worker_size: usize,
        etcd_client: EtcdFacade,
    ) -> Self {
        let rt = Builder::new_multi_thread()
            .worker_threads(worker_size)
            .enable_all()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                format!("TsoLeadershipWorker@{}", id)
            })
            .build()
            .expect("Create TSO LeadershipWorkerPool failed");

        Self {
            purpose: purpose.into(),
            lease: Mutex::new(None),
            keep_alive_exit_signal: Mutex::new(None),
            etcd_client: Arc::from(etcd_client),
            leader_key: leader_key.to_owned(),
            leader_value: Mutex::new("".to_owned()),
            primary_watch: false.into(),
            rt,
        }
    }

    fn is_primary(&self) -> bool {
        self.primary_watch
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
