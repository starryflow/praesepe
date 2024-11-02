use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use coarsetime::Instant;
use etcd_client::{Compare, CompareOp, EventType, PutOptions, Txn, TxnOp, WatchOptions};

use parking_lot::Mutex;
use tokio::{
    runtime::{Builder, Runtime},
    sync::OnceCell,
};

use crate::{
    bootstrap::ExitSignal,
    error::TsoError,
    util::{constant::Constant, etcd_client::EtcdClient},
    TsoResult,
};

use super::{lease::Lease, ParticipantInfo, TsoLeadership};

pub struct EtcdLeadership {
    purpose: String,

    /// The lease which is used to get this leadership
    lease: OnceCell<Lease>,
    etcd_client: Arc<EtcdClient>,
    /// leader_key and leader_value are key-value pair in etcd
    leader_key: String,
    leader_value: Mutex<String>,
    /// primary_watch is for the primary watch only,
    /// which is used to reuse `Watch` interface in `Leadership`.
    primary_watch: AtomicBool,

    rt: Runtime,
}

#[async_trait]
impl TsoLeadership for EtcdLeadership {
    fn campaign(&self, lease_timeout_sec: i64, leader_data: &str) -> TsoResult<()> {
        *self.leader_value.lock() = leader_data.to_owned();

        // Create a new lease to campaign
        let mut new_lease = Lease::new(&self.purpose);
        new_lease.grant(lease_timeout_sec, &self.etcd_client)?;

        // The leader key must not exist, so the CreateRevision is 0
        let txn = Txn::new()
            .when(vec![Compare::create_revision(
                self.leader_key.to_owned(),
                CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(
                self.leader_key.to_owned(),
                leader_data.to_owned(),
                Some(PutOptions::new().with_lease(new_lease.get_lease_id())),
            )]);
        let resp = self.etcd_client.do_in_txn(txn);
        log::info!("check campaign resp: {:?}", resp);
        match resp {
            Ok(resp) => {
                if !resp.succeeded() {
                    new_lease.close(&self.etcd_client);
                    anyhow::bail!(TsoError::EtcdTxnConflict)
                }
            }
            Err(e) => {
                new_lease.close(&self.etcd_client);
                anyhow::bail!(TsoError::EtcdTxnInternal(e))
            }
        }

        log::info!(
            "write leaderData to leaderPath ok, leader-key: {}, purpose: {}",
            self.leader_key,
            self.purpose
        );
        self.lease.set(new_lease).expect("lease set duplicate");
        Ok(())
    }

    fn delete_leader_key(&self) -> TsoResult<()> {
        let resp = self.etcd_client.delete(&self.leader_key)?;
        if resp.deleted() > 0 {
            // Reset the lease as soon as possible
            self.reset();
            log::info!(
                "delete the leader key ok, leader-key: {}, purpose: {}",
                self.leader_key,
                self.purpose
            );
            Ok(())
        } else {
            anyhow::bail!(TsoError::EtcdTxnConflict)
        }
    }

    fn check(&self) -> bool {
        self.lease.get().map(|x| !x.is_expired()).unwrap_or(false)
    }

    fn get_persistent_leader(&self) -> TsoResult<(Option<ParticipantInfo>, i64)> {
        if let Some((value, mod_rev)) = self.etcd_client.get_with_mod_rev(&self.leader_key)? {
            if let Ok(info) = serde_json::from_slice::<ParticipantInfo>(&value) {
                return Ok((Some(info), mod_rev));
            }
        }
        Ok((None, 0))
    }

    fn keep(&self, exit_signal: ExitSignal) {
        if let Some(lease) = self.lease.get() {
            lease.keep_alive(&self.rt, self.etcd_client.clone(), exit_signal);
        }
    }

    fn watch(&self, mut revision: i64, mut exit_signal: ExitSignal) {
        // let start = Instant::now();
        let last_received_response_time = Instant::now();
        loop {
            // When etcd is not available, the watcher.Watch will block, so we check the etcd availability first
            if !self.etcd_client.is_healthy() {
                if last_received_response_time
                    .elapsed_since_recent()
                    .as_millis()
                    > Constant::WATCH_LOOP_UNHEALTHY_TIMEOUT_MILLIS
                {
                    log::error!("the connection is unhealthy for a while, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}", revision, self.leader_key, self.purpose);
                    return;
                }
                log::warn!("the connection maybe unhealthy, retry to watch later, revision: {}, leader-key: {}, purpose: {}", revision, self.leader_key, self.purpose);

                if exit_signal.try_exit() {
                    log::info!("server is closed, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}",revision, self.leader_key, self.purpose);
                    return;
                }

                // continue to check the etcd availability
                std::thread::sleep(Duration::from_millis(
                    Constant::REQUEST_PROGRESS_INTERVAL_MILLIS,
                ));
                continue;
            }

            // watcher_cancel()
            // watcher_close()

            // let mut watcher = self.etcd_client.new_watcher().unwrap();

            // In order to prevent a watch stream being stuck in a partitioned node, make sure to wrap context with "WithRequireLeader"

            // TODO: 如果 watch 操作三秒未完成，调用 cancel 取消
            let (mut watcher, mut watch_stream) = match self.etcd_client.watch(
                &self.leader_key,
                Some(
                    WatchOptions::new()
                        .with_start_revision(revision)
                        .with_progress_notify(),
                ),
            ) {
                Ok((watcher, watch_stream)) => (watcher, watch_stream),
                Err(e) => {
                    log::warn!("error occurred while creating watch channel and retry it later in watch loop, cause: {}, revision: {}, leader-key: {}, purpose: {}",e,revision,self.leader_key,self.purpose);

                    if exit_signal.try_exit() {
                        log::info!("server is closed, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}",revision, self.leader_key, self.purpose);
                        return;
                    }

                    // continue to check the etcd availability
                    std::thread::sleep(Duration::from_millis(
                        Constant::REQUEST_PROGRESS_INTERVAL_MILLIS,
                    ));
                    continue;
                }
            };

            Instant::update();
            log::info!(
                "watch channel is created, revision: {}, leader-key: {}, purpose: {}",
                revision,
                self.leader_key,
                self.purpose
            );

            // watch_stream process:
            if exit_signal.try_exit() {
                log::info!("server is closed, exit leader watch loop, revision: {}, leader-key: {}, purpose: {}",revision, self.leader_key, self.purpose);
                return;
            }

            // TODO: watch timeout (no message)
            // When etcd is not available, the watcher.RequestProgress will block, so we check the etcd availability first

            // We need to request progress to etcd to prevent etcd hold the watchChan,
            // note: the ctx must be from watcherCtx, otherwise, the RequestProgress request cannot be sent properly
            // watcher.request_progress();

            // If no message comes from an etcd watchChan for WatchChTimeoutDuration,
            // create a new one and need not to reset lastReceivedResponseTime

            //
            match self.etcd_client.poll_watch_stream(&mut watch_stream) {
                Ok(Some(resp)) => {
                    Instant::update();
                    if resp.compact_revision() != 0 {
                        log::warn!("required revision has been compacted, use the compact revision: {}, compact-revision: {}, leader-key: {}, purpose: {}",revision, resp.compact_revision(), self.leader_key,self.purpose);
                        revision = resp.compact_revision();
                        continue;
                    }

                    // if resp.is_progress_notify() {}
                    // log::debug!("watcher receives progress notify in watch loop, revision: {}, leader-key: {}, purpose: {}", revision, self.leader_key, self.purpose);
                    // goto watch_chan_loop

                    for e in resp.events() {
                        match e.event_type() {
                            EventType::Delete => {
                                log::info!("current leadership is deleted, revision: {}, leader-key: {}, purpose: {}", resp.header().unwrap().revision(), self.leader_key,self.purpose);
                                return;
                            }
                            EventType::Put => {
                                // ONLY `{service}/primary/transfer` API update primary will meet this condition
                                log::info!("current leadership is updated, revision: {}, leader_key: {}, cur-value: {}, purpose: {}", resp.header().unwrap().revision(), self.leader_key, String::from_utf8_lossy(e.kv().unwrap().value()),self.purpose);
                                return;
                            }
                        }
                    }
                    revision = resp.header().unwrap().revision() + 1;

                    // goto to avoid creating a new watcher
                    continue;
                }
                Ok(None) => {
                    // avoid creating a new watchChan
                    continue;
                }
                Err(e) => {
                    log::error!("leadership watcher is canceled with {}, revision: {}, leader-key: {}, purpose: {}",e,revision,self.leader_key,self.purpose);
                    return;
                }
            }
        }
    }

    fn reset(&self) {
        if let Some(lease) = self.lease.get() {
            lease.close(&self.etcd_client);
            self.primary_watch
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

impl Debug for EtcdLeadership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "EtcdLeadership use client: {:?}",
            &self.etcd_client
        ))
    }
}

impl EtcdLeadership {
    pub fn new(
        leader_key: &str,
        purpose: &str,
        worker_size: usize,
        etcd_client: EtcdClient,
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
            lease: OnceCell::new(),
            etcd_client: Arc::from(etcd_client),
            leader_key: leader_key.to_owned(),
            leader_value: Mutex::new("".to_owned()),
            primary_watch: false.into(),
            rt,
        }
    }
}
