use std::sync::{atomic::AtomicU64, Arc};

use coarsetime::{Clock, Duration};
use scopeguard::defer;
use tokio::{sync::mpsc::UnboundedReceiver, time::Instant};

use crate::{
    allocator::UnixTimeStamp, bootstrap::ExitSignal, error::TsoError, etcd::EtcdFacade,
    util::constant::Constant, TsoResult,
};

/// Lease is used as the low-level mechanism for campaigning and renewing elected leadership.
/// The way to gain and maintain leadership is to update and keep the lease alive continuously.
#[derive(Debug)]
pub struct Lease {
    /// purpose is used to show what this election for
    purpose: String,
    /// lease info
    lease_id: Option<i64>,
    /// lease_timeout and expire_time are used to control the lease's lifetime
    lease_timeout: Duration,
    expire_time: AtomicU64,
}

impl Lease {
    pub fn new(purpose: &str) -> Self {
        Self {
            purpose: purpose.into(),
            lease_id: None,
            lease_timeout: 0.into(),
            expire_time: 0.into(),
        }
    }

    /// initialize the lease and expire_time
    pub fn grant(&mut self, lease_timeout_sec: i64, lease_client: &EtcdFacade) -> TsoResult<()> {
        let start = Clock::now_since_epoch().as_millis();

        let lease_resp = lease_client
            .try_grant(lease_timeout_sec, Constant::DEFAULT_REQUEST_TIMEOUT_MILLIS)
            .map_err(|e| anyhow::anyhow!(TsoError::EtcdGrantLease(e)))?;

        let cost = Clock::now_since_epoch().as_millis() - start;
        if cost > Constant::SLOW_REQUEST_TIME_MILLIS {
            log::warn!(
                "lease grants too slow, cost: {} millis, purpose: {}",
                cost,
                self.purpose
            );
        }
        log::info!(
            "lease granted, lease-id: {}, lease-timeout: {}, purpose: {}",
            lease_resp.id(),
            lease_resp.ttl(),
            self.purpose
        );

        self.lease_id = Some(lease_resp.id());
        self.lease_timeout = Duration::from_secs(lease_timeout_sec as u64);
        self.set_expire_time(start + lease_resp.ttl() as u64 * 1000);
        Ok(())
    }

    pub fn close(&self, etcd_client: &EtcdFacade) {
        // Reset expire time
        self.set_expire_time(0);

        // Try to revoke lease to make subsequent elections faster
        if let Some(lease_id) = self.lease_id {
            if let Err(e) = etcd_client.try_revoke(lease_id, Constant::REVOKE_LEASE_TIMEOUT_MILLIS)
            {
                log::error!(
                    "revoke lease failed, lease_id: {}, purpose: {}, error: {}",
                    lease_id,
                    self.purpose,
                    e
                );
            }
        }
    }

    /// checks if the lease is expired. If it returns true, current leader should step down and try to re-elect again
    pub fn is_expired(&self) -> bool {
        let expire_time = self.get_expire_time();
        if expire_time == 0 {
            true
        } else {
            Clock::now_since_epoch().as_millis() > expire_time
        }
    }

    /// KeepAlive auto renews the lease and update expire_time
    /// Will block_on until keep_alive failed or exit_signal
    pub async fn keep_alive(
        self: Arc<Lease>,
        etcd_client: Arc<EtcdFacade>,
        mut exit_signal: ExitSignal,
    ) {
        defer! {
            log::info!("lease keep alive stopped, purpose: {}", self.purpose);
        };

        let mut time_ch =
            self.keep_alive_worker(self.lease_timeout / 3, etcd_client, exit_signal.clone());

        let timer = tokio::time::sleep(self.lease_timeout.into());
        tokio::pin!(timer);

        let mut max_expire = 0;
        loop {
            tokio::select! {
                biased;
                time = time_ch.recv() =>{
                    if let Some(t) = time {
                        if t > max_expire {
                            max_expire = t;
                            // Check again to make sure the `expireTime` still needs to be updated
                            if exit_signal.try_exit() {
                                return;
                            }
                            self.set_expire_time(t);
                        }
                    } else {
                        return;
                    }

                    // Stop the timer if it's not stopped
                    if !timer.is_elapsed(){
                        tokio::select! {
                            _ = &mut timer => {}   // try to drain from the channel
                            else => {}
                        }
                    }

                    // We need be careful here
                    timer.as_mut().reset(Instant::now()+self.lease_timeout.into());
                }
                _ = &mut timer => {
                    log::info!("keep alive lease too slow, timeout-duration: {} millis, actual-expire: {} millis, purpose: {}",self.lease_timeout.as_millis(), self.get_expire_time(), self.purpose);
                    return;
                }
                _ = exit_signal.recv() => {
                    return;
                }
            }
        }
    }

    /// Periodically call `lease.keep_alive_once` and post back latest received expire time into the channel
    fn keep_alive_worker(
        &self,
        interval: Duration,
        etcd_client: Arc<EtcdFacade>,
        mut exit_signal: ExitSignal,
    ) -> UnboundedReceiver<UnixTimeStamp> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let purpose = self.purpose.to_owned();
        let lease_id = self.lease_id;
        let lease_timeout = self.lease_timeout;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval.into());
            let tick = ticker.tick();
            tokio::pin!(tick);

            log::info!(
                "start lease keep alive worker, interval: {} millis, purpose: {}",
                interval.as_millis(),
                purpose
            );
            defer! {
                log::info!("stop lease keep alive worker, purpose: {}", purpose);
            };

            let mut last_time = Clock::now_since_epoch().as_millis();

            // continue call keep_alive_once, until exit_signal
            loop {
                let start = Clock::now_since_epoch().as_millis();
                if start - last_time > interval.as_millis() * 2 {
                    log::warn!(
                        "the interval between keeping alive lease is too long, last-time: {}",
                        last_time
                    );
                }

                // because try_keep_alive_once use some time, so call in another thread
                let purpose_clone = purpose.clone();
                let tx_clone = tx.clone();
                let etcd_client_clone = etcd_client.clone();
                tokio::spawn(async move {
                    if let Some(lease_id) = lease_id {
                        match etcd_client_clone
                            .try_keep_alive_once(lease_id, lease_timeout.as_millis())
                        {
                            Ok(resp) => {
                                if resp.ttl() > 0 {
                                    let expire = start + resp.ttl() as u64 * 1000;
                                    let _ = tx_clone.send(expire);
                                } else {
                                    log::error!(
                                        "keep alive response ttl is zero, purpose: {}",
                                        purpose_clone
                                    );
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "lease keep alive failed, purpose: {}, start: {}, error: {}",
                                    purpose_clone,
                                    start,
                                    e
                                );
                                return;
                            }
                        }
                    }
                });

                tokio::select! {
                    biased;
                    _ = exit_signal.recv() => {
                        return;
                    }
                    _ = &mut tick => {
                        last_time = start;
                    }
                }
            }
        });

        rx
    }
}

impl Lease {
    pub(super) fn get_lease_id(&self) -> i64 {
        self.lease_id.expect("lease id not none after grant")
    }

    fn get_expire_time(&self) -> UnixTimeStamp {
        self.expire_time.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn set_expire_time(&self, expire_time: UnixTimeStamp) {
        self.expire_time
            .store(expire_time, std::sync::atomic::Ordering::Relaxed);
    }
}
