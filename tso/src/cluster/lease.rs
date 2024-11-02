use std::sync::atomic::AtomicU64;

use coarsetime::{Clock, Duration, Instant};
use crossbeam::channel::Receiver;
use rayon::ThreadPool;

use crate::{
    allocator::UnixTimeStamp,
    bootstrap::ExitSignal,
    util::{constant::Constant, etcd_client::EtcdClient},
    TsoResult,
};

#[derive(Debug)]
pub struct Lease {
    /// purpose is used to show what this election for
    purpose: String,
    /// lease info
    pub(super) lease_id: i64,
    /// lease_timeout and expire_time are used to control the lease's lifetime
    lease_timeout: Duration,
    expire_time: AtomicU64,
}

impl Lease {
    pub fn new(purpose: &str) -> Self {
        Self {
            purpose: purpose.into(),
            lease_id: 0,
            lease_timeout: 0.into(),
            expire_time: 0.into(),
        }
    }

    /// initialize the lease and expire_time
    pub fn grant(&mut self, lease_timeout_sec: i64, lease_client: &EtcdClient) -> TsoResult<()> {
        let start = Clock::now_since_epoch().as_millis();

        let lease_resp =
            lease_client.try_grant(lease_timeout_sec, Constant::DEFAULT_REQUEST_TIMEOUT_MILLIS)?;

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

        self.lease_id = lease_resp.id();
        self.lease_timeout = Duration::from_secs(lease_timeout_sec as u64);
        self.set_expire_time(start + (lease_resp.ttl() * 1000) as u64);
        Ok(())
    }

    pub fn close(&self, etcd_client: &EtcdClient) {
        // Reset expire time
        self.set_expire_time(0);

        // Try to revoke lease to make subsequent elections faster
        if self.lease_id != 0 {
            if let Err(e) =
                etcd_client.try_revoke(self.lease_id, Constant::REVOKE_LEASE_TIMEOUT_MILLIS)
            {
                log::error!(
                    "revoke lease failed, lease_id: {}, purpose: {}, error: {}",
                    self.lease_id,
                    self.purpose,
                    e
                );
            }
        }
        etcd_client.close();
    }

    /// checks if the lease is expired. If it returns true, current leader should step down and try to re-elect again
    pub fn is_expired(&self) -> bool {
        Clock::now_since_epoch().as_millis() > self.get_expire_time()
    }

    /// KeepAlive auto renews the lease and update expire_time.
    pub fn keep_alive(
        &self,
        etcd_client: &EtcdClient,
        thread_pool: &ThreadPool,
        mut exit_signal: ExitSignal,
    ) {
        let time_receiver = self.keep_alive_worker(
            self.lease_timeout / 3,
            etcd_client,
            thread_pool,
            exit_signal.clone(),
        );

        thread_pool.install(||{
            let mut max_expire = 0;
            let mut start = Instant::now();
            loop {
                if exit_signal.try_exit() {
                    break;
                }

                let now = Instant::now();

                if let Ok(time) = time_receiver.try_recv() {
                    if time > max_expire {
                        max_expire = time;
                        // Check again to make sure the `expireTime` still needs to be updated
                        if exit_signal.try_exit() {
                            break;
                        }
                        self.set_expire_time(time);
                    }

                    // reset start time
                    start = Instant::now();
                } else if now - start > self.lease_timeout {
                    // if too long, return keep alive
                    log::info!("keep alive lease too slow, timeout-duration: {} millis, actual-expire: {} millis, purpose: {}",self.lease_timeout.as_millis(), self.get_expire_time(), self.purpose);
                    break;
                } else {
                    std::thread::sleep((self.lease_timeout / 10).into())
                };
            }

            log::info!("lease keep alive stopped, purpose: {}", self.purpose);
        });
    }

    /// Periodically call `lease.keep_alive_once` and post back latest received expire time into the channel
    fn keep_alive_worker(
        &self,
        interval: Duration,
        etcd_client: &EtcdClient,
        thread_pool: &ThreadPool,
        mut exit_signal: ExitSignal,
    ) -> Receiver<UnixTimeStamp> {
        let (sender, receiver) = crossbeam::channel::unbounded();

        thread_pool.install(|| {
            log::info!(
                "start lease keep alive worker, interval: {} millis, purpose: {}",
                interval.as_millis(),
                self.purpose
            );

            let mut last_time = Clock::now_since_epoch().as_millis();
            loop {
                if exit_signal.try_exit() {
                    break;
                }

                let start = Clock::now_since_epoch().as_millis();
                if start - last_time > interval.as_millis() * 2 {
                    log::warn!(
                        "the interval between keeping alive lease is too long, last-time: {}",
                        last_time
                    );
                }

                thread_pool.install(|| {
                    if self.lease_id > 0 {
                        match etcd_client
                            .try_keep_alive_once(self.lease_id, self.lease_timeout.as_millis())
                        {
                            Ok(resp) => {
                                if resp.ttl() > 0 {
                                    let expire = start + resp.ttl() as u64 * 1000;
                                    let _ = sender.send(expire);
                                } else {
                                    log::error!(
                                        "keep alive response ttl is zero, purpose: {}",
                                        self.purpose
                                    );
                                }
                            }
                            Err(e) => {
                                log::warn!(
                                    "lease keep alive failed, purpose: {}, start: {}, error: {}",
                                    self.purpose,
                                    start,
                                    e
                                );
                                return;
                            }
                        }
                    }
                });

                std::thread::sleep(interval.into());
                last_time = start;
            }

            log::info!("stop lease keep alive worker, purpose: {}", self.purpose);
        });

        receiver
    }
}

impl Lease {
    fn get_expire_time(&self) -> UnixTimeStamp {
        self.expire_time.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn set_expire_time(&self, expire_time: UnixTimeStamp) {
        self.expire_time
            .store(expire_time, std::sync::atomic::Ordering::Relaxed);
    }
}
