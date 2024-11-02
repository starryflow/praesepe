use std::{fmt::Debug, sync::Arc, time::Duration};

use coarsetime::Instant;
use etcd_client::{
    Client, DeleteResponse, GetResponse, LeaseGrantResponse, LeaseKeepAliveResponse,
    LeaseRevokeResponse, Txn, TxnResponse, WatchOptions, WatchResponse, WatchStream, Watcher,
};
use parking_lot::Mutex;
use tokio::runtime::Runtime;

use crate::{bootstrap::ExitSignal, error::TsoError, util::constant::Constant, TsoResult};

pub struct EtcdClient {
    endpoints: String,
    client: Arc<Mutex<Client>>,
    runtime: Runtime,
    exit_signal: ExitSignal,
}

impl Debug for EtcdClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.endpoints)
    }
}

impl EtcdClient {
    pub fn new(url: &str, exit_signal: ExitSignal) -> Self {
        let runtime = Runtime::new().unwrap();
        let endpoints = [url];
        let client = runtime.block_on(async { Client::connect(endpoints, None).await.unwrap() });
        Self {
            endpoints: url.to_owned(),
            client: Arc::from(Mutex::new(client)),
            runtime,
            exit_signal,
        }
    }

    /// checks if the etcd is healthy
    pub fn is_healthy(&self) -> bool {
        // just now , always return true
        true
    }

    pub fn close(&self) {
        // do nothing
    }
}

//
impl EtcdClient {
    pub fn get(&self, key: &str) -> TsoResult<GetResponse> {
        let start = Instant::now();
        let resp = self
            .runtime
            .block_on(async { self.client.lock().get(key, None).await })
            .map_err(|e| anyhow::anyhow!(e));
        let cost = start.elapsed().as_millis();
        if cost > Constant::SLOW_REQUEST_TIME_MILLIS {
            log::warn!("kv gets too slow, request key: {}, cost: {}", key, cost);
        }

        if let Err(e) = &resp {
            log::error!("load from etcd meet error, key: {}, cause: {}", key, e);
        }

        resp
    }

    pub fn get_with_mod_rev(&self, key: &str) -> TsoResult<Option<(Vec<u8>, i64)>> {
        self.runtime.block_on(async {
            match self.client.lock().get(key, None).await {
                Ok(mut resp) => {
                    if resp.count() > 0 {
                        let kv = resp.take_kvs().remove(0);
                        let rv = kv.mod_revision();
                        Ok(Some((kv.into_key_value().1, rv)))
                    } else {
                        Ok(None)
                    }
                }
                Err(e) => anyhow::bail!(e),
            }
        })
    }

    pub fn delete(&self, key: &str) -> TsoResult<DeleteResponse> {
        self.runtime
            .block_on(async { self.client.lock().delete(key, None).await })
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn do_in_txn(&self, txn: Txn) -> TsoResult<TxnResponse> {
        self.runtime
            .block_on(async { self.client.lock().txn(txn).await })
            .map_err(|e| anyhow::anyhow!(e))
    }
}

// lease api
impl EtcdClient {
    pub fn grant(&self, ttl_sec: i64) -> TsoResult<LeaseGrantResponse> {
        self.runtime
            .block_on(async { self.client.lock().lease_grant(ttl_sec, None).await })
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn try_grant(&self, ttl_sec: i64, timeout: u64) -> TsoResult<LeaseGrantResponse> {
        self.runtime.block_on(async {
            let mut lock = self.client.lock();
            let handle = lock.lease_grant(ttl_sec, None);
            tokio::pin!(handle);

            let timeout = tokio::time::sleep(Duration::from_millis(timeout));
            tokio::pin!(timeout);

            let exit_signal = self.exit_signal.clone();
            tokio::pin!(exit_signal);

            loop {
                tokio::select! {
                    biased;
                    resp = &mut handle => {
                        return resp.map_err(|e| anyhow::anyhow!(e));
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                };
            }
        })
    }

    pub fn keep_alive_once(&self, lease_id: i64) -> TsoResult<LeaseKeepAliveResponse> {
        self.runtime.block_on(async {
            match self.client.lock().lease_keep_alive(lease_id).await {
                Ok((_, mut s)) => match s.message().await {
                    Ok(Some(r)) => Ok(r),
                    Ok(None) => anyhow::bail!("keep alive failed, no response"),
                    Err(e) => anyhow::bail!(e),
                },
                Err(e) => anyhow::bail!(e),
            }
        })
    }

    pub fn try_keep_alive_once(
        &self,
        lease_id: i64,
        timeout: u64,
    ) -> TsoResult<LeaseKeepAliveResponse> {
        self.runtime.block_on(async {
            let mut lock = self.client.lock();
            let handle = lock.lease_keep_alive(lease_id);
            tokio::pin!(handle);

            let timeout = tokio::time::sleep(Duration::from_millis(timeout));
            tokio::pin!(timeout);

            let exit_signal = self.exit_signal.clone();
            tokio::pin!(exit_signal);

            loop {
                tokio::select! {
                    biased;
                    resp = &mut handle => {
                        match resp {
                            Ok((_, mut s)) => match s.message().await {
                                Ok(Some(r)) => return Ok(r),
                                Ok(None) => anyhow::bail!("keep alive failed, no response"),
                                Err(e) => anyhow::bail!(e),
                            },
                            Err(e) => anyhow::bail!(e),
                        }
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                };
            }
        })
    }

    pub fn revoke(&self, lease_id: i64) -> TsoResult<LeaseRevokeResponse> {
        self.runtime.block_on(async {
            self.client
                .lock()
                .lease_revoke(lease_id)
                .await
                .map_err(|e| anyhow::anyhow!(e))
        })
    }

    pub fn try_revoke(&self, lease_id: i64, timeout: u64) -> TsoResult<LeaseRevokeResponse> {
        self.runtime.block_on(async {
            let mut lock = self.client.lock();
            let handle = lock.lease_revoke(lease_id);
            tokio::pin!(handle);

            let timeout = tokio::time::sleep(Duration::from_millis(timeout));
            tokio::pin!(timeout);

            let exit_signal = self.exit_signal.clone();
            tokio::pin!(exit_signal);

            loop {
                tokio::select! {
                    biased;
                    resp = &mut handle => {
                        return resp.map_err(|e| anyhow::anyhow!(e));
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                };
            }
        })
    }
}

impl EtcdClient {
    pub fn watch(
        &self,
        key: &str,
        options: Option<WatchOptions>,
    ) -> TsoResult<(Watcher, WatchStream)> {
        self.runtime
            .block_on(async { self.client.lock().watch(key, options).await })
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn poll_watch_stream(&self, stream: &mut WatchStream) -> TsoResult<Option<WatchResponse>> {
        self.runtime
            .block_on(async { stream.message().await })
            .map_err(|e| anyhow::anyhow!(e))
    }
}
