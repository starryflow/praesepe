use std::{fmt::Debug, sync::Arc, time::Duration};

use super::{LeaseGrantResponse, LeaseKeepAliveResponse, WatchOptions, WatchStream, Watcher};
use coarsetime::Instant;
use etcd_client::{Client, Compare, PutOptions, Txn, TxnOp, TxnOpResponse, TxnResponse};
use parking_lot::Mutex;
use tokio::runtime::Runtime;

use crate::{
    bootstrap::ExitSignal,
    cluster::ParticipantInfo,
    error::TsoError,
    util::{constant::Constant, utils::Utils},
    TsoResult,
};

pub struct EtcdClient {
    endpoints: String,
    client: Arc<Mutex<Client>>,
    exit_signal: ExitSignal,
    rt: Runtime,
}

impl Debug for EtcdClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.endpoints)
    }
}

impl EtcdClient {
    pub fn new(url: &str, exit_signal: ExitSignal) -> Self {
        let rt = Runtime::new().unwrap();
        let endpoints = [url];
        let client = rt.block_on(async { Client::connect(endpoints, None).await.unwrap() });
        Self {
            endpoints: url.to_owned(),
            client: Arc::from(Mutex::new(client)),
            exit_signal,
            rt,
        }
    }

    /// checks if the etcd is healthy
    pub fn is_healthy(&self) -> bool {
        // just now , always return true
        true
    }
}

// kv api
impl EtcdClient {
    pub fn get_u64(&self, key: &str) -> TsoResult<Option<u64>> {
        let start = Instant::now();
        let resp = self
            .rt
            .block_on(async { self.client.lock().get(key, None).await });
        let cost = start.elapsed().as_millis();
        if cost > Constant::SLOW_REQUEST_TIME_MILLIS {
            log::warn!("kv gets too slow, request key: {}, cost: {}", key, cost);
        }

        match resp {
            Ok(resp) => {
                if resp.count() == 0 {
                    Ok(None)
                } else {
                    Ok(Some(Utils::bytes_to_u64(resp.kvs()[0].value())?))
                }
            }
            Err(e) => {
                log::error!("load from etcd meet error, key: {}, cause: {}", key, e);
                anyhow::bail!(e)
            }
        }
    }

    pub fn compare_and_set_u64(
        &self,
        key: &str,
        value: u64,
        expected_create_revision: i64,
    ) -> TsoResult<u64> {
        let value_bytes = Utils::u64_to_bytes(value);

        let txn = Txn::new()
            .when(vec![Compare::create_revision(
                key,
                etcd_client::CompareOp::Equal,
                expected_create_revision,
            )])
            .and_then(vec![TxnOp::put(key, value_bytes, None)])
            .or_else(vec![TxnOp::get(key, None)]);
        let resp = self.do_in_txn(txn)?;

        // Txn commits ok, return the generated cluster ID
        if resp.succeeded() {
            Ok(value)
        }
        // Otherwise, parse the committed cluster ID
        else if resp.op_responses().len() == 0 {
            anyhow::bail!(TsoError::EtcdTxnConflict)
        } else {
            let resp = resp.op_responses().remove(0);
            match resp {
                TxnOpResponse::Get(r) => {
                    if r.kvs().len() == 1 {
                        Utils::bytes_to_u64(r.kvs()[0].value())
                    } else {
                        anyhow::bail!(TsoError::EtcdTxnConflict)
                    }
                }
                _ => anyhow::bail!(TsoError::EtcdTxnConflict),
            }
        }
    }

    pub fn compare_and_set_str(
        &self,
        key: &str,
        value: &str,
        expected_create_revision: i64,
        lease_id: i64,
    ) -> TsoResult<()> {
        let value_string = value.to_owned();

        let txn = Txn::new()
            .when(vec![Compare::create_revision(
                key,
                etcd_client::CompareOp::Equal,
                expected_create_revision,
            )])
            .and_then(vec![TxnOp::put(
                key,
                value_string,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        let resp = self.do_in_txn(txn);
        log::info!(
            "compare_and_set_str, key: {}, value: {}, lease_id: {}, resp: {:?}",
            key,
            value,
            lease_id,
            resp
        );

        match resp {
            Ok(resp) => {
                if !resp.succeeded() {
                    anyhow::bail!(TsoError::EtcdTxnConflict)
                } else {
                    Ok(())
                }
            }
            Err(e) => {
                anyhow::bail!(TsoError::EtcdTxnInternal(e))
            }
        }
    }

    pub fn get_part_info_with_mod_rev(
        &self,
        key: &str,
    ) -> TsoResult<Option<(ParticipantInfo, i64)>> {
        self.rt.block_on(async {
            match self.client.lock().get(key, None).await {
                Ok(mut resp) => {
                    if resp.count() > 0 {
                        let kv = resp.take_kvs().remove(0);
                        let rv = kv.mod_revision();

                        if let Ok(info) =
                            serde_json::from_slice::<ParticipantInfo>(&kv.into_key_value().1)
                        {
                            Ok(Some((info, rv)))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                }
                Err(e) => anyhow::bail!(e),
            }
        })
    }

    pub fn delete(&self, key: &str) -> TsoResult<()> {
        let resp = self
            .do_in_txn(Txn::new().and_then(vec![TxnOp::delete(key.to_owned(), None)]))
            .map_err(|e| anyhow::anyhow!(TsoError::EtcdKVDelete(e)))?;
        if !resp.succeeded() {
            anyhow::bail!(TsoError::EtcdTxnConflict)
        } else {
            Ok(())
        }
    }

    fn do_in_txn(&self, txn: Txn) -> TsoResult<TxnResponse> {
        self.rt
            .block_on(async { self.client.lock().txn(txn).await })
            .map_err(|e| anyhow::anyhow!(e))
    }
}

// lease api
impl EtcdClient {
    pub fn try_grant(&self, ttl_sec: i64, timeout: u64) -> TsoResult<LeaseGrantResponse> {
        self.rt.block_on(async {
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
                        return resp.map(|x|x.into()).map_err(|e| anyhow::anyhow!(e));
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                };
            }
        })
    }

    pub fn try_keep_alive_once(
        &self,
        lease_id: i64,
        timeout: u64,
    ) -> TsoResult<LeaseKeepAliveResponse> {
        self.rt.block_on(async {
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
                                Ok(Some(r)) => return Ok(r.into()),
                                Ok(None) => anyhow::bail!("keep alive failed, no response"),
                                Err(e) => anyhow::bail!(e),
                            },
                            Err(e) => anyhow::bail!(e),
                        }
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                };
            }
        })
    }

    pub fn try_revoke(&self, lease_id: i64, timeout: u64) -> TsoResult<()> {
        self.rt.block_on(async {
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
                        return resp.map(|_|()).map_err(|e| anyhow::anyhow!(e));
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                };
            }
        })
    }
}

// watch api
impl EtcdClient {
    pub fn try_watch(
        &self,
        key: &str,
        options: Option<WatchOptions>,
        timeout: u64,
    ) -> TsoResult<(Watcher, WatchStream)> {
        self.rt
            .block_on(async {
                let mut lock = self.client.lock();
                let handle = lock.watch(key, options.map(|x| x.into()));
                tokio::pin!(handle);

                let timeout = tokio::time::sleep(Duration::from_millis(timeout));
                tokio::pin!(timeout);

                let exit_signal = self.exit_signal.clone();
                tokio::pin!(exit_signal);

                loop {
                    tokio::select! {
                        biased;
                        resp = &mut handle => {
                            return resp.map(|(x,y)|(x.into(),y.into())).map_err(|e| anyhow::anyhow!(e));
                        }
                        _ = exit_signal.recv() => {
                            anyhow::bail!(TsoError::TaskCancel);
                        }
                        _ = &mut timeout => {
                            anyhow::bail!(TsoError::TaskTimeout);
                        }
                    };
                }
            })
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub fn try_request_progress(&self, watcher: &mut Watcher, timeout: u64) -> TsoResult<()> {
        self.rt.block_on(async {
            let handle = watcher.request_progress();
            tokio::pin!(handle);

            let timeout = tokio::time::sleep(Duration::from_millis(timeout));
            tokio::pin!(timeout);

            let exit_signal = self.exit_signal.clone();
            tokio::pin!(exit_signal);

            loop {
                tokio::select! {
                    biased;
                    resp = &mut handle => {
                        return resp;
                    }
                    _ = exit_signal.recv() => {
                        anyhow::bail!(TsoError::TaskCancel);
                    }
                    _ = &mut timeout => {
                        anyhow::bail!(TsoError::TaskTimeout);
                    }
                };
            }
        })
    }
}
