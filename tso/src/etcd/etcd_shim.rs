use std::fmt::Debug;

use crate::{cluster::ParticipantInfo, TsoResult};

use super::{LeaseGrantResponse, LeaseKeepAliveResponse, WatchOptions, WatchStream, Watcher};

pub struct EtcdShim;

impl Debug for EtcdShim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("EtcdShim",))
    }
}

impl EtcdShim {
    pub fn new(_url: &str) -> Self {
        Self
    }

    /// checks if the etcd is healthy
    pub fn is_healthy(&self) -> bool {
        // just now , always return true
        true
    }
}

// kv api
impl EtcdShim {
    pub fn get_u64(&self, _key: &str) -> TsoResult<Option<u64>> {
        // Default 0
        Ok(Some(0))
    }

    pub fn compare_and_set_u64(
        &self,
        _key: &str,
        _value: u64,
        _expected_create_revision: i64,
    ) -> TsoResult<u64> {
        unreachable!()
    }

    pub fn compare_and_set_str(
        &self,
        _key: &str,
        _value: &str,
        _expected_create_revision: i64,
        _lease_id: i64,
    ) -> TsoResult<()> {
        // always return Ok
        Ok(())
    }

    pub fn get_part_info_with_mod_rev(
        &self,
        _key: &str,
    ) -> TsoResult<Option<(ParticipantInfo, i64)>> {
        // always return None
        Ok(Some((ParticipantInfo::default(), 0)))
    }

    pub fn delete(&self, _key: &str) -> TsoResult<()> {
        // do nothing
        Ok(())
    }
}

// lease api
impl EtcdShim {
    pub fn try_grant(&self, _ttl_sec: i64, _timeout: u64) -> TsoResult<LeaseGrantResponse> {
        Ok(LeaseGrantResponse::default())
    }

    pub fn try_keep_alive_once(
        &self,
        _lease_id: i64,
        _timeout: u64,
    ) -> TsoResult<LeaseKeepAliveResponse> {
        Ok(LeaseKeepAliveResponse::default())
    }

    pub fn try_revoke(&self, _lease_id: i64, _timeout: u64) -> TsoResult<()> {
        Ok(())
    }
}

// watch api
impl EtcdShim {
    pub fn try_watch(
        &self,
        _key: &str,
        _options: Option<WatchOptions>,
        _timeout: u64,
    ) -> TsoResult<(Watcher, WatchStream)> {
        Ok((Watcher::default(), WatchStream::default()))
    }

    pub fn try_request_progress(&self, _watcher: &mut Watcher, _timeout: u64) -> TsoResult<()> {
        Ok(())
    }
}
