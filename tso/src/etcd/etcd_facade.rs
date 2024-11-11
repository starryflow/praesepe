use std::fmt::Debug;

use crate::{bootstrap::ExitSignal, cluster::ParticipantInfo, TsoResult};

use super::{
    etcd_client::EtcdClient, etcd_shim::EtcdShim, LeaseGrantResponse, LeaseKeepAliveResponse,
    WatchOptions, WatchStream, Watcher,
};

pub enum EtcdFacade {
    Shim(EtcdShim),
    Raw(EtcdClient),
}

impl Debug for EtcdFacade {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Shim(s) => s.fmt(f),
            Self::Raw(r) => r.fmt(f),
        }
    }
}

impl EtcdFacade {
    pub fn new(kind: TsoEtcdKind, url: &str, exit_signal: ExitSignal) -> Self {
        match kind {
            TsoEtcdKind::Shim => Self::Shim(EtcdShim),
            TsoEtcdKind::Raw => Self::Raw(EtcdClient::new(url, exit_signal)),
        }
    }

    pub fn is_healthy(&self) -> bool {
        match self {
            Self::Shim(s) => s.is_healthy(),
            Self::Raw(r) => r.is_healthy(),
        }
    }
}

// kv api
impl EtcdFacade {
    pub fn get_u64(&self, key: &str) -> TsoResult<Option<u64>> {
        match self {
            Self::Shim(s) => s.get_u64(key),
            Self::Raw(r) => r.get_u64(key),
        }
    }

    pub fn compare_and_set_u64(
        &self,
        key: &str,
        value: u64,
        expected_create_revision: i64,
    ) -> TsoResult<u64> {
        match self {
            Self::Shim(s) => s.compare_and_set_u64(key, value, expected_create_revision),
            Self::Raw(r) => r.compare_and_set_u64(key, value, expected_create_revision),
        }
    }

    pub fn compare_and_set_str(
        &self,
        key: &str,
        value: &str,
        expected_create_revision: i64,
        lease_id: i64,
    ) -> TsoResult<()> {
        match self {
            Self::Shim(s) => s.compare_and_set_str(key, value, expected_create_revision, lease_id),
            Self::Raw(r) => r.compare_and_set_str(key, value, expected_create_revision, lease_id),
        }
    }

    pub fn get_part_info_with_mod_rev(
        &self,
        key: &str,
    ) -> TsoResult<Option<(ParticipantInfo, i64)>> {
        match self {
            Self::Shim(s) => s.get_part_info_with_mod_rev(key),
            Self::Raw(r) => r.get_part_info_with_mod_rev(key),
        }
    }

    pub fn delete(&self, key: &str) -> TsoResult<()> {
        match self {
            Self::Shim(s) => s.delete(key),
            Self::Raw(r) => r.delete(key),
        }
    }
}

// lease api
impl EtcdFacade {
    pub fn try_grant(&self, ttl_sec: i64, timeout: u64) -> TsoResult<LeaseGrantResponse> {
        match self {
            Self::Shim(s) => s.try_grant(ttl_sec, timeout),
            Self::Raw(r) => r.try_grant(ttl_sec, timeout),
        }
    }

    pub fn try_revoke(&self, lease_id: i64, timeout: u64) -> TsoResult<()> {
        match self {
            Self::Shim(s) => s.try_revoke(lease_id, timeout),
            Self::Raw(r) => r.try_revoke(lease_id, timeout),
        }
    }

    pub fn try_keep_alive_once(
        &self,
        lease_id: i64,
        timeout: u64,
    ) -> TsoResult<LeaseKeepAliveResponse> {
        match self {
            Self::Shim(s) => s.try_keep_alive_once(lease_id, timeout),
            Self::Raw(r) => r.try_keep_alive_once(lease_id, timeout),
        }
    }
}

// watch api
impl EtcdFacade {
    pub fn try_watch(
        &self,
        key: &str,
        options: Option<WatchOptions>,
        timeout: u64,
    ) -> TsoResult<(Watcher, WatchStream)> {
        match self {
            Self::Shim(s) => s.try_watch(key, options, timeout),
            Self::Raw(r) => r.try_watch(key, options, timeout),
        }
    }

    pub fn try_request_progress(&self, watcher: &mut Watcher, timeout: u64) -> TsoResult<()> {
        match self {
            Self::Shim(s) => s.try_request_progress(watcher, timeout),
            Self::Raw(r) => r.try_request_progress(watcher, timeout),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsoEtcdKind {
    Shim,
    Raw,
}
