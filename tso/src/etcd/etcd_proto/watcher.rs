use crate::TsoResult;

pub enum Watcher {
    Shim,
    Raw(etcd_client::Watcher),
}

impl Default for Watcher {
    fn default() -> Self {
        Self::Shim
    }
}

impl From<etcd_client::Watcher> for Watcher {
    fn from(value: etcd_client::Watcher) -> Self {
        Self::Raw(value)
    }
}

impl Watcher {
    pub async fn request_progress(&mut self) -> TsoResult<()> {
        match self {
            Self::Shim => unreachable!(),
            Self::Raw(r) => r.request_progress().await.map_err(|e| anyhow::anyhow!(e)),
        }
    }

    pub async fn cancel(&mut self) -> TsoResult<()> {
        match self {
            Self::Shim => unreachable!(),
            Self::Raw(r) => r.cancel().await.map_err(|e| anyhow::anyhow!(e)),
        }
    }
}

pub enum WatchOptions {
    Shim,
    Raw(etcd_client::WatchOptions),
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self::Shim
    }
}

impl Into<etcd_client::WatchOptions> for WatchOptions {
    fn into(self) -> etcd_client::WatchOptions {
        match self {
            Self::Shim => unreachable!(),
            Self::Raw(r) => r,
        }
    }
}

impl From<etcd_client::WatchOptions> for WatchOptions {
    fn from(value: etcd_client::WatchOptions) -> Self {
        Self::Raw(value)
    }
}
