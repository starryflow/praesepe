pub enum LeaseKeepAliveResponse {
    Shim,
    Raw(etcd_client::LeaseKeepAliveResponse),
}

impl Default for LeaseKeepAliveResponse {
    fn default() -> Self {
        LeaseKeepAliveResponse::Shim
    }
}

impl From<etcd_client::LeaseKeepAliveResponse> for LeaseKeepAliveResponse {
    fn from(value: etcd_client::LeaseKeepAliveResponse) -> Self {
        Self::Raw(value)
    }
}

impl LeaseKeepAliveResponse {
    pub fn ttl(&self) -> i64 {
        match self {
            Self::Shim => 0,
            Self::Raw(v) => v.ttl(),
        }
    }
}
