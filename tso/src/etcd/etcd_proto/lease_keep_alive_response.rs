pub enum LeaseKeepAliveResponse {
    Shim(i64),
    Raw(etcd_client::LeaseKeepAliveResponse),
}

impl From<etcd_client::LeaseKeepAliveResponse> for LeaseKeepAliveResponse {
    fn from(value: etcd_client::LeaseKeepAliveResponse) -> Self {
        Self::Raw(value)
    }
}

impl LeaseKeepAliveResponse {
    pub fn new_shim(ttl: i64) -> Self {
        Self::Shim(ttl)
    }

    pub fn ttl(&self) -> i64 {
        match self {
            Self::Shim(ttl) => *ttl,
            Self::Raw(v) => v.ttl(),
        }
    }
}
