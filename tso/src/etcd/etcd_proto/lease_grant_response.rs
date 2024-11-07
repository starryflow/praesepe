pub enum LeaseGrantResponse {
    Shim,
    Raw(etcd_client::LeaseGrantResponse),
}

impl Default for LeaseGrantResponse {
    fn default() -> Self {
        LeaseGrantResponse::Shim
    }
}

impl From<etcd_client::LeaseGrantResponse> for LeaseGrantResponse {
    fn from(value: etcd_client::LeaseGrantResponse) -> Self {
        Self::Raw(value)
    }
}

impl LeaseGrantResponse {
    pub fn id(&self) -> i64 {
        match self {
            Self::Shim => 0,
            Self::Raw(v) => v.id(),
        }
    }

    pub fn ttl(&self) -> i64 {
        match self {
            Self::Shim => 0,
            Self::Raw(v) => v.ttl(),
        }
    }
}
