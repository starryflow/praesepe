pub enum LeaseGrantResponse {
    Shim(i64),
    Raw(etcd_client::LeaseGrantResponse),
}

impl From<etcd_client::LeaseGrantResponse> for LeaseGrantResponse {
    fn from(value: etcd_client::LeaseGrantResponse) -> Self {
        Self::Raw(value)
    }
}

impl LeaseGrantResponse {
    pub fn new_shim(ttl: i64) -> Self {
        Self::Shim(ttl)
    }

    pub fn id(&self) -> i64 {
        match self {
            Self::Shim(_) => 0,
            Self::Raw(v) => v.id(),
        }
    }

    pub fn ttl(&self) -> i64 {
        match self {
            Self::Shim(ttl) => *ttl,
            Self::Raw(v) => v.ttl(),
        }
    }
}
