use etcd_client::EventType;

use crate::TsoResult;

pub enum WatchStream {
    Shim,
    Raw(etcd_client::WatchStream),
}

impl Default for WatchStream {
    fn default() -> Self {
        Self::Shim
    }
}

impl From<etcd_client::WatchStream> for WatchStream {
    fn from(value: etcd_client::WatchStream) -> Self {
        Self::Raw(value)
    }
}

impl WatchStream {
    pub async fn message(&mut self) -> TsoResult<Option<WatchResponse>> {
        match self {
            Self::Shim => Ok(Some(WatchResponse::default())),
            Self::Raw(r) => r
                .message()
                .await
                .map(|x| x.map(|x| x.into()))
                .map_err(|e| anyhow::anyhow!(e)),
        }
    }
}

pub enum WatchResponse {
    Shim,
    Raw(etcd_client::WatchResponse),
}

impl Default for WatchResponse {
    fn default() -> Self {
        Self::Shim
    }
}

impl From<etcd_client::WatchResponse> for WatchResponse {
    fn from(value: etcd_client::WatchResponse) -> Self {
        Self::Raw(value)
    }
}

impl WatchResponse {
    pub fn compact_revision(&self) -> i64 {
        match self {
            Self::Shim => 0,
            Self::Raw(r) => r.compact_revision(),
        }
    }

    pub fn events(&self) -> Vec<Event> {
        match self {
            Self::Shim => vec![],
            Self::Raw(r) => r
                .events()
                .iter()
                .map(|x| Event::from_raw_event(x))
                .collect::<Vec<_>>(),
        }
    }

    pub fn header(&self) -> Option<&etcd_client::ResponseHeader> {
        match self {
            Self::Shim => unreachable!(),
            Self::Raw(r) => r.header(),
        }
    }
}

pub enum Event<'a> {
    Shim,
    Raw(&'a etcd_client::Event),
}

impl<'a> Default for Event<'a> {
    fn default() -> Self {
        Self::Shim
    }
}

impl<'a> Event<'a> {
    fn from_raw_event(e: &'a etcd_client::Event) -> Self {
        Self::Raw(e)
    }

    pub fn event_type(&self) -> EventType {
        match self {
            Self::Shim => unreachable!(),
            Self::Raw(r) => r.event_type(),
        }
    }

    pub fn kv(&self) -> Option<&etcd_client::KeyValue> {
        match self {
            Self::Shim => unreachable!(),
            Self::Raw(r) => r.kv(),
        }
    }
}
