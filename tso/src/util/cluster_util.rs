use byteorder::{ByteOrder, LittleEndian};
use coarsetime::Clock;

use crate::{
    util::{constant::Constant, etcd_client::EtcdClient},
    TsoResult,
};

pub struct ClusterUtil;

impl ClusterUtil {
    pub fn root_path(cluster_id: u64) -> String {
        format!("{}/{}", Constant::ROOT_PATH, cluster_id)
    }

    pub fn init_cluster_id(etcd_client: &EtcdClient, key: &str) -> TsoResult<u64> {
        // Get cluster key to parse the cluster ID
        let resp = etcd_client.get_u64(key)?;

        // If no key exist, generate a random cluster ID
        if let Some(value) = resp {
            Ok(value)
        } else {
            Self::init_or_get_cluster_id(etcd_client, key)
        }
    }

    fn init_or_get_cluster_id(etcd_client: &EtcdClient, key: &str) -> TsoResult<u64> {
        // Generate a random cluster ID
        let mut buf: [u8; 32] = [0u8; 32];
        getrandom::getrandom(&mut buf).map_err(|e| anyhow::anyhow!(e))?;

        let ts = Clock::now_since_epoch().as_secs();

        let cluster_id = (ts << 32) | (LittleEndian::read_u32(&buf) as u64);

        // Multiple servers may try to init the cluster ID at the same time.
        // Only one server can commit this transaction, then other servers
        // can get the committed cluster ID
        etcd_client.compare_and_set_u64(key, cluster_id, 0)
    }
}
