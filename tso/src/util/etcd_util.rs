use byteorder::{ByteOrder, LittleEndian};
use coarsetime::Clock;
use etcd_client::{Compare, Txn, TxnOp, TxnOpResponse};

use crate::{
    error::TsoError,
    util::{etcd_client::EtcdClient, utils::Utils},
    TsoResult,
};

pub struct EtcdUtil;

impl EtcdUtil {
    pub fn init_cluster_id(etcd_client: &EtcdClient, key: &str) -> TsoResult<u64> {
        // Get cluster key to parse the cluster ID
        let resp = etcd_client.get(key)?;

        // If no key exist, generate a random cluster ID
        if resp.count() == 0 {
            Self::init_or_get_cluster_id(etcd_client, key)
        } else {
            Utils::bytes_to_u64(resp.kvs()[0].value())
        }
    }

    fn init_or_get_cluster_id(etcd_client: &EtcdClient, key: &str) -> TsoResult<u64> {
        // Generate a random cluster ID
        let mut buf: [u8; 32] = [0u8; 32];
        getrandom::getrandom(&mut buf).map_err(|e| anyhow::anyhow!(e))?;

        let ts = Clock::now_since_epoch().as_secs();

        let cluster_id = (ts << 32) | (LittleEndian::read_u32(&buf) as u64);
        let value = Utils::u64_to_bytes(cluster_id);

        // Multiple servers may try to init the cluster ID at the same time.
        // Only one server can commit this transaction, then other servers
        // can get the committed cluster ID
        let txn = Txn::new()
            .when(vec![Compare::create_revision(
                key,
                etcd_client::CompareOp::Equal,
                0,
            )])
            .and_then(vec![TxnOp::put(key, value, None)])
            .or_else(vec![TxnOp::get(key, None)]);
        let resp = etcd_client.do_in_txn(txn)?;

        // Txn commits ok, return the generated cluster ID
        if resp.succeeded() {
            Ok(cluster_id)
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
}
