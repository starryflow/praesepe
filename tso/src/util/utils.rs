use byteorder::{BigEndian, ByteOrder, LittleEndian};
use sha2::{Digest, Sha256};

use crate::{error::TsoError, TsoResult};

pub struct Utils;

impl Utils {
    /// generates a unique ID based on the given seed
    pub fn generate_unique_id(seed: &str) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(seed);
        let result = hasher.finalize();

        let mut buf = [0; 8];
        buf.copy_from_slice(&result[..8]);
        LittleEndian::read_u64(&buf)
    }

    pub fn bytes_to_u64(b: &[u8]) -> TsoResult<u64> {
        if b.len() != 8 {
            anyhow::bail!(TsoError::BytesToU64)
        } else {
            Ok(BigEndian::read_u64(b))
        }
    }

    pub fn u64_to_bytes(v: u64) -> [u8; 8] {
        let mut b = [0; 8];
        BigEndian::write_u64(&mut b, v);
        b
    }
}
