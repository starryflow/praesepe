use std::{cmp::Ordering, fmt::Display};

use coarsetime::Duration;

// Millisecond
pub type UnixTimeStamp = u64;

///
pub struct Timestamp {
    pub physical_millis: u64, // 42bit, max 140 years
    pub logical: u32,         // 18bit, max 262k
    // Number of suffix bits used for global distinction,
    // Caller will use this to compute a TSO's logical part.
    pub suffix_bits: u32, // 4bit, reserved
}

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp {
            physical_millis: 0,
            logical: 0,
            suffix_bits: 0,
        }
    }
}

impl PartialEq for Timestamp {
    fn eq(&self, other: &Self) -> bool {
        self.physical_millis == other.physical_millis && self.logical == other.logical
    }
}
impl Eq for Timestamp {}
impl PartialOrd for Timestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(if self.eq(other) {
            Ordering::Equal
        } else if self.physical_millis > other.physical_millis
            || (self.physical_millis == other.physical_millis && self.logical > other.logical)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        })
    }
}
impl Ord for Timestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Timestamp as {:?}|{}|{}, binary: {}",
            std::time::SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_millis(self.physical_millis).into())
                .unwrap(),
            self.logical,
            self.suffix_bits,
            self.as_u64()
        ))
    }
}

impl Timestamp {
    /// maxLogical is the max upper limit for logical time.
    /// When a TSO's logical time reaches this limit, the physical time will be forced to increase.
    pub const MAX_LOGICAL_BITS: u32 = 18;
    pub const MAX_LOGICAL: u32 = 1 << Self::MAX_LOGICAL_BITS;
    /// MaxSuffixBits indicates the max number of suffix bits.
    pub const MAX_SUFFIX_BITS: u32 = 4;
}

impl Timestamp {
    pub fn new(physical: u64, logical: u32) -> Self {
        Self {
            physical_millis: physical,
            logical,
            suffix_bits: 0,
        }
    }

    pub fn as_u64(&self) -> u64 {
        let physical_42 = self.physical_millis << (Self::MAX_LOGICAL_BITS + Self::MAX_SUFFIX_BITS);
        let logical = (self.logical & 0x0003_FFFF) << Self::MAX_SUFFIX_BITS;
        let reserved = self.suffix_bits & 0x0000_000F;

        physical_42 | logical as u64 | reserved as u64
    }

    pub fn from_u64(from: u64) -> Self {
        let physical_millis = from >> (Self::MAX_LOGICAL_BITS + Self::MAX_SUFFIX_BITS);
        let logical = ((from >> Self::MAX_SUFFIX_BITS) & 0x0003_FFFF) as u32;
        let reserved = (from & 0x0000_000F) as u32;

        Self {
            physical_millis,
            logical,
            suffix_bits: reserved,
        }
    }
}
