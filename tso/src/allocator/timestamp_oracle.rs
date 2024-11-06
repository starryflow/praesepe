use std::{sync::atomic::AtomicU64, time::Duration};

use coarsetime::{Clock, Instant};
use parking_lot::RwLock;

use crate::{
    allocator::timestamp::UnixTimeStamp, cluster::TsoLeadership, config::Config, metric::TsoMetric,
    store::TsoStore, util::constant::Constant, TsoResult,
};

use super::timestamp::Timestamp;

/// UpdateTimestampGuard is the min timestamp interval.
/// (Millisecond)
const UPDATE_TIMESTAMP_GUARD: i64 = 1;
/// JET_LAG_WARNING_THRESHOLD is the warning threshold of jetLag in `TSO::update_timestamp`.
/// In case of small `update_physical_interval`, the `3 * update_physical_interval` would also is small, and trigger unnecessary warnings about clock offset.
/// It's an empirical value.
/// (Millisecond)
const JET_LAG_WARNING_THRESHOLD: i64 = 150;
/// If logical >= Timestamp::MAX_LOGICAL, retry get
const GET_TS_MAX_RETRY_COUNT: usize = 10;

/// TsoObject is used to store the current TSO in memory
#[derive(Default)]
struct TsoObject {
    physical_millis: UnixTimeStamp,
    logical: u32,
    update_time_millis: UnixTimeStamp,
}

/// TimestampOracle is used to maintain the logic of TSO.
pub struct TimestampOracle {
    /// tso service node name
    node_name: String,
    /// When tso_path is empty, it means that it is a global timestampOracle
    tso_path: String,
    /// Used suffix to distinguish different data-center
    suffix: u32,
    /// Config
    save_interval: u64,
    update_physical_interval: u64,
    max_reset_ts_gap: u64,
    /// memory stored
    tso_obj: RwLock<TsoObject>,
    /// last timestamp window stored
    last_saved_time: AtomicU64,
    /// observability
    pub(crate) metric: TsoMetric,
}

impl TimestampOracle {
    pub fn new(config: &Config) -> Self {
        Self {
            node_name: config.name.to_owned(),
            tso_path: "".into(),
            suffix: 0,
            save_interval: config.save_interval_millis,
            update_physical_interval: config.update_physical_interval_millis,
            max_reset_ts_gap: config.max_reset_ts_gap_millis,
            tso_obj: RwLock::new(TsoObject::default()),
            last_saved_time: 0.into(),
            metric: TsoMetric::default(),
        }
    }

    fn set_tso_obj_physical(&self, next_millis: u64, force: bool) {
        let mut tso_obj = self.tso_obj.upgradable_read();
        // Do not update the zero physical time if the `force` flag is false
        if tso_obj.physical_millis == 0 && !force {
            return;
        }
        // make sure the ts won't fall back
        if next_millis > tso_obj.physical_millis {
            tso_obj.with_upgraded(|x| {
                x.physical_millis = next_millis;
                x.logical = 0;
                x.update_time_millis = Clock::now_since_epoch().as_millis();
            })
        }
    }

    pub fn get_tso_obj(&self) -> (u64, u32) {
        let tso_obj = self.tso_obj.read();

        if tso_obj.physical_millis == 0 {
            (0, 0)
        } else {
            (tso_obj.physical_millis, tso_obj.logical)
        }
    }

    pub fn get_last_saved_time(&self) -> UnixTimeStamp {
        self.last_saved_time
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_last_saved_time(&self, ts: UnixTimeStamp) {
        self.last_saved_time
            .store(ts, std::sync::atomic::Ordering::Relaxed);
    }

    /// add the TSO's logical part with the given count and returns the new TSO result (used in estimate_max_ts for cluster TSO)
    #[fastrace::trace]
    fn generate(&self, count: u32, suffix_bits: u32) -> (u64, u32, u64) {
        let mut tso_obj = self.tso_obj.write();

        if tso_obj.physical_millis == 0 {
            return (0, 0, 0);
        }

        let physical_millis = tso_obj.physical_millis;
        tso_obj.logical += count;
        let mut logical = tso_obj.logical;
        if suffix_bits > 0 {
            logical = self.calibrate_logical(logical, suffix_bits)
        }

        // Return the last update time
        let last_update_time = tso_obj.update_time_millis;
        tso_obj.update_time_millis = Clock::now_since_epoch().as_millis();
        (physical_millis, logical, last_update_time)
    }

    /// Because the Local TSO in each Local TSO Allocator is independent, so they are possible to be the same at sometimes, to avoid this case, we need to use the logical part of the Local TSO to do some differentiating work.
    /// For example, we have three DCs: dc-1, dc-2 and dc-3. The bits of suffix is defined by the const suffixBits. Then, for dc-2, the suffix may be 1 because it's persisted with the value of 1. Once we get a normal TSO like this (18 bits): xxxxxxxxxxxxxxxxxx. We will make the TSO's low bits of logical part from each DC looks like:
    ///
    ///	global: xxxxxxxxxx00000000
    ///	  dc-1: xxxxxxxxxx00000001
    ///	  dc-2: xxxxxxxxxx00000010
    ///	  dc-3: xxxxxxxxxx00000011
    fn calibrate_logical(&self, raw_logical: u32, suffix_bits: u32) -> u32 {
        raw_logical << (suffix_bits + self.suffix)
    }
}

impl TimestampOracle {
    /// Synchronous Timestamp, if the current system time is earlier than the persistence time, uses the persistence time as the physical time, and vice versa, saves the system time to store
    ///
    /// synchronize the timestamp
    #[fastrace::trace]
    pub fn sync_timestamp(&self, store: &dyn TsoStore) -> TsoResult<()> {
        log::info!("start to sync timestamp");
        self.metric.sync_event.inc();

        let last = store.load_timestamp(self.tso_path.as_str())?;
        let last_saved_time = self.get_last_saved_time();

        // We could skip the synchronization if the following conditions are met:
        //   1. The timestamp in memory has been initialized.
        //   2. The last saved timestamp in store is not zero.
        //   3. The last saved timestamp in memory is not zero.
        //   4. The last saved timestamp in store is equal to the last saved timestamp in memory.
        // 1 is to ensure the timestamp in memory could always be initialized. 2-4 are to ensure
        // the synchronization could be skipped safely.
        if self.is_initialized() && last != 0 && last_saved_time != 0 && last == last_saved_time {
            log::info!(
                "skip sync timestamp, last: {}, last-saved: {}",
                last,
                last_saved_time
            );
            self.metric.skip_sync_event.inc();
            return Ok(());
        }

        let mut next = Clock::now_since_epoch().as_millis();

        // If the current system time minus the saved store timestamp is less than `UpdateTimestampGuard`, the timestamp allocation will start from the saved store timestamp temporarily.
        if next < last || next - last < UPDATE_TIMESTAMP_GUARD as u64 {
            log::warn!(
                "system time may be incorrect, last: {}, last-saved: {}, next: {}",
                last,
                last_saved_time,
                next
            );
            next = last + UPDATE_TIMESTAMP_GUARD as u64;
        }
        let save = next + self.save_interval;
        let start = Instant::now();
        if let Err(e) = store.save_timestamp(&self.tso_path, save, &self.node_name) {
            self.metric.err_save_sync_ts_event.inc();
            anyhow::bail!(e);
        }
        self.set_last_saved_time(save);
        self.metric
            .sync_save_duration
            .observe(start.elapsed().as_secs() as f64);

        self.metric.sync_ok_event.inc();
        log::info!(
            "sync and save timestamp, last: {}, last-saved: {}, next:{}",
            last,
            last_saved_time,
            next
        );

        self.set_tso_obj_physical(next, true);
        Ok(())
    }

    /// Check whether the timestampOracle is initialized.
    /// There are two situations we have an uninitialized TSO:
    /// 1. When the sync_timestamp has not been called yet.
    /// 2. When the reset_user_timestamp has been called already.
    fn is_initialized(&self) -> bool {
        self.tso_obj.read().physical_millis != 0
    }

    /// Use externally generated TSOs to update to local, for example, compare multiple local TSOs, take the largest as the cluster TSOs and update to individual local TSOs
    ///
    /// update the TSO in memory with specified TSO by an atomically way.
    /// When ignore_smaller is true, will ignore the smaller tso resetting error and do nothing.
    /// It's used to write MaxTS during the Global TSO synchronization without failing the writing as much as possible.
    /// cannot set timestamp to one which >= current + maxResetTSGap
    #[fastrace::trace]
    pub fn reset_user_timestamp(
        &self,
        store: &dyn TsoStore,
        leadership: &TsoLeadership,
        tso: u64,
        ignore_smaller: bool,
        skip_upper_bound_check: bool,
    ) -> TsoResult<()> {
        if !leadership.check() {
            self.metric.err_lease_reset_ts_event.inc();
            anyhow::bail!("lease expired");
        }

        let mut tso_obj = self.tso_obj.upgradable_read();

        let next_ts = Timestamp::from_u64(tso);
        let logical_diff = (next_ts.logical as i64) - (tso_obj.logical as i64);
        let physical_diff = (next_ts.physical_millis as i64) - (tso_obj.physical_millis as i64);

        // do not update if next physical time is less/before than prev
        if physical_diff < 0 {
            self.metric.err_reset_small_physical_ts_event.inc();
            if ignore_smaller {
                return Ok(());
            } else {
                anyhow::bail!("the specified ts is smaller than now");
            }
        }

        // do not update if next logical time is less/before/equal than prev
        if physical_diff == 0 && logical_diff <= 0 {
            self.metric.err_reset_small_logical_ts_event.inc();
            if ignore_smaller {
                return Ok(());
            } else {
                anyhow::bail!("the specified counter is smaller than now");
            }
        }

        // do not update if physical time is too greater than prev
        if !skip_upper_bound_check && physical_diff >= self.max_reset_ts_gap as i64 {
            self.metric.err_reset_large_ts_event.inc();
            anyhow::bail!("the specified ts is too larger than now");
        }

        // save into store only if nextPhysical is close to lastSavedTime
        if (self.get_last_saved_time() as i64) - (next_ts.physical_millis as i64)
            <= UPDATE_TIMESTAMP_GUARD
        {
            let save = next_ts.physical_millis + self.save_interval;
            let start = Instant::now();
            if let Err(e) = store.save_timestamp(&self.tso_path, save, &self.node_name) {
                self.metric.err_save_reset_ts_event.inc();
                anyhow::bail!(e);
            }
            self.set_last_saved_time(save);
            self.metric
                .reset_save_duration
                .observe(start.elapsed().as_secs() as f64);
        }

        // save into memory only if nextPhysical or nextLogical is greater
        tso_obj.with_upgraded(|x| {
            x.physical_millis = next_ts.physical_millis;
            x.logical = next_ts.logical;
            x.update_time_millis = Clock::now_since_epoch().as_millis();
        });
        self.metric.reset_tso_ok_event.inc();
        Ok(())
    }

    /// Timed calls to update_timestamp drive TSO increments, When the difference between the system time and the latest persistence time exceeds the threshold, the current system time is saved to store
    ///
    /// This function will do two things:
    ///  1. When the logical time is going to be used up, increase the current physical time.
    ///  2. When the time window is not big enough, which means the saved time minus the next physical time will be less than or equal to `UpdateTimestampGuard`, then the time window needs to be updated and we also need to save the next physical time plus `TSOSaveInterval`.
    ///
    /// Here is some constraints that this function must satisfy:
    /// 1. The saved time is monotonically increasing.
    /// 2. The physical time is monotonically increasing.
    /// 3. The physical time is always less than the saved timestamp.
    ///
    /// NOTICE: this function should be called after the TSO in memory has been initialized
    /// and should not be called when the TSO in memory has been reset anymore.
    #[fastrace::trace]
    pub fn update_timestamp(&self, store: &dyn TsoStore) -> TsoResult<()> {
        if !self.is_initialized() {
            anyhow::bail!("timestamp in memory has not been initialized");
        }

        let (prev_physical, prev_logical) = self.get_tso_obj();
        self.metric.tso_physical_gauge.set(prev_physical as f64);
        self.metric
            .tso_physical_gap_gauge
            .set((Clock::now_since_epoch().as_millis() - prev_physical) as f64);

        let now = Clock::now_since_epoch().as_millis();

        self.metric.save_event.inc();

        // warn if too long time no save
        let jet_lag = (now as i64) - (prev_physical as i64);
        if jet_lag > 3 * self.update_physical_interval as i64 && jet_lag > JET_LAG_WARNING_THRESHOLD
        {
            log::warn!(
                "clock offset, jet-lag {:?}, prev-physical {:?}, now {:?}, update-physical-interval {:?}",
                jet_lag,
                prev_physical,
                now,
                self.update_physical_interval
            );
            self.metric.slow_save_event.inc();
        }

        // system time fallback
        if jet_lag < 0 {
            self.metric.system_time_slow_event.inc();
        }

        // If the system time is greater, it will be synchronized with the system time.
        let next = if jet_lag > UPDATE_TIMESTAMP_GUARD {
            now
        } else if prev_logical > Timestamp::MAX_LOGICAL / 2 {
            // The reason choosing maxLogical/2 here is that it's big enough for common cases.
            // Because there is enough timestamp can be allocated before next update.
            log::warn!(
                "the logical time maybe not enough, prev-logical {}",
                prev_logical
            );
            prev_physical + 1
        } else {
            // It will still use the previous physical time to alloc the timestamp.
            self.metric.skip_save_event.inc();
            return Ok(());
        };

        // It is not safe to increase the physical time to `next`.
        // The time window needs to be updated and saved to store.
        if (self.get_last_saved_time() as i64) - (next as i64) <= UPDATE_TIMESTAMP_GUARD {
            let save = next + self.save_interval;
            let start = Instant::now();
            if let Err(e) = store.save_timestamp(&self.tso_path, save, &self.node_name) {
                log::warn!(
                    "save timestamp failed, tso_path: {}, error: {}",
                    self.tso_path,
                    e
                );
                self.metric.err_save_update_ts_event.inc();
                anyhow::bail!(e);
            }

            self.set_last_saved_time(save);
            self.metric
                .update_save_duration
                .observe(start.elapsed().as_secs() as f64);
        }

        // save into memory
        self.set_tso_obj_physical(next, false);

        Ok(())
    }

    /// Get a Timestamp and when the logical time runs out, the blocking waits for update_timestamp to advance the TSO increment
    ///
    /// Get a timestamp
    #[fastrace::trace]
    pub fn get_timestamp(
        &self,
        leadership: &TsoLeadership,
        count: u32,
        suffix_bits: u32,
    ) -> TsoResult<Timestamp> {
        if count == 0 {
            anyhow::bail!("tso count should be positive");
        }

        for i in 0..GET_TS_MAX_RETRY_COUNT {
            let (current_physical, _) = self.get_tso_obj();

            if current_physical == 0 {
                // If it's leader, maybe SyncTimestamp hasn't completed yet
                if leadership.check() {
                    std::thread::sleep(Duration::from_millis(Constant::LOOP_MIN_INTERVAL_MILLIS));
                    continue;
                }
                self.metric.not_leader_anymore_event.inc();
                anyhow::bail!("timestamp in memory isn't initialized");
            }

            // get a new TSO result with the given count
            let (physical_millis, logical, _) = self.generate(count, suffix_bits);
            if physical_millis == 0 {
                anyhow::bail!("timestamp in memory has been reset");
            }
            if logical >= Timestamp::MAX_LOGICAL {
                log::warn!("logical part outside of max logical interval, please check ntp time, or adjust config item `tso-update-physical-interval`, retry-count {}", i);
                self.metric.logical_overflow_event.inc();
                std::thread::sleep(Duration::from_millis(self.update_physical_interval));
                continue;
            }

            // In case lease expired after the first check
            if !leadership.check() {
                anyhow::bail!("requested NotLeaderErr anymore");
            }

            return Ok(Timestamp {
                physical_millis,
                logical,
                suffix_bits,
            });
        }
        self.metric.exceeded_max_retry_event.inc();
        anyhow::bail!("generate tso maximum number of retries exceeded");
    }

    /// When the system stops running, the TSO is revalued
    ///
    /// Reset the timestamp in memory
    pub fn reset_timestamp(&self) {
        log::info!("reset the timestamp in memory");

        let mut tso_obj = self.tso_obj.write();
        tso_obj.physical_millis = 0;
        tso_obj.logical = 0;
        tso_obj.update_time_millis = 0;

        self.set_last_saved_time(0);
    }
}
