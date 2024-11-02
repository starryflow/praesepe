use prometheus::{Counter, Gauge, Histogram, HistogramOpts};

pub struct TsoMetric {
    // timestampOracle event counter
    pub sync_event: Counter,
    pub skip_sync_event: Counter,
    pub sync_ok_event: Counter,
    pub err_save_sync_ts_event: Counter,
    pub err_lease_reset_ts_event: Counter,
    pub err_reset_small_physical_ts_event: Counter,
    pub err_reset_small_logical_ts_event: Counter,
    pub err_reset_large_ts_event: Counter,
    pub err_save_reset_ts_event: Counter,
    pub reset_tso_ok_event: Counter,
    pub save_event: Counter,
    pub slow_save_event: Counter,
    pub system_time_slow_event: Counter,
    pub skip_save_event: Counter,
    pub err_save_update_ts_event: Counter,
    pub not_leader_anymore_event: Counter,
    pub logical_overflow_event: Counter,
    pub exceeded_max_retry_event: Counter,
    // timestampOracle operation duration
    pub sync_save_duration: Histogram,
    pub reset_save_duration: Histogram,
    pub update_save_duration: Histogram,
    // allocator event counter
    pub not_leader_event: Counter,
    // pub global_tso_sync_event: Counter,
    // pub global_tso_estimate_event: Counter,
    // pub global_tso_persist_event: Counter,
    // pub precheck_logical_overflow_event: Counter,
    // pub err_global_tso_persist_event: Counter,
    // // others
    pub tso_physical_gauge: Gauge,
    pub tso_physical_gap_gauge: Gauge,
    // pub global_tso_sync_rtt_gauge: Gauge,
}

impl Default for TsoMetric {
    fn default() -> Self {
        Self {
            // synchronize
            sync_event: Counter::new("sync_event", "TSO synchronize times").unwrap(),
            skip_sync_event: Counter::new("skip_sync_event", "TSO synchronize skip times").unwrap(),
            sync_ok_event: Counter::new("sync_ok_event", "TSO synchronize ok times").unwrap(),
            err_save_sync_ts_event: Counter::new(
                "err_save_sync_ts_event",
                "TSO synchronize but save error times",
            )
            .unwrap(),
            // reset
            err_lease_reset_ts_event: Counter::new(
                "err_lease_reset_ts_event",
                "TSO reset but lease expired times",
            )
            .unwrap(),
            err_reset_small_physical_ts_event: Counter::new(
                "err_reset_small_physical_ts_event",
                "TSO reset but `less/before than prev` times",
            )
            .unwrap(),
            err_reset_small_logical_ts_event: Counter::new(
                "err_reset_small_logical_ts_event",
                "TSO reset but `less/before/equal than prev` times",
            )
            .unwrap(),
            err_reset_large_ts_event: Counter::new(
                "err_reset_large_ts_event",
                "TSO reset but `too greater than prev` times",
            )
            .unwrap(),
            err_save_reset_ts_event: Counter::new(
                "err_save_reset_ts_event",
                "TSO reset but save error times",
            )
            .unwrap(),
            reset_tso_ok_event: Counter::new("reset_tso_ok_event", "TSO reset ok times").unwrap(),
            // update
            save_event: Counter::new("save_event", "TSO update save times").unwrap(),
            slow_save_event: Counter::new("slow_save_event", "TSO update slow times").unwrap(),
            system_time_slow_event: Counter::new(
                "system_time_slow_event",
                "TSO update as time-fallback times",
            )
            .unwrap(),
            skip_save_event: Counter::new("skip_save_event", "TSO update as save skip times")
                .unwrap(),
            err_save_update_ts_event: Counter::new(
                "err_save_update_ts_event",
                "TSO update but save error times",
            )
            .unwrap(),
            // get
            not_leader_anymore_event: Counter::new(
                "not_leader_anymore_event",
                "TSO get but not as leader times",
            )
            .unwrap(),
            logical_overflow_event: Counter::new(
                "logical_overflow_event",
                "TSO get but logical overflow times",
            )
            .unwrap(),
            exceeded_max_retry_event: Counter::new(
                "exceeded_max_retry_event",
                "TSO get but `exceed max retry` times",
            )
            .unwrap(),
            // tso operation duration
            sync_save_duration: Histogram::with_opts(HistogramOpts::new(
                "sync_save_duration",
                "TSO synchronize duration",
            ))
            .unwrap(),
            reset_save_duration: Histogram::with_opts(HistogramOpts::new(
                "reset_save_duration",
                "TSO reset duration",
            ))
            .unwrap(),
            update_save_duration: Histogram::with_opts(HistogramOpts::new(
                "update_save_duration",
                "TSO update duration",
            ))
            .unwrap(),
            // allocator event counter
            not_leader_event: Counter::new("not_leader_event", "TSO synchronize times").unwrap(),
            // global_tso_sync_event: Counter::new("sync_event", "TSO synchronize times").unwrap(),
            // global_tso_estimate_event: Counter::new("sync_event", "TSO synchronize times").unwrap(),
            // global_tso_persist_event: Counter::new("sync_event", "TSO synchronize times").unwrap(),
            // precheck_logical_overflow_event: Counter::new("sync_event", "TSO synchronize times")
            //     .unwrap(),
            // err_global_tso_persist_event: Counter::new("sync_event", "TSO synchronize times")
            //     .unwrap(),
            //
            tso_physical_gauge: Gauge::new("tso_physical_gauge", "TSO update prev physical value")
                .unwrap(),
            tso_physical_gap_gauge: Gauge::new(
                "tso_physical_gap_gauge",
                "TSO update physical gap value",
            )
            .unwrap(),
            // global_tso_sync_rtt_gauge: Counter::new("sync_event", "TSO synchronize times").unwrap(),
        }
    }
}
