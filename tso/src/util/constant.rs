pub struct Constant;

impl Constant {
    pub const ROOT_PATH: &'static str = "/tso";
    pub const CLUSTER_ID_PATH: &'static str = "/tso/cluster_id";

    /// minimus loop interval
    pub const LOOP_MIN_INTERVAL_MILLIS: u64 = 100;

    pub const LEADER_CHECK_SKIP_MILLIS: u64 = 200;

    /// 1s for the threshold for normal request, for those longer then 1s, they are considered as slow requests
    pub const SLOW_REQUEST_TIME_MILLIS: u64 = 1_000;
    pub const DEFAULT_REQUEST_TIMEOUT_MILLIS: u64 = 10_000;

    /// Lease
    pub const REVOKE_LEASE_TIMEOUT_MILLIS: u64 = 1_000;

    /// Watch
    pub const WATCH_LOOP_UNHEALTHY_TIMEOUT_MILLIS: u64 = 60_000;
    pub const REQUEST_PROGRESS_INTERVAL_MILLIS: u64 = 1_000;
}
