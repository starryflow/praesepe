use std::{sync::Arc, thread, time::Duration};

use coarsetime::Clock;
use prometheus::Gauge;

use crate::{
    bootstrap::ExitSignal,
    cluster::{Participant, TsoLeadership},
    config::Config,
    error::TsoError,
    store::TsoStore,
    util::constant::Constant,
    Timestamp, TsoResult,
};

use super::timestamp_oracle::TimestampOracle;

/// Allocator is a Timestamp Oracle allocator
pub struct GlobalTsoAllocator {
    // for election use
    member: Participant,
    timestamp_oracle: Arc<TimestampOracle>,
    /// pre-initialized metrics
    tso_allocator_role_gauge: Gauge,
}

impl GlobalTsoAllocator {
    pub fn new(config: &Config, member: Participant) -> Self {
        Self {
            timestamp_oracle: Arc::from(TimestampOracle::new(config)),
            member,
            tso_allocator_role_gauge: Gauge::new(
                "tso_allocator_role_gauge",
                "TSO on serving(`1`) or not(`0`)",
            )
            .expect("Metric failed"),
        }
    }

    /// primary_election_loop is used to maintain the TSO primary election and TSO's running allocator.
    pub fn primary_election_loop(
        &mut self,
        store: &dyn TsoStore,
        config: &Config,
        mut exit_signal: ExitSignal,
    ) {
        loop {
            if exit_signal.try_exit() {
                break;
            }

            let (primary, check_again) = self.member.check_leader();
            if check_again {
                thread::yield_now();
                continue;
            }
            if let Some((primary, revision)) = primary {
                // as follower, need watch (will block) ...
                log::info!("start to watch the primary: {:?}", primary);
                // Watch will keep looping and never return unless the primary has changed
                self.member
                    .watch_leader(primary, revision, exit_signal.clone());
                log::info!("the tso primary has changed, try to re-campaign a primary");
            }

            self.campaign_leader(store, config, exit_signal.clone());
        }

        log::info!("exit the global tso primary election loop");
    }

    fn campaign_leader(
        &mut self,
        store: &dyn TsoStore,
        config: &Config,
        mut exit_signal: ExitSignal,
    ) {
        log::info!(
            "start to campaign the primary, campaign-tso-primary-name: {}",
            self.member.get_name()
        );

        if let Err(e) = self.member.campaign_leader(config.leader_lease_millis) {
            if let Some(tso_e) = e.downcast_ref::<TsoError>() {
                match tso_e {
                    TsoError::EtcdTxnConflict => {
                        log::info!("campaign tso primary meets error due to txn conflict, another tso server may campaign successfully, campaign-tso-primary-name: {}", self.member.get_name());
                    }
                    TsoError::CheckCampaign => {
                        log::info!("campaign tso primary meets error due to pre-check campaign failed, the tso keyspace group may be in split, campaign-tso-primary-name: {}", self.member.get_name());
                    }
                    ee @ _ => {
                        log::error!("campaign tso primary meets error due to etcd error, campaign-tso-primary-name: {}, cause: {}", self.member.get_name(), ee);
                    }
                }
            } else {
                log::error!("campaign tso primary meets error due to etcd error, campaign-tso-primary-name: {}, cause: {}", self.member.get_name(), e);
            }
            return;
        }

        // Start keep-alive the leadership and enable TSO service.
        // TSO service is strictly enabled/disabled by the leader lease for 2 reasons:
        //   1. lease based approach is not affected by thread pause, slow runtime schedule, etc.
        //   2. load region could be slow. Based on lease we can recover TSO service faster.
        struct ResetLeaderOnce<'a>(&'a GlobalTsoAllocator);
        impl<'a> Drop for ResetLeaderOnce<'a> {
            fn drop(&mut self) {
                self.0.member.reset_leader()
            }
        }
        let _reset_leader_once = ResetLeaderOnce(self);

        // maintain the leadership, after this, TSO can be service
        self.member.keep_leader(exit_signal.clone());
        log::info!(
            "campaign tso primary ok, campaign-tso-primary-name: {}",
            self.member.get_name()
        );

        log::info!("initializing the global tso allocator");
        if let Err(e) = self.initialize(store) {
            log::error!(
                "failed to initialize the global tso allocator, error: {}",
                e
            );
            return;
        }

        // TODO:
        // check expected primary and watch the primary

        self.member.enable_leader();

        log::info!(
            "tso primary is ready to serve, campaign-tso-primary-name: {}",
            self.member.get_name()
        );

        let mut last_time = Clock::now_since_epoch().as_millis();
        loop {
            if exit_signal.try_exit() {
                log::info!("exit leader campaign");
                break;
            }

            let start = Clock::now_since_epoch().as_millis();
            if start - last_time < Constant::LEADER_TICK_INTERVAL_MILLIS {
                thread::sleep(Duration::from_secs(10));
                continue;
            }

            if !self.member.is_leader() {
                log::info!(
                    "no longer a primary because lease has expired, the tso primary will step down"
                );
                return;
            }

            last_time = Clock::now_since_epoch().as_millis();
        }
    }

    /// initialize is used to initialize a TSO allocator
    /// It will synchronize TSO with store and initialize the memory for later allocation work
    pub fn initialize(&self, store: &dyn TsoStore) -> TsoResult<()> {
        self.tso_allocator_role_gauge.set(1 as f64);
        self.timestamp_oracle.sync_timestamp(store)
    }

    /// update_tso is used to update the TSO in memory and the time window in store
    pub fn update_tso(&self, store: &dyn TsoStore) -> TsoResult<()> {
        self.timestamp_oracle.update_timestamp(store)
    }

    /// generate_ts is used to generate a given number of `TS`'s.
    /// Make sure you have initialized the TSO allocator before calling
    ///
    /// Basically, there are two ways to generate a Global TSO:
    ///  1. The old way to generate a normal TSO from memory directly, which makes the TSO service node become single point.
    ///  2. The new way to generate a Global TSO by synchronizing with all other Local TSO Allocators.
    ///
    /// And for the new way, there are two different strategies:
    ///  1. Collect the max Local TSO from all Local TSO Allocator leaders and write it back to them as max_ts.
    ///  2. Estimate a max_ts and try to write it to all Local TSO Allocator leaders directly to reduce the RTT.
    /// During the process, if the estimated max_ts is not accurate, it will fallback to the collecting way
    pub fn generate_ts(&self, leadership: &TsoLeadership, count: u32) -> TsoResult<Timestamp> {
        if !leadership.check() {
            self.timestamp_oracle.metric.not_leader_event.inc();
            return Ok(Timestamp::default());
        }

        self.timestamp_oracle.get_timestamp(leadership, count, 0)
    }

    /// set_tso sets the physical part with given TSO
    ///
    /// Cannot set the TSO smaller than now in any case.
    /// if ignore_smaller=true, if input ts is smaller than current, ignore silently, else return error
    /// if skip_upper_bound_check=true, skip tso upper bound check
    #[allow(unused)]
    pub fn set_tso(
        &self,
        store: &dyn TsoStore,
        leadership: &TsoLeadership,
        ts: u64,
        ignore_smaller: bool,
        skip_upper_bound_check: bool,
    ) -> TsoResult<()> {
        self.timestamp_oracle.reset_user_timestamp(
            store,
            leadership,
            ts,
            ignore_smaller,
            skip_upper_bound_check,
        )
    }

    /// current TSO in memory
    #[allow(unused)]
    pub fn get_current_tso(&self) -> TsoResult<Timestamp> {
        let (current_physical, current_logical) = self.timestamp_oracle.get_tso_obj();
        if current_physical == 0 {
            anyhow::bail!("timestamp in memory isn't initialized");
        }
        Ok(Timestamp::new(current_physical, current_logical))
    }

    /// set the maxTS as current TSO in memory
    ///
    /// used in multi-master(dc-locate-tso)
    #[allow(unused)]
    pub fn write_tso(
        &self,
        store: &dyn TsoStore,
        leadership: &TsoLeadership,
        max_ts: Timestamp,
    ) -> TsoResult<()> {
        let current_tso = self.get_current_tso()?;

        // If current local TSO has already been greater or equal to maxTS, then do not update it
        if current_tso >= max_ts {
            Ok(())
        } else {
            self.timestamp_oracle.reset_user_timestamp(
                store,
                leadership,
                max_ts.as_u64(),
                true,
                false,
            )
        }
    }

    /// reset is used to reset the TSO allocator
    pub fn reset(&self) {
        self.tso_allocator_role_gauge.set(0 as f64);
        self.timestamp_oracle.reset_timestamp();
    }
}
