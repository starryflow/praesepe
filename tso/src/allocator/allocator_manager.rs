use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use coarsetime::Clock;
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::{
    allocator::global_allocator::GlobalTsoAllocator,
    bootstrap::ExitSignal,
    cluster::{Participant, TsoLeadership},
    config::Config,
    store::TsoStore,
    util::constant::Constant,
    Timestamp, TsoResult,
};

/// AllocatorManager is used to manage the TSO Allocators
/// It is in charge of maintaining TSO allocators' leadership, checking election priority, and forwarding TSO allocation requests to correct TSO Allocators
pub struct AllocatorManager {
    global_tso_allocator: GlobalTsoAllocator,

    thread_pool: ThreadPool,
    leadership: Arc<TsoLeadership>,
    store: Box<dyn TsoStore>,

    physical_last_update_millis: AtomicU64,
    config: Config,
}

impl AllocatorManager {
    pub fn new_and_start(
        config: Config,
        member: Participant,
        store: Box<dyn TsoStore>,
        exit_signal: ExitSignal,
    ) -> Self {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(config.allocator_worker_size)
            .thread_name(|worker_idx| format!("TsoAllocatorWorker@{}", worker_idx))
            .build()
            .expect("Create TSO AllocatorWorkerPool failed");

        // setup_global_allocator is used to set up the global allocator, which will initialize the allocator and put it into an allocator daemon. An TSO Allocator should only be set once, and may be initialized and reset multiple times depending on the election
        let leadership = member.leadership.clone();
        let global_tso_allocator = GlobalTsoAllocator::new(&config, member);

        let instance = Self {
            global_tso_allocator,
            thread_pool,
            leadership,
            store,
            physical_last_update_millis: 0.into(),
            config,
        };

        // update tso loop
        instance.thread_pool.install(|| {
            instance.tso_allocator_loop(exit_signal);
        });

        instance
    }

    /// leader election, if successful, then initialize allocator
    pub fn start_global_allocator_loop(&mut self, exit_signal: ExitSignal) {
        self.thread_pool.install(|| {
            self.global_tso_allocator.primary_election_loop(
                self.store.as_ref(),
                &self.config,
                exit_signal,
            )
        })
    }

    /// HandleRequest forwards TSO allocation requests to correct TSO Allocators
    #[fastrace::trace]
    pub fn handle_request(&self, count: u32) -> TsoResult<Timestamp> {
        self.global_tso_allocator
            .generate_ts(self.leadership.as_ref(), count)
    }

    /// used for dc-location-tso
    #[fastrace::trace]
    pub fn sync_max_ts(&self) {
        unimplemented!("sync max ts")
    }

    /// tso_allocator_loop is used to run the TSO Allocator updating daemon
    fn tso_allocator_loop(&self, mut exit_signal: ExitSignal) {
        loop {
            if exit_signal.try_exit() {
                break;
            }

            let now = Clock::now_since_epoch().as_millis();
            let last = self.physical_last_update_millis.load(Ordering::Relaxed);

            // Skip if time not fallback and less than interval
            if now > last && now < last + self.config.update_physical_interval_millis as u64 {
                thread::sleep(Duration::from_millis(Constant::LOOP_MIN_INTERVAL_MILLIS));
                continue;
            }

            log::info!("entering into allocator daemon");

            // Update the initialized TSO Allocator to advance TSO
            self.update_allocator();
        }

        self.global_tso_allocator.reset();
        log::info!("exit allocator loop");
    }

    // update_allocator is used to update the allocator
    fn update_allocator(&self) {
        if !self.leadership.check() {
            log::info!("allocator doesn't campaign leadership yet");
            thread::sleep(Duration::from_millis(Constant::LOOP_MIN_INTERVAL_MILLIS));
            return;
        }

        if let Err(e) = self.global_tso_allocator.update_tso(self.store.as_ref()) {
            log::warn!("failed to update allocator's timestamp, error: {}", e);
            self.reset_allocator(false);
        }
    }

    // reset_allocator will reset the allocator's leadership and TSO initialized in memory.
    // It usually should be called before re-triggering an Allocator leader campaign.
    fn reset_allocator(&self, skip_reset_leader: bool) {
        self.global_tso_allocator.reset();

        // Reset if it still has the leadership. Otherwise the data race may occur because of the re-campaigning.
        if !skip_reset_leader && self.leadership.check() {
            self.leadership.reset()
        }
    }
}
