use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use coarsetime::Clock;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    allocator::UnixTimeStamp,
    bootstrap::ExitSignal,
    config::Config,
    error::TsoError,
    etcd::EtcdClient,
    util::{constant::Constant, utils::Utils},
    TsoResult,
};

use super::TsoLeadership;

/// Participant is used for the election related logic
pub struct Participant {
    pub(crate) leadership: Arc<TsoLeadership>,
    leader: Mutex<Option<ParticipantInfo>>,

    member: ParticipantInfo,
    /// member_value is the serialized string of `member`. It will be saved in the leader key when this participant is successfully elected as the leader of the group. Every write will use it to check the leadership
    member_value: String,
    /// campaignChecker is used to check whether the additional constraints for a campaign are satisfied. If it returns false, the campaign will fail.
    // campaign_checker:
    /// the last time when the leader is updated
    last_leader_updated_time: AtomicU64,
    ///// expected lease for the primary
    // expected_primary_lease: Lease,
}

impl Participant {
    pub fn new_and_start(
        config: &Config,
        root_path: String,
        etcd_client: EtcdClient,
    ) -> Participant {
        let leader_info = ParticipantInfo::new(
            &config.name,
            Utils::generate_unique_id(&config.name),
            &config.etcd_server_urls,
        );

        let serialize_data = serde_json::to_string(&leader_info).expect("Serialize can't fail");

        Self::new(
            config.leadership_worker_size,
            root_path,
            leader_info,
            serialize_data,
            Clock::now_since_epoch().as_millis(),
            etcd_client,
        )
    }

    fn new(
        leadership_worker_size: usize,
        root_path: String,
        member: ParticipantInfo,
        member_value: String,
        last_leader_updated_time: UnixTimeStamp,
        etcd_client: EtcdClient,
    ) -> Self {
        let leader_path = format!("{}/leader", root_path,);
        let leadership = TsoLeadership::new(
            leader_path.as_str(),
            "TSO leader election",
            leadership_worker_size,
            etcd_client,
        );

        Self {
            leadership: Arc::from(leadership),
            leader: None.into(),
            member,
            member_value,
            last_leader_updated_time: last_leader_updated_time.into(),
        }
    }
}

impl Participant {
    pub fn get_name(&self) -> &str {
        &self.member.name
    }

    /// returns whether the participant is the leader or not by checking its leadership's lease and leader info
    pub fn is_leader(&self) -> bool {
        self.leadership.check()
            && self.get_leader_id() == self.member.member_id
            && self.campaign_check()
    }

    fn get_leader_id(&self) -> u64 {
        self.leader
            .lock()
            .as_ref()
            .map(|x| x.member_id)
            .unwrap_or(0)
    }

    fn set_leader(&self, member: ParticipantInfo) {
        self.leader.lock().replace(member);
        self.last_leader_updated_time.store(
            Clock::now_since_epoch().as_millis(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn unset_leader(&self) {
        self.leader.lock().take();
        self.last_leader_updated_time.store(
            Clock::now_since_epoch().as_millis(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// EnableLeader declares the member itself to be the leader
    pub fn enable_leader(&self) {
        self.leader.lock().replace(self.member.clone());
    }

    /// campaign the leadership and make it become a leader
    pub fn campaign_leader(&mut self, lease_timeout: i64) -> TsoResult<()> {
        if !self.campaign_check() {
            anyhow::bail!(TsoError::CheckCampaign);
        }
        self.leadership.campaign(lease_timeout, &self.member_value)
    }

    /// keep the leader's leadership
    pub fn keep_leader(&self, exit_signal: ExitSignal) {
        self.leadership.keep(exit_signal);
    }

    /// checks if someone else is taking the leadership. If yes, returns the leader;
    /// otherwise returns a bool which indicates if it is needed to check later
    pub fn check_leader(&self) -> (Option<(ParticipantInfo, i64)>, bool) {
        match self.leadership.get_leader() {
            Ok((Some(leader), revision)) => {
                if self.is_same_leader(&leader) {
                    // oh, we are already the leader, which indicates we may meet something wrong in previous CampaignLeader. We should delete the leadership and campaign again
                    log::warn!(
                        "the leader has not changed, delete and campaign again, old-leader: {:?}",
                        leader
                    );
                    // Delete the leader itself and let others start a new election again
                    if let Err(e) = self.leadership.delete_leader_key() {
                        log::error!("deleting the leader key meets error, cause: {}", e);
                        std::thread::sleep(Duration::from_millis(
                            Constant::LOOP_MIN_INTERVAL_MILLIS,
                        ));
                        (None, true)
                    } else {
                        // Return nil and false to make sure the campaign will start immediately
                        (None, false)
                    }
                } else {
                    (Some((leader, revision)), false)
                }
            }
            Ok((None, _)) => {
                // no leader yet
                (None, false)
            }
            Err(e) => {
                log::error!("getting the leader meets error, cause: {}", e);
                std::thread::sleep(Duration::from_millis(Constant::LOOP_MIN_INTERVAL_MILLIS));
                (None, true)
            }
        }
    }

    /// watch the changes of the leader
    pub fn watch_leader(
        &mut self,
        leader: ParticipantInfo,
        revision: i64,
        exit_signal: ExitSignal,
    ) {
        self.set_leader(leader);
        self.leadership.watch(revision, exit_signal);
        self.unset_leader();
    }

    /// reset the member's current leadership. Basically it will reset the leader lease and unset leader info
    pub fn reset_leader(&self) {
        self.leadership.reset();
        self.unset_leader();
    }

    fn is_same_leader(&self, info: &ParticipantInfo) -> bool {
        self.member.member_id.eq(&info.member_id)
    }

    fn campaign_check(&self) -> bool {
        // ignore campaignChecker, always return true
        true
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParticipantInfo {
    name: String,
    // id is unique among all participants
    member_id: u64,
    // etc server urls
    client_urls: Vec<String>,
}

impl ParticipantInfo {
    pub fn new(name: &str, id: u64, client_urls: &str) -> ParticipantInfo {
        Self {
            name: name.into(),
            member_id: id,
            client_urls: client_urls
                .split(",")
                .map(|x| x.to_owned())
                .collect::<Vec<_>>(),
        }
    }
}
