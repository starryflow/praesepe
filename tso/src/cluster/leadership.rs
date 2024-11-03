use std::fmt::Debug;

use async_trait::async_trait;

use crate::{bootstrap::ExitSignal, TsoResult};

use super::ParticipantInfo;

#[async_trait]
pub trait TsoLeadership: Send + Sync + Debug {
    /// campaign the leader with given lease and returns a leadership
    fn campaign(&self, lease_timeout: i64, leader_data: &str) -> TsoResult<()>;

    /// keep the leadership available by update the lease's expired time continuously
    fn keep(&self, exit_signal: ExitSignal);
    /// returns whether the leadership is still available
    fn check(&self) -> bool;

    /// watch the changes of the leadership, usually is used to detect the leadership stepping down and restart an election as soon as possible
    fn watch(&self, revision: i64, exit_signal: ExitSignal);
    async fn async_watch(&self, _revision: i64, _exit_signal: ExitSignal) {}

    /// Reset does some defer jobs such as closing lease, resetting lease etc
    fn reset(&self);

    /// GetLeader gets the corresponding leader from etcd by given leader_path (as the key)
    fn get_leader(&self) -> TsoResult<(Option<ParticipantInfo>, i64)>;
    /// deletes the corresponding leader by the leader_path as the key
    fn delete_leader_key(&self) -> TsoResult<()>;
}

pub enum TsoLeadershipKind {
    Etcd,
    AlwaysLeader,
}

pub struct AlwaysLeader;

#[async_trait]
impl TsoLeadership for AlwaysLeader {
    fn campaign(&self, _: i64, _: &str) -> TsoResult<()> {
        unreachable!("As AlwaysLeader, not need implement campaign logic")
    }

    fn keep(&self, _: ExitSignal) {
        unreachable!("As AlwaysLeader, not need implement keep logic")
    }

    fn check(&self) -> bool {
        // As AlwaysLeader, always return true
        true
    }

    fn watch(&self, _: i64, _: ExitSignal) {
        unreachable!("As AlwaysLeader, not need implement watch logic")
    }

    fn reset(&self) {
        // do nothing
    }

    fn get_leader(&self) -> TsoResult<(Option<ParticipantInfo>, i64)> {
        unreachable!("As AlwaysLeader, not need implement get_persistent_leader logic")
    }

    fn delete_leader_key(&self) -> TsoResult<()> {
        unreachable!("As AlwaysLeader, not need implement delete_leader_key logic")
    }
}

impl Debug for AlwaysLeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AlwaysLeader")
    }
}
