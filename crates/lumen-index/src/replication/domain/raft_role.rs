//! A pod's role in its shard's raft group, as the OpenAPI-visible wrapper over
//! `raft_runtime`'s role.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum RaftRole {
    Leader,
    Follower,
    Learner,
    Candidate,
}

impl RaftRole {
    /// Decode the `AtomicU8` encoding [`ClusterState::role`] stores (the
    /// enum's own discriminant order — see the `as u8` encode side in
    /// [`ClusterState::set_role`]). Unknown values fall back to `Candidate`
    /// (never silently claim `Leader`/a stale role for a corrupt byte).
    ///
    /// [`ClusterState::role`]: crate::replication::domain::cluster_state::ClusterState::role
    /// [`ClusterState::set_role`]: crate::replication::domain::cluster_state::ClusterState::set_role
    pub(super) fn from_u8(v: u8) -> Self {
        match v {
            v if v == Self::Leader as u8 => Self::Leader,
            v if v == Self::Follower as u8 => Self::Follower,
            v if v == Self::Learner as u8 => Self::Learner,
            _ => Self::Candidate,
        }
    }
}

impl From<raft_runtime::RaftRole> for RaftRole {
    fn from(role: raft_runtime::RaftRole) -> Self {
        match role {
            raft_runtime::RaftRole::Leader => Self::Leader,
            raft_runtime::RaftRole::Follower => Self::Follower,
            raft_runtime::RaftRole::Learner => Self::Learner,
            raft_runtime::RaftRole::Candidate => Self::Candidate,
        }
    }
}
