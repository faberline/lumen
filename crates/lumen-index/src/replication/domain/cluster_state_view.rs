//! The wire shape of `/debug/cluster`: one snapshot of a pod's cluster state.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::replication::domain::peer_addr::PeerAddr;
use crate::replication::domain::raft_role::RaftRole;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ClusterStateView {
    pub pod_name: String,
    pub shard_index: u32,
    pub replica_index: u32,
    pub role: RaftRole,
    pub peers: Vec<PeerAddr>,
    pub applied_index: u64,
    pub leader_term: u64,
    pub replication_lag_ms: u64,
}

impl From<raft_runtime::ClusterStateView> for ClusterStateView {
    fn from(v: raft_runtime::ClusterStateView) -> Self {
        Self {
            pod_name: v.pod_name,
            shard_index: v.shard_index,
            replica_index: v.replica_index,
            role: v.role.into(),
            peers: v.peers.into_iter().map(Into::into).collect(),
            applied_index: v.applied_index,
            leader_term: v.leader_term,
            replication_lag_ms: v.replication_lag_ms,
        }
    }
}
