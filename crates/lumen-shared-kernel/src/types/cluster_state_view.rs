//! The wire shape of `/debug/cluster`: one snapshot of a pod's cluster state.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::peer_addr::PeerAddr;
use super::raft_role::RaftRole;

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
