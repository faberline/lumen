//! Conversions from `raft_runtime`'s canonical cluster shapes to lumen's
//! OpenAPI-visible mirrors in the shared kernel.
//!
//! The mirrors exist so `raft_runtime`'s shared types need not derive
//! `utoipa::ToSchema` for every adopter. The kernel cannot depend on
//! `raft_runtime`, so the conversions live here, in the context that wraps it.

use crate::shared_kernel::types::cluster_state_view::ClusterStateView;
use crate::shared_kernel::types::peer_addr::PeerAddr;
use crate::shared_kernel::types::raft_role::RaftRole;

pub fn raft_role(role: raft_runtime::RaftRole) -> RaftRole {
    match role {
        raft_runtime::RaftRole::Leader => RaftRole::Leader,
        raft_runtime::RaftRole::Follower => RaftRole::Follower,
        raft_runtime::RaftRole::Learner => RaftRole::Learner,
        raft_runtime::RaftRole::Candidate => RaftRole::Candidate,
    }
}

pub fn peer_addr(p: raft_runtime::PeerAddr) -> PeerAddr {
    PeerAddr {
        pod_name: p.pod_name,
        host: p.host,
        raft_port: p.raft_port,
        client_port: p.client_port,
        role: raft_role(p.role),
    }
}

pub fn cluster_state_view(v: raft_runtime::ClusterStateView) -> ClusterStateView {
    ClusterStateView {
        pod_name: v.pod_name,
        shard_index: v.shard_index,
        replica_index: v.replica_index,
        role: raft_role(v.role),
        peers: v.peers.into_iter().map(peer_addr).collect(),
        applied_index: v.applied_index,
        leader_term: v.leader_term,
        replication_lag_ms: v.replication_lag_ms,
    }
}
