//! `ClusterState` as the `app::http` cluster-status port, which the
//! read-consistency guard and `/debug/cluster` read through.

use crate::domain::cluster_state::ClusterState;
use crate::shared_kernel::ports::cluster_status::ClusterStatus;
use crate::shared_kernel::types::cluster_state_view::ClusterStateView;
use crate::shared_kernel::types::peer_addr::PeerAddr;
use crate::shared_kernel::types::raft_role::RaftRole;

impl ClusterStatus for ClusterState {
    fn pod_name(&self) -> &str {
        &self.pod_name
    }

    fn shard_index(&self) -> u32 {
        self.shard_index
    }

    fn role(&self) -> RaftRole {
        ClusterState::role(self)
    }

    fn leader_peer(&self) -> Option<&PeerAddr> {
        ClusterState::leader_peer(self)
    }

    fn replication_lag_ms(&self) -> u64 {
        self.replication_lag_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn snapshot(&self) -> ClusterStateView {
        ClusterState::snapshot(self)
    }
}
