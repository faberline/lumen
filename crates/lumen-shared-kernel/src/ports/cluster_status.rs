//! The port the HTTP layer reads this pod's raft cluster state through.
//!
//! `lumen-replication` owns the live state (`ClusterState`) and implements this
//! trait for it. The trait lives in the kernel so that neither `lumen-index`
//! nor `lumen-replication` depends on the other: the read-consistency guard and
//! `/debug/cluster` read the port, and the binary hands them the state. Standalone and legacy external-log
//! builds leave `AppState::cluster` empty.

use crate::types::cluster_state_view::ClusterStateView;
use crate::types::peer_addr::PeerAddr;
use crate::types::raft_role::RaftRole;

pub trait ClusterStatus: Send + Sync {
    /// This pod's name, for error messages.
    fn pod_name(&self) -> &str;

    /// The shard this pod serves.
    fn shard_index(&self) -> u32;

    /// This pod's current role in its shard's raft group.
    fn role(&self) -> RaftRole;

    /// The peer this pod currently believes leads the shard, if any.
    fn leader_peer(&self) -> Option<&PeerAddr>;

    /// How far this pod's applied state trails the leader, in milliseconds.
    fn replication_lag_ms(&self) -> u64;

    /// The `/debug/cluster` view of this pod's cluster state.
    fn snapshot(&self) -> ClusterStateView;
}
