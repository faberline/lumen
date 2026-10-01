//! `crate::raft` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::replication::domain::cluster_state::{ClusterState, ReadConsistency};
pub use crate::replication::domain::cluster_state_view::ClusterStateView;
pub use crate::replication::domain::peer_addr::{PeerAddr, RaftGroup};
pub use crate::replication::domain::raft_role::RaftRole;
