//! A shard's peers and roles, the live cluster state `/debug/cluster` reports,
//! and the read-consistency contract checked against it.

pub mod cluster_state;
pub mod cluster_state_view;
pub mod peer_addr;
pub mod raft_role;

#[cfg(test)]
mod tests;
