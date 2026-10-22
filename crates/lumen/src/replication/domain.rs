//! A shard's peers and roles, the live cluster state `/debug/cluster` reports,
//! and the read-consistency contract checked against it.

pub(crate) mod cluster_state;
pub(crate) mod cluster_state_view;
pub(crate) mod peer_addr;
pub(crate) mod raft_role;

#[cfg(test)]
mod tests;
