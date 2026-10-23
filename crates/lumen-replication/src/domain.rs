//! A shard's peers and roles, the cluster shape this pod reads from its
//! environment, the live cluster state `/debug/cluster` reports, and the
//! read-consistency contract checked against it.

pub mod cluster_config;
pub mod cluster_state;
pub mod peer_addr;
pub mod runtime_shapes;

#[cfg(test)]
mod tests;
