//! Runtime config — sourced from env so it can be wired through the K8s
//! ConfigMap without any rebuild.
//!
//! Two orthogonal dimensions:
//!   shard_count          — how data is partitioned (collection_id hash)
//!   replicas_per_shard   — Raft group size per shard
//!   voter_count          — first N replicas vote; the rest are learners
//!
//! pod ordinal → (shard_index, replica_index) is pure integer math, so
//! peers can be found via headless DNS with no extra discovery service.
//!
//! The env keys and the ordinal/shard/replica/voter derivation are the same
//! StatefulSet downward-API math every raft_core service needs, and live in
//! `libs/raft-runtime::cluster::ClusterDims` (#1002); this module is a thin
//! adapter that keeps lumen's own `ClusterConfig` type (compiled unconditionally,
//! unlike the raft-wal-only peer/DNS wiring in `raft.rs`) while delegating the
//! actual math so it can't drift from `raft_runtime::cluster::ClusterTopology`.

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub shard_count: u32,
    pub replicas_per_shard: u32,
    pub voter_count: u32,
    pub pod_name: String,
}

impl From<raft_runtime::cluster::ClusterDims> for ClusterConfig {
    fn from(d: raft_runtime::cluster::ClusterDims) -> Self {
        Self {
            shard_count: d.shard_count,
            replicas_per_shard: d.replicas_per_shard,
            voter_count: d.voter_count,
            pod_name: d.pod_name,
        }
    }
}

impl From<ClusterConfig> for raft_runtime::cluster::ClusterDims {
    fn from(c: ClusterConfig) -> Self {
        Self {
            shard_count: c.shard_count,
            replicas_per_shard: c.replicas_per_shard,
            voter_count: c.voter_count,
            pod_name: c.pod_name,
        }
    }
}

impl ClusterConfig {
    pub fn from_env() -> Result<Self> {
        Ok(raft_runtime::cluster::ClusterDims::from_env()?.into())
    }

    pub fn pod_ordinal(&self) -> Result<u32> {
        raft_runtime::cluster::ClusterDims::from(self.clone()).pod_ordinal()
    }

    pub fn shard_index(&self) -> Result<u32> {
        raft_runtime::cluster::ClusterDims::from(self.clone()).shard_index()
    }

    pub fn replica_index(&self) -> Result<u32> {
        raft_runtime::cluster::ClusterDims::from(self.clone()).replica_index()
    }

    pub fn is_voter(&self) -> Result<bool> {
        raft_runtime::cluster::ClusterDims::from(self.clone()).is_voter()
    }

    /// This pod's StatefulSet name — the peer-DNS prefix. Delegated for the
    /// same reason as the ordinal math (#1002): one derivation, so
    /// `RaftGroup`'s peer names cannot drift from `ClusterTopology`'s.
    pub fn pod_prefix(&self) -> Result<String> {
        raft_runtime::cluster::ClusterDims::from(self.clone())
            .pod_prefix()
            .map(str::to_string)
    }
}

#[cfg(test)]
mod tests;
