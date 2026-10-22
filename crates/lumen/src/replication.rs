//! Replication: a shard's raft group, this pod's live role in it, and lumen's
//! engine driven as the shared raft host's state machine.

#[cfg(feature = "raft-wal")]
pub(crate) mod application;
pub(crate) mod domain;
