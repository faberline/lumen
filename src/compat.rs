//! Module paths from before the DDD split.
//!
//! Each child re-exports the public items of a module the split broke up, so
//! callers outside the crate keep compiling. They go once those callers use the
//! new paths.

pub mod aof;
pub mod auth;
#[cfg(feature = "backup")]
pub mod backup;
pub mod backup_sink;
pub mod config;
pub mod coordinator;
#[cfg(feature = "operator")]
pub mod operator;
pub mod raft;
#[cfg(feature = "raft-wal")]
pub mod raft_sm;
pub mod rdb;
pub mod reshard;
pub mod routing;
#[cfg(feature = "operator")]
pub mod routing_remote;
pub mod segment_checkpoint;
pub mod segment_rdb;
pub mod segment_restore;
pub mod types;
pub mod wal;
