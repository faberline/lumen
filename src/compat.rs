//! Module paths from before the DDD split.
//!
//! Each child re-exports the public items of a module the split broke up, so
//! callers outside the crate keep compiling. They go once those callers use the
//! new paths.

pub mod auth;
pub mod config;
#[cfg(feature = "operator")]
pub mod operator;
pub mod raft;
#[cfg(feature = "raft-wal")]
pub mod raft_sm;
pub mod reshard;
pub mod routing;
#[cfg(feature = "operator")]
pub mod routing_remote;
pub mod types;
pub mod wal;
