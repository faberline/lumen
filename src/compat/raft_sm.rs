//! `crate::raft_sm` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::replication::application::engine_sm::write_sink::RaftWriteSink;
pub use crate::replication::application::engine_sm::EngineSm;
