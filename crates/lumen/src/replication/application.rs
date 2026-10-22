//! `EngineSm`: the engine as a raft state machine, and the `--wal raft` write
//! sink that proposes through the shared host.

pub(crate) mod engine_sm;
