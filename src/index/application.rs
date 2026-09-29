//! The index use cases: the Engine, one per shard, that creates collections,
//! applies records to their field indexes, answers searches and duplicate
//! groups and takes part in the checkpoint protocol, with the capture it
//! freezes for a checkpoint and the live deltas it serves reads from until a
//! base absorbs them.

pub(crate) mod admission;
pub(crate) mod apply;
pub(crate) mod checkpoint_capture;
pub(crate) mod engine;
pub(crate) mod frozen_checkpoint;
mod live_base;
pub(crate) mod live_delta;
pub(crate) mod recovery_profile;
mod text_preparation;
