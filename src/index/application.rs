//! The index use cases: the Engine, one per shard, that creates collections,
//! applies records to their field indexes, answers searches and duplicate
//! groups and takes part in the checkpoint protocol, with the capture it
//! freezes for a checkpoint and the live deltas it serves reads from until a
//! base absorbs them; and the search port the HTTP handlers call, with its
//! local Engine implementation.

pub(crate) mod admission;
mod apply;
pub(crate) mod checkpoint_capture;
pub(crate) mod engine;
pub(crate) mod frozen_checkpoint;
mod live_base;
mod live_delta;
pub(crate) mod ports;
pub(crate) mod recovery_profile;
mod text_preparation;
