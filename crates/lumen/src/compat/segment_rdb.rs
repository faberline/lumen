//! `crate::segment_rdb` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::persistence::infrastructure::segment_rdb_store::startup::{
    SegmentStartupDecision, SegmentStartupOutcome,
};
pub use crate::persistence::infrastructure::segment_rdb_store::{
    LoadedSegmentGeneration, MergeObserver, MergePhase, SegmentRdbStore,
};
