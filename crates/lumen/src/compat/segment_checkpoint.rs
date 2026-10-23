//! `crate::segment_checkpoint` before the DDD split. Re-exports its public
//! items from their new homes so callers outside the crate keep compiling.

pub use crate::persistence::application::segment_checkpoint_sink::driver::SegmentCheckpointDriver;
pub use crate::persistence::application::segment_checkpoint_sink::pending_spill::PendingChangeSpill;
pub use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
