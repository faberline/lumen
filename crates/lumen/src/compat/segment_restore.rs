//! `crate::segment_restore` before the DDD split. Re-exports its public items
//! from their new homes so callers outside the crate keep compiling.

pub use crate::persistence::application::restore::{
    RestorePublicationObserver, SegmentRestoreSink, UnavailableRestoreSink,
};
