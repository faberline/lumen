//! `crate::backup` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::persistence::application::backup::{
    fetch_snapshot_bytes, restore_snapshot_bytes, run_backup,
};
