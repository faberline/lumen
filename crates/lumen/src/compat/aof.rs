//! `crate::aof` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
pub use crate::persistence::infrastructure::aof::replay::{replay_aof_into, AofReader};
pub use crate::persistence::infrastructure::aof::{FramedLogTrimObserver, FsyncPolicy};
