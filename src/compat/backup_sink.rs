//! `crate::backup_sink` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::persistence::infrastructure::backup_sink::{
    run_backup_once, sink_from_destination, BackupDestination, BackupObject, BackupPolicy,
    BackupRunResult, BackupSink, LocalFsSink, RetentionPolicy, UnsupportedCloudSink,
};
