//! `crate::backup` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::persistence::application::backup::{
    fetch_snapshot_bytes, restore_snapshot_bytes, run_backup,
};
pub use crate::persistence::domain::backup_manifest::{
    build_backup_manifest, read_backup_manifest, verify_artifact, BackupManifest, ManifestError,
    ShardArtifact, ShardEntry, BACKUP_MANIFEST_KEY, BACKUP_MANIFEST_VERSION,
};
