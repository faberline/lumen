//! Restore a multi-shard backup set into a fresh runtime. The manifest and every shard
//! artifact are verified before the first write, so a failed check leaves every target
//! untouched. Writes then run shard by shard. There is no cross-shard transaction: a
//! write error after some shards were written leaves those shards restored.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;

use crate::index::application::engine::Engine;
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::persistence::domain::backup_manifest::{
    build_backup_manifest, read_backup_manifest, verify_artifact, ManifestError, ShardArtifact,
    ShardEntry, BACKUP_MANIFEST_KEY,
};
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

/// Reads the objects of one backup set by key. `Ok(None)` means the object is absent.
pub trait BackupSetSource {
    fn read(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>>;
}

/// A backup set kept as files in one directory, one file per object key.
pub struct DirSource(pub PathBuf);

impl BackupSetSource for DirSource {
    fn read(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        match std::fs::read(self.0.join(key)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

/// The shards of a target runtime, numbered `0..shard_count()`.
pub trait ShardRestoreTarget {
    fn shard_count(&self) -> u32;
    fn shard_is_empty(&self, shard: u32) -> anyhow::Result<bool>;
    fn restore_shard(&self, shard: u32, snapshot: SnapshotV1) -> anyhow::Result<()>;
}

/// One engine per shard: shard `i` is element `i`.
pub struct EngineSet(pub Vec<Arc<Engine>>);

impl EngineSet {
    fn engine(&self, shard: u32) -> anyhow::Result<&Engine> {
        self.0
            .get(shard as usize)
            .map(Arc::as_ref)
            .ok_or_else(|| anyhow!("no engine for shard {shard}"))
    }
}

impl ShardRestoreTarget for EngineSet {
    fn shard_count(&self) -> u32 {
        self.0.len() as u32
    }

    fn shard_is_empty(&self, shard: u32) -> anyhow::Result<bool> {
        Ok(self.engine(shard)?.list_collections()?.is_empty())
    }

    fn restore_shard(&self, shard: u32, snapshot: SnapshotV1) -> anyhow::Result<()> {
        self.engine(shard)?.restore(snapshot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RestoreOptions {
    /// Restore into a target that already holds data.
    pub replace: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct RestoreReport {
    pub backup_id: String,
    pub shards_restored: u32,
    pub documents: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("backup manifest lumen-backup-manifest.json is missing from the backup set")]
    ManifestMissing,
    #[error("{0}")]
    Manifest(#[from] ManifestError),
    #[error("backup was taken from {manifest} shards but the target has {target}")]
    TopologyMismatch { manifest: u32, target: u32 },
    #[error("shard {shard} artifact {key} is missing from the backup set")]
    MissingArtifact { shard: u32, key: String },
    #[error("shard {shard} artifact {key} does not match the manifest digest")]
    CorruptArtifact { shard: u32, key: String },
    #[error("backup manifest does not describe the stored objects; refusing to restore")]
    IdentityMismatch,
    #[error("shard {shard} already holds data; restore without --replace is refused")]
    TargetNotEmpty { shard: u32 },
    #[error("cannot read the backup set: {0}")]
    Source(String),
    #[error("restoring shard {shard} failed after {written} shard(s) were written: {reason}")]
    WriteFailed {
        shard: u32,
        written: u32,
        reason: String,
    },
}

/// Verify the whole set, then restore each shard in order. Every check that can fail runs
/// before the first `restore_shard` call. A target that cannot be inspected or written
/// fails with `WriteFailed`.
pub fn restore_backup_set(
    source: &dyn BackupSetSource,
    targets: &dyn ShardRestoreTarget,
    options: &RestoreOptions,
) -> Result<RestoreReport, RestoreError> {
    // 1. The manifest is present and internally consistent.
    let manifest_bytes = source
        .read(BACKUP_MANIFEST_KEY)
        .map_err(|err| RestoreError::Source(format!("{err:#}")))?
        .ok_or(RestoreError::ManifestMissing)?;
    let manifest = read_backup_manifest(&manifest_bytes)?;

    // 2. The target has the same number of shards as the backup.
    let target_count = targets.shard_count();
    if target_count != manifest.physical_shard_count {
        return Err(RestoreError::TopologyMismatch {
            manifest: manifest.physical_shard_count,
            target: target_count,
        });
    }

    // 3. Every shard artifact is present and matches its digest, in shard order.
    let mut entries: Vec<&ShardEntry> = manifest.shards.iter().collect();
    entries.sort_by_key(|entry| entry.shard);
    let mut artifacts = Vec::with_capacity(entries.len());
    for entry in &entries {
        let bytes = source
            .read(&entry.key)
            .map_err(|err| RestoreError::Source(format!("{err:#}")))?
            .ok_or_else(|| RestoreError::MissingArtifact {
                shard: entry.shard,
                key: entry.key.clone(),
            })?;
        verify_artifact(entry, &bytes).map_err(|_| RestoreError::CorruptArtifact {
            shard: entry.shard,
            key: entry.key.clone(),
        })?;
        artifacts.push(ShardArtifact {
            shard: entry.shard,
            key: entry.key.clone(),
            bytes,
        });
    }

    // 4. The objects describe what the manifest says. A forged schema or document count
    // with a recomputed backup_id passes the manifest's own check; only a rebuild from the
    // objects catches it.
    let map = VirtualBucketShardMap::new(
        manifest.shard_map_version,
        manifest.assignments.clone(),
        manifest.physical_shard_count,
    )
    .map_err(|err| ManifestError::InvalidTopology(err.to_string()))?;
    let rebuilt = build_backup_manifest(&map, &artifacts)?;
    if rebuilt.backup_id != manifest.backup_id {
        return Err(RestoreError::IdentityMismatch);
    }

    // 5. A target that holds data is refused unless the caller asks to replace it. An
    // inspection error is reported as a write failure before any shard is written.
    if !options.replace {
        for entry in &entries {
            let empty =
                targets
                    .shard_is_empty(entry.shard)
                    .map_err(|err| RestoreError::WriteFailed {
                        shard: entry.shard,
                        written: 0,
                        reason: format!("{err:#}"),
                    })?;
            if !empty {
                return Err(RestoreError::TargetNotEmpty { shard: entry.shard });
            }
        }
    }

    // Parse every snapshot before the first write, so no parse error can follow a write.
    let mut snapshots = Vec::with_capacity(artifacts.len());
    for artifact in &artifacts {
        let snapshot: SnapshotV1 = serde_json::from_slice(&artifact.bytes).map_err(|err| {
            ManifestError::UnreadableArtifact {
                shard: artifact.shard,
                reason: err.to_string(),
            }
        })?;
        snapshots.push((artifact.shard, snapshot));
    }

    // 6. Write shard by shard. This is the only step that can leave shards restored.
    for (written, (shard, snapshot)) in snapshots.into_iter().enumerate() {
        targets
            .restore_shard(shard, snapshot)
            .map_err(|err| RestoreError::WriteFailed {
                shard,
                written: written as u32,
                reason: format!("{err:#}"),
            })?;
    }

    Ok(RestoreReport {
        backup_id: manifest.backup_id,
        shards_restored: manifest.physical_shard_count,
        documents: manifest.shards.iter().map(|entry| entry.documents).sum(),
    })
}

#[cfg(test)]
mod tests;
