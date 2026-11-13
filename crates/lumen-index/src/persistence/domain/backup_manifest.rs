//! The backup manifest: one versioned document that ties the per-shard backup
//! artifacts of one point-in-time set together. It records the schema, the
//! shard topology, one digest per shard artifact and a content identity for
//! the whole set. The shard artifacts themselves are not changed.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

pub const BACKUP_MANIFEST_VERSION: u32 = 1;
pub const BACKUP_MANIFEST_KEY: &str = "lumen-backup-manifest.json";

/// One shard's stored artifact, as the caller has it in hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardArtifact {
    pub shard: u32,
    pub key: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardEntry {
    pub shard: u32,
    pub key: String,
    pub sha256: String,
    pub bytes: u64,
    pub documents: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: u32,
    pub virtual_bucket_count: u32,
    pub physical_shard_count: u32,
    pub shard_map_version: u64,
    /// Bucket to shard, one entry per virtual bucket.
    pub assignments: Vec<u32>,
    /// Collection id to its schema as JSON.
    pub collections: BTreeMap<String, serde_json::Value>,
    /// Sorted by shard; shard `i` is at index `i`.
    pub shards: Vec<ShardEntry>,
    pub backup_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("backup manifest needs at least one shard artifact")]
    NoShards,
    #[error("shard {0} appears more than once")]
    DuplicateShard(u32),
    #[error("shard {0} is missing")]
    MissingShard(u32),
    #[error("shard {shard} is outside 0..{count}")]
    ShardOutOfRange { shard: u32, count: u32 },
    #[error("shard {shard} artifact is not a SnapshotV1 document: {reason}")]
    UnreadableArtifact { shard: u32, reason: String },
    #[error("shard {0} carries a different collection schema than shard 0")]
    SchemaMismatch(u32),
    #[error("unsupported backup manifest version {0}")]
    UnsupportedVersion(u32),
    #[error("malformed backup manifest: {0}")]
    Malformed(String),
    #[error("invalid backup manifest topology: {0}")]
    InvalidTopology(String),
    #[error("artifact {key} does not match the manifest digest")]
    DigestMismatch { key: String },
    #[error("backup_id does not match the manifest content")]
    IdentityMismatch,
}

impl BackupManifest {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec_pretty(self).expect("manifest serializes")
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// The content identity: the digest of the manifest with `backup_id` empty.
fn identity(manifest: &BackupManifest) -> String {
    let mut unsigned = manifest.clone();
    unsigned.backup_id = String::new();
    sha256_hex(&unsigned.to_bytes())
}

/// Build the manifest for one complete set of shard artifacts.
pub fn build_backup_manifest(
    map: &VirtualBucketShardMap,
    artifacts: &[ShardArtifact],
) -> Result<BackupManifest, ManifestError> {
    if artifacts.is_empty() {
        return Err(ManifestError::NoShards);
    }
    let count = map.physical_shard_count();
    let mut seen = BTreeSet::new();
    for artifact in artifacts {
        if artifact.shard >= count {
            return Err(ManifestError::ShardOutOfRange {
                shard: artifact.shard,
                count,
            });
        }
        if !seen.insert(artifact.shard) {
            return Err(ManifestError::DuplicateShard(artifact.shard));
        }
    }
    if let Some(missing) = (0..count).find(|s| !seen.contains(s)) {
        return Err(ManifestError::MissingShard(missing));
    }

    let mut sorted: Vec<&ShardArtifact> = artifacts.iter().collect();
    sorted.sort_by_key(|a| a.shard);

    let mut parsed = Vec::with_capacity(sorted.len());
    for artifact in &sorted {
        let snapshot: SnapshotV1 = serde_json::from_slice(&artifact.bytes).map_err(|e| {
            ManifestError::UnreadableArtifact {
                shard: artifact.shard,
                reason: e.to_string(),
            }
        })?;
        parsed.push(snapshot);
    }

    let schema_of = |snapshot: &SnapshotV1| -> BTreeMap<String, serde_json::Value> {
        snapshot
            .collections
            .iter()
            .map(|(id, c)| {
                (
                    id.clone(),
                    serde_json::to_value(&c.schema).expect("schema serializes"),
                )
            })
            .collect()
    };
    let collections = schema_of(&parsed[0]);
    for (artifact, snapshot) in sorted.iter().zip(&parsed).skip(1) {
        if schema_of(snapshot) != collections {
            return Err(ManifestError::SchemaMismatch(artifact.shard));
        }
    }

    let shards = sorted
        .iter()
        .zip(&parsed)
        .map(|(artifact, snapshot)| ShardEntry {
            shard: artifact.shard,
            key: artifact.key.clone(),
            sha256: sha256_hex(&artifact.bytes),
            bytes: artifact.bytes.len() as u64,
            documents: snapshot
                .collections
                .values()
                .map(|c| c.eid_fields.len() as u64)
                .sum(),
        })
        .collect();

    let virtual_bucket_count = map.virtual_bucket_count();
    let mut manifest = BackupManifest {
        version: BACKUP_MANIFEST_VERSION,
        virtual_bucket_count,
        physical_shard_count: count,
        shard_map_version: map.version(),
        assignments: (0..virtual_bucket_count)
            .map(|b| map.assignment_for_bucket(b).expect("bucket in range"))
            .collect(),
        collections,
        shards,
        backup_id: String::new(),
    };
    manifest.backup_id = identity(&manifest);
    Ok(manifest)
}

/// Parse and check a manifest: version, topology, shard set and identity.
pub fn read_backup_manifest(bytes: &[u8]) -> Result<BackupManifest, ManifestError> {
    let manifest: BackupManifest =
        serde_json::from_slice(bytes).map_err(|e| ManifestError::Malformed(e.to_string()))?;
    if manifest.version != BACKUP_MANIFEST_VERSION {
        return Err(ManifestError::UnsupportedVersion(manifest.version));
    }
    let count = manifest.physical_shard_count;
    if count == 0 {
        return Err(ManifestError::InvalidTopology(
            "physical_shard_count is 0".into(),
        ));
    }
    if manifest.assignments.len() != manifest.virtual_bucket_count as usize {
        return Err(ManifestError::InvalidTopology(format!(
            "{} assignments for {} virtual buckets",
            manifest.assignments.len(),
            manifest.virtual_bucket_count
        )));
    }
    if let Some(shard) = manifest.assignments.iter().find(|s| **s >= count) {
        return Err(ManifestError::InvalidTopology(format!(
            "a bucket is assigned to shard {shard}, outside 0..{count}"
        )));
    }

    let mut seen = BTreeSet::new();
    for entry in &manifest.shards {
        if entry.shard >= count {
            return Err(ManifestError::ShardOutOfRange {
                shard: entry.shard,
                count,
            });
        }
        if !seen.insert(entry.shard) {
            return Err(ManifestError::DuplicateShard(entry.shard));
        }
    }
    if let Some(missing) = (0..count).find(|s| !seen.contains(s)) {
        return Err(ManifestError::MissingShard(missing));
    }
    for entry in &manifest.shards {
        let hex = entry.sha256.len() == 64
            && entry
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if !hex {
            return Err(ManifestError::Malformed(format!(
                "shard {} sha256 is not 64 hex characters",
                entry.shard
            )));
        }
    }
    if identity(&manifest) != manifest.backup_id {
        return Err(ManifestError::IdentityMismatch);
    }
    Ok(manifest)
}

/// Check one stored artifact against its manifest entry.
pub fn verify_artifact(entry: &ShardEntry, bytes: &[u8]) -> Result<(), ManifestError> {
    if bytes.len() as u64 == entry.bytes && sha256_hex(bytes) == entry.sha256 {
        Ok(())
    } else {
        Err(ManifestError::DigestMismatch {
            key: entry.key.clone(),
        })
    }
}

#[cfg(test)]
mod tests;
