//! Read-only plan of a Standalone segment root, the source side of the
//! Standalone to Managed migration (faberline/lumen#22, slice 1a).
//!
//! The plan reads `CURRENT`, the generation manifest and the collection
//! segments. It never opens a `SegmentRdbStore`, which sweeps staging and can
//! write `CURRENT`, and it creates, renames or deletes no file.
//!
//! The in-use check is best effort. The code has no root lock file, so a
//! running node that writes after the check is not detected.
//!
//! `eid_rows` is an upper bound on live documents. It counts every distinct
//! external id found in the collection's base eid segment and in every delta
//! `local_rows` file, including ids that were later replaced or deleted.

use super::records::{parse_legacy_name, parse_revision_name};
use super::{AOF_COMPACT_TEMP_FILE, AOF_FILE, CURRENT_FILE, GENERATION_MANIFEST_FILE};
use crate::persistence::domain::generation_manifest::{SegmentKind, SegmentRole};
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::storage::infrastructure::segment::sparse_rows::decode_sparse_local_rows;
use crate::storage::infrastructure::segment::SegmentReader;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::Path;

const GENERATION_PREFIX: &str = "generation:";
const EMPTY_CURRENT: &str = "empty";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PlanRefusal {
    #[error("source has no CURRENT file: not a Standalone segment root")]
    MissingCurrent,
    #[error("cannot read CURRENT: {0}")]
    UnreadableCurrent(String),
    #[error("CURRENT must be a regular file")]
    CurrentNotRegularFile,
    #[error(
        "CURRENT is empty but generation {0} exists beside it; refusing to pick another generation"
    )]
    CurrentEmptyWithUnpointedGeneration(String),
    #[error("CURRENT names `{0}`, which is not a supported generation name")]
    UnknownGenerationFormat(String),
    #[error("CURRENT points at {0}, which does not exist")]
    GenerationMissing(String),
    #[error("cannot read generation manifest: {0}")]
    UnreadableGeneration(String),
    #[error("cannot read collection segment: {0}")]
    UnreadableCollection(String),
    #[error("source looks in use ({0}); stop the running node first")]
    InUse(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CollectionPlan {
    pub id: String,
    /// Distinct external ids of the collection; an upper bound on live documents.
    pub eid_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SourcePlan {
    /// The generation name without the `generation:` prefix, for example `gen-42-rev-1`.
    pub generation: Option<String>,
    pub collections: Vec<CollectionPlan>,
    /// Lowercase hex SHA-256 of `CURRENT` bytes, then `0x00`, then the manifest bytes.
    pub source_identity: String,
}

/// Plans a Standalone segment root without writing to it.
pub fn plan_standalone_source(root: &Path) -> Result<SourcePlan, PlanRefusal> {
    check_current_is_regular_file(root)?;
    check_not_in_use(root)?;

    let raw = std::fs::read(root.join(CURRENT_FILE))
        .map_err(|error| PlanRefusal::UnreadableCurrent(error.to_string()))?;
    let text = String::from_utf8_lossy(&raw);
    let line = text.strip_suffix('\n').unwrap_or(&text);

    if line == EMPTY_CURRENT {
        if let Some(name) = child_names(root)?
            .into_iter()
            .find(|name| is_generation_name(name))
        {
            return Err(PlanRefusal::CurrentEmptyWithUnpointedGeneration(name));
        }
        return Ok(SourcePlan {
            generation: None,
            collections: Vec::new(),
            source_identity: identity(&raw, None),
        });
    }

    let name = line
        .strip_prefix(GENERATION_PREFIX)
        .filter(|name| is_generation_name(name))
        .ok_or_else(|| PlanRefusal::UnknownGenerationFormat(line.to_string()))?;
    let dir = root.join(name);
    let is_real_dir = std::fs::symlink_metadata(&dir).is_ok_and(|meta| meta.is_dir());
    if !is_real_dir {
        return Err(PlanRefusal::GenerationMissing(name.to_string()));
    }

    let manifest = read_generation_manifest(&dir)
        .map_err(|error| PlanRefusal::UnreadableGeneration(format!("{error:#}")))?;
    let manifest_bytes = std::fs::read(dir.join(GENERATION_MANIFEST_FILE))
        .map_err(|error| PlanRefusal::UnreadableGeneration(error.to_string()))?;

    let mut collections = Vec::with_capacity(manifest.collections.len());
    for catalog in &manifest.collections {
        let mut ids = BTreeSet::new();
        for segment in &catalog.segments {
            if segment.role == SegmentRole::CollectionEids && segment.kind == SegmentKind::Base {
                let reader = SegmentReader::open(&dir.join(&segment.path))
                    .map_err(|error| PlanRefusal::UnreadableCollection(format!("{error:#}")))?;
                let eids = reader.eids_all().ok_or_else(|| {
                    PlanRefusal::UnreadableCollection(format!(
                        "{}: eid column is torn",
                        segment.path
                    ))
                })?;
                ids.extend(eids);
            }
            if let Some(local) = &segment.local_rows {
                let rows = decode_sparse_local_rows(&dir.join(&local.path), local.count)
                    .map_err(|error| PlanRefusal::UnreadableCollection(format!("{error:#}")))?;
                ids.extend(rows.into_external_ids());
            }
        }
        collections.push(CollectionPlan {
            id: catalog.collection_id.clone(),
            eid_rows: ids.len() as u64,
        });
    }
    collections.sort_by(|a, b| a.id.cmp(&b.id));

    Ok(SourcePlan {
        generation: Some(name.to_string()),
        collections,
        source_identity: identity(&raw, Some(&manifest_bytes)),
    })
}

fn check_current_is_regular_file(root: &Path) -> Result<(), PlanRefusal> {
    match std::fs::symlink_metadata(root.join(CURRENT_FILE)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Err(PlanRefusal::MissingCurrent)
        }
        Err(error) => Err(PlanRefusal::UnreadableCurrent(error.to_string())),
        Ok(meta) if meta.file_type().is_symlink() || !meta.is_file() => {
            Err(PlanRefusal::CurrentNotRegularFile)
        }
        Ok(_) => Ok(()),
    }
}

/// Best effort: a staging leftover or a non-empty append log means a writer
/// may be running. No lock exists in this code, so this cannot prove that
/// nothing writes.
fn check_not_in_use(root: &Path) -> Result<(), PlanRefusal> {
    for name in child_names(root)? {
        if is_staging_name(&name) {
            return Err(PlanRefusal::InUse(name));
        }
    }
    let aof_is_non_empty =
        std::fs::symlink_metadata(root.join(AOF_FILE)).is_ok_and(|meta| meta.len() > 0);
    if aof_is_non_empty {
        return Err(PlanRefusal::InUse("aof.log is not empty".to_string()));
    }
    Ok(())
}

/// Direct child names of `root`, sorted.
fn child_names(root: &Path) -> Result<Vec<String>, PlanRefusal> {
    let entries = std::fs::read_dir(root)
        .map_err(|error| PlanRefusal::UnreadableCurrent(error.to_string()))?;
    let mut names = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| PlanRefusal::UnreadableCurrent(error.to_string()))?;
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    Ok(names)
}

fn is_staging_name(name: &str) -> bool {
    name == "CURRENT.tmp"
        || name == AOF_COMPACT_TEMP_FILE
        || name.starts_with(".stage-gen-")
        || is_gen_tmp_name(name)
}

/// `.gen-<digits>.tmp`
fn is_gen_tmp_name(name: &str) -> bool {
    name.strip_prefix(".gen-")
        .and_then(|rest| rest.strip_suffix(".tmp"))
        .is_some_and(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
}

fn is_generation_name(name: &str) -> bool {
    parse_legacy_name(name).is_some() || parse_revision_name(name).is_some()
}

/// Lowercase hex SHA-256 of `current`, then, when present, a `0x00` byte and `manifest`.
fn identity(current: &[u8], manifest: Option<&[u8]>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(current);
    if let Some(manifest) = manifest {
        hasher.update([0u8]);
        hasher.update(manifest);
    }
    format!("{:x}", hasher.finalize())
}
