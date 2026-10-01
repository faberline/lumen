//! The checkpoint directory on disk: the `_schema.json` sidecar that tells a
//! reopen each field's type, the field layout under a collection, the
//! hex-encoded collection directory names, and the hard-link copy of a
//! checkpoint origin.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use crate::shared_kernel::types::schema::FieldSpec;

#[cfg(test)]
thread_local! {
    pub(crate) static CHECKPOINT_WRITE_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

pub(in crate::index) fn checkpoint_write_boundary() {
    #[cfg(test)]
    CHECKPOINT_WRITE_HOOK.with(|hook| {
        let hook = hook.borrow_mut().take();
        if let Some(hook) = hook {
            hook();
        }
    });
}

pub(in crate::index) fn hard_link_checkpoint_tree(
    origin: &std::path::Path,
    target: &std::path::Path,
) -> Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(origin)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            bail!("checkpoint origin contains a symlink");
        }
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            hard_link_checkpoint_tree(&entry.path(), &destination)?;
        } else if metadata.is_file() {
            std::fs::hard_link(entry.path(), destination)
                .map_err(|e| anyhow!("hard link checkpoint origin: {e}"))?;
        } else {
            bail!("checkpoint origin contains a nonregular file");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Production checkpoint (Stage 2 Phase 2f-2): the disk engine as the running
// binary's persistence — a segment checkpoint supersedes the CBOR RDB.
// ---------------------------------------------------------------------------
//
// A checkpoint is a directory `dir/<collection>/` per collection, each holding
// `<field>.lseg` segments, the `_collection.lmeta.lseg` EID column, any vector
// `<field>.eids.lseg` sidecars, and a `_schema.json` (the field specs + version
// + applied_seq, carried out-of-band so reopen knows each field's type without a
// CBOR snapshot). `flush_to_segments` is the periodic snapshotter's call:
// re-seal-capable (`Collection::seal_to_segments` gathers base-doc values through
// the segment-aware dispatch, so a checkpoint AFTER a prior seal+drop is correct),
// idempotent, and repeatable. `reopen_from_segment_dir` is cold-start: reopen
// every collection via `Collection::open_from_segments` (no whole-collection load)
// and return the max applied_seq so the WAL tail replays from there.
//
// Atomicity is the caller's (`SegmentRdbStore`): it stages a whole generation
// under a temp dir and atomically renames it into place, so a torn checkpoint
// never replaces a good one. `flush_to_segments` writes into whatever `dir` it is
// handed; it does not own the atomic-rename.

/// Per-collection checkpoint sidecar persisted next to the segments so a reopen
/// knows each field's type + the collection version + the WAL position the seal
/// is current as of — the schema the live `Collection::open_from_segments` needs
/// out-of-band. Phase 2f-2.
#[derive(Debug, Serialize, Deserialize)]
pub(in crate::index) struct CheckpointSchema {
    pub(in crate::index) version: u32,
    pub(in crate::index) applied_seq: u64,
    pub(in crate::index) fields: BTreeMap<String, FieldSpec>,
    #[serde(default)]
    pub(in crate::index) segment_layout: CheckpointLayout,
}

/// The marker is absent in shipped checkpoints. Public field names never
/// become path components in newly published generations.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum CheckpointLayout {
    #[default]
    #[serde(rename = "legacy-raw-v0")]
    Legacy,
    #[serde(rename = "encoded-fields-v1")]
    Encoded,
}

impl CheckpointLayout {
    pub(crate) fn field_stem(self, name: &str) -> String {
        match self {
            Self::Legacy => name.to_owned(),
            Self::Encoded => format!("fields/{}", collection_dir_name(name)),
        }
    }

    pub(crate) fn from_sidecar(sidecar: &serde_json::Value) -> Result<Self> {
        sidecar
            .get("segment_layout")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map(|layout| layout.unwrap_or_default())
            .map_err(Into::into)
    }
}

pub(in crate::index) const CHECKPOINT_SCHEMA_FILE: &str = "_schema.json";

/// A collection id → filename-safe subdir name (hex-encoded), so any
/// collection id is a valid directory.
pub(in crate::index) fn collection_dir_name(name: &str) -> String {
    name.bytes().map(|b| format!("{b:02x}")).collect()
}

/// Decode a checkpoint subdir's hex-encoded name back to the collection id.
/// `None` if the leaf is not valid hex (a stray file/dir in the checkpoint).
pub(crate) fn collection_name_from_dir(dir: &std::path::Path) -> Option<String> {
    let leaf = dir.file_name()?.to_str()?;
    if leaf.is_empty() || leaf.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(leaf.len() / 2);
    let raw = leaf.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    String::from_utf8(bytes).ok()
}
