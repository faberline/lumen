//! Delta integrity: validating a field delta against its manifest entry, and
//! the payload digests of base and delta segments.

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentReference, SegmentRole,
};
use crate::persistence::infrastructure::segment::sparse_rows::decode_sparse_local_rows;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::field_deltas::delta_path_prefix;
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::{
    collection_checkpoint_dir_name, flat_layout, flat_payload_name,
};
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::PriorCatalog;
use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

pub(super) fn validate_field_delta(
    root: &Path,
    checkpoint_sequence: u64,
    collection: &CollectionCatalog,
    segment: &SegmentReference,
    fields: &BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec>,
    paths: &mut BTreeSet<String>,
    ordinals: &mut BTreeMap<String, u32>,
    prior: PriorCatalog<'_>,
) -> Result<()> {
    let field = segment
        .field
        .as_deref()
        .ok_or_else(|| anyhow!("delta must name a field"))?;
    if !matches!(segment.role, SegmentRole::Field)
        || !fields.get(field).is_some_and(|spec| {
            matches!(
                spec.field_type,
                crate::shared_kernel::types::schema::FieldType::Keyword
                    | crate::shared_kernel::types::schema::FieldType::Number
                    | crate::shared_kernel::types::schema::FieldType::Set
                    | crate::shared_kernel::types::schema::FieldType::Hash
                    | crate::shared_kernel::types::schema::FieldType::Text
                    | crate::shared_kernel::types::schema::FieldType::Vector
            )
        })
    {
        bail!("unsupported delta field type or role");
    }
    let ordinal = ordinals.entry(field.to_owned()).or_default();
    if segment.ordinal <= *ordinal {
        bail!("delta ordinals must be strictly increasing and ordered");
    }
    *ordinal = segment.ordinal;
    let local = segment
        .local_rows
        .as_ref()
        .ok_or_else(|| anyhow!("delta must include local rows"))?;
    let (expected_segment_path, expected_rows_path) = if flat_layout(collection) {
        let field_dir = collection_checkpoint_dir_name(field);
        (
            flat_payload_name(
                &collection.collection_id,
                Path::new(&format!("__delta/{field_dir}/{}.lseg", segment.ordinal)),
            ),
            flat_payload_name(
                &collection.collection_id,
                Path::new(&format!(
                    "__delta/{field_dir}/{}.rows.cbor",
                    segment.ordinal
                )),
            ),
        )
    } else {
        let prefix = delta_path_prefix(&collection.collection_id, field, segment.ordinal);
        (format!("{prefix}.lseg"), format!("{prefix}.rows.cbor"))
    };
    if segment.path != expected_segment_path
        || local.path != expected_rows_path
        || local.format != "lumen-local-eids-cbor-v1"
        || local.count == 0
    {
        bail!("invalid delta path or local row format");
    }
    for path in [&segment.path, &local.path] {
        if !paths.insert(path.clone()) {
            bail!("duplicate delta reference");
        }
        let metadata = std::fs::symlink_metadata(root.join(path))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!("delta reference must be a regular file");
        }
    }
    let reader = SegmentReader::open(&root.join(&segment.path))?;
    if reader.n_docs() != local.count
        || reader.applied_seq()
            != segment
                .applied_seq
                .ok_or_else(|| anyhow!("v2 delta is missing applied_seq"))?
        || reader.applied_seq() > checkpoint_sequence
    {
        bail!("delta row count or sequence does not match catalog");
    }
    let expected = segment
        .payload_sha256
        .as_deref()
        .ok_or_else(|| anyhow!("v2 delta is missing payload_sha256"))?;
    if !is_inherited_delta(root, collection, segment, prior)? {
        decode_sparse_local_rows(&root.join(&local.path), local.count)?;
        if delta_payload_sha256(&root.join(&segment.path), &root.join(&local.path))? != expected {
            bail!("delta payload checksum does not match catalog");
        }
    }
    Ok(())
}

/// Reuse validation only for the exact prior reference and the same immutable
/// hard-linked files. Cold recovery never supplies a predecessor.
pub(super) fn is_inherited_delta(
    root: &Path,
    collection: &CollectionCatalog,
    segment: &SegmentReference,
    prior: PriorCatalog<'_>,
) -> Result<bool> {
    let Some((old_root, manifest)) = prior else {
        return Ok(false);
    };
    let Some(old) = manifest.collections.iter().find(|old| {
        old.collection_id == collection.collection_id
            && old.collection_generation == collection.collection_generation
            && old.schema_version == collection.schema_version
            && old.schema == collection.schema
    }) else {
        return Ok(false);
    };
    let reference = serde_json::to_value(segment)?;
    let mut matched = false;
    for old_segment in &old.segments {
        if serde_json::to_value(old_segment)? == reference {
            matched = true;
            break;
        }
    }
    if !matched {
        return Ok(false);
    }
    for relative in
        std::iter::once(&segment.path).chain(segment.local_rows.iter().map(|rows| &rows.path))
    {
        let old_file = std::fs::symlink_metadata(old_root.join(relative))?;
        let new_file = std::fs::symlink_metadata(root.join(relative))?;
        if !old_file.is_file() || !new_file.is_file() {
            return Ok(false);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if old_file.dev() != new_file.dev() || old_file.ino() != new_file.ino() {
                return Ok(false);
            }
        }
        #[cfg(not(unix))]
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn base_payload_sha256(path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"lumen.base.payload-sha256.v1\0");
    hash_delta_component(&mut hasher, b"segment", path)?;
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Hash the two immutable delta files as separate, length-framed components.
/// The framing prevents a payload/row-map boundary ambiguity. Input is read
/// in fixed-size chunks, and the observed count must equal the metadata length.
pub(super) fn delta_payload_sha256(segment: &Path, rows: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"lumen.delta.payload-sha256.v1\0");
    for (tag, path) in [(b"segment".as_slice(), segment), (b"rows".as_slice(), rows)] {
        hash_delta_component(&mut hasher, tag, path)?;
    }
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(test)]
thread_local! { pub(super) static CHECKSUM_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }

fn hash_delta_component(hasher: &mut Sha256, tag: &[u8], path: &Path) -> Result<()> {
    let expected = std::fs::metadata(path)
        .with_context(|| format!("inspect delta payload component {}", path.display()))?
        .len();
    hasher.update((tag.len() as u64).to_be_bytes());
    hasher.update(tag);
    hasher.update(expected.to_be_bytes());
    let mut file = File::open(path)
        .with_context(|| format!("open delta payload component {}", path.display()))?;
    let mut observed = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("read delta payload component {}", path.display()))?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(count as u64)
            .ok_or_else(|| anyhow!("delta payload length overflow"))?;
        if observed > expected {
            bail!(
                "delta payload component changed while hashing: {}",
                path.display()
            );
        }
        #[cfg(test)]
        CHECKSUM_BYTES.with(|bytes| bytes.set(bytes.get() + count as u64));
        hasher.update(&buffer[..count]);
    }
    if observed != expected {
        bail!(
            "delta payload component changed while hashing: {}",
            path.display()
        );
    }
    Ok(())
}
