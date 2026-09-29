//! The flat payload layout: where each collection's schema and segments live
//! inside a generation, materializing the reopen tree from it, and checking the
//! catalog against it.

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentGenerationManifest, SegmentKind, SegmentRole,
};
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::{
    base_payload_sha256, delta_payload_sha256, is_inherited_delta,
};
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::PriorCatalog;
use crate::persistence::infrastructure::segment_rdb_store::{
    CHECKPOINT_SCHEMA_FILE, FLAT_PAYLOAD_DIR,
};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub(super) fn validate_flat_catalog_references(
    root: &Path,
    manifest: &SegmentGenerationManifest,
    prior: PriorCatalog<'_>,
) -> Result<()> {
    let mut paths = BTreeSet::new();
    let mut generations = BTreeSet::new();
    for collection in &manifest.collections {
        if collection.collection_generation == 0
            || !generations.insert(collection.collection_generation)
        {
            bail!("invalid or duplicate collection generation");
        }
        let schema = serde_json::from_slice::<serde_json::Value>(&std::fs::read(
            collection_schema_path(root, collection),
        )?)?;
        if schema.get("fields") != Some(&collection.schema) {
            bail!("flat catalog schema does not match checkpoint schema");
        }
        let mut delta_counts = BTreeMap::<String, usize>::new();
        for segment in &collection.segments {
            if matches!(segment.kind, SegmentKind::Delta) {
                let field = segment
                    .field
                    .as_ref()
                    .ok_or_else(|| anyhow!("delta must name a field"))?;
                let count = delta_counts.entry(field.clone()).or_default();
                *count += 1;
                if *count > 16 {
                    bail!("field exceeds sixteen delta segments");
                }
            }
            if Path::new(&segment.path).is_absolute()
                || segment
                    .path
                    .split('/')
                    .any(|part| matches!(part, "" | "." | ".."))
            {
                bail!("flat segment path escapes generation");
            }
            if !paths.insert(segment.path.clone()) {
                bail!("duplicate flat segment reference: {}", segment.path);
            }
            let target = root.join(&segment.path);
            let metadata = std::fs::symlink_metadata(&target).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    anyhow!("catalogued segment is missing: {}", target.display())
                } else {
                    anyhow::Error::new(error)
                        .context(format!("inspect flat segment {}", target.display()))
                }
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                bail!("flat segment is not a regular file: {}", target.display());
            }
            if let Some(local) = &segment.local_rows {
                if !paths.insert(local.path.clone()) {
                    bail!("duplicate flat row reference: {}", local.path);
                }
                let rows = root.join(&local.path);
                if !std::fs::symlink_metadata(&rows)
                    .with_context(|| format!("inspect flat row map {}", rows.display()))?
                    .is_file()
                {
                    bail!("flat row map is not a regular file");
                }
                if !is_inherited_delta(root, collection, segment, prior)?
                    && delta_payload_sha256(&target, &rows)?
                        != segment.payload_sha256.as_deref().unwrap_or_default()
                {
                    bail!("flat payload checksum does not match catalog");
                }
            } else if !is_inherited_delta(root, collection, segment, prior)?
                && base_payload_sha256(&target)?
                    != segment.payload_sha256.as_deref().unwrap_or_default()
            {
                bail!("flat base checksum does not match catalog");
            }
        }
    }
    Ok(())
}

pub(in crate::persistence) fn collection_checkpoint_dir_name(name: &str) -> String {
    name.bytes().map(|byte| format!("{byte:02x}")).collect()
}

pub(in crate::persistence) fn flat_payload_name(collection: &str, relative: &Path) -> String {
    let mut value = collection
        .bytes()
        .chain(std::iter::once(b'_'))
        .collect::<Vec<_>>();
    for byte in relative.to_string_lossy().bytes() {
        value.extend(format!("{byte:02x}").bytes());
    }
    format!(
        "{}/{}",
        FLAT_PAYLOAD_DIR,
        String::from_utf8(value).expect("ascii")
    )
}

pub(super) fn flat_layout(collection: &CollectionCatalog) -> bool {
    collection
        .segments
        .iter()
        .any(|segment| segment.path.starts_with("payload/"))
}

pub(super) fn collection_schema_path(root: &Path, collection: &CollectionCatalog) -> PathBuf {
    if flat_layout(collection) {
        root.join(flat_payload_name(
            &collection.collection_id,
            Path::new(CHECKPOINT_SCHEMA_FILE),
        ))
    } else {
        root.join(collection_checkpoint_dir_name(&collection.collection_id))
            .join(CHECKPOINT_SCHEMA_FILE)
    }
}

pub(super) fn collection_output_path(
    root: &Path,
    collection: &CollectionCatalog,
    relative: &str,
) -> PathBuf {
    if flat_layout(collection) {
        root.join(flat_payload_name(
            &collection.collection_id,
            Path::new(relative),
        ))
    } else {
        root.join(collection_checkpoint_dir_name(&collection.collection_id))
            .join(relative)
    }
}

pub(super) fn collection_output_relative(collection: &CollectionCatalog, relative: &str) -> String {
    if flat_layout(collection) {
        flat_payload_name(&collection.collection_id, Path::new(relative))
    } else {
        format!(
            "{}/{}",
            collection_checkpoint_dir_name(&collection.collection_id),
            relative
        )
    }
}

pub(super) fn materialize_flat_reopen_tree(
    root: &Path,
    manifest: &SegmentGenerationManifest,
) -> Result<Vec<PathBuf>> {
    let mut created = Vec::new();
    for collection in &manifest.collections {
        let dir = root.join(collection_checkpoint_dir_name(&collection.collection_id));
        std::fs::create_dir_all(&dir)?;
        created.push(dir.clone());
        let schema = collection_schema_path(root, collection);
        let schema_alias = dir.join(CHECKPOINT_SCHEMA_FILE);
        std::fs::hard_link(&schema, &schema_alias).with_context(|| {
            format!(
                "link v3 collection schema {} -> {}",
                schema.display(),
                schema_alias.display()
            )
        })?;
        let sidecar: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&schema_alias).with_context(|| {
                format!("read materialized schema {}", schema_alias.display())
            })?)?;
        let layout = crate::storage::CheckpointLayout::from_sidecar(&sidecar)?;
        for segment in &collection.segments {
            let relative = if matches!(segment.kind, SegmentKind::Delta) {
                let field = segment
                    .field
                    .as_deref()
                    .ok_or_else(|| anyhow!("flat delta has no field"))?;
                format!(
                    "{}/__delta/{}/{}.lseg",
                    collection_checkpoint_dir_name(&collection.collection_id),
                    collection_checkpoint_dir_name(field),
                    segment.ordinal
                )
            } else {
                match segment.role {
                    SegmentRole::CollectionEids => format!(
                        "{}/_collection.lmeta.lseg",
                        collection_checkpoint_dir_name(&collection.collection_id)
                    ),
                    SegmentRole::Field => format!(
                        "{}/{}.lseg",
                        collection_checkpoint_dir_name(&collection.collection_id),
                        layout.field_stem(
                            segment
                                .field
                                .as_deref()
                                .ok_or_else(|| anyhow!("flat field has no name"))?
                        )
                    ),
                    SegmentRole::VectorEids => format!(
                        "{}/{}.eids.lseg",
                        collection_checkpoint_dir_name(&collection.collection_id),
                        layout.field_stem(
                            segment
                                .field
                                .as_deref()
                                .ok_or_else(|| anyhow!("flat vector field has no name"))?
                        )
                    ),
                }
            };
            let destination = root.join(&relative);
            std::fs::create_dir_all(destination.parent().unwrap())?;
            let source = root.join(&segment.path);
            std::fs::hard_link(&source, &destination).with_context(|| {
                format!(
                    "link v3 segment {} -> {}",
                    source.display(),
                    destination.display()
                )
            })?;
            if let Some(rows) = &segment.local_rows {
                let row_relative = format!(
                    "{}.rows.cbor",
                    relative.strip_suffix(".lseg").unwrap_or(&relative)
                );
                let row_destination = root.join(row_relative);
                let rows_source = root.join(&rows.path);
                std::fs::hard_link(&rows_source, &row_destination).with_context(|| {
                    format!(
                        "link v3 local rows {} -> {}",
                        rows_source.display(),
                        row_destination.display()
                    )
                })?;
            }
        }
    }
    Ok(created)
}

fn flatten_checkpoint_payload(root: &Path, collections: &mut [CollectionCatalog]) -> Result<()> {
    let payload = root.join(FLAT_PAYLOAD_DIR);
    std::fs::create_dir_all(&payload)?;
    for collection in collections {
        let source = root.join(collection_checkpoint_dir_name(&collection.collection_id));
        let mut files = Vec::new();
        let mut pending = vec![source.clone()];
        while let Some(dir) = pending.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                let metadata = std::fs::symlink_metadata(&path)?;
                if metadata.is_dir() {
                    pending.push(path);
                } else if metadata.is_file() {
                    files.push(path);
                } else {
                    bail!(
                        "checkpoint contains unsupported payload entry: {}",
                        path.display()
                    );
                }
            }
        }
        let mut moved = BTreeMap::new();
        for file in files {
            let relative = file.strip_prefix(&source)?;
            let old = format!(
                "{}/{}",
                collection_checkpoint_dir_name(&collection.collection_id),
                relative.to_string_lossy()
            );
            let target_rel = flat_payload_name(&collection.collection_id, relative);
            let target = root.join(&target_rel);
            std::fs::rename(&file, &target)?;
            moved.insert(old, target_rel);
        }
        for segment in &mut collection.segments {
            if let Some(path) = moved.get(&segment.path) {
                segment.path = path.clone();
            }
            if let Some(rows) = &mut segment.local_rows {
                if let Some(path) = moved.get(&rows.path) {
                    rows.path = path.clone();
                }
            }
        }
        let old_schema = format!(
            "{}/{}",
            collection_checkpoint_dir_name(&collection.collection_id),
            CHECKPOINT_SCHEMA_FILE
        );
        if !moved.contains_key(&old_schema) {
            bail!("collection schema was not staged");
        }
        std::fs::remove_dir_all(source)?;
    }
    Ok(())
}
