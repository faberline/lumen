//! The collection catalog a generation publishes, and the checks that every
//! segment it references is canonical, present and consistent with its
//! predecessor.

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentFormat, SegmentGenerationManifest, SegmentKind, SegmentReference,
    SegmentRole,
};
use crate::persistence::infrastructure::segment::sparse_rows::decode_sparse_local_rows;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::{
    base_payload_sha256, delta_payload_sha256, is_inherited_delta, validate_field_delta,
};
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::{
    collection_checkpoint_dir_name, validate_flat_catalog_references,
};
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::PriorCatalog;
use crate::persistence::infrastructure::segment_rdb_store::{
    CHECKPOINT_SCHEMA_FILE, GENERATION_MANIFEST_FILE, GENERATION_MANIFEST_V3,
};
use anyhow::{anyhow, bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Build the v2 catalog from the staged, self-contained checkpoint tree.  The
/// catalog never points at a predecessor: every referenced byte is below the
/// generation being published, which keeps retained checkpoints independently
/// reopenable after their predecessors are pruned.
pub(super) fn catalog_collections(
    root: &Path,
    checkpoint_sequence: u64,
) -> Result<Vec<CollectionCatalog>> {
    let mut collections = Vec::new();
    for entry in std::fs::read_dir(root)
        .with_context(|| format!("read staged checkpoint {}", root.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.file_name().and_then(|name| name.to_str()) == Some(GENERATION_MANIFEST_FILE) {
            continue;
        }
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!(
                "catalogued collection must be a real directory: {}",
                path.display()
            );
        }
        let collection_id = crate::storage::collection_name_from_dir(&path)
            .ok_or_else(|| anyhow!("undecodable checkpoint subdir {}", path.display()))?;
        let collection_dir = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("non-utf8 checkpoint subdir {}", path.display()))?
            .to_owned();
        let schema_path = path.join(CHECKPOINT_SCHEMA_FILE);
        let schema: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&schema_path)
                .with_context(|| format!("read checkpoint schema {}", schema_path.display()))?,
        )
        .with_context(|| format!("decode checkpoint schema {}", schema_path.display()))?;
        let schema_version = schema
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                anyhow!(
                    "checkpoint schema has no version: {}",
                    schema_path.display()
                )
            })?
            .try_into()
            .context("checkpoint schema version exceeds u32")?;
        let layout = crate::storage::CheckpointLayout::from_sidecar(&schema)?;
        let fields = schema
            .get("fields")
            .cloned()
            .ok_or_else(|| anyhow!("checkpoint schema has no fields: {}", schema_path.display()))?;
        // Derive roles from schema: a keyword called `tag.eids` is not a vector sidecar.
        let specs: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
            serde_json::from_value(fields.clone())?;
        let mut segments = vec![SegmentReference {
            role: SegmentRole::CollectionEids,
            field: None,
            ordinal: 0,
            kind: SegmentKind::Base,
            format: SegmentFormat::LsegV1,
            path: format!("{collection_dir}/_collection.lmeta.lseg"),
            local_rows: None,
            applied_seq: None,
            payload_sha256: None,
        }];
        for (name, spec) in specs {
            let stem = layout.field_stem(&name);
            segments.push(SegmentReference {
                role: SegmentRole::Field,
                field: Some(name.clone()),
                ordinal: 0,
                kind: SegmentKind::Base,
                format: SegmentFormat::LsegV1,
                path: format!("{collection_dir}/{stem}.lseg"),
                local_rows: None,
                applied_seq: None,
                payload_sha256: None,
            });
            if spec.field_type == crate::shared_kernel::types::schema::FieldType::Vector {
                segments.push(SegmentReference {
                    role: SegmentRole::VectorEids,
                    field: Some(name.clone()),
                    ordinal: 0,
                    kind: SegmentKind::Base,
                    format: SegmentFormat::LsegV1,
                    path: format!("{collection_dir}/{stem}.eids.lseg"),
                    local_rows: None,
                    applied_seq: None,
                    payload_sha256: None,
                });
            }
        }
        collections.push(CollectionCatalog {
            collection_id,
            // First v2 publication assigns durable ids. Subsequent reuse-aware
            // saves replace this provisional value with the inherited id.
            collection_generation: 0,
            schema_version,
            data_version: checkpoint_sequence,
            schema: fields,
            segments,
        });
    }
    collections.sort_by(|left, right| left.collection_id.cmp(&right.collection_id));
    for (index, collection) in collections.iter_mut().enumerate() {
        collection.collection_generation = (index as u64) + 1;
    }
    Ok(collections)
}

pub(super) fn validate_catalog_references(
    root: &Path,
    manifest: &SegmentGenerationManifest,
) -> Result<()> {
    validate_catalog_references_with_prior(root, manifest, None)
}

pub(super) fn validate_catalog_references_with_prior(
    root: &Path,
    manifest: &SegmentGenerationManifest,
    prior: PriorCatalog<'_>,
) -> Result<()> {
    if manifest.schema_version == GENERATION_MANIFEST_V3 {
        return validate_flat_catalog_references(root, manifest, prior);
    }
    let mut paths = BTreeSet::new();
    let mut catalog_ids = BTreeSet::new();
    let mut generations = BTreeSet::new();
    if manifest.next_collection_generation == 0 {
        bail!("invalid collection generation allocator");
    }
    let mut disk_ids = BTreeSet::new();
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            disk_ids.insert(
                crate::storage::collection_name_from_dir(&path)
                    .ok_or_else(|| anyhow!("undecodable checkpoint subdir {}", path.display()))?,
            );
        }
    }
    for collection in &manifest.collections {
        if !catalog_ids.insert(collection.collection_id.clone()) {
            bail!("duplicate catalog collection");
        }
        if collection.collection_generation == 0
            || collection.collection_generation >= manifest.next_collection_generation
            || !generations.insert(collection.collection_generation)
        {
            bail!("invalid or duplicate collection generation");
        }
        let collection_dir = collection_checkpoint_dir_name(&collection.collection_id);
        let disk_dir = root.join(&collection_dir);
        let sidecar: serde_json::Value =
            serde_json::from_slice(&std::fs::read(disk_dir.join(CHECKPOINT_SCHEMA_FILE))?)?;
        let layout = crate::storage::CheckpointLayout::from_sidecar(&sidecar)?;
        if sidecar.get("fields") != Some(&collection.schema) {
            bail!("catalog schema does not match checkpoint schema");
        }
        if sidecar.get("version").and_then(serde_json::Value::as_u64)
            != Some(collection.schema_version as u64)
        {
            bail!("catalog schema version does not match checkpoint schema");
        }
        let fields: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
            serde_json::from_value(collection.schema.clone()).context("decode catalog schema")?;
        let mut expected = BTreeSet::from([format!("{collection_dir}/_collection.lmeta.lseg")]);
        for (name, spec) in &fields {
            let stem = layout.field_stem(name);
            expected.insert(format!("{collection_dir}/{stem}.lseg"));
            if spec.field_type == crate::shared_kernel::types::schema::FieldType::Vector {
                expected.insert(format!("{collection_dir}/{stem}.eids.lseg"));
            }
        }
        let mut present = BTreeSet::new();
        let mut ordinals = BTreeMap::<String, u32>::new();
        let mut delta_counts = BTreeMap::<String, usize>::new();
        for segment in &collection.segments {
            if matches!(segment.kind, SegmentKind::Delta) {
                let field = segment
                    .field
                    .clone()
                    .ok_or_else(|| anyhow!("delta must name a field"))?;
                let count = delta_counts.entry(field).or_default();
                *count += 1;
                if *count > 16 {
                    bail!("field exceeds sixteen delta segments");
                }
                validate_field_delta(
                    root,
                    manifest.checkpoint_sequence,
                    collection,
                    segment,
                    &fields,
                    &mut paths,
                    &mut ordinals,
                    prior,
                )?;
                continue;
            }
            if segment.ordinal != 0 {
                bail!("unsupported base segment layout");
            }
            match segment.role {
                SegmentRole::CollectionEids if segment.field.is_some() => {
                    bail!("collection_eids segment must not name a field")
                }
                SegmentRole::Field | SegmentRole::VectorEids if segment.field.is_none() => {
                    bail!("field segment must name a field")
                }
                _ => {}
            }
            let expected_path = match segment.role {
                SegmentRole::CollectionEids => format!("{collection_dir}/_collection.lmeta.lseg"),
                SegmentRole::Field | SegmentRole::VectorEids => {
                    let name = segment.field.as_ref().unwrap();
                    let stem = layout.field_stem(name);
                    let spec = fields
                        .get(name)
                        .ok_or_else(|| anyhow!("catalog segment field is absent from schema"))?;
                    if matches!(segment.role, SegmentRole::VectorEids) {
                        if spec.field_type != crate::shared_kernel::types::schema::FieldType::Vector
                        {
                            bail!("vector_eids segment must name a vector field");
                        }
                        format!("{collection_dir}/{stem}.eids.lseg")
                    } else {
                        format!("{collection_dir}/{stem}.lseg")
                    }
                }
            };
            let relative = Path::new(&segment.path);
            if relative.is_absolute()
                || segment
                    .path
                    .split('/')
                    .any(|part| matches!(part, "" | "." | ".."))
            {
                bail!(
                    "segment reference path escapes generation: {}",
                    segment.path
                );
            }
            if segment.path != expected_path {
                bail!("segment reference does not match its collection and field");
            }
            present.insert(segment.path.clone());
            if !paths.insert(segment.path.clone()) {
                bail!("duplicate segment reference: {}", segment.path);
            }
            let target = root.join(relative);
            let metadata = std::fs::symlink_metadata(&target)
                .map_err(|_| anyhow!("catalogued segment is missing: {}", target.display()))?;
            if metadata.file_type().is_symlink() {
                bail!(
                    "catalogued segment must not be a symlink: {}",
                    target.display()
                );
            }
            if !metadata.is_file() {
                bail!(
                    "catalogued segment must be a regular file: {}",
                    target.display()
                );
            }
            let expected_checksum = segment
                .payload_sha256
                .as_deref()
                .ok_or_else(|| anyhow!("v2 base is missing payload_sha256"))?;
            if let Some(local) = &segment.local_rows {
                if !matches!(segment.role, SegmentRole::Field)
                    || local.path
                        != format!(
                            "{}.rows.cbor",
                            segment
                                .path
                                .strip_suffix(".lseg")
                                .ok_or_else(|| anyhow!("mapped base path has no suffix"))?
                        )
                    || local.format != "lumen-local-eids-cbor-v1"
                {
                    bail!("unsupported mapped base row layout");
                }
                if !paths.insert(local.path.clone()) {
                    bail!("duplicate mapped base row reference");
                }
                let rows_path = root.join(&local.path);
                let metadata = std::fs::symlink_metadata(&rows_path)?;
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    bail!("mapped base rows must be a regular file");
                }
                let reader = SegmentReader::open(&target)?;
                if reader.n_docs() != local.count
                    || reader.applied_seq()
                        != segment
                            .applied_seq
                            .ok_or_else(|| anyhow!("mapped base has no sequence"))?
                    || reader.applied_seq() > manifest.checkpoint_sequence
                {
                    bail!("mapped base row count or sequence differs from catalog");
                }
                if !is_inherited_delta(root, collection, segment, prior)? {
                    let rows = decode_sparse_local_rows(&rows_path, local.count)?;
                    if delta_payload_sha256(&target, &rows_path)? != expected_checksum {
                        bail!("mapped base checksum differs from catalog");
                    }
                    let field = segment.field.as_deref().expect("mapped field validated");
                    if fields[field].field_type
                        == crate::shared_kernel::types::schema::FieldType::Vector
                    {
                        let stem = layout.field_stem(field);
                        let ids = SegmentReader::open(&disk_dir.join(format!("{stem}.eids.lseg")))?
                            .eids_all()
                            .ok_or_else(|| anyhow!("mapped vector EID sidecar is torn"))?;
                        if ids.len() != local.count as usize
                            || ids.iter().enumerate().any(|(row, eid)| {
                                rows.external_id(row as u32) != Some(eid.as_str())
                            })
                        {
                            bail!("mapped vector row map differs from EID sidecar");
                        }
                    }
                }
            } else if !is_inherited_delta(root, collection, segment, prior)?
                && base_payload_sha256(&target)? != expected_checksum
            {
                bail!("base payload checksum does not match catalog");
            }
        }
        if present != expected {
            bail!("catalog is missing a required base segment");
        }
    }
    if catalog_ids != disk_ids {
        bail!("catalog collection set does not match checkpoint");
    }
    Ok(())
}
