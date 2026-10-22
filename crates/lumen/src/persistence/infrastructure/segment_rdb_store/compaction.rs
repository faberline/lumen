//! Field-level immutable segment compaction for segment store staging trees.
//!
//! This is a child of `segment_rdb_store`: it deliberately uses that module's
//! catalog identities and checksum helpers. It reads every input before
//! changing either output pathname. Streaming writers publish a new inode by
//! rename; local-row and EID sidecars use the same discipline in
//! `sidecar_files`.

pub(crate) mod sidecar_files;

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, LocalRowsReference, SegmentFormat, SegmentKind, SegmentReference,
    SegmentRole,
};
use crate::persistence::infrastructure::composed_segment::replacement::compose_checkpoint_layers;
use crate::persistence::infrastructure::segment::sparse_rows::decode_sparse_local_rows;
use crate::persistence::infrastructure::segment::{stream, SegmentReader};
use crate::persistence::infrastructure::segment_rdb_store::compaction::sidecar_files::{
    checked_bytes, file_len, write_eids_atomic, write_sparse_rows_atomic,
};
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::{
    base_payload_sha256, delta_payload_sha256,
};
use crate::persistence::infrastructure::segment_rdb_store::field_deltas::delta_path_prefix;
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::{
    collection_checkpoint_dir_name, collection_output_relative, collection_schema_path, flat_layout,
};
use crate::shared_kernel::types::schema::FieldType;
use anyhow::{anyhow, bail, Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One field output prepared for a later catalog replacement. This function
/// never changes `collection.segments`, a manifest, or `CURRENT`.
#[derive(Debug, Clone)]
pub(in crate::persistence) struct CompactedField {
    pub(in crate::persistence) output: SegmentReference,
    pub(in crate::persistence) vector_eids: Option<SegmentReference>,
    pub(in crate::persistence) inputs: Vec<SegmentReference>,
    pub(in crate::persistence) logical_read_bytes: u64,
    pub(in crate::persistence) logical_write_bytes: u64,
}

/// Compact one field from immutable inputs in oldest-to-newest order.
///
/// `includes_base` accepts exactly the field's base plus every captured delta.
/// Without it, `inputs` must be one adjacent delta run. The returned reference
/// is only a candidate: the caller owns catalog replacement, validation, and
/// publication after this function returns successfully.
pub(in crate::persistence) fn write_compacted_field(
    root: &Path,
    sequence: u64,
    collection: &CollectionCatalog,
    field: &str,
    inputs: &[SegmentReference],
    includes_base: bool,
) -> Result<CompactedField> {
    validate_compaction_inputs(collection, field, inputs, includes_base)?;
    let specs: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
        serde_json::from_value(collection.schema.clone()).context("decode compaction schema")?;
    let spec = specs
        .get(field)
        .ok_or_else(|| anyhow!("compacted field is absent from schema: {field}"))?;
    if !matches!(
        spec.field_type,
        FieldType::Keyword
            | FieldType::Number
            | FieldType::Set
            | FieldType::Hash
            | FieldType::Text
            | FieldType::Vector
    ) {
        bail!("compaction does not support field type for {field}");
    }

    let mut logical_read_bytes = 0u64;
    let mut layers = Vec::with_capacity(inputs.len());
    for input in inputs {
        let segment_path = confined(root, &input.path)?;
        logical_read_bytes = checked_bytes(logical_read_bytes, file_len(&segment_path)?)?;
        let reader = Arc::new(SegmentReader::open(&segment_path)?);
        let ids = input_external_ids(root, collection, field, input, &mut logical_read_bytes)?;
        if reader.n_docs() as usize != ids.len() {
            bail!("compaction input row map does not match segment row count");
        }
        layers.push((reader, ids));
    }
    let (view, ids) = compose_checkpoint_layers(layers)?;

    let sidecar: serde_json::Value = serde_json::from_slice(
        &std::fs::read(collection_schema_path(root, collection))
            .context("read staged checkpoint schema for compaction")?,
    )?;
    if sidecar.get("fields") != Some(&collection.schema) {
        bail!("staged checkpoint schema differs from catalog during compaction");
    }
    let layout =
        crate::index::infrastructure::checkpoint_fs::CheckpointLayout::from_sidecar(&sidecar)?;
    let stem = layout.field_stem(field);
    let last = inputs.last().expect("validated non-empty");
    let (segment_rel, rows_rel, kind, ordinal) = if includes_base {
        let existing = inputs
            .iter()
            .find(|input| matches!(input.kind, SegmentKind::Base))
            .map(|input| input.path.clone());
        (
            existing
                .unwrap_or_else(|| collection_output_relative(collection, &format!("{stem}.lseg"))),
            collection_output_relative(collection, &format!("{stem}.rows.cbor")),
            SegmentKind::Base,
            0,
        )
    } else {
        let field_dir = collection_checkpoint_dir_name(field);
        let (segment_rel, rows_rel) = if flat_layout(collection) {
            (
                collection_output_relative(
                    collection,
                    &format!("__delta/{field_dir}/{}.lseg", last.ordinal),
                ),
                collection_output_relative(
                    collection,
                    &format!("__delta/{field_dir}/{}.rows.cbor", last.ordinal),
                ),
            )
        } else {
            let prefix = delta_path_prefix(&collection.collection_id, field, last.ordinal);
            (format!("{prefix}.lseg"), format!("{prefix}.rows.cbor"))
        };
        (segment_rel, rows_rel, SegmentKind::Delta, last.ordinal)
    };
    let segment_path = confined(root, &segment_rel)?;
    match spec.field_type {
        FieldType::Text => {
            stream::text_projection::write_text_stream(&segment_path, sequence, &view)?
        }
        FieldType::Keyword => {
            stream::keyword::write_keyword_stream(&segment_path, sequence, &view)?
        }
        FieldType::Set => stream::set::write_set_stream(&segment_path, sequence, &view)?,
        FieldType::Number => stream::number::write_number_stream(&segment_path, sequence, &view)?,
        FieldType::Hash => stream::hash::write_hash_stream(&segment_path, sequence, &view)?,
        FieldType::Vector => {
            let dim = spec
                .dim
                .ok_or_else(|| anyhow!("vector compaction dimension missing"))?
                as usize;
            stream::vector::write_vector_stream(
                &segment_path,
                sequence,
                view.n_docs(),
                dim,
                |row| Ok(view.vector_at(row, dim).map(ToOwned::to_owned)),
            )?;
        }
        _ => unreachable!("checked above"),
    }
    write_sparse_rows_atomic(&confined(root, &rows_rel)?, &ids)?;

    let vector_eids = if spec.field_type == FieldType::Vector && includes_base {
        let rel = collection_output_relative(collection, &format!("{stem}.eids.lseg"));
        let path = confined(root, &rel)?;
        write_eids_atomic(&path, sequence, &ids)?;
        Some(SegmentReference {
            role: SegmentRole::VectorEids,
            field: Some(field.to_owned()),
            ordinal: 0,
            kind: SegmentKind::Base,
            format: SegmentFormat::LsegV1,
            path: rel,
            local_rows: None,
            applied_seq: Some(sequence),
            payload_sha256: Some(base_payload_sha256(&path)?),
        })
    } else {
        None
    };
    let rows_path = confined(root, &rows_rel)?;
    let mut logical_write_bytes = checked_bytes(file_len(&segment_path)?, file_len(&rows_path)?)?;
    if let Some(sidecar) = &vector_eids {
        logical_write_bytes = checked_bytes(
            logical_write_bytes,
            file_len(&confined(root, &sidecar.path)?)?,
        )?;
    }
    // A local row map changes the base's identity too. Base-only, unmapped
    // files retain `base_payload_sha256`; every output here has a row map.
    let payload_sha256 = delta_payload_sha256(&segment_path, &rows_path)?;
    Ok(CompactedField {
        output: SegmentReference {
            role: SegmentRole::Field,
            field: Some(field.to_owned()),
            ordinal,
            kind,
            format: SegmentFormat::LsegV1,
            path: segment_rel,
            local_rows: Some(LocalRowsReference {
                format: "lumen-local-eids-cbor-v1".to_owned(),
                path: rows_rel,
                count: u32::try_from(ids.len()).context("compacted rows exceed u32")?,
            }),
            applied_seq: Some(sequence),
            payload_sha256: Some(payload_sha256),
        },
        vector_eids,
        inputs: inputs.to_vec(),
        logical_read_bytes,
        logical_write_bytes,
    })
}

fn validate_compaction_inputs(
    collection: &CollectionCatalog,
    field: &str,
    inputs: &[SegmentReference],
    includes_base: bool,
) -> Result<()> {
    if inputs.is_empty() {
        bail!("compaction needs at least one input");
    }
    let field_segments: Vec<_> = collection
        .segments
        .iter()
        .filter(|reference| {
            matches!(reference.role, SegmentRole::Field)
                && reference.field.as_deref() == Some(field)
        })
        .collect();
    if includes_base {
        if inputs.len() != field_segments.len() {
            bail!("base compaction must include the base and every captured delta");
        }
        for (input, expected) in inputs.iter().zip(field_segments) {
            if !same_reference(input, expected)? {
                bail!("base compaction inputs do not match catalog identity");
            }
        }
    } else {
        for input in inputs {
            if !matches!(input.role, SegmentRole::Field)
                || input.field.as_deref() != Some(field)
                || !matches!(input.kind, SegmentKind::Delta)
                || input.local_rows.is_none()
            {
                bail!("partial compaction requires field delta inputs with row maps");
            }
        }
        let deltas: Vec<_> = field_segments
            .into_iter()
            .filter(|reference| matches!(reference.kind, SegmentKind::Delta))
            .collect();
        let first = inputs.first().expect("checked non-empty");
        let Some(start) = deltas
            .iter()
            .position(|candidate| same_reference(first, candidate).unwrap_or(false))
        else {
            bail!("partial compaction input is not an exact catalog reference");
        };
        let Some(window) = deltas.get(start..start + inputs.len()) else {
            bail!("partial compaction inputs run past the catalog delta window");
        };
        for (input, expected) in inputs.iter().zip(window) {
            if !same_reference(input, expected)? {
                bail!("partial compaction inputs must be one contiguous catalog window");
            }
        }
    }
    if !matches!(inputs[0].kind, SegmentKind::Base) && includes_base {
        bail!("base compaction must start with a base segment");
    }
    Ok(())
}

fn input_external_ids(
    root: &Path,
    collection: &CollectionCatalog,
    field: &str,
    input: &SegmentReference,
    bytes: &mut u64,
) -> Result<Vec<String>> {
    if let Some(local) = &input.local_rows {
        let path = confined(root, &local.path)?;
        *bytes = checked_bytes(*bytes, file_len(&path)?)?;
        let rows = decode_sparse_local_rows(&path, local.count)?;
        return (0..local.count)
            .map(|row| {
                rows.external_id(row)
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("local row map is incomplete"))
            })
            .collect();
    }
    if !matches!(input.kind, SegmentKind::Base) {
        bail!("delta input is missing local row map");
    }
    let role = if is_vector_field(collection, field)? {
        SegmentRole::VectorEids
    } else {
        SegmentRole::CollectionEids
    };
    let sidecar = collection
        .segments
        .iter()
        .find(|reference| {
            std::mem::discriminant(&reference.role) == std::mem::discriminant(&role)
                && matches!(reference.kind, SegmentKind::Base)
                && (matches!(&role, SegmentRole::CollectionEids)
                    || reference.field.as_deref() == Some(field))
        })
        .ok_or_else(|| anyhow!("base compaction has no required EID sidecar"))?;
    let path = confined(root, &sidecar.path)?;
    *bytes = checked_bytes(*bytes, file_len(&path)?)?;
    SegmentReader::open(&path)?
        .eids_all()
        .ok_or_else(|| anyhow!("base EID sidecar is invalid"))
}

fn is_vector_field(collection: &CollectionCatalog, field: &str) -> Result<bool> {
    let specs: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
        serde_json::from_value(collection.schema.clone())?;
    Ok(specs
        .get(field)
        .is_some_and(|spec| spec.field_type == FieldType::Vector))
}

fn same_reference(left: &SegmentReference, right: &SegmentReference) -> Result<bool> {
    Ok(serde_json::to_value(left)? == serde_json::to_value(right)?)
}

pub(in crate::persistence) fn confined(root: &Path, relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    if path.is_absolute()
        || relative
            .split('/')
            .any(|part| matches!(part, "" | "." | ".."))
    {
        bail!("compaction reference escapes root: {relative}");
    }
    Ok(root.join(path))
}

#[cfg(test)]
mod tests;
