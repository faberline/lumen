//! Per-field delta segments: reading the live delta readers and their values,
//! and writing the changed rows of each field as a new delta.

use crate::index::infrastructure::checkpoint_projection::{scalar_projection, text_projection};
use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, LocalRowsReference, SegmentFormat, SegmentKind, SegmentReference,
    SegmentRole,
};
use crate::persistence::infrastructure::segment::hash_writer::write_hash_segment;
use crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::set_writer::write_set_segment;
use crate::persistence::infrastructure::segment::sparse_rows::{
    decode_sparse_local_rows, encode_sparse_local_rows,
};
use crate::persistence::infrastructure::segment::vector_writer::write_vector_segment;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::delta_payload_sha256;
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::collection_checkpoint_dir_name;
use anyhow::{anyhow, bail, Result};
use std::collections::BTreeMap;
use std::path::Path;

pub(super) fn prepare_live_delta_readers(
    root: &Path,
    collections: &[CollectionCatalog],
    capture: &mut crate::index::application::checkpoint_capture::CheckpointCapture,
) -> Result<()> {
    for collection in collections {
        let Some(fields) = capture.field_deltas.get(&collection.collection_id) else {
            continue;
        };
        let specs: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
            serde_json::from_value(collection.schema.clone())?;
        for field in fields.keys() {
            let spec = specs
                .get(field)
                .ok_or_else(|| anyhow!("captured delta field is absent from schema"))?;
            if matches!(
                spec.field_type,
                crate::shared_kernel::types::schema::FieldType::Vector
            ) && spec.vector_spec()?.is_some_and(|vector| {
                vector.backend != crate::shared_kernel::types::schema::VectorBackend::FlatCpu
            }) {
                continue;
            }
            let segment = collection
                .segments
                .iter()
                .filter(|segment| {
                    matches!(segment.kind, SegmentKind::Delta)
                        && segment.field.as_deref() == Some(field.as_str())
                })
                .max_by_key(|segment| segment.ordinal)
                .ok_or_else(|| anyhow!("captured delta has no catalog segment"))?;
            let local = segment
                .local_rows
                .as_ref()
                .ok_or_else(|| anyhow!("delta has no row map"))?;
            let rows = decode_sparse_local_rows(&root.join(&local.path), local.count)?;
            let external_ids = (0..local.count)
                .map(|row| {
                    rows.external_id(row)
                        .map(str::to_owned)
                        .ok_or_else(|| anyhow!("delta row map is incomplete"))
                })
                .collect::<Result<Vec<_>>>()?;
            let reader = std::sync::Arc::new(SegmentReader::open(&root.join(&segment.path))?);
            capture
                .live_delta_inputs
                .entry(collection.collection_id.clone())
                .or_default()
                .entry(field.clone())
                .or_default()
                .push(reader.clone());
            capture
                .prepared_deltas
                .entry(collection.collection_id.clone())
                .or_default()
                .push(
                    crate::index::application::checkpoint_capture::PreparedCheckpointDelta {
                        field: field.clone(),
                        reader,
                        external_ids,
                    },
                );
        }
    }
    Ok(())
}

pub(super) fn read_delta_values(
    reader: &SegmentReader,
    spec: &crate::shared_kernel::types::schema::FieldSpec,
) -> Result<Vec<Option<crate::index::application::checkpoint_capture::CheckpointValue>>> {
    use crate::index::application::checkpoint_capture::CheckpointValue;
    use crate::shared_kernel::types::schema::FieldType;
    if spec.field_type == FieldType::Text {
        let mut values: Vec<_> = (0..reader.n_docs())
            .map(|row| {
                reader.text_is_present(row).then(|| CheckpointValue::Text {
                    doc_len: reader.text_doc_len(row),
                    tokens: BTreeMap::new(),
                })
            })
            .collect();
        let tokens = reader
            .text_tokens_all()
            .ok_or_else(|| anyhow!("invalid text delta postings"))?;
        for (token, rows, tfs) in tokens {
            if rows.len() != tfs.len() {
                bail!("text delta posting length mismatch");
            }
            for (row, tf) in rows.into_iter().zip(tfs) {
                let Some(Some(CheckpointValue::Text { tokens, .. })) = values.get_mut(row as usize)
                else {
                    bail!("text delta posting names absent row");
                };
                if tf == 0 || tokens.insert(token.clone(), tf).is_some() {
                    bail!("invalid text delta frequency");
                }
            }
        }
        let mut count = 0;
        let mut total = 0;
        for value in values.iter().flatten() {
            let CheckpointValue::Text { doc_len, tokens } = value else {
                unreachable!()
            };
            if tokens.values().map(|tf| *tf as u64).sum::<u64>() != *doc_len as u64 {
                bail!("text delta lengths and postings disagree");
            }
            count += 1;
            total += *doc_len as u64;
        }
        if count != reader.text_doc_count() || total != reader.text_total_doc_len() {
            bail!("text delta corpus statistics disagree");
        }
        return Ok(values);
    }
    if spec.field_type == FieldType::Vector {
        let dim = spec
            .dim
            .ok_or_else(|| anyhow!("vector delta dimension missing"))? as usize;
        if reader.vectors_slice(dim).is_none() {
            bail!("invalid vector delta geometry");
        }
    }
    (0..reader.n_docs())
        .map(|row| {
            Ok(match spec.field_type {
                FieldType::Keyword => reader.keyword_at(row).map(CheckpointValue::Keyword),
                FieldType::Number => reader.number_at(row).map(CheckpointValue::Number),
                FieldType::Set => reader.set_at(row).map(CheckpointValue::Set),
                FieldType::Hash => reader.hash_at(row).map(CheckpointValue::Hash),
                FieldType::Vector => reader
                    .vector_at(
                        row,
                        spec.dim
                            .ok_or_else(|| anyhow!("vector delta dimension missing"))?
                            as usize,
                    )
                    .map(|value| CheckpointValue::Vector(value.to_vec())),
                _ => bail!("unsupported delta field type"),
            })
        })
        .collect()
}

pub(super) fn delta_path_prefix(collection: &str, field: &str, ordinal: u32) -> String {
    format!(
        "{}/__delta/{}/{ordinal}",
        collection_checkpoint_dir_name(collection),
        collection_checkpoint_dir_name(field)
    )
}

pub(super) fn write_field_deltas(
    root: &Path,
    sequence: u64,
    collection: &mut CollectionCatalog,
    fields: &crate::index::application::checkpoint_capture::CheckpointDeltas,
) -> Result<()> {
    for (field, rows) in fields {
        if rows.is_empty() {
            continue;
        }
        let ordinal = collection
            .segments
            .iter()
            .filter(|segment| {
                segment.field.as_deref() == Some(field.as_str())
                    && matches!(segment.role, SegmentRole::Field)
            })
            .map(|segment| segment.ordinal)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("segment ordinal exhausted"))?;
        let field_dir = collection_checkpoint_dir_name(field);
        let prefix = delta_path_prefix(&collection.collection_id, field, ordinal);
        let segment_path = format!("{prefix}.lseg");
        let rows_path = format!("{prefix}.rows.cbor");
        std::fs::create_dir_all(root.join(&segment_path).parent().unwrap())?;
        let ids: Vec<_> = rows.iter().map(|(id, _)| id.clone()).collect();
        use crate::index::application::checkpoint_capture::CheckpointValue;
        let specs: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
            serde_json::from_value(collection.schema.clone())?;
        let spec = specs
            .get(field)
            .ok_or_else(|| anyhow!("delta field absent from schema"))?;
        let staged_scalar = rows.iter().any(|(_, value)| {
            matches!(value.as_deref(), Some(CheckpointValue::StagedScalar { .. }))
        });
        if staged_scalar
            && matches!(
                spec.field_type,
                crate::shared_kernel::types::schema::FieldType::Keyword
                    | crate::shared_kernel::types::schema::FieldType::Number
                    | crate::shared_kernel::types::schema::FieldType::Set
            )
        {
            scalar_projection::write_checkpoint_rows(
                &root.join(&segment_path),
                sequence,
                spec.field_type,
                rows,
            )?;
        } else {
            match spec.field_type {
                crate::shared_kernel::types::schema::FieldType::Keyword => {
                    let values: Vec<_> = rows
                        .iter()
                        .map(|(_, value)| match value.as_deref() {
                            Some(CheckpointValue::Keyword(value)) => Ok(Some(value.as_str())),
                            None => Ok(None),
                            _ => bail!("keyword delta value mismatch"),
                        })
                        .collect::<Result<_>>()?;
                    let mut postings: BTreeMap<String, roaring::RoaringBitmap> = BTreeMap::new();
                    for (row, value) in values.iter().enumerate() {
                        if let Some(value) = value {
                            postings
                                .entry((*value).to_owned())
                                .or_default()
                                .insert(u32::try_from(row)?);
                        }
                    }
                    write_keyword_segment(&root.join(&segment_path), sequence, &values, &postings)?;
                }
                crate::shared_kernel::types::schema::FieldType::Number => {
                    let values: Vec<_> = rows
                        .iter()
                        .map(|(_, value)| match value.as_deref() {
                            Some(CheckpointValue::Number(value)) => Ok(Some(*value)),
                            None => Ok(None),
                            _ => bail!("number delta value mismatch"),
                        })
                        .collect::<Result<_>>()?;
                    write_number_segment(&root.join(&segment_path), sequence, &values)?;
                }
                crate::shared_kernel::types::schema::FieldType::Hash => {
                    let values: Vec<_> = rows
                        .iter()
                        .map(|(_, value)| match value.as_deref() {
                            Some(CheckpointValue::Hash(value)) => Ok(Some(*value)),
                            None => Ok(None),
                            _ => bail!("hash delta value mismatch"),
                        })
                        .collect::<Result<_>>()?;
                    write_hash_segment(&root.join(&segment_path), sequence, &values)?;
                }
                crate::shared_kernel::types::schema::FieldType::Set => {
                    let values: Vec<_> = rows
                        .iter()
                        .map(|(_, value)| match value.as_deref() {
                            Some(CheckpointValue::Set(value)) => Ok(Some(value.as_slice())),
                            None => Ok(None),
                            _ => bail!("set delta value mismatch"),
                        })
                        .collect::<Result<_>>()?;
                    let mut postings: BTreeMap<String, roaring::RoaringBitmap> = BTreeMap::new();
                    for (row, values) in values.iter().enumerate() {
                        if let Some(values) = values {
                            for value in *values {
                                postings
                                    .entry(value.clone())
                                    .or_default()
                                    .insert(u32::try_from(row)?);
                            }
                        }
                    }
                    write_set_segment(&root.join(&segment_path), sequence, &values, &postings)?;
                }
                crate::shared_kernel::types::schema::FieldType::Text => {
                    text_projection::write_checkpoint_rows(
                        &root.join(&segment_path),
                        sequence,
                        rows,
                    )?;
                }
                crate::shared_kernel::types::schema::FieldType::Vector => {
                    let values: Vec<_> = rows
                        .iter()
                        .map(|(_, value)| match value.as_deref() {
                            Some(CheckpointValue::Vector(value)) => Ok(Some(value.as_slice())),
                            Some(CheckpointValue::StagedVector(value)) => {
                                Ok(Some(value.as_f32_slice()))
                            }
                            None => Ok(None),
                            _ => bail!("vector delta value mismatch"),
                        })
                        .collect::<Result<_>>()?;
                    let dim = spec
                        .dim
                        .ok_or_else(|| anyhow!("vector delta dimension missing"))?
                        as usize;
                    write_vector_segment(&root.join(&segment_path), sequence, dim, &values)?;
                }
                _ => bail!("unsupported delta field"),
            }
        }
        encode_sparse_local_rows(&root.join(&rows_path), &ids)?;
        let payload_sha256 =
            delta_payload_sha256(&root.join(&segment_path), &root.join(&rows_path))?;
        collection.segments.push(SegmentReference {
            role: SegmentRole::Field,
            field: Some(field.clone()),
            ordinal,
            kind: SegmentKind::Delta,
            format: SegmentFormat::LsegV1,
            path: segment_path,
            local_rows: Some(LocalRowsReference {
                format: "lumen-local-eids-cbor-v1".to_owned(),
                path: rows_path,
                count: u32::try_from(rows.len())?,
            }),
            applied_seq: Some(sequence),
            payload_sha256: Some(payload_sha256),
        });
    }
    Ok(())
}
