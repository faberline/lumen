//! Compacting the selected delta windows and replacing their references in the
//! catalog with the compacted segment.

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentKind, SegmentReference, SegmentRole,
};
use crate::persistence::infrastructure::segment::sparse_rows::decode_sparse_local_rows;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::compaction::{
    write_compacted_field, CompactedField,
};
use crate::persistence::infrastructure::segment_rdb_store::merge_selection::StagedMergeCandidate;
use crate::persistence::infrastructure::segment_rdb_store::{MergeObserver, MergePhase};
use anyhow::{anyhow, bail, Result};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

/// Fold every selected candidate's layers into one compacted output each,
/// inside the staged generation already prepared for this job. Every
/// candidate shares the same collection, so each fold reads and writes only
/// that collection's directory; the publication side folds each output as a
/// sequence of rebases, and keeping outputs in candidate order leaves the
/// identity check per output unchanged.
pub(in crate::persistence) fn compact_staged_delta_windows(
    root: &Path,
    sequence: u64,
    collections: &mut [CollectionCatalog],
    capture: &mut crate::storage::CheckpointCapture,
    scratch_delta_readers: &mut BTreeMap<String, Vec<Arc<SegmentReader>>>,
    observer: &dyn MergeObserver,
    candidates: Vec<StagedMergeCandidate>,
) -> Result<Vec<CompactedField>> {
    encode_staged_candidates_in_order(candidates, observer, |candidate| {
        let collection = collections[candidate.collection_index].clone();
        let selected = if candidate.includes_base {
            let mut selected = vec![candidate.base.clone()];
            selected.extend(candidate.inputs.iter().cloned());
            selected
        } else {
            candidate.inputs.clone()
        };
        let output = write_compacted_field(
            root,
            sequence,
            &collection,
            &candidate.field,
            &selected,
            candidate.includes_base,
        )?;
        let StagedMergeCandidate {
            collection_index,
            collection_id: _,
            field,
            inputs,
            base,
            includes_base,
        } = candidate;
        finish_compacted_field(
            root,
            &mut collections[collection_index],
            field,
            inputs,
            base,
            includes_base,
            capture,
            scratch_delta_readers,
            output,
        )
    })
}

/// Notify and encode one candidate before moving to the next one. The merge
/// worker is already serialized, so this deliberately has no nested pool.
pub(super) fn encode_staged_candidates_in_order<T, F>(
    candidates: Vec<StagedMergeCandidate>,
    observer: &dyn MergeObserver,
    mut encode: F,
) -> Result<Vec<T>>
where
    F: FnMut(StagedMergeCandidate) -> Result<T>,
{
    let mut outputs = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        observer.observe(MergePhase::BeforeEncode)?;
        outputs.push(encode(candidate)?);
    }
    Ok(outputs)
}

#[allow(clippy::too_many_arguments)]
fn finish_compacted_field(
    root: &Path,
    collection: &mut CollectionCatalog,
    field: String,
    inputs: Vec<SegmentReference>,
    base: SegmentReference,
    includes_base: bool,
    capture: &mut crate::storage::CheckpointCapture,
    scratch_delta_readers: &mut BTreeMap<String, Vec<Arc<SegmentReader>>>,
    output: CompactedField,
) -> Result<CompactedField> {
    let selected = if includes_base {
        let mut selected = vec![base];
        selected.extend(inputs.iter().cloned());
        selected
    } else {
        inputs.clone()
    };
    let specs: BTreeMap<String, crate::shared_kernel::types::schema::FieldSpec> =
        serde_json::from_value(collection.schema.clone())?;
    let is_hnsw = specs
        .get(&field)
        .ok_or_else(|| anyhow!("compacted field has no schema"))?
        .vector_spec()?
        .is_some_and(|spec| {
            spec.backend != crate::shared_kernel::types::schema::VectorBackend::FlatCpu
        });
    let live_inputs = if is_hnsw {
        Vec::new()
    } else {
        let readers = scratch_delta_readers
            .get(&field)
            .cloned()
            .ok_or_else(|| anyhow!("compaction has no captured live input identity"))?;
        let catalog_deltas: Vec<_> = collection
            .segments
            .iter()
            .filter(|segment| {
                matches!(segment.role, SegmentRole::Field)
                    && matches!(segment.kind, SegmentKind::Delta)
                    && segment.field.as_deref() == Some(field.as_str())
            })
            .collect();
        if readers.len() != catalog_deltas.len() {
            bail!("compaction catalog differs from captured live layer count");
        }
        let mut selected_readers = Vec::with_capacity(inputs.len());
        for input in &inputs {
            let position = catalog_deltas
                .iter()
                .position(|segment| *segment == input)
                .ok_or_else(|| anyhow!("compaction input is absent from captured catalog"))?;
            selected_readers.push(
                readers
                    .get(position)
                    .cloned()
                    .ok_or_else(|| anyhow!("compaction input has no captured reader"))?,
            );
        }
        selected_readers
    };
    let live_base = if includes_base && !is_hnsw {
        Some(
            capture
                .live_base_inputs
                .get(&collection.collection_id)
                .and_then(|fields| fields.get(&field))
                .cloned()
                .ok_or_else(|| anyhow!("compaction has no captured live base identity"))?,
        )
    } else {
        None
    };
    if !is_hnsw {
        let local = output
            .output
            .local_rows
            .as_ref()
            .ok_or_else(|| anyhow!("compacted delta has no row map"))?;
        let rows = decode_sparse_local_rows(&root.join(&local.path), local.count)?;
        let external_ids = (0..local.count)
            .map(|row| {
                rows.external_id(row)
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("compacted row map is incomplete"))
            })
            .collect::<Result<_>>()?;
        let reader = std::sync::Arc::new(SegmentReader::open(&root.join(&output.output.path))?);
        let staged_inputs = scratch_delta_readers
            .get_mut(&field)
            .expect("staged live input identity validated");
        if includes_base {
            staged_inputs.clear();
            capture
                .live_base_inputs
                .entry(collection.collection_id.clone())
                .or_default()
                .insert(field.clone(), reader.clone());
        } else {
            let start = staged_inputs
                .windows(live_inputs.len())
                .position(|window| {
                    window
                        .iter()
                        .zip(&live_inputs)
                        .all(|(actual, expected)| std::sync::Arc::ptr_eq(actual, expected))
                })
                .ok_or_else(|| anyhow!("staged compaction input identity changed"))?;
            staged_inputs.splice(start..start + live_inputs.len(), [reader.clone()]);
        }
        capture
            .live_delta_inputs
            .entry(collection.collection_id.clone())
            .or_default()
            .insert(field.clone(), staged_inputs.clone());
        capture
            .prepared_compactions
            .entry(collection.collection_id.clone())
            .or_default()
            .push(crate::storage::PreparedCheckpointCompaction {
                field: field.clone(),
                base: live_base,
                inputs: live_inputs,
                reader,
                external_ids,
                scalar: None,
            });
    }
    replace_compacted_delta_references(collection, &output)?;
    if let Some(sidecar) = &output.vector_eids {
        let old = collection
            .segments
            .iter_mut()
            .find(|segment| {
                matches!(segment.role, SegmentRole::VectorEids) && segment.field == sidecar.field
            })
            .ok_or_else(|| anyhow!("compacted vector base has no catalog sidecar"))?;
        *old = sidecar.clone();
    }
    for input in &selected {
        if input.path != output.output.path {
            std::fs::remove_file(root.join(&input.path))?;
        }
        if let Some(rows) = &input.local_rows {
            if output
                .output
                .local_rows
                .as_ref()
                .is_none_or(|output_rows| output_rows.path != rows.path)
            {
                std::fs::remove_file(root.join(&rows.path))?;
            }
        }
    }
    Ok(output)
}

pub(super) fn segment_reference_bytes(root: &Path, segment: &SegmentReference) -> Result<u64> {
    std::iter::once(&segment.path)
        .chain(segment.local_rows.iter().map(|rows| &rows.path))
        .try_fold(0u64, |total, path| {
            total
                .checked_add(std::fs::metadata(root.join(path))?.len())
                .ok_or_else(|| anyhow!("segment byte count overflow"))
        })
}

fn replace_compacted_delta_references(
    collection: &mut CollectionCatalog,
    compacted: &CompactedField,
) -> Result<()> {
    let last = compacted
        .inputs
        .last()
        .ok_or_else(|| anyhow!("compaction has no inputs"))?;
    let mut replaced = Vec::with_capacity(collection.segments.len());
    for segment in collection.segments.drain(..) {
        if compacted
            .inputs
            .iter()
            .any(|input| serde_json::to_value(input).ok() == serde_json::to_value(&segment).ok())
        {
            if serde_json::to_value(&segment)? == serde_json::to_value(last)? {
                replaced.push(compacted.output.clone());
            }
            continue;
        }
        replaced.push(segment);
    }
    collection.segments = replaced;
    Ok(())
}
