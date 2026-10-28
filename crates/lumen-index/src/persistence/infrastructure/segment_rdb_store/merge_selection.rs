//! Choosing what a background merge compacts: the staged candidates and the
//! bounded delta and field windows.

use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, SegmentKind, SegmentReference, SegmentRole,
};
use crate::persistence::infrastructure::segment_rdb_store::compacted_fields::segment_reference_bytes;
use crate::storage::infrastructure::composed_segment::MAX_INCREMENTAL_LAYERS;
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::path::Path;

/// One field a merge job will compact: an eligible delta stack in the
/// catalog, with the base segment it may fold in.
///
/// Selection reads only file sizes, so it runs against the read-only source
/// generation before the job stages anything. That is what lets the job link
/// exactly one collection into its scratch stage: the collection it is about
/// to compact is known before the first hard link is made. Every candidate a
/// single call to `select_staged_delta_window` returns shares that same
/// `collection_index`/`collection_id`.
pub(in crate::persistence) struct StagedMergeCandidate {
    pub(super) collection_index: usize,
    /// The collection whose directory the compaction reads and writes. It is
    /// the only directory a job's scratch stage needs.
    pub(in crate::persistence) collection_id: String,
    pub(in crate::persistence) field: String,
    pub(in crate::persistence) inputs: Vec<SegmentReference>,
    pub(in crate::persistence) base: SegmentReference,
    pub(in crate::persistence) includes_base: bool,
}

/// A stack this deep has used half of the query-time layer bound.
const DEEP_DELTA_STACK: usize = MAX_INCREMENTAL_LAYERS / 2;
/// Each checkpoint adds one delta per dirty field, so a merge that folds
/// only a pair just holds a deep stack's depth. Before the stack reaches the
/// bound and stalls checkpoints, fold a wider adjacent run instead.
const DEEP_DELTA_WINDOW: usize = 4;

fn delta_window_len(depth: usize) -> usize {
    if depth >= DEEP_DELTA_STACK {
        DEEP_DELTA_WINDOW
    } else {
        2
    }
}

/// Select one field-level merge from `collections`, reading segment sizes from
/// `root`.
///
/// The scheduler first chooses the collection with the deepest eligible delta
/// stack. Ties are deterministic: catalog order wins. Every field tied at
/// that collection depth is then returned in field-name order. Shallower
/// eligible fields remain for a later scheduling pass. Unless a selected
/// field's complete delta stack has reached the base size, that field
/// contributes exactly the adjacent window with the smallest combined on-disk
/// size: a pair, or [`DEEP_DELTA_WINDOW`] deltas once the stack is
/// [`DEEP_DELTA_STACK`] deep.
/// Once the delta stack reaches the base size, the base and all captured
/// deltas are folded together so the base can be replaced.
pub(in crate::persistence) fn select_staged_delta_window(
    root: &Path,
    collections: &[CollectionCatalog],
) -> Result<Vec<StagedMergeCandidate>> {
    let mut selected_collection: Option<(usize, usize)> = None;
    for (collection_index, collection) in collections.iter().enumerate() {
        let mut by_field = BTreeMap::<String, Vec<&SegmentReference>>::new();
        for segment in &collection.segments {
            if matches!(segment.role, SegmentRole::Field)
                && matches!(segment.kind, SegmentKind::Delta)
            {
                by_field
                    .entry(segment.field.clone().expect("field delta has field"))
                    .or_default()
                    .push(segment);
            }
        }
        let deepest = by_field
            .values()
            .filter(|deltas| deltas.len() >= 4)
            .map(Vec::len)
            .max()
            .unwrap_or(0);
        if deepest > 0
            && selected_collection
                .as_ref()
                .is_none_or(|(_, selected_depth)| deepest > *selected_depth)
        {
            selected_collection = Some((collection_index, deepest));
        }
    }
    let Some((collection_index, selected_depth)) = selected_collection else {
        return Ok(Vec::new());
    };
    let collection = &collections[collection_index];
    // A small durable-cut cohort should share the whole-root publication
    // cost. Expand its field bound only when the complete job stays inside
    // the existing byte budget. Larger pair cohorts retain their old bound.
    const MAX_FIELDS_PER_MERGE_JOB: usize = 8;
    const MAX_BYTE_BOUNDED_FIELDS_PER_MERGE_JOB: usize = 16;
    const MAX_LOGICAL_READ_BYTES_PER_MERGE_JOB: u64 = 24 * 1024 * 1024;
    let mut candidates = Vec::new();
    let mut by_field = BTreeMap::<String, Vec<&SegmentReference>>::new();
    for segment in &collection.segments {
        if matches!(segment.role, SegmentRole::Field) && matches!(segment.kind, SegmentKind::Delta)
        {
            by_field
                .entry(segment.field.clone().expect("field delta has field"))
                .or_default()
                .push(segment);
        }
    }
    for (field, deltas) in by_field {
        if deltas.len() < 4 || deltas.len() != selected_depth {
            continue;
        }
        let delta_bytes = deltas.iter().try_fold(0u64, |total, segment| {
            total
                .checked_add(segment_reference_bytes(root, segment)?)
                .ok_or_else(|| anyhow!("delta byte count overflow"))
        })?;
        let base = collection
            .segments
            .iter()
            .find(|segment| {
                matches!(segment.role, SegmentRole::Field)
                    && matches!(segment.kind, SegmentKind::Base)
                    && segment.field.as_deref() == Some(field.as_str())
            })
            .ok_or_else(|| anyhow!("delta field has no base segment"))?;
        let mut base_bytes = segment_reference_bytes(root, base)?;
        if let Some(sidecar) = collection.segments.iter().find(|segment| {
            matches!(segment.role, SegmentRole::VectorEids) && segment.field == base.field
        }) {
            base_bytes = base_bytes
                .checked_add(segment_reference_bytes(root, sidecar)?)
                .ok_or_else(|| anyhow!("base byte count overflow"))?;
        }
        let includes_base = delta_bytes >= base_bytes;
        let mut full_stack_bytes = delta_bytes;
        if includes_base {
            full_stack_bytes = full_stack_bytes
                .checked_add(base_bytes)
                .ok_or_else(|| anyhow!("merge input byte count overflow"))?;
        }
        let (inputs, merge_bytes) = if includes_base {
            (
                deltas.iter().map(|segment| (*segment).clone()).collect(),
                full_stack_bytes,
            )
        } else {
            let window_len = delta_window_len(deltas.len());
            let mut smallest: Option<(u64, usize)> = None;
            for (index, window) in deltas.windows(window_len).enumerate() {
                let window_bytes = window.iter().try_fold(0u64, |total, segment| {
                    total
                        .checked_add(segment_reference_bytes(root, segment)?)
                        .ok_or_else(|| anyhow!("adjacent delta byte count overflow"))
                })?;
                if smallest.is_none_or(|(best_bytes, _)| window_bytes < best_bytes) {
                    smallest = Some((window_bytes, index));
                }
            }
            let (window_bytes, index) =
                smallest.ok_or_else(|| anyhow!("delta field has no adjacent window"))?;
            (
                deltas[index..index + window_len]
                    .iter()
                    .map(|segment| (*segment).clone())
                    .collect(),
                window_bytes,
            )
        };
        candidates.push((
            deltas.len(),
            field.clone(),
            merge_bytes,
            StagedMergeCandidate {
                collection_index,
                collection_id: collection.collection_id.clone(),
                field,
                inputs,
                base: base.clone(),
                includes_base,
            },
        ));
    }
    candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    let mut selected = Vec::new();
    let mut total = 0u64;
    for (_depth, _field, estimate, candidate) in candidates {
        let same_pair_cohort = !candidate.includes_base
            && selected.iter().all(|selected: &StagedMergeCandidate| {
                !selected.includes_base && selected.inputs.len() == 2
            });
        let within_byte_budget = total
            .checked_add(estimate)
            .is_some_and(|next| next <= MAX_LOGICAL_READ_BYTES_PER_MERGE_JOB);
        if selected.is_empty()
            || (selected.len() < MAX_BYTE_BOUNDED_FIELDS_PER_MERGE_JOB && within_byte_budget)
            || (selected.len() < MAX_FIELDS_PER_MERGE_JOB
                && same_pair_cohort
                && estimate <= MAX_LOGICAL_READ_BYTES_PER_MERGE_JOB)
        {
            total = total
                .checked_add(estimate)
                .ok_or_else(|| anyhow!("merge input byte count overflow"))?;
            selected.push(candidate);
        } else {
            break;
        }
    }
    Ok(selected)
}
