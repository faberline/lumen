//! The checkpoint deltas a field serves reads from until a base absorbs them: a
//! delta segment attached as a reader, the live delta and base readers a
//! capture pins, a collection's deltas replaced at publication, and the live
//! overlay rows a published delta covers retired.

use anyhow::{anyhow, Result};

use crate::index::application::checkpoint_capture::{
    PreparedCheckpointCompaction, PreparedCheckpointDelta,
};
use crate::index::domain::collection::{Collection, FieldDirtySnapshot};
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::{SortableF64, MISSING_SORTABLE_F64_BITS};
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

pub(super) fn attach_delta_reader(
    index: &mut FieldIndex,
    reader: std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
    ids: Vec<u32>,
) -> Result<()> {
    let attach = |segment: &mut Option<std::sync::Arc<ComposedSegmentReader>>| {
        let base = segment
            .as_ref()
            .ok_or_else(|| anyhow!("delta has no base segment"))?;
        *segment = Some(std::sync::Arc::new(base.with_delta(reader, ids)?));
        Ok(())
    };
    match index {
        FieldIndex::Keyword(index) => attach(&mut index.segment),
        FieldIndex::Number(index) => attach(&mut index.segment),
        FieldIndex::Set(index) => attach(&mut index.segment),
        FieldIndex::Hash(index) => attach(&mut index.segment),
        FieldIndex::Text { idx, .. } => attach(&mut idx.segment),
        FieldIndex::Vector { .. } => Ok(()),
    }
}

pub(super) fn live_delta_readers(
    index: &FieldIndex,
) -> Vec<std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>> {
    let segment = match index {
        FieldIndex::Keyword(index) => &index.segment,
        FieldIndex::Number(index) => &index.segment,
        FieldIndex::Set(index) => &index.segment,
        FieldIndex::Hash(index) => &index.segment,
        FieldIndex::Text { idx, .. } => &idx.segment,
        FieldIndex::Vector { idx, .. } => return idx.checkpoint_delta_readers(),
    };
    segment
        .as_ref()
        .map(|segment| segment.delta_readers())
        .unwrap_or_default()
}

pub(super) fn live_base_reader(
    index: &FieldIndex,
) -> Option<std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>> {
    let segment = match index {
        FieldIndex::Keyword(index) => &index.segment,
        FieldIndex::Number(index) => &index.segment,
        FieldIndex::Set(index) => &index.segment,
        FieldIndex::Hash(index) => &index.segment,
        FieldIndex::Text { idx, .. } => &idx.segment,
        FieldIndex::Vector { idx, .. } => return idx.checkpoint_base_reader(),
    };
    segment
        .as_ref()
        .and_then(|segment| segment.catalog_base_reader())
}

pub(super) fn replace_live_checkpoint_deltas(
    coll: &mut Collection,
    compacted: PreparedCheckpointCompaction,
) -> Result<()> {
    let index = coll
        .fields
        .get_mut(&compacted.field)
        .ok_or_else(|| anyhow!("compacted field is absent from live collection"))?;
    if let FieldIndex::Vector { idx, .. } = index {
        if let Some(base) = compacted.base {
            return idx.replace_checkpoint_base(
                &base,
                &compacted.inputs,
                compacted.reader,
                &compacted.external_ids,
            );
        }
        return idx.replace_checkpoint_deltas(
            &compacted.inputs,
            compacted.reader,
            &compacted.external_ids,
        );
    }
    let prepared = compacted
        .scalar
        .ok_or_else(|| anyhow!("scalar compaction was not prepared before publication"))?;
    let segment = match index {
        FieldIndex::Keyword(index) => &mut index.segment,
        FieldIndex::Number(index) => &mut index.segment,
        FieldIndex::Set(index) => &mut index.segment,
        FieldIndex::Hash(index) => &mut index.segment,
        FieldIndex::Text { idx, .. } => &mut idx.segment,
        FieldIndex::Vector { .. } => unreachable!(),
    };
    let old = segment
        .as_ref()
        .ok_or_else(|| anyhow!("compacted field has no live base"))?;
    let replacement = old.install_prepared_replacement(&prepared)?;
    *segment = Some(std::sync::Arc::new(replacement));
    Ok(())
}

pub(super) fn retire_live_delta_overlay(index: &mut FieldIndex, id: u32) {
    match index {
        FieldIndex::Keyword(index) => {
            if let Some(value) = index.remove_keyword(id) {
                if let Some(posting) = index.terms.get_mut(&value) {
                    posting.remove(id);
                    if posting.len() < 2 {
                        index.dup_values.remove(&value);
                    }
                    if posting.is_empty() {
                        index.terms.remove(&value);
                    }
                }
            }
            index.tombstones.remove(id);
        }
        FieldIndex::Number(index) => {
            let dense = index.dense_forward.get_mut(id as usize).and_then(|slot| {
                let value =
                    (*slot != MISSING_SORTABLE_F64_BITS).then(|| SortableF64::from_bits(*slot));
                *slot = MISSING_SORTABLE_F64_BITS;
                value
            });
            let sparse = index.forward.remove(&id);
            for value in [dense, sparse].into_iter().flatten() {
                if let Some(posting) = index.values.get_mut(&value) {
                    posting.remove(id);
                    if posting.len() < 2 {
                        index.dup_values.remove(&value);
                    }
                    if posting.is_empty() {
                        index.values.remove(&value);
                    }
                }
            }
            index.tombstones.remove(id);
            index.clear_keyword_range_cache();
        }
        FieldIndex::Set(index) => {
            if let Some(values) = index.forward.remove(&id) {
                for value in values {
                    if let Some(posting) = index.elements.get_mut(&value) {
                        posting.remove(id);
                        if posting.len() < 2 {
                            index.dup_values.remove(&value);
                        }
                        if posting.is_empty() {
                            index.elements.remove(&value);
                        }
                    }
                }
            }
            index.tombstones.remove(id);
        }
        FieldIndex::Hash(index) => {
            index.forward.remove(&id);
            index.tombstones.remove(id);
        }
        FieldIndex::Text { idx, .. } => {
            idx.staged_rows.remove(&id);
            if let Some(tokens) = idx.take_distinct(id) {
                for token in tokens.iter() {
                    if let Some(posting) = idx.tokens.get_mut(token) {
                        posting.remove(id);
                        if posting.docids().is_empty() {
                            idx.tokens.remove(token);
                        }
                    }
                }
            }
            if let Some(len) = idx.lens.get_mut(id as usize) {
                *len = 0;
            }
            idx.tombstones.remove(id);
            idx.clear_match_rank_cache();
        }
        FieldIndex::Vector { .. } => {}
    }
}

pub(super) fn attach_live_checkpoint_delta(
    coll: &mut Collection,
    delta: PreparedCheckpointDelta,
    captured: &FieldDirtySnapshot,
) -> Result<()> {
    let rows = captured.get(&delta.field);
    let ids = delta
        .external_ids
        .iter()
        .map(|eid| {
            coll.interner
                .id(eid)
                .ok_or_else(|| anyhow!("delta external ID is absent from live collection"))
        })
        .collect::<Result<Vec<_>>>()?;
    let retire: Vec<_> = delta
        .external_ids
        .iter()
        .map(|eid| {
            rows.is_some_and(|rows| {
                coll.field_dirty
                    .get(&delta.field)
                    .and_then(|current| current.get(eid))
                    == rows.get(eid)
            })
        })
        .collect();
    let index = coll
        .fields
        .get_mut(&delta.field)
        .ok_or_else(|| anyhow!("delta field is absent from live collection"))?;
    if let FieldIndex::Vector { idx, bytes, .. } = index {
        idx.attach_checkpoint_delta(delta.reader, &delta.external_ids, &retire)?;
        if let Some(resident) = idx.checkpoint_resident_bytes() {
            *bytes = resident;
        }
    } else {
        attach_delta_reader(index, delta.reader, ids.clone())?;
    }
    for (retire, id) in retire.into_iter().zip(ids) {
        if retire && !matches!(index, FieldIndex::Vector { .. }) {
            retire_live_delta_overlay(index, id);
        }
    }
    Ok(())
}
