//! Installing a prepared base: the scalar layer a publication prepared, the
//! check that a row still matches the captured cut before it retires, and a
//! base installed while writes continue.

use std::collections::HashMap;

use anyhow::{bail, Result};

use crate::index::application::checkpoint_capture::PreparedCheckpointField;
use crate::index::application::live_delta::retire_live_delta_overlay;
use crate::index::domain::collection::{Collection, FieldDirtySnapshot};
use crate::index::domain::field_index::FieldIndex;
use crate::persistence::infrastructure::composed_segment::{
    ComposedSegmentReader, PreparedScalarPublication,
};

pub(super) fn scalar_prepared_segment(
    index: &FieldIndex,
) -> Option<std::sync::Arc<ComposedSegmentReader>> {
    match index {
        FieldIndex::Keyword(index) => index.segment.clone(),
        FieldIndex::Number(index) => index.segment.clone(),
        FieldIndex::Set(index) => index.segment.clone(),
        _ => None,
    }
}

pub(super) fn install_scalar_checkpoint_publication(
    index: &mut FieldIndex,
    publication: &PreparedScalarPublication,
) -> Result<()> {
    let segment = match index {
        FieldIndex::Keyword(index) => &mut index.segment,
        FieldIndex::Number(index) => &mut index.segment,
        FieldIndex::Set(index) => &mut index.segment,
        _ => bail!("checkpoint scalar publication targets a non-scalar field"),
    };
    let next = match segment.as_ref() {
        Some(live) => live.install_checkpoint_publication(publication)?,
        None => publication.install_without_composition()?,
    };
    *segment = Some(std::sync::Arc::new(next));
    Ok(())
}

pub(super) fn checkpoint_scalar_retire_matches(
    current: Option<&u64>,
    captured_revision: u64,
) -> bool {
    current == Some(&captured_revision)
}

fn install_prepared_base_segment(current: &mut FieldIndex, prepared: FieldIndex) -> bool {
    match (current, prepared) {
        (FieldIndex::Keyword(current), FieldIndex::Keyword(prepared)) => {
            current.segment = prepared.segment;
            true
        }
        (FieldIndex::Number(current), FieldIndex::Number(prepared)) => {
            current.segment = prepared.segment;
            true
        }
        (FieldIndex::Set(current), FieldIndex::Set(prepared)) => {
            current.segment = prepared.segment;
            true
        }
        (FieldIndex::Hash(current), FieldIndex::Hash(prepared)) => {
            current.segment = prepared.segment;
            true
        }
        (FieldIndex::Text { idx: current, .. }, FieldIndex::Text { idx: prepared, .. }) => {
            current.segment = prepared.segment;
            true
        }
        _ => false,
    }
}

pub(super) fn install_concurrent_prepared_base(
    coll: &mut Collection,
    prepared: PreparedCheckpointField,
    captured: &FieldDirtySnapshot,
) -> Result<()> {
    if let Some((reader, external_ids)) = prepared.vector_base {
        let captured_rows = captured.get(&prepared.name);
        let current_rows = coll.field_dirty.get(&prepared.name);
        let mut acknowledged = HashMap::new();
        for eid in external_ids
            .iter()
            .chain(current_rows.into_iter().flat_map(|rows| rows.keys()))
            .chain(captured_rows.into_iter().flat_map(|rows| rows.keys()))
        {
            acknowledged.insert(
                eid.clone(),
                current_rows.and_then(|rows| rows.get(eid))
                    == captured_rows.and_then(|rows| rows.get(eid)),
            );
        }
        if let Some(FieldIndex::Vector { idx, bytes, .. }) = coll.fields.get_mut(&prepared.name) {
            idx.install_checkpoint_base(reader, &external_ids, &acknowledged)?;
            if let Some(resident) = idx.checkpoint_resident_bytes() {
                *bytes = resident;
            }
        }
        return Ok(());
    }
    let retire: Vec<_> = captured
        .get(&prepared.name)
        .into_iter()
        .flat_map(|rows| rows.iter())
        .filter_map(|(eid, revision)| {
            (coll
                .field_dirty
                .get(&prepared.name)
                .and_then(|current| current.get(eid))
                == Some(revision))
            .then(|| coll.interner.id(eid))
            .flatten()
        })
        .collect();
    // These mutations happened before a segment existed, so their ordinary
    // mutation paths could not have marked a base tombstone. Installing the
    // captured base must add that mask now, including field-only deletions.
    let newer: Vec<_> = coll
        .field_dirty
        .get(&prepared.name)
        .into_iter()
        .flat_map(|rows| rows.iter())
        .filter_map(|(eid, revision)| {
            (captured.get(&prepared.name).and_then(|rows| rows.get(eid)) != Some(revision))
                .then(|| coll.interner.id(eid))
                .flatten()
        })
        .collect();
    let Some(current) = coll.fields.get_mut(&prepared.name) else {
        return Ok(());
    };
    if install_prepared_base_segment(current, prepared.index) {
        for id in retire {
            retire_live_delta_overlay(current, id);
        }
        match current {
            FieldIndex::Keyword(index) => index.tombstones.extend(newer),
            FieldIndex::Number(index) => index.tombstones.extend(newer),
            FieldIndex::Set(index) => index.tombstones.extend(newer),
            FieldIndex::Hash(index) => index.tombstones.extend(newer),
            FieldIndex::Text { idx, .. } => idx.tombstones.extend(newer),
            FieldIndex::Vector { .. } => {}
        }
    }
    Ok(())
}
