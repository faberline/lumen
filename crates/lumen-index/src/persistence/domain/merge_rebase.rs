//! Rebase a completed field merge onto the latest complete catalog.
//! Input identity includes the row map, checksum, format, sequence, and order.
//! A full-base merge must cover the entire older prefix. Later layers survive.

use crate::persistence::domain::generation_manifest::{
    SegmentGenerationManifest, SegmentKind, SegmentReference, SegmentRole,
};
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub(in crate::persistence) struct MergeSelection {
    pub collection_id: String,
    pub collection_generation: u64,
    pub schema_version: u32,
    pub schema: serde_json::Value,
    pub field: String,
    /// The selected deltas in their original order; the optional base is separate.
    pub inputs: Vec<SegmentReference>,
    pub base: Option<SegmentReference>,
    pub vector_sidecar: Option<SegmentReference>,
}

#[derive(Clone, Debug)]
pub(in crate::persistence) struct VerifiedOutput {
    pub field: SegmentReference,
    pub vector_sidecar: Option<SegmentReference>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::persistence) enum RebaseRefusal {
    MissingCollection,
    CollectionIdentityChanged,
    EmptyInputs,
    InputIdentityChanged,
    OutputShape,
}

pub(in crate::persistence) fn rebase_manifest(
    latest: &SegmentGenerationManifest,
    selection: &MergeSelection,
    output: VerifiedOutput,
) -> Result<SegmentGenerationManifest, RebaseRefusal> {
    use RebaseRefusal::*;
    if selection.inputs.is_empty() {
        return Err(EmptyInputs);
    }
    let collection_index = latest
        .collections
        .iter()
        .position(|collection| collection.collection_id == selection.collection_id)
        .ok_or(MissingCollection)?;
    let collection = &latest.collections[collection_index];
    if collection.collection_generation != selection.collection_generation
        || collection.schema_version != selection.schema_version
        || collection.schema != selection.schema
    {
        return Err(CollectionIdentityChanged);
    }
    let field_matches = |reference: &SegmentReference| {
        reference.role == SegmentRole::Field
            && reference.field.as_deref() == Some(selection.field.as_str())
    };
    if selection
        .inputs
        .iter()
        .any(|input| !field_matches(input) || input.kind != SegmentKind::Delta)
        || selection
            .inputs
            .windows(2)
            .any(|pair| pair[0].ordinal >= pair[1].ordinal)
    {
        return Err(InputIdentityChanged);
    }
    let positions: Vec<_> = collection
        .segments
        .iter()
        .enumerate()
        .filter(|(_, reference)| field_matches(reference) && reference.kind == SegmentKind::Delta)
        .map(|(index, _)| index)
        .collect();
    let current: Vec<_> = positions
        .iter()
        .map(|index| &collection.segments[*index])
        .collect();
    let selected: Vec<_> = selection.inputs.iter().collect();
    let start = current
        .windows(selected.len())
        .position(|window| window == selected.as_slice())
        .ok_or(InputIdentityChanged)?;
    let base_merge = selection.base.is_some();
    if base_merge && start != 0 {
        // Omitting an older delta would let a discarded deletion revive its value.
        return Err(InputIdentityChanged);
    }
    let mut removals: BTreeSet<usize> = positions[start..start + selected.len()]
        .iter()
        .copied()
        .collect();
    if let Some(base) = &selection.base {
        if !field_matches(base) || base.kind != SegmentKind::Base || base.ordinal != 0 {
            return Err(InputIdentityChanged);
        }
        let bases: Vec<_> = collection
            .segments
            .iter()
            .enumerate()
            .filter(|(_, reference)| {
                field_matches(reference) && reference.kind == SegmentKind::Base
            })
            .collect();
        if bases.len() != 1 || bases[0].1 != base {
            return Err(InputIdentityChanged);
        }
        removals.insert(bases[0].0);
    }
    let sidecars: Vec<_> = collection
        .segments
        .iter()
        .enumerate()
        .filter(|(_, reference)| {
            reference.role == SegmentRole::VectorEids
                && reference.field.as_deref() == Some(selection.field.as_str())
        })
        .collect();
    if base_merge {
        match (&selection.vector_sidecar, sidecars.as_slice()) {
            (None, []) => {}
            (Some(expected), [(index, actual)]) if expected == *actual => {
                removals.insert(*index);
            }
            _ => return Err(InputIdentityChanged),
        }
    } else if selection.vector_sidecar.is_some() {
        return Err(InputIdentityChanged);
    }
    let expected_ordinal = if base_merge {
        0
    } else {
        selection.inputs.last().unwrap().ordinal
    };
    let expected_kind = if base_merge {
        SegmentKind::Base
    } else {
        SegmentKind::Delta
    };
    if !field_matches(&output.field)
        || output.field.kind != expected_kind
        || output.field.ordinal != expected_ordinal
        || output.field.local_rows.is_none()
        || output.field.payload_sha256.is_none()
        || output.vector_sidecar.is_some() != selection.vector_sidecar.is_some()
    {
        return Err(OutputShape);
    }
    let newest_input = selection
        .inputs
        .iter()
        .filter_map(|input| input.applied_seq)
        .max()
        .unwrap_or(0);
    if !output
        .field
        .applied_seq
        .is_some_and(|sequence| sequence >= newest_input && sequence <= latest.checkpoint_sequence)
    {
        return Err(OutputShape);
    }
    if let Some(sidecar) = &output.vector_sidecar {
        if !base_merge
            || sidecar.role != SegmentRole::VectorEids
            || sidecar.field != output.field.field
            || sidecar.kind != SegmentKind::Base
            || sidecar.ordinal != 0
            || sidecar.local_rows.is_some()
            || sidecar.payload_sha256.is_none()
            || sidecar.applied_seq != output.field.applied_seq
        {
            return Err(OutputShape);
        }
    }
    // An output can replace the selected input's pathname. It cannot alias any
    // payload or row map that remains referenced by another field or collection.
    let mut occupied = BTreeSet::new();
    for (ci, catalog) in latest.collections.iter().enumerate() {
        for (index, reference) in catalog.segments.iter().enumerate() {
            if ci == collection_index && removals.contains(&index) {
                continue;
            }
            occupied.insert(reference.path.as_str());
            if let Some(rows) = &reference.local_rows {
                occupied.insert(rows.path.as_str());
            }
        }
    }
    for reference in std::iter::once(&output.field).chain(output.vector_sidecar.iter()) {
        if reference.path.is_empty() || !occupied.insert(reference.path.as_str()) {
            return Err(OutputShape);
        }
        if let Some(rows) = &reference.local_rows {
            if rows.path.is_empty() || !occupied.insert(rows.path.as_str()) {
                return Err(OutputShape);
            }
        }
    }
    let insert_at = *removals.first().expect("nonempty selected inputs");
    let mut next = latest.clone();
    let references = &mut next.collections[collection_index].segments;
    let mut replacement = Some(output.field);
    let mut sidecar = output.vector_sidecar;
    *references = collection
        .segments
        .iter()
        .enumerate()
        .flat_map(|(index, reference)| {
            if index == insert_at {
                let mut result = vec![replacement.take().expect("one insertion position")];
                result.extend(sidecar.take());
                result
            } else if removals.contains(&index) {
                Vec::new()
            } else {
                vec![reference.clone()]
            }
        })
        .collect();
    Ok(next)
}

#[cfg(test)]
mod tests;
