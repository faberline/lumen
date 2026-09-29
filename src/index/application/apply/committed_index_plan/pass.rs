//! The planning pass: a request id seen within its deadline plans nothing;
//! otherwise each borrowed item is interned, skipped when an equal or newer
//! version holds its cell, and recorded in the action ledger with the apply
//! cost its borrowed value will have, up to the first business error.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{bail, Result};

use crate::index::application::apply::committed_index_plan::{
    metadata_bound, BusinessError, ParsedHashes, PlanResult, PlanStamp, PlanView, PlannedCell,
    RequestOutcome, ScalarAction, ScalarPlan,
};
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::types::schema::FieldType;

#[cfg(test)]
use crate::index::domain::hash_index::parse_hash_number;

#[cfg(test)]
pub(super) fn plan<V: PlanView>(
    scanner: &FastIndexScanner<'_>,
    view: &V,
    now: Instant,
    mut reserve_total: impl FnMut(usize) -> Result<()>,
) -> Result<PlanResult> {
    reserve_total(metadata_bound(scanner)?)?;
    let hashes = scanner
        .items()
        .enumerate()
        .filter_map(|(ordinal, item)| {
            if view.field_type(item.field) != Some(FieldType::Hash) {
                return None;
            }
            let FastIndexValue::String(value) = item.value else {
                return None;
            };
            Some((ordinal, parse_hash_number(value).ok()))
        })
        .collect();
    plan_with_hashes(scanner, view, &hashes, now, reserve_total)
}

pub(in crate::index::application::apply) fn plan_with_hashes<V: PlanView>(
    scanner: &FastIndexScanner<'_>,
    view: &V,
    hashes: &ParsedHashes,
    now: Instant,
    mut reserve_total: impl FnMut(usize) -> Result<()>,
) -> Result<PlanResult> {
    let reservation = metadata_bound(scanner)?;
    reserve_total(reservation)?;
    if !view.is_live() {
        bail!(StorageError::Gone(
            "collection disappeared during planning".into()
        ));
    }
    let request = match scanner.request_id() {
        Some(id) => match view.request_deadline(id) {
            Some(deadline) if now <= deadline => RequestOutcome::Duplicate {
                request_id: id.to_owned(),
                deadline,
            },
            _ => RequestOutcome::Register(id.to_owned()),
        },
        None => RequestOutcome::None,
    };
    let stamp = PlanStamp {
        engine_epoch: view.engine_epoch(),
        collection_generation: view.collection_generation(),
        schema_version: view.schema_version(),
        data_version: view.data_version(),
        revision: view.revision(),
        interner_watermark: view.interner_len(),
    };
    if matches!(request, RequestOutcome::Duplicate { .. }) {
        return Ok(PlanResult::Planned(ScalarPlan {
            stamp,
            request,
            new_external_ids: vec![],
            actions: vec![],
            winners: BTreeMap::new(),
            bytes_by_field: BTreeMap::new(),
            applied: 0,
            source_items: scanner.cost().item_count,
            business_error: None,
            reservation,
        }));
    }

    let mut ids: BTreeMap<String, u32> = BTreeMap::new();
    let mut new_external_ids = Vec::new();
    let mut cells: BTreeMap<PlannedCell, bool> = BTreeMap::new();
    let mut versions: BTreeMap<PlannedCell, u64> = BTreeMap::new();
    let mut actions = Vec::new();
    let mut winners = BTreeMap::new();
    let mut bytes_by_field = BTreeMap::new();
    let mut applied = 0u32;

    for (ordinal, item) in scanner.items().enumerate() {
        let id = planned_id(
            view,
            &mut ids,
            &mut new_external_ids,
            stamp.interner_watermark,
            item.external_id,
        )?;
        let cell = PlannedCell {
            id,
            field: item.field.to_owned(),
        };
        let stored = versions
            .get(&cell)
            .copied()
            .or_else(|| view.cell_version(id, item.field));
        if item
            .version
            .is_some_and(|v| stored.is_some_and(|old| old >= v))
        {
            continue;
        }
        let Some(kind) = view.field_type(item.field) else {
            return Ok(PlanResult::Planned(finish(
                stamp,
                request,
                new_external_ids,
                actions,
                winners,
                bytes_by_field,
                applied,
                scanner.cost().item_count,
                reservation,
                Some(BusinessError::UnknownField {
                    field: item.field.to_owned(),
                }),
            )));
        };
        if !matches!(
            kind,
            FieldType::Keyword
                | FieldType::Number
                | FieldType::Set
                | FieldType::Text
                | FieldType::Hash
                | FieldType::Vector
        ) {
            return Ok(PlanResult::Unsupported {
                ordinal,
                field: item.field.to_owned(),
                kind,
            });
        }
        let cost = if kind == FieldType::Hash && matches!(item.value, FastIndexValue::String(_)) {
            let Some(value) = hashes.get(&ordinal) else {
                return Ok(PlanResult::NeedsHash);
            };
            value
                .map(|_| 12)
                .ok_or(BusinessError::InvalidHash { ordinal })
        } else {
            borrowed_apply_cost(
                kind,
                &item.value,
                item.external_id,
                item.field,
                (kind == FieldType::Vector).then(|| {
                    view.vector_dimension(item.field)
                        .expect("vector field exposes a declared dimension")
                }),
            )
        };
        match cost {
            Ok(bytes) => {
                cells.insert(cell.clone(), true);
                if let Some(v) = item.version {
                    versions.insert(cell.clone(), v);
                }
                winners.insert(cell.clone(), ordinal);
                *bytes_by_field.entry(cell.field.clone()).or_insert(0) += bytes;
                actions.push(ScalarAction::Apply {
                    ordinal,
                    cell,
                    kind,
                    bytes,
                });
                applied = applied.checked_add(1).expect("Index item count is bounded");
            }
            Err(error) => {
                // The live path marks the dirty cell even if no old cell exists.
                // `Drop` is therefore unconditional; attach performs its current
                // no-op drop when `cells` says it was absent, then journals it.
                let existed = cells
                    .get(&cell)
                    .copied()
                    .unwrap_or_else(|| view.has_cell(id, item.field));
                if existed {
                    cells.insert(cell.clone(), false);
                }
                winners.remove(&cell);
                actions.push(ScalarAction::Drop {
                    cell,
                    ordinal,
                    kind,
                });
                return Ok(PlanResult::Planned(finish(
                    stamp,
                    request,
                    new_external_ids,
                    actions,
                    winners,
                    bytes_by_field,
                    applied,
                    scanner.cost().item_count,
                    reservation,
                    Some(error),
                )));
            }
        }
    }
    Ok(PlanResult::Planned(finish(
        stamp,
        request,
        new_external_ids,
        actions,
        winners,
        bytes_by_field,
        applied,
        scanner.cost().item_count,
        reservation,
        None,
    )))
}

fn planned_id<V: PlanView>(
    view: &V,
    ids: &mut BTreeMap<String, u32>,
    new_ids: &mut Vec<String>,
    watermark: usize,
    eid: &str,
) -> Result<u32> {
    if let Some(id) = ids.get(eid) {
        return Ok(*id);
    }
    let id = match view.id(eid) {
        Some(id) => id,
        None => {
            let next = watermark
                .checked_add(new_ids.len())
                .ok_or_else(|| anyhow::anyhow!("interner watermark overflow"))?;
            let id = u32::try_from(next).map_err(|_| anyhow::anyhow!("interner id exceeds u32"))?;
            new_ids.push(eid.to_owned());
            id
        }
    };
    ids.insert(eid.to_owned(), id);
    Ok(id)
}

fn finish(
    stamp: PlanStamp,
    request: RequestOutcome,
    new_external_ids: Vec<String>,
    actions: Vec<ScalarAction>,
    winners: BTreeMap<PlannedCell, usize>,
    bytes_by_field: BTreeMap<String, u64>,
    applied: u32,
    source_items: usize,
    reservation: usize,
    business_error: Option<BusinessError>,
) -> ScalarPlan {
    ScalarPlan {
        stamp,
        request,
        new_external_ids,
        actions,
        winners,
        bytes_by_field,
        applied,
        source_items,
        business_error,
        reservation,
    }
}

/// Keep this body in lockstep with the scalar arms of `apply_value`.  It uses
/// borrowed terms.  `Set` scans the next lexical distinct member on each pass,
/// as `committed_scalar_files::Selected::set_row` does, so it owns no member
/// set while planning.
fn borrowed_apply_cost(
    kind: FieldType,
    value: &FastIndexValue<'_>,
    eid: &str,
    field: &str,
    vector_dimension: Option<u32>,
) -> std::result::Result<u64, BusinessError> {
    match (kind, value) {
        // Text is staged after the planner has captured its small metadata.
        // Keep the ordered action now; exact row bytes are written into it
        // before attachment.
        (FieldType::Text, FastIndexValue::String(_)) => Ok(0),
        (FieldType::Keyword, FastIndexValue::String(value)) => Ok((value.len() + eid.len()) as u64),
        (FieldType::Number, FastIndexValue::Number(value)) => SortableF64::new(*value)
            .map(|_| (8 + eid.len()) as u64)
            .map_err(|e| BusinessError::InvalidNumber(e.to_string())),
        (FieldType::Set, FastIndexValue::StringList(values)) => set_bytes(*values, eid.len()),
        (FieldType::Vector, FastIndexValue::Vector { len, .. }) => {
            let expected = vector_dimension.expect("vector dimension supplied for vector action");
            if *len != expected as usize {
                return Err(BusinessError::InvalidVectorDimension {
                    field: field.to_owned(),
                    expected,
                    got: *len,
                });
            }
            Ok((expected as u64) * std::mem::size_of::<f32>() as u64 + eid.len() as u64)
        }
        (FieldType::Set, FastIndexValue::String(_)) => Err(BusinessError::TypeMismatch {
            field: field.to_owned(),
            expected: FieldType::Set,
            got: "string (expected array of strings)",
        }),
        (expected, value) => Err(BusinessError::TypeMismatch {
            field: field.to_owned(),
            expected,
            got: fast_kind(value),
        }),
    }
}

fn set_bytes(
    values: crate::ingest::infrastructure::wal::fast_index_scanner::FastStringList<'_>,
    eid_len: usize,
) -> std::result::Result<u64, BusinessError> {
    let mut total = 0u64;
    let mut previous = None;
    loop {
        let next = values
            .values()
            .filter(|value| previous.is_none_or(|old| *value > old))
            .min();
        let Some(value) = next else {
            return Ok(total);
        };
        total = total
            .checked_add((value.len() + eid_len) as u64)
            .expect("Index wire length fits usize and u64");
        previous = Some(value);
    }
}

fn fast_kind(value: &FastIndexValue<'_>) -> &'static str {
    match value {
        FastIndexValue::String(_) => "string",
        FastIndexValue::Number(_) => "number",
        FastIndexValue::Vector { .. } => "f32[]",
        FastIndexValue::StringList(_) => "string[]",
    }
}
