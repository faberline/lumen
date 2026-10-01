//! The replacement planning pass: every document id interned first, as the
//! owned path does, then each document in request order dropped when stale,
//! left as it was when a field fails validation, or turned into actions for its
//! present and omitted fields, with the final document and checksum metadata
//! apply installs.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::{bail, Result};

use crate::index::application::apply::committed_index_plan::{
    PlanStamp, PlanView, PlannedCell, RequestOutcome, ScalarAction, ScalarPlan,
};
use crate::index::application::apply::committed_replace_plan::source_value::{
    same_source, validate_borrowed, ValidationError,
};
use crate::index::application::apply::committed_replace_plan::{
    metadata_bound, ParsedValues, ReplaceChecksumFinal, ReplaceDocFinal, ReplaceItemError,
    ReplaceItemOutcome, ReplacePlan, ReplacePlanView,
};
use crate::index::domain::storage_error::StorageError;
use crate::ingest::infrastructure::wal::borrowed_replace_spool::BorrowedReplaceSpoolDoc as ReplaceDocDescriptor;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::types::schema::FieldType;

pub(in crate::index::application::apply) fn plan<V: ReplacePlanView>(
    scanner: &FastIndexScanner<'_>,
    docs: &[ReplaceDocDescriptor<'_>],
    parsed: &ParsedValues,
    view: &V,
    now: Instant,
    mut reserve_total: impl FnMut(usize) -> Result<()>,
) -> Result<ReplacePlan> {
    let reservation = metadata_bound(scanner, docs, view)?;
    reserve_total(reservation)?;
    if !view.is_live() {
        bail!(StorageError::Gone(scanner.collection_id().to_owned()));
    }
    let stamp = PlanStamp {
        engine_epoch: view.engine_epoch(),
        collection_generation: view.collection_generation(),
        schema_version: view.schema_version(),
        data_version: view.data_version(),
        revision: view.revision(),
        interner_watermark: view.interner_len(),
    };
    let mut ids = BTreeMap::<String, u32>::new();
    let mut new_external_ids = Vec::new();
    // Preserve owned replace behavior: intern all document IDs before any
    // stale or schema check. This is also why there is no request-id outcome.
    for doc in docs {
        planned_id(
            view,
            &mut ids,
            &mut new_external_ids,
            stamp.interner_watermark,
            doc.external_id,
        )?;
    }

    let mut actions = Vec::new();
    let mut winners = BTreeMap::new();
    let mut bytes_by_field = BTreeMap::<String, u64>::new();
    let mut outcomes = Vec::with_capacity(docs.len());
    let mut final_docs = BTreeMap::<u32, ReplaceDocFinal>::new();
    let mut final_checksums = BTreeMap::<PlannedCell, Option<u64>>::new();
    let mut virtual_versions = BTreeMap::<u32, Option<u64>>::new();
    let mut source_winners = BTreeMap::<PlannedCell, (usize, FieldType, Option<u64>)>::new();
    let mut written = 0u64;
    let mut skipped = 0u64;

    for doc in docs {
        let id = *ids.get(doc.external_id).expect("all document ids planned");
        let stored = virtual_versions
            .get(&id)
            .copied()
            .flatten()
            .or_else(|| view.doc_version(id));
        if doc
            .version
            .is_some_and(|incoming| stored.is_some_and(|old| old >= incoming))
        {
            outcomes.push(ReplaceItemOutcome::Dropped {
                current_version: stored.expect("stale has stored version"),
            });
            continue;
        }
        let items: Vec<_> = scanner
            .items()
            .enumerate()
            .skip(doc.fields_start)
            .take(doc.fields_end - doc.fields_start)
            .collect();
        // The adapter's descriptor ranges must cover exactly this document.
        if items
            .iter()
            .any(|(_, item)| item.external_id != doc.external_id)
        {
            bail!("replace descriptor does not match flattened source");
        }
        let mut supplied = BTreeSet::new();
        let mut validated = Vec::with_capacity(items.len());
        let mut error: Option<ReplaceItemError> = None;
        for (ordinal, item) in &items {
            if !supplied.insert(item.field.to_owned()) {
                bail!(
                    "replacement descriptor has duplicate sorted field `{}`",
                    item.field
                );
            }
            let Some(kind) = view.field_type(item.field) else {
                error = Some(ReplaceItemError::UnknownField {
                    field: item.field.to_owned(),
                });
                break;
            };
            match validate_borrowed(
                *ordinal,
                kind,
                &item.value,
                item.field,
                doc.external_id.len(),
                view.vector_dimension(item.field),
                parsed.get(ordinal).copied(),
            ) {
                Ok((bytes, checksum)) => {
                    validated.push((*ordinal, item.field, kind, bytes, checksum))
                }
                Err(ValidationError::Domain(domain)) => {
                    error = Some(domain);
                    break;
                }
                Err(ValidationError::Preparation(message)) => {
                    bail!("replacement preparation required: {message}")
                }
            }
        }
        if let Some(error) = error {
            outcomes.push(ReplaceItemOutcome::Error { error });
            continue;
        }
        let old: BTreeSet<String> = final_docs
            .get(&id)
            .map(|d| d.fields.iter().cloned().collect())
            .or_else(|| view.old_fields(id).map(|f| f.iter().cloned().collect()))
            .unwrap_or_default();
        // Omitted scalar actions carry `usize::MAX`: the common Vector drop
        // uses `cell.id` for its external id and must never index this sentinel.
        for field in old.difference(&supplied) {
            let kind = view
                .field_type(field)
                .expect("old coverage field has schema");
            let cell = PlannedCell {
                id,
                field: field.clone(),
            };
            actions.push(ScalarAction::Drop {
                cell: cell.clone(),
                ordinal: usize::MAX,
                kind,
            });
            winners.remove(&cell);
            source_winners.remove(&cell);
            final_checksums.insert(cell, None);
        }
        let mut item_written = 0u32;
        let mut item_skipped = 0u32;
        for (ordinal, field, kind, bytes, checksum) in validated {
            let cell = PlannedCell {
                id,
                field: field.to_owned(),
            };
            let unchanged = match source_winners.get(&cell) {
                Some((prior, prior_kind, prior_checksum)) => {
                    *prior_kind == kind
                        && same_source(
                            scanner,
                            *prior,
                            ordinal,
                            kind,
                            *prior_checksum,
                            checksum,
                            parsed,
                        )
                }
                None if old.contains(field) => {
                    view.existing_unchanged(id, field, kind, ordinal, checksum)
                }
                None => false,
            };
            if unchanged {
                item_skipped = item_skipped
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("replace item fields_skipped overflow"))?;
                continue;
            }
            if old.contains(field) || source_winners.contains_key(&cell) {
                actions.push(ScalarAction::Drop {
                    cell: cell.clone(),
                    ordinal,
                    kind,
                });
            }
            actions.push(ScalarAction::Apply {
                ordinal,
                cell: cell.clone(),
                kind,
                bytes,
            });
            winners.insert(cell.clone(), ordinal);
            let total = bytes_by_field.entry(field.to_owned()).or_insert(0);
            *total = total
                .checked_add(bytes)
                .ok_or_else(|| anyhow::anyhow!("replace field bytes overflow"))?;
            source_winners.insert(cell.clone(), (ordinal, kind, checksum));
            final_checksums.insert(
                cell,
                if matches!(kind, FieldType::Text | FieldType::Vector) {
                    checksum
                } else {
                    None
                },
            );
            item_written = item_written
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("replace item fields_written overflow"))?;
        }
        if supplied.is_empty() {
            final_checksums.retain(|cell, _| cell.id != id);
        }
        let effective_version = doc.version.or(stored);
        final_docs.insert(
            id,
            ReplaceDocFinal {
                id,
                fields: supplied.into_iter().collect(),
                version: effective_version,
            },
        );
        virtual_versions.insert(id, effective_version);
        written = written
            .checked_add(u64::from(item_written))
            .ok_or_else(|| anyhow::anyhow!("replace fields_written overflow"))?;
        skipped = skipped
            .checked_add(u64::from(item_skipped))
            .ok_or_else(|| anyhow::anyhow!("replace fields_skipped overflow"))?;
        outcomes.push(ReplaceItemOutcome::Ok {
            fields_written: item_written,
            fields_skipped: item_skipped,
        });
    }
    let applied = u32::try_from(
        actions
            .iter()
            .filter(|a| matches!(a, ScalarAction::Apply { .. }))
            .count(),
    )
    .map_err(|_| anyhow::anyhow!("replace apply count exceeds u32"))?;
    Ok(ReplacePlan {
        scalar: ScalarPlan {
            stamp,
            request: RequestOutcome::None,
            new_external_ids,
            actions,
            winners,
            bytes_by_field,
            applied,
            source_items: scanner.cost().item_count,
            business_error: None,
            reservation,
        },
        outcomes,
        final_docs: final_docs.into_values().collect(),
        final_checksums: final_checksums
            .into_iter()
            .map(|(cell, checksum)| ReplaceChecksumFinal { cell, checksum })
            .collect(),
        fields_written: written,
        fields_skipped: skipped,
        reservation,
    })
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
            let id = u32::try_from(
                watermark
                    .checked_add(new_ids.len())
                    .ok_or_else(|| anyhow::anyhow!("interner watermark overflow"))?,
            )
            .map_err(|_| anyhow::anyhow!("interner id exceeds u32"))?;
            new_ids.push(eid.to_owned());
            id
        }
    };
    ids.insert(eid.to_owned(), id);
    Ok(id)
}
