//! The committed fast path both routes share: a retained Index record's fields,
//! or a staged replacement's, planned under the state read lock, priced and
//! prepared outside every lock, then applied in source order under one apply
//! lease once the capture stamp still matches, with versions, coverage and
//! checksums installed for the successful prefix.

mod attach;
mod stage;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::{anyhow, Result};

use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::apply::committed_index_apply::{
    cell_bytes, composed, require, scalar_bytes, FieldPlan, Prepared, View,
};
#[cfg(test)]
use crate::index::application::apply::committed_index_apply::{
    BEFORE_ATTACH, TEXT_WORKSPACE_RETRIES,
};
use crate::index::application::apply::committed_index_plan::{
    self as plan, PlanView, ScalarAction,
};
use crate::index::application::apply::committed_replace_apply::{
    ReplacementInput, ReplacementLedger,
};
use crate::index::application::apply::committed_replace_plan as replace_plan;
use crate::index::application::engine::index::MAX_INDEX_ITEMS;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::parse_hash_number;
use crate::index::domain::storage_error::StorageError;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::capture_barrier::ApplyLease;
use crate::shared_kernel::types::document::{FieldValue, MAX_BATCH_REPLACE_SIZE};
use crate::shared_kernel::types::schema::FieldType;

impl Engine {
    pub(in crate::index::application::apply) fn try_apply_committed_fields_with_capacity_owner(
        &self,
        scanner: &FastIndexScanner<'_>,
        replacement: Option<&ReplacementInput<'_>>,
        sequence: u64,
        ensure_owner: &mut dyn FnMut() -> Result<()>,
        complete: impl FnOnce(&ApplyLease<'_>, Result<ApplyOutcome>),
    ) -> Result<bool> {
        let collection = scanner.collection_id();
        let (item_count, item_limit) = replacement
            .map_or((scanner.cost().item_count, MAX_INDEX_ITEMS), |input| {
                (input.docs.len(), MAX_BATCH_REPLACE_SIZE)
            });
        // These business errors need no value decoder or preparation. Recheck
        // them inside apply so restore cannot turn a stale refusal into a cut.
        {
            let apply = self.capture_barrier.apply();
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            let error = match state.collections.get(collection) {
                None => Some(StorageError::CollectionNotFound(collection.to_owned())),
                Some(_) if item_count > item_limit => Some(StorageError::BulkLimit {
                    got: item_count,
                    max: item_limit,
                }),
                Some(coll) if coll.deleted_at.is_some() => {
                    Some(StorageError::Gone(collection.to_owned()))
                }
                _ => None,
            };
            if let Some(error) = error {
                drop(state);
                complete(&apply, Err(error.into()));
                return Ok(true);
            }
        }
        let metadata = match replacement {
            None => plan::metadata_bound(scanner)?,
            Some(input) => {
                let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
                match state.collections.get(collection) {
                    Some(coll) => replace_plan::metadata_bound(
                        scanner,
                        input.docs,
                        &crate::index::application::apply::committed_replace_view::View {
                            engine: self,
                            coll,
                            scanner,
                            parsed: &input.parsed,
                            now: Instant::now(),
                        },
                    )?,
                    None => 0,
                }
            }
        };
        // Price the additional adapter maps, row handles and snapshot Arc
        // vectors before constructing them. Value bytes stay in the source.
        // Scalar-only records keep this exact existing floor. Text metadata
        // is added only after the ledger has proved a Text action exists.
        let mut floor = metadata
            .checked_mul(3)
            .ok_or(RecordAdmissionError::Overflow)?;
        let text_metadata = if replacement.is_some() {
            text_preparation::borrowed_field_metadata_bound(scanner)?
        } else {
            text_preparation::borrowed_text_metadata_bound(scanner)?
        };
        let request = self.record_ram_request_from_bound(floor, 0);
        let mut reservation = match self.try_reserve_record_ram(&request) {
            Ok(reservation) => reservation,
            Err(RecordAdmissionError::Capacity(
                crate::ingest::domain::change_budget::AdmissionError::Full { .. },
            )) => self.wait_reserve_record_ram(&request)?,
            Err(error) => return Err(error.into()),
        };
        let mut hashes = plan::ParsedHashes::new();
        if let Some(input) = replacement {
            hashes.extend(
                input
                    .parsed
                    .iter()
                    .map(|(ordinal, value)| (*ordinal, value.hash)),
            );
        }
        loop {
            // Only the schema/ordinal selection holds a state lock. Parsing a
            // valid giant leading-zero string must never hold state or apply.
            let hash_ordinals: BTreeSet<_> = {
                let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
                scanner
                    .items()
                    .enumerate()
                    .filter_map(|(ordinal, item)| {
                        (!hashes.contains_key(&ordinal)
                            && matches!(item.value, FastIndexValue::String(_))
                            && state
                                .collections
                                .get(collection)
                                .and_then(|coll| coll.fields.get(item.field))
                                .is_some_and(|index| index.field_type() == FieldType::Hash))
                        .then_some(ordinal)
                    })
                    .collect()
            };
            for (ordinal, item) in scanner.items().enumerate() {
                if hash_ordinals.contains(&ordinal) {
                    let FastIndexValue::String(value) = item.value else {
                        unreachable!("selected Hash source is a String")
                    };
                    hashes.insert(ordinal, parse_hash_number(value).ok());
                }
            }
            drop(hash_ordinals);
            let mut prepared = {
                let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
                let Some(coll) = state.collections.get(collection) else {
                    drop(state);
                    // Re-enter through the small business-error path.
                    drop(reservation);
                    return self.try_apply_committed_fields_with_capacity_owner(
                        scanner,
                        replacement,
                        sequence,
                        ensure_owner,
                        complete,
                    );
                };
                if coll.deleted_at.is_some() {
                    drop(state);
                    drop(reservation);
                    return self.try_apply_committed_fields_with_capacity_owner(
                        scanner,
                        replacement,
                        sequence,
                        ensure_owner,
                        complete,
                    );
                }
                let now = Instant::now();
                let view = View {
                    engine: self,
                    coll,
                    now,
                };
                let mut replacement_ledger = None;
                let plan = if let Some(input) = replacement {
                    let replace_view =
                        crate::index::application::apply::committed_replace_view::View {
                            engine: self,
                            coll,
                            scanner,
                            parsed: &input.parsed,
                            now,
                        };
                    let required =
                        replace_plan::metadata_bound(scanner, input.docs, &replace_view)?
                            .checked_mul(3)
                            .ok_or(RecordAdmissionError::Overflow)?;
                    if reservation.bytes() < required {
                        drop(state);
                        require(&mut reservation, required)?;
                        floor = floor.max(required);
                        continue;
                    }
                    floor = floor.max(required);
                    let replacement_plan = replace_plan::pass::plan(
                        scanner,
                        input.docs,
                        &input.parsed,
                        &replace_view,
                        now,
                        |bytes| {
                            anyhow::ensure!(
                                bytes <= reservation.bytes(),
                                "replacement metadata was not reserved"
                            );
                            Ok(())
                        },
                    )?;
                    replacement_ledger = Some(ReplacementLedger {
                        outcomes: replacement_plan.outcomes,
                        results: Vec::new(),
                        final_docs: replacement_plan.final_docs,
                        final_checksums: replacement_plan.final_checksums,
                        fields_written: replacement_plan.fields_written,
                        fields_skipped: replacement_plan.fields_skipped,
                    });
                    replacement_plan.scalar
                } else {
                    match plan::pass::plan_with_hashes(scanner, &view, &hashes, now, |bytes| {
                        anyhow::ensure!(
                            bytes <= reservation.bytes(),
                            "borrowed planner metadata was not reserved"
                        );
                        Ok(())
                    })? {
                        plan::PlanResult::Unsupported { .. } => return Ok(false),
                        plan::PlanResult::NeedsHash => continue,
                        plan::PlanResult::Planned(plan) => *plan,
                    }
                };
                let blocked = plan.winners.keys().find_map(|cell| {
                    let view = coll.fields.get(&cell.field).and_then(composed)?;
                    (view.incremental_layer_count() >= self.layer_maintenance.append_limit()).then(
                        || {
                            if view.has_private_layers() {
                                crate::persistence::application::capacity::Work::Checkpoint
                            } else {
                                crate::persistence::application::capacity::Work::Merge
                            }
                        },
                    )
                });
                if let Some(work) = blocked {
                    drop(plan);
                    drop(state);
                    if self.layer_maintenance.owner().is_none() {
                        ensure_owner()?;
                    }
                    if let Some(owner) = self.layer_maintenance.owner() {
                        owner.wait_for(work)?;
                    }
                    continue;
                }
                // A duplicate, stale item or failed replacement has no reader
                // append. Price the Arc-vector copy once per winning field.
                let mut view_bytes = 0usize;
                let mut seen_fields = BTreeSet::new();
                for cell in plan.winners.keys() {
                    if !seen_fields.insert(cell.field.as_str()) {
                        continue;
                    }
                    if let Some(view) = coll.fields.get(&cell.field).and_then(composed) {
                        view_bytes = view_bytes
                            .checked_add(
                                view.private_append_metadata_bound()
                                    .ok_or(RecordAdmissionError::Overflow)?,
                            )
                            .ok_or(RecordAdmissionError::Overflow)?;
                    }
                }
                let required = floor
                    .checked_add(view_bytes)
                    .and_then(|bytes| {
                        plan.has_text()
                            .then_some(text_metadata)
                            .map_or(Some(bytes), |text| bytes.checked_add(text))
                    })
                    .ok_or(RecordAdmissionError::Overflow)?;
                if reservation.bytes() < required {
                    drop(seen_fields);
                    drop(plan);
                    drop(state);
                    require(&mut reservation, required)?;
                    continue;
                }
                let mut fields = BTreeMap::new();
                let mut sizes = BTreeMap::new();
                let mut rows = BTreeMap::new();
                let mut text_actions = BTreeMap::new();
                let mut text_rows = BTreeMap::new();
                let mut old_text_rows = Vec::new();
                let mut hash_rows = BTreeMap::new();
                let mut vector_codebooks = BTreeMap::new();
                for action in &plan.actions {
                    let (cell, next) = match action {
                        ScalarAction::Apply {
                            cell,
                            kind: FieldType::Vector,
                            ..
                        } => {
                            if !vector_codebooks.contains_key(&cell.field) {
                                let FieldIndex::Vector { idx, .. } = &coll.fields[&cell.field]
                                else {
                                    unreachable!("matched Vector schema")
                                };
                                vector_codebooks.insert(
                                    cell.field.clone(),
                                    idx.checkpoint_codebook_for_preparation()?,
                                );
                            }
                            continue;
                        }
                        ScalarAction::Drop {
                            kind: FieldType::Vector,
                            ..
                        } => continue,
                        ScalarAction::Apply {
                            cell,
                            kind: FieldType::Hash,
                            ordinal,
                            ..
                        } => {
                            hash_rows.insert(
                                cell.clone(),
                                Some(hashes[ordinal].expect("planned valid Hash")),
                            );
                            continue;
                        }
                        ScalarAction::Drop {
                            cell,
                            kind: FieldType::Hash,
                            ..
                        } => {
                            hash_rows.insert(cell.clone(), None);
                            continue;
                        }
                        ScalarAction::Apply {
                            cell,
                            kind: FieldType::Text,
                            ordinal,
                            ..
                        } => {
                            text_actions.insert(*ordinal, cell.field.clone());
                            if let Some(FieldIndex::Text { idx, .. }) = coll.fields.get(&cell.field)
                            {
                                if let Some(row) = idx.staged_rows.get(&cell.id) {
                                    old_text_rows.push(row.clone());
                                }
                            }
                            continue;
                        }
                        ScalarAction::Drop {
                            cell,
                            kind: FieldType::Text,
                            ..
                        } => {
                            if let Some(FieldIndex::Text { idx, .. }) = coll.fields.get(&cell.field)
                            {
                                if let Some(row) = idx.staged_rows.get(&cell.id) {
                                    old_text_rows.push(row.clone());
                                }
                            }
                            text_rows.insert(cell.clone(), None);
                            continue;
                        }
                        ScalarAction::Apply { cell, bytes, .. } => (cell, *bytes),
                        ScalarAction::Drop { cell, .. } => (cell, 0),
                    };
                    let index = &coll.fields[&cell.field];
                    let field = fields
                        .entry(cell.field.clone())
                        .or_insert_with(|| FieldPlan {
                            kind: index.field_type(),
                            before: composed(index).cloned(),
                            after: None,
                            winners: Vec::new(),
                            final_bytes: scalar_bytes(index),
                        });
                    let old = sizes.entry(cell.clone()).or_insert_with(|| {
                        if view.has_cell(cell.id, &cell.field) {
                            cell_bytes(index, cell.id, coll.interner.resolve(cell.id).len())
                        } else {
                            0
                        }
                    });
                    field.final_bytes = field.final_bytes.saturating_sub(*old).saturating_add(next);
                    *old = next;
                    rows.insert(cell.clone(), None);
                }
                for (cell, ordinal) in &plan.winners {
                    if matches!(
                        coll.fields[&cell.field].field_type(),
                        FieldType::Text | FieldType::Hash | FieldType::Vector
                    ) {
                        continue;
                    }
                    fields
                        .get_mut(&cell.field)
                        .expect("winner has action")
                        .winners
                        .push((cell.id, *ordinal));
                }
                let old_vector_rows = Vec::with_capacity(
                    plan.actions
                        .iter()
                        .filter(|action| {
                            matches!(
                                action,
                                ScalarAction::Apply {
                                    kind: FieldType::Vector,
                                    ..
                                } | ScalarAction::Drop {
                                    kind: FieldType::Vector,
                                    ..
                                }
                            )
                        })
                        .count(),
                );
                Prepared {
                    plan,
                    business_error: None,
                    replacement: replacement_ledger,
                    fields,
                    hash_rows,
                    vector_codebooks,
                    vector_rows: BTreeMap::new(),
                    old_vector_rows,
                    rows,
                    text_rows,
                    text_actions,
                    text_prepared: None,
                    _old_text_rows: old_text_rows,
                    text_placeholder: FieldValue::String(String::new()),
                    retained: required,
                }
            };

            // Error response formatting can include the caller's original Hash
            // string. It is response data, never a retained checkpoint value,
            // and it must finish before state/apply is acquired.
            prepared.business_error = prepared
                .plan
                .business_error
                .take()
                .map(|error| error.into_error(collection, scanner));

            if let Some(ledger) = &mut prepared.replacement {
                ledger.render(collection, scanner);
            }

            if !prepared.text_actions.is_empty() {
                let rows = match self.prepare_borrowed_text_rows_for_actions(
                    scanner,
                    &prepared.text_actions,
                    prepared.retained,
                    &mut reservation,
                ) {
                    Ok(rows) => rows,
                    Err(error)
                        if error
                            .downcast_ref::<text_preparation::RequiredBorrowedTextWorkspace>()
                            .is_some() =>
                    {
                        let required = error
                            .downcast_ref::<text_preparation::RequiredBorrowedTextWorkspace>()
                            .expect("matched borrowed Text workspace")
                            .required_bytes;
                        drop(prepared);
                        #[cfg(test)]
                        TEXT_WORKSPACE_RETRIES
                            .with(|retries| retries.set(retries.get().saturating_add(1)));
                        require(&mut reservation, required)?;
                        continue;
                    }
                    Err(error)
                        if error
                            .downcast_ref::<text_preparation::StaleBorrowedTextPreparation>()
                            .is_some() =>
                    {
                        drop(prepared);
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                prepared.retained = prepared
                    .retained
                    .checked_add(rows.retained_reader_bytes())
                    .ok_or(RecordAdmissionError::Overflow)?;
                let planned_text: Vec<_> = prepared
                    .plan
                    .text_actions()
                    .map(|(cell, ordinal)| (cell.clone(), ordinal))
                    .collect();
                for (cell, ordinal) in planned_text {
                    let row = rows
                        .get(ordinal, &cell.field)
                        .expect("accepted Text action has a staged row");
                    let external_id = scanner
                        .items()
                        .nth(ordinal)
                        .expect("validated Text action ordinal")
                        .external_id;
                    prepared
                        .plan
                        .set_text_bytes(ordinal, row.indexed_bytes(external_id))?;
                    if prepared.plan.winners.get(&cell) == Some(&ordinal) {
                        prepared.text_rows.insert(cell, Some(row.clone()));
                    }
                }
                // Keep every staged row alive through attach. Non-winner rows
                // drop only after the apply lease, along with displaced rows.
                prepared.text_prepared = Some(rows);
            }
            // The source and canonical SQ checkpoint view use private aligned
            // mmap files. Only a bounded decode chunk and reader metadata are
            // admitted. No input or checkpoint Vec is built under apply.
            stage::stage_committed_files(scanner, sequence, &mut prepared, &mut reservation)?;
            require(&mut reservation, prepared.retained)?;
            reservation
                .finish_borrowed_preparation(prepared.retained)
                .map_err(RecordAdmissionError::Capacity)?;
            #[cfg(test)]
            if let Some(hook) = BEFORE_ATTACH.with(|hook| hook.borrow_mut().take()) {
                hook();
            }
            let apply = self.capture_barrier.apply();
            let mut telemetry = self.metrics.committed_apply_telemetry();
            let state_write_wait_started = Instant::now();
            let state_write = self.state.write();
            telemetry.record_state_write_lock_wait(state_write_wait_started.elapsed());
            let mut state = state_write.map_err(|_| anyhow!("state poisoned"))?;
            telemetry.start_state_write_lock_hold();
            let matches = state.collections.get(collection).is_some_and(|coll| {
                prepared.plan.matches(
                    &View {
                        engine: self,
                        coll,
                        now: Instant::now(),
                    },
                    Instant::now(),
                )
            });
            let capacity_changed = prepared.fields.values().any(|field| {
                !field.winners.is_empty()
                    && field.before.as_ref().is_some_and(|view| {
                        view.incremental_layer_count() >= self.layer_maintenance.append_limit()
                    })
            });
            if !matches || capacity_changed {
                drop(state);
                telemetry.finish_state_write_lock_hold();
                drop(apply);
                drop(prepared); // private file owners and old views drop outside apply
                reservation
                    .shrink_borrowed_to(floor)
                    .map_err(RecordAdmissionError::Capacity)?;
                continue;
            }
            let charge = self.retain_borrowed_reservation(reservation)?;
            let coll = state
                .collections
                .get_mut(collection)
                .expect("matched collection");
            attach::attach_scalar_text_and_hash(
                coll,
                replacement,
                item_count,
                &mut prepared,
                &charge,
            )?;
            attach::attach_vectors(coll, &mut prepared, &charge, &apply, &mut telemetry)?;
            attach::attach_versions_and_journal(coll, scanner, replacement, &mut prepared, &charge);
            let outcome = self.committed_apply_outcome(coll, &mut prepared);
            self.publish_storage_bytes(&state);
            drop(state);
            telemetry.finish_state_write_lock_hold();
            complete(&apply, outcome);
            drop(apply);
            drop(prepared);
            return Ok(true);
        }
    }
}
