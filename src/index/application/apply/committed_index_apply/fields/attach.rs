//! The attach steps that run under the apply lease and the state write lock,
//! once the plan still matches: scalar segments, Text and Hash rows, Vector
//! backend updates in source order, versions, coverage and checksums, the
//! scalar journal, and the outcome.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use crate::app::observability::metrics::apply_telemetry::CommittedApplyTelemetry;
use crate::index::application::apply::apply_prepared_value;
use crate::index::application::apply::committed_index_apply::Prepared;
use crate::index::application::apply::committed_index_plan::{RequestOutcome, ScalarAction};
use crate::index::application::apply::committed_replace_apply::ReplacementInput;
use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::application::live_delta::retire_live_delta_overlay;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::domain::change_budget::RetainedCharge;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::capture_barrier::ApplyLease;
use crate::shared_kernel::types::document::{IndexResponse, ReplaceDocResult, ReplaceDocsResponse};
use crate::shared_kernel::types::schema::FieldType;

/// Install the scalar segments and byte counts, then the Text and Hash rows.
pub(super) fn attach_scalar_text_and_hash(
    coll: &mut Collection,
    replacement: Option<&ReplacementInput<'_>>,
    item_count: usize,
    prepared: &mut Prepared,
    charge: &RetainedCharge,
) -> Result<()> {
    if replacement.is_none() {
        coll.gc_requests();
    }
    if let RequestOutcome::Register(id) = &prepared.plan.request {
        coll.seen_requests.push_back((id.clone(), Instant::now()));
    }
    let duplicate = matches!(prepared.plan.request, RequestOutcome::Duplicate { .. });
    if !duplicate && item_count != 0 {
        coll.clear_search_cache();
    }
    if prepared.plan.has_text() || (replacement.is_some() && item_count != 0) {
        coll.clear_text_rank_caches();
    }
    for eid in &prepared.plan.new_external_ids {
        coll.interner.intern(eid);
    }
    if prepared
        .fields
        .values()
        .any(|f| matches!(f.kind, FieldType::Keyword | FieldType::Number))
    {
        coll.clear_number_filter_caches();
    }
    for (name, field) in &mut prepared.fields {
        let index = coll.fields.get_mut(name).expect("matched scalar field");
        if let Some(after) = field.after.take() {
            match index {
                FieldIndex::Keyword(k) => k.segment = Some(after),
                FieldIndex::Number(n) => n.segment = Some(after),
                FieldIndex::Set(s) => s.segment = Some(after),
                _ => unreachable!("matched scalar kind"),
            }
        }
        match index {
            FieldIndex::Keyword(k) => k.bytes = field.final_bytes,
            FieldIndex::Number(n) => n.bytes = field.final_bytes,
            FieldIndex::Set(s) => s.bytes = field.final_bytes,
            _ => unreachable!("matched scalar kind"),
        }
    }
    for (cell, row) in &prepared.text_rows {
        let eid = coll.interner.resolve(cell.id).to_owned();
        let index = coll
            .fields
            .get_mut(&cell.field)
            .expect("matched Text row field");
        // `drop_eid` maintains Text doc_count, total_doc_len and
        // indexed-byte counters. Keep old row Arcs in Prepared until
        // after state and the apply lease are released.
        index.drop_eid(cell.id, &eid);
        if let Some(row) = row {
            apply_prepared_value(
                index,
                cell.id,
                &eid,
                &prepared.text_placeholder,
                &cell.field,
                Some(row),
                None,
            )?;
        }
        coll.next_field_dirty_revision = coll.next_field_dirty_revision.saturating_add(1);
        coll.field_dirty
            .entry(cell.field.clone())
            .or_default()
            .insert(eid.clone(), coll.next_field_dirty_revision);
        let value = row
            .as_ref()
            .map(|row| Arc::new(CheckpointValue::StagedText(row.clone())));
        coll.change_journal.record_charged(
            cell.field.clone(),
            eid,
            coll.next_field_dirty_revision,
            value,
            Some(charge.clone()),
        );
    }
    for (cell, value) in &prepared.hash_rows {
        let eid = coll.interner.resolve(cell.id).to_owned();
        let index = coll
            .fields
            .get_mut(&cell.field)
            .expect("matched Hash field");
        index.drop_eid(cell.id, &eid);
        if let Some(value) = value {
            let FieldIndex::Hash(hash) = index else {
                unreachable!("matched Hash kind")
            };
            hash.forward.insert(cell.id, *value);
            hash.bytes += 12;
        }
        coll.next_field_dirty_revision = coll.next_field_dirty_revision.saturating_add(1);
        coll.field_dirty
            .entry(cell.field.clone())
            .or_default()
            .insert(eid.clone(), coll.next_field_dirty_revision);
        coll.change_journal.record_charged(
            cell.field.clone(),
            eid,
            coll.next_field_dirty_revision,
            value.map(|value| Arc::new(CheckpointValue::Hash(value))),
            Some(charge.clone()),
        );
    }
    Ok(())
}

/// Apply each Vector action to its backend. A backend fault marks the apply
/// lease uncertain and returns the error.
pub(super) fn attach_vectors(
    coll: &mut Collection,
    prepared: &mut Prepared,
    charge: &RetainedCharge,
    apply: &ApplyLease<'_>,
    telemetry: &mut CommittedApplyTelemetry<'_>,
) -> Result<()> {
    // Apply each Vector action in source order. Even a superseded row
    // can widen SQ's codebook or change HNSW's online graph.
    for action in &prepared.plan.actions {
        let (cell, _ordinal, row) = match action {
            ScalarAction::Apply {
                cell,
                ordinal,
                kind: FieldType::Vector,
                ..
            } => (cell, *ordinal, Some(&prepared.vector_rows[ordinal])),
            ScalarAction::Drop {
                cell,
                ordinal,
                kind: FieldType::Vector,
            } => (cell, *ordinal, None),
            _ => continue,
        };
        // Omitted replacement fields have no source ordinal.
        let eid = coll.interner.resolve(cell.id);
        let index = coll
            .fields
            .get_mut(&cell.field)
            .expect("matched Vector field");
        let hnsw_write = matches!(
            &*index,
            FieldIndex::Vector { spec, .. }
                if matches!(spec.backend, crate::shared_kernel::types::schema::VectorBackend::HnswCpu)
        );
        index.drop_eid(cell.id, eid);
        if hnsw_write {
            let FieldIndex::Vector { idx, .. } = index else {
                unreachable!("matched Vector kind")
            };
            if let Some((wait, held)) = idx.take_hnsw_write_lock_timing() {
                telemetry.record_hnsw_write_lock(wait, held);
            }
        }
        if let Some(row) = row {
            let FieldIndex::Vector {
                idx, bytes, spec, ..
            } = index
            else {
                unreachable!("matched Vector kind")
            };
            let add = if matches!(
                spec.backend,
                crate::shared_kernel::types::schema::VectorBackend::HnswCpu
            ) {
                let hnsw_add_started = Instant::now();
                let add = idx.add(eid, row.raw.as_f32_slice());
                if let Some((wait, held)) = idx.take_hnsw_write_lock_timing() {
                    telemetry.record_hnsw_write_lock(wait, held);
                }
                if let Some(elapsed) = idx.take_hnsw_graph_rebuild_timing() {
                    telemetry.record_hnsw_graph_rebuild(elapsed);
                }
                telemetry.record_hnsw_add(hnsw_add_started.elapsed());
                add
            } else {
                idx.add(eid, row.raw.as_f32_slice())
            };
            if let Err(error) = add {
                // A backend fault after a validated prepare is not a
                // business rejection. The partial apply must never be
                // published or acknowledged as a successful cut.
                apply.mark_uncertain();
                return Err(error);
            }
            *bytes += (row.raw.dim() * 4 + eid.len()) as u64;
        }
        coll.next_field_dirty_revision = coll.next_field_dirty_revision.saturating_add(1);
        coll.field_dirty
            .entry(cell.field.clone())
            .or_default()
            .insert(eid.to_owned(), coll.next_field_dirty_revision);
        let value = row.map(|row| Arc::new(CheckpointValue::StagedVector(row.canonical.clone())));
        if let Some(old) = coll.change_journal.replace_charged(
            cell.field.clone(),
            eid.to_owned(),
            coll.next_field_dirty_revision,
            value,
            Some(charge.clone()),
        ) {
            prepared.old_vector_rows.push(old);
        }
    }
    Ok(())
}

/// Record versions, coverage and checksums, then journal the scalar rows.
pub(super) fn attach_versions_and_journal(
    coll: &mut Collection,
    scanner: &FastIndexScanner<'_>,
    replacement: Option<&ReplacementInput<'_>>,
    prepared: &mut Prepared,
    charge: &RetainedCharge,
) {
    // Preserve versions and coverage for every successful prefix item.
    // Failed replacements intentionally retain their previous coverage.
    if replacement.is_none() {
        let mut items = scanner.items().enumerate().peekable();
        for action in &prepared.plan.actions {
            if let ScalarAction::Apply { cell, ordinal, .. } = action {
                while items.peek().is_some_and(|(i, _)| i < ordinal) {
                    items.next();
                }
                let (_, item) = items.next().expect("validated action ordinal");
                if let Some(version) = item.version {
                    coll.cell_versions
                        .entry(cell.id)
                        .or_default()
                        .insert(cell.field.clone(), version);
                }
                let coverage = coll.eid_fields.entry(cell.id).or_default();
                if !coverage.contains(&cell.field) {
                    coverage.insert_absent(cell.field.clone());
                }
            }
        }
    }
    if let (Some(input), Some(ledger)) = (replacement, &prepared.replacement) {
        // Any accepted empty document clears all checksum history,
        // including when a later duplicate adds fields in this batch.
        for (doc, result) in input.docs.iter().zip(&ledger.results) {
            if doc.fields_start == doc.fields_end && matches!(result, ReplaceDocResult::Ok { .. }) {
                let id = coll
                    .interner
                    .id(doc.external_id)
                    .expect("planned replacement id");
                coll.field_checksums.remove(&id);
            }
        }
        for doc in &ledger.final_docs {
            if doc.fields.is_empty() {
                coll.eid_fields.remove(&doc.id);
            } else {
                coll.eid_fields.insert(
                    doc.id,
                    FieldCoverage {
                        names: doc.fields.clone(),
                    },
                );
            }
            if let Some(version) = doc.version {
                coll.doc_versions.insert(doc.id, version);
            }
        }
        for row in &ledger.final_checksums {
            match row.checksum {
                Some(value) => {
                    coll.field_checksums
                        .entry(row.cell.id)
                        .or_default()
                        .insert(row.cell.field.clone(), value);
                }
                None => {
                    if let Some(fields) = coll.field_checksums.get_mut(&row.cell.id) {
                        fields.remove(&row.cell.field);
                    }
                }
            }
        }
    }
    for (cell, value) in &prepared.rows {
        let index = coll
            .fields
            .get_mut(&cell.field)
            .expect("matched scalar row field");
        retire_live_delta_overlay(index, cell.id);
        if value.is_none() {
            match index {
                FieldIndex::Keyword(k) => {
                    k.tombstones.insert(cell.id);
                }
                FieldIndex::Number(n) => {
                    n.tombstones.insert(cell.id);
                }
                FieldIndex::Set(s) => {
                    s.tombstones.insert(cell.id);
                }
                _ => unreachable!("matched scalar kind"),
            }
        }
        let eid = coll.interner.resolve(cell.id).to_owned();
        coll.next_field_dirty_revision = coll.next_field_dirty_revision.saturating_add(1);
        coll.field_dirty
            .entry(cell.field.clone())
            .or_default()
            .insert(eid.clone(), coll.next_field_dirty_revision);
        coll.change_journal.record_charged(
            cell.field.clone(),
            eid,
            coll.next_field_dirty_revision,
            value.clone(),
            Some(charge.clone()),
        );
    }
}

impl Engine {
    /// The apply outcome: the replacement results, the business error, or the
    /// indexed counts. Records the index metrics.
    pub(super) fn committed_apply_outcome(
        &self,
        coll: &mut Collection,
        prepared: &mut Prepared,
    ) -> Result<ApplyOutcome> {
        if let Some(ledger) = &mut prepared.replacement {
            if ledger
                .results
                .iter()
                .any(|result| matches!(result, ReplaceDocResult::Ok { .. }))
            {
                coll.last_indexed_at = Some(std::time::SystemTime::now());
            }
            self.metrics.incr_index(
                ledger.fields_written,
                prepared.plan.bytes_by_field.values().sum(),
            );
            self.metrics.incr_replace_skipped(ledger.fields_skipped);
            Ok(ApplyOutcome::Replaced(ReplaceDocsResponse {
                results: std::mem::take(&mut ledger.results),
            }))
        } else {
            match prepared.business_error.take() {
                Some(error) => Err(error),
                None => {
                    if prepared.plan.applied > 0 {
                        coll.last_indexed_at = Some(std::time::SystemTime::now());
                    }
                    let bytes = std::mem::take(&mut prepared.plan.bytes_by_field);
                    self.metrics
                        .incr_index(prepared.plan.applied as u64, bytes.values().sum());
                    Ok(ApplyOutcome::Indexed(IndexResponse {
                        indexed: prepared.plan.applied,
                        bytes_written: bytes,
                        shard_lag_ms: 0,
                    }))
                }
            }
        }
    }
}
