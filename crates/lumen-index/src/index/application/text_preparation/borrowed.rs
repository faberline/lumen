//! Text staged from a borrowed fast Index command: a private apply-only entry
//! whose Text values are empty strings paired with their staged rows, for the
//! whole command or only the Text actions the unified planner accepted.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};

use crate::index::application::admission;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation::{
    borrowed_text_metadata_bound, borrowed_text_pre_stage_workspace_bound, PreparedTextRows,
    RequiredBorrowedTextWorkspace, StaleBorrowedTextPreparation, TEXT_SCRATCH_BYTES,
};
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

impl Engine {
    /// Convert a validated borrowed fast Index command into a private, small
    /// apply entry and staged Text rows.  This entry is an apply-only adapter:
    /// it must never become an AOF source because Text values are represented
    /// by empty strings paired with their staged rows.
    ///
    /// `None` is an internal fallback signal.  An existing non-Text field
    /// needs the owned path.  Unknown fields intentionally remain in the
    /// private entry so live Index validation retains its valid-prefix order.
    pub(in crate::index::application) fn prepare_borrowed_text_rows(
        &self,
        scanner: &FastIndexScanner<'_>,
        reservation: &mut admission::record_reservation::RecordReservation,
    ) -> Result<Option<(RaftLogEntry, PreparedTextRows)>> {
        let metadata = borrowed_text_metadata_bound(scanner)?;
        let initial_required = metadata
            .checked_add(TEXT_SCRATCH_BYTES)
            .ok_or_else(|| anyhow!("borrowed Text preparation reservation overflow"))?;
        anyhow::ensure!(
            reservation.bytes() >= initial_required,
            "borrowed Text metadata and scratch were not reserved"
        );

        // Capture only the referenced Text analyzer metadata.  Do not clone
        // the schema, and do not grow staging workspace while this lock lives.
        let (mut prepared, analyzers) = {
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            // A missing collection has no stable Text analyzer.  Keep the raw
            // source path so a concurrent create cannot make an empty private
            // placeholder value observable at apply.
            let Some(coll) = state.collections.get(scanner.collection_id()) else {
                return Ok(None);
            };
            let mut analyzers = BTreeMap::new();
            for item in scanner.items() {
                match coll.fields.get(item.field) {
                    Some(FieldIndex::Text { analyzer, .. }) => {
                        analyzers.insert(item.field.to_owned(), *analyzer);
                    }
                    Some(_) => return Ok(None),
                    None => {}
                }
            }
            (
                PreparedTextRows {
                    epoch: self.capture_barrier.epoch(),
                    collection: Some((
                        scanner.collection_id().to_owned(),
                        coll.collection_generation,
                        coll.version,
                    )),
                    rows: BTreeMap::new(),
                    retained_reader_bytes: 0,
                    scratch_growth_bytes: 0,
                },
                analyzers,
            )
        };

        let pre_stage_workspace = borrowed_text_pre_stage_workspace_bound(
            scanner,
            |field| analyzers.get(field).copied(),
            |_| true,
        )?;
        let required = metadata
            .checked_add(TEXT_SCRATCH_BYTES)
            .and_then(|bytes| bytes.checked_add(pre_stage_workspace))
            .ok_or_else(|| anyhow!("borrowed Text preparation reservation overflow"))?;
        if reservation.bytes() < required {
            return Err(RequiredBorrowedTextWorkspace {
                required_bytes: required,
            }
            .into());
        }

        let mut items = Vec::with_capacity(scanner.cost().item_count);
        for (ordinal, item) in scanner.items().enumerate() {
            let value = match item.value {
                // The staged row is the value for a declared Text field.  The
                // empty String is only an internal placeholder for the normal
                // apply function; it must never be persisted or encoded.
                FastIndexValue::String(input) => {
                    if let Some(analyzer) = analyzers.get(item.field).copied() {
                        let (row, reader_bytes, scratch_growth) =
                            self.stage_text_row(input, analyzer, reservation)?;
                        prepared.retained_reader_bytes = prepared
                            .retained_reader_bytes
                            .checked_add(reader_bytes)
                            .ok_or_else(|| anyhow!("borrowed Text reader metadata overflow"))?;
                        prepared.scratch_growth_bytes = prepared
                            .scratch_growth_bytes
                            .checked_add(scratch_growth)
                            .ok_or_else(|| anyhow!("borrowed Text workspace overflow"))?;
                        prepared
                            .rows
                            .entry(ordinal)
                            .or_default()
                            .insert(item.field.to_owned(), Arc::new(row));
                        self.metrics.text_row_stage_rows_total.incr();
                        self.metrics
                            .text_row_stage_input_bytes_total
                            .add(input.len() as u64);
                    }
                    FieldValue::String(String::new())
                }
                // Keep the original type for a Text mismatch.  In particular,
                // do not turn a vector or string list into a String: live
                // validation must still report the same type mismatch.
                FastIndexValue::Number(value) => FieldValue::Number(value),
                FastIndexValue::Vector { .. } => FieldValue::Vector(Vec::new()),
                FastIndexValue::StringList(_) => FieldValue::StringList(Vec::new()),
            };
            items.push(IndexItem {
                external_id: item.external_id.to_owned(),
                field: item.field.to_owned(),
                value,
                version: item.version,
            });
        }
        Ok(Some((
            RaftLogEntry::Index {
                collection_id: scanner.collection_id().to_owned(),
                req: IndexRequest {
                    items,
                    request_id: scanner.request_id().map(str::to_owned),
                },
            },
            prepared,
        )))
    }

    /// Stage only the Text actions accepted by the unified borrowed planner.
    /// The caller supplies original ordinals from that single ledger, so an
    /// invalid later item cannot cause staging beyond the live valid prefix.
    pub(in crate::index::application) fn prepare_borrowed_text_rows_for_actions(
        &self,
        scanner: &FastIndexScanner<'_>,
        actions: &BTreeMap<usize, String>,
        live_baseline: usize,
        reservation: &mut admission::record_reservation::RecordReservation,
    ) -> Result<PreparedTextRows> {
        // `live_baseline` already prices the planner, scalar adapter and this
        // method's action/analyzer/row metadata. Scratch is additive: it must
        // never reuse bytes whose owners are still live.
        let initial_required = live_baseline
            .checked_add(TEXT_SCRATCH_BYTES)
            .ok_or_else(|| anyhow!("borrowed Text preparation reservation overflow"))?;
        if reservation.bytes() < initial_required {
            return Err(RequiredBorrowedTextWorkspace {
                required_bytes: initial_required,
            }
            .into());
        }
        let (mut prepared, analyzers) = {
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            let Some(coll) = state.collections.get(scanner.collection_id()) else {
                return Err(StaleBorrowedTextPreparation.into());
            };
            let mut analyzers = BTreeMap::new();
            for field in actions.values() {
                let Some(FieldIndex::Text { analyzer, .. }) = coll.fields.get(field) else {
                    return Err(StaleBorrowedTextPreparation.into());
                };
                analyzers.insert(field.clone(), *analyzer);
            }
            (
                PreparedTextRows {
                    epoch: self.capture_barrier.epoch(),
                    collection: Some((
                        scanner.collection_id().to_owned(),
                        coll.collection_generation,
                        coll.version,
                    )),
                    rows: BTreeMap::new(),
                    retained_reader_bytes: 0,
                    scratch_growth_bytes: 0,
                },
                analyzers,
            )
        };
        let pre_stage_workspace = borrowed_text_pre_stage_workspace_bound(
            scanner,
            |field| analyzers.get(field).copied(),
            |ordinal| actions.contains_key(&ordinal),
        )?;
        let required = live_baseline
            .checked_add(TEXT_SCRATCH_BYTES)
            .and_then(|bytes| bytes.checked_add(pre_stage_workspace))
            .ok_or_else(|| anyhow!("borrowed Text preparation reservation overflow"))?;
        if reservation.bytes() < required {
            return Err(RequiredBorrowedTextWorkspace {
                required_bytes: required,
            }
            .into());
        }
        for (ordinal, item) in scanner.items().enumerate() {
            let Some(field) = actions.get(&ordinal) else {
                continue;
            };
            debug_assert_eq!(field, item.field);
            let FastIndexValue::String(input) = item.value else {
                bail!("planned Text action lost its string value")
            };
            let analyzer = analyzers[field];
            let (row, reader_bytes, scratch_growth) =
                self.stage_text_row(input, analyzer, reservation)?;
            prepared.retained_reader_bytes = prepared
                .retained_reader_bytes
                .checked_add(reader_bytes)
                .ok_or_else(|| anyhow!("borrowed Text reader metadata overflow"))?;
            prepared.scratch_growth_bytes = prepared
                .scratch_growth_bytes
                .checked_add(scratch_growth)
                .ok_or_else(|| anyhow!("borrowed Text workspace overflow"))?;
            prepared
                .rows
                .entry(ordinal)
                .or_default()
                .insert(field.clone(), Arc::new(row));
            self.metrics.text_row_stage_rows_total.incr();
            self.metrics
                .text_row_stage_input_bytes_total
                .add(input.len() as u64);
        }
        Ok(prepared)
    }
}
