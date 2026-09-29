//! Staging a record's Text values after the state lock is gone: the bound its
//! reservation must cover, each row staged in item order, and the reader and
//! workspace charged to the record before apply can see them.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::index::application::admission;
use crate::index::application::engine::cost::EngineCostContext;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation::{
    bounded_whitespace_workspace, collection_id, visit_text_values, PreparedTextRows,
    TEXT_SCRATCH_BYTES,
};
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::schema::Analyzer;
use crate::storage::staged_text_row;

#[cfg(feature = "jieba")]
use crate::index::infrastructure::analysis::jieba_disk_route;

impl Engine {
    pub(in crate::index::application) fn prepared_text_bound(
        &self,
        entry: &RaftLogEntry,
    ) -> Result<usize, admission::RecordAdmissionError> {
        use admission::RecordAdmissionError as Error;
        let state = self
            .state
            .read()
            .map_err(|_| Error::Preparation("state poisoned".into()))?;
        let cost = crate::ingest::domain::change_record_cost::estimate_record_prepared_text(
            entry,
            &EngineCostContext { state: &state },
        )
        .map_err(Error::NeedsPreparation)?;
        let mut metadata = Some(0usize);
        let mut normalized_token = 0usize;
        #[cfg(feature = "jieba")]
        let mut dictionary_route_bytes = 0usize;
        #[cfg(not(feature = "jieba"))]
        let dictionary_route_bytes = 0usize;
        let coll = collection_id(entry).and_then(|name| state.collections.get(name));
        visit_text_values(entry, |_, field, input| {
            if let Some(FieldIndex::Text { analyzer, .. }) =
                coll.and_then(|coll| coll.fields.get(field))
            {
                #[cfg(feature = "jieba")]
                if *analyzer == Analyzer::Jieba {
                    dictionary_route_bytes = jieba_disk_route::ROUTE_CACHE_BYTES;
                }
                metadata = metadata.and_then(|n| {
                    field
                        .len()
                        .checked_mul(4)
                        .and_then(|name| n.checked_add(name))
                        .and_then(|n| n.checked_add(4096))
                });
                match analyzer {
                    Analyzer::WhitespaceLower => {
                        normalized_token = normalized_token
                            .max(bounded_whitespace_workspace(input).unwrap_or(usize::MAX));
                    }
                    Analyzer::Jieba => {
                        normalized_token =
                            normalized_token.max(input.len().saturating_mul(3).saturating_add(64));
                    }
                    Analyzer::Ngram => {}
                }
            }
        });
        cost.active
            .checked_add(cost.frozen)
            .and_then(|n| n.checked_add(cost.prepublish))
            .and_then(|n| n.checked_add(metadata?))
            .and_then(|n| n.checked_add(TEXT_SCRATCH_BYTES))
            .and_then(|n| n.checked_add(normalized_token))
            .and_then(|n| n.checked_add(dictionary_route_bytes))
            .ok_or(Error::Overflow)
    }

    pub(in crate::index::application) fn prepare_text_rows(
        &self,
        entry: &RaftLogEntry,
        reservation: &mut admission::record_reservation::RecordReservation,
    ) -> Result<PreparedTextRows> {
        let (mut prepared, analyzers) = {
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            let coll = collection_id(entry)
                .and_then(|name| state.collections.get(name).map(|coll| (name, coll)));
            let mut analyzers = BTreeMap::new();
            if let Some((_, coll)) = coll {
                for (name, field) in &coll.fields {
                    if let FieldIndex::Text { analyzer, .. } = field {
                        analyzers.insert(name.clone(), *analyzer);
                    }
                }
            }
            (
                PreparedTextRows {
                    epoch: self.capture_barrier.epoch(),
                    collection: coll.map(|(name, coll)| {
                        (name.to_owned(), coll.collection_generation, coll.version)
                    }),
                    rows: BTreeMap::new(),
                    retained_reader_bytes: 0,
                    scratch_growth_bytes: 0,
                },
                analyzers,
            )
        };
        let mut failure = None;
        visit_text_values(entry, |ordinal, field, input| {
            if failure.is_some() {
                return;
            }
            let Some(analyzer) = analyzers.get(field).copied() else {
                return;
            };
            let result = self.stage_text_row(input, analyzer, reservation);
            match result {
                Ok((row, reader_bytes, scratch_growth)) => {
                    prepared.retained_reader_bytes =
                        match prepared.retained_reader_bytes.checked_add(reader_bytes) {
                            Some(bytes) => bytes,
                            None => {
                                failure = Some(anyhow!("owned Text reader metadata overflow"));
                                return;
                            }
                        };
                    prepared.scratch_growth_bytes =
                        match prepared.scratch_growth_bytes.checked_add(scratch_growth) {
                            Some(bytes) => bytes,
                            None => {
                                failure = Some(anyhow!("owned Text workspace overflow"));
                                return;
                            }
                        };
                    self.metrics.text_row_stage_rows_total.incr();
                    self.metrics
                        .text_row_stage_input_bytes_total
                        .add(row.input_bytes() as u64);
                    prepared
                        .rows
                        .entry(ordinal)
                        .or_default()
                        .insert(field.to_owned(), Arc::new(row));
                }
                Err(error) => failure = Some(error),
            }
        });
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(prepared)
    }

    /// Stage one borrowed or owned Text value after the state lock is gone.
    /// Reader metadata and expanded workspace are charged to the record before
    /// their allocation becomes observable to apply.
    pub(super) fn stage_text_row(
        &self,
        input: &str,
        analyzer: Analyzer,
        reservation: &mut admission::record_reservation::RecordReservation,
    ) -> Result<(staged_text_row::StagedTextRow, usize, usize)> {
        let mut scratch = TEXT_SCRATCH_BYTES;
        let mut scratch_growth = 0usize;
        loop {
            let mut retained_reader_bytes = 0usize;
            let mut transient_bytes = 0usize;
            let result = staged_text_row::StagedTextRow::stage_charged(
                input,
                analyzer,
                scratch,
                |allocation| {
                    use staged_text_row::StageAllocation;
                    let (StageAllocation::Reader(bytes) | StageAllocation::Workspace(bytes)) =
                        allocation;
                    self.request_pending_checkpoint();
                    reservation
                        .grow_preparation(bytes)
                        .map_err(admission::RecordAdmissionError::Capacity)?;
                    match allocation {
                        StageAllocation::Reader(_) => {
                            retained_reader_bytes = retained_reader_bytes
                                .checked_add(bytes)
                                .ok_or_else(|| anyhow!("Text reader metadata overflow"))?
                        }
                        StageAllocation::Workspace(_) => {
                            transient_bytes = transient_bytes
                                .checked_add(bytes)
                                .ok_or_else(|| anyhow!("Text transient workspace overflow"))?
                        }
                    }
                    Ok(())
                },
            );
            // stage_charged has dropped its private helper mappings and all
            // temporary allocations. A failed final reader has no live owner.
            let released = transient_bytes
                .checked_add(if result.is_err() {
                    retained_reader_bytes
                } else {
                    0
                })
                .ok_or_else(|| anyhow!("Text released workspace overflow"))?;
            reservation
                .release_preparation_workspace(released)
                .map_err(admission::RecordAdmissionError::Capacity)?;
            match result {
                Err(error)
                    if error
                        .downcast_ref::<crate::persistence::infrastructure::segment::text_row_stage::RequiredTextRowWorkspace>()
                        .is_some() =>
                {
                    let required = error
                        .downcast_ref::<crate::persistence::infrastructure::segment::text_row_stage::RequiredTextRowWorkspace>()
                        .expect("matched required Text workspace")
                        .required_bytes;
                    let delta = required.saturating_sub(scratch);
                    reservation
                        .grow_preparation(delta)
                        .map_err(admission::RecordAdmissionError::Capacity)?;
                    scratch_growth = scratch_growth
                        .checked_add(delta)
                        .ok_or_else(|| anyhow!("Text workspace overflow"))?;
                    scratch = required;
                }
                Ok(row) => return Ok((row, retained_reader_bytes, scratch_growth)),
                Err(error) => return Err(error),
            }
        }
    }
}
