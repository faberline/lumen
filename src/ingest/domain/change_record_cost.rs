//! Borrowed, conservative pending-change cost estimates for committed records.

pub(crate) mod ngram_distinct_table;
pub(crate) mod text_representation;
pub(crate) mod text_upper_bound;

use crate::ingest::domain::change_record_cost::ngram_distinct_table::NgramDistinctTable;
use crate::ingest::domain::change_record_cost::text_representation::{
    estimate_record_with_text_representation, TextRepresentation,
};
use crate::ingest::domain::change_record_cost::text_upper_bound::NormalizeError;

/// Borrowed record normalizer used by the Engine admission seam.
use crate::ingest::domain::change_memory_cost::{estimate_change, Change, Cost, CostError};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::FieldValue,
    schema::{Analyzer, FieldSpec, FieldType},
};

/// Borrowed collection view while Engine holds the same state write lock
/// that will make the following mutation. No request, schema, coverage, or
/// posting-list clone is allowed.
pub trait CostContext {
    fn collection_exists(&self, collection_id: &str) -> bool;
    fn index_cell_is_stale(
        &self,
        collection_id: &str,
        external_id: &str,
        field: &str,
        version: Option<u64>,
    ) -> bool;
    fn field_spec<'a>(&'a self, collection_id: &str, field: &str) -> Option<&'a FieldSpec>;
    fn known_external_id(&self, collection_id: &str, external_id: &str) -> bool;
    fn visit_coverage(&self, collection_id: &str, external_id: &str, visit: &mut dyn FnMut(&str));
    fn request_is_deduplicated(&self, collection_id: &str, request_id: Option<&str>) -> bool;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordCost {
    pub active: usize,
    pub frozen: usize,
    pub prepublish: usize,
}

/// An explicit retain signal. A committed entry that cannot be normalized
/// or reserved stays queued for wait/spill; it is never discarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordEstimate {
    Ready(RecordCost),
    Retain { cause: NormalizeError },
}

impl RecordCost {
    fn add_cost(&mut self, cost: Cost) -> Result<(), NormalizeError> {
        self.active = self
            .active
            .checked_add(cost.active)
            .ok_or(NormalizeError::Overflow)?;
        self.frozen = self
            .frozen
            .checked_add(cost.frozen)
            .ok_or(NormalizeError::Overflow)?;
        self.prepublish = self
            .prepublish
            .checked_add(cost.prepublish)
            .ok_or(NormalizeError::Overflow)?;
        Ok(())
    }
    fn add_change(&mut self, change: &Change) -> Result<(), NormalizeError> {
        self.add_cost(
            estimate_change(change).map_err(|CostError::Overflow| NormalizeError::Overflow)?,
        )
    }
    fn add_request_id(&mut self, request_id: Option<&str>) -> Result<(), NormalizeError> {
        let Some(request_id) = request_id else {
            return Ok(());
        };
        // seen_requests is VecDeque<(String, Instant)> and is not part of
        // FrozenCheckpoint. This is one String allocation bound plus its
        // queue tuple and deque slot; it is active-only.
        let string = request_id
            .len()
            .checked_add(16)
            .and_then(usize::checked_next_power_of_two)
            .ok_or(NormalizeError::Overflow)?;
        self.active = self
            .active
            .checked_add(string)
            .and_then(|n| n.checked_add(32))
            .ok_or(NormalizeError::Overflow)?;
        Ok(())
    }
}

pub fn estimate_record_or_retain(entry: &RaftLogEntry, ctx: &impl CostContext) -> RecordEstimate {
    match estimate_record(entry, ctx) {
        Ok(cost) => RecordEstimate::Ready(cost),
        Err(cause) => RecordEstimate::Retain { cause },
    }
}

/// Estimate one committed entry before its first state mutation. Validation
/// precedes this call; ContextMissing means retain the entry and normalize
/// under the owning Engine lock after validation.
pub fn estimate_record(
    entry: &RaftLogEntry,
    ctx: &impl CostContext,
) -> Result<RecordCost, NormalizeError> {
    estimate_record_with_text_representation(entry, ctx, TextRepresentation::Normalized, None)
}

/// Estimate a record whose valid Text cells will be prepared as immutable
/// on-disk rows before apply.
///
/// The caller must separately reserve raw transport, staging workspace, row
/// handles, and reader metadata. This estimate alone never authorizes apply.
pub(crate) fn estimate_record_prepared_text(
    entry: &RaftLogEntry,
    ctx: &impl CostContext,
) -> Result<RecordCost, NormalizeError> {
    estimate_record_with_text_representation(entry, ctx, TextRepresentation::PreparedRow, None)
}

/// Select potential Ngram work without allocating a token table. This may
/// over-select invalid or stale cells; the shared cost walker decides their
/// actual semantics later. Non-Text records need no new workspace reservation.
pub(crate) fn may_need_default_ngram_workspace(
    entry: &RaftLogEntry,
    ctx: &impl CostContext,
) -> bool {
    let is_ngram = |collection: &str, field: &str, value: &FieldValue| {
        matches!(value, FieldValue::String(_))
            && ctx.field_spec(collection, field).is_some_and(|spec| {
                spec.field_type == FieldType::Text && spec.analyzer == Some(Analyzer::Ngram)
            })
    };
    match entry {
        RaftLogEntry::Index { collection_id, req } => req
            .items
            .iter()
            .any(|item| is_ngram(collection_id, &item.field, &item.value)),
        RaftLogEntry::ReplaceDocs { collection_id, req } => req.docs.iter().any(|doc| {
            doc.fields
                .iter()
                .any(|(field, value)| is_ngram(collection_id, field, value))
        }),
        _ => false,
    }
}

/// The caller owns DEFAULT_NGRAM_COST_WORKSPACE_BYTES before entering this
/// function. The fixed table is gone before it returns the final record price.
pub(crate) fn estimate_record_exact_default_ngram(
    entry: &RaftLogEntry,
    ctx: &impl CostContext,
) -> RecordEstimate {
    let mut table = NgramDistinctTable::new();
    match estimate_record_with_text_representation(
        entry,
        ctx,
        TextRepresentation::Normalized,
        Some(&mut table),
    ) {
        Ok(cost) => RecordEstimate::Ready(cost),
        Err(cause) => RecordEstimate::Retain { cause },
    }
}

#[cfg(test)]
mod tests;
