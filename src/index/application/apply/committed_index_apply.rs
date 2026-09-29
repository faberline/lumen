//! Apply a retained fast-Index record for scalar and Text fields without
//! owning its field values.
//!
//! Planning, admission, file IO and row-map construction precede apply. The
//! apply interval only rechecks the cut, installs prepared readers and changes
//! small metadata. The caller advances its durable watermark in that interval.

mod fields;

use super::committed_index_plan::{self as plan, PlanView, PlannedCell};
use super::committed_replace_apply::ReplacementLedger;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Result};

use crate::index::application::admission::{
    record_reservation::RecordReservation, RecordAdmissionError,
};
use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation;
use crate::index::domain::collection::{Collection, IDEMPOTENCY_TTL};
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::index::infrastructure::staging::{staged_text_row, staged_vector_row};
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::shared_kernel::capture_barrier::ApplyLease;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::FieldType;

#[cfg(test)]
thread_local! {
    static BEFORE_ATTACH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = Default::default();
    static TEXT_WORKSPACE_RETRIES: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_text_workspace_retries_for_test() {
    TEXT_WORKSPACE_RETRIES.with(|retries| retries.set(0));
}

#[cfg(test)]
pub(super) fn text_workspace_retries_for_test() -> u32 {
    TEXT_WORKSPACE_RETRIES.with(std::cell::Cell::get)
}

struct View<'a> {
    engine: &'a Engine,
    coll: &'a Collection,
    now: Instant,
}

impl PlanView for View<'_> {
    fn engine_epoch(&self) -> u64 {
        self.engine.capture_barrier.epoch()
    }
    fn collection_generation(&self) -> u64 {
        self.coll.collection_generation
    }
    fn schema_version(&self) -> u32 {
        self.coll.version
    }
    fn data_version(&self) -> u64 {
        self.coll.data_version
    }
    fn revision(&self) -> u64 {
        self.engine.capture_barrier.apply_revision()
    }
    fn interner_len(&self) -> usize {
        self.coll.interner.to_eid.len()
    }
    fn is_live(&self) -> bool {
        self.coll.deleted_at.is_none()
    }
    fn field_type(&self, field: &str) -> Option<FieldType> {
        self.coll.fields.get(field).map(FieldIndex::field_type)
    }
    fn vector_dimension(&self, field: &str) -> Option<u32> {
        match self.coll.fields.get(field)? {
            FieldIndex::Vector { spec, .. } => Some(spec.dim),
            _ => None,
        }
    }
    fn id(&self, external_id: &str) -> Option<u32> {
        self.coll.interner.id(external_id)
    }
    fn has_cell(&self, id: u32, field: &str) -> bool {
        self.coll
            .eid_fields
            .get(&id)
            .is_some_and(|fields| fields.contains(field))
    }
    fn cell_version(&self, id: u32, field: &str) -> Option<u64> {
        self.coll.cell_versions.get(&id)?.get(field).copied()
    }
    fn request_deadline(&self, request_id: &str) -> Option<Instant> {
        self.coll.seen_requests.iter().find_map(|(key, at)| {
            let deadline = *at + IDEMPOTENCY_TTL;
            (key == request_id && self.now <= deadline).then_some(deadline)
        })
    }
}

struct FieldPlan {
    kind: FieldType,
    before: Option<Arc<ComposedSegmentReader>>,
    after: Option<Arc<ComposedSegmentReader>>,
    winners: Vec<(u32, usize)>,
    final_bytes: u64,
}

struct Prepared {
    plan: plan::ScalarPlan,
    business_error: Option<anyhow::Error>,
    replacement: Option<ReplacementLedger>,
    fields: BTreeMap<String, FieldPlan>,
    /// Hash has an eight-byte canonical value, regardless of wire string size.
    hash_rows: BTreeMap<PlannedCell, Option<u64>>,
    vector_codebooks: BTreeMap<String, Option<ScalarCodebook>>,
    /// Every source action remains alive through the ordered backend updates.
    vector_rows: BTreeMap<usize, VectorRows>,
    /// Replaced journal payloads may own files. Release them outside apply.
    old_vector_rows: Vec<crate::ingest::domain::change_journal::Row<CheckpointValue>>,
    // The per-cell journal points into the same reader that queries use.
    rows: BTreeMap<PlannedCell, Option<Arc<CheckpointValue>>>,
    /// Final Text values use staged rows, not a scalar composed segment.
    text_rows: BTreeMap<PlannedCell, Option<Arc<staged_text_row::StagedTextRow>>>,
    text_actions: BTreeMap<usize, String>,
    text_prepared: Option<text_preparation::PreparedTextRows>,
    /// Old staged file owners must outlive the state write and apply lease.
    old_text_rows: Vec<Arc<staged_text_row::StagedTextRow>>,
    text_placeholder: FieldValue,
    retained: usize,
}

struct VectorRows {
    raw: Arc<staged_vector_row::StagedVectorRow>,
    canonical: Arc<staged_vector_row::StagedVectorRow>,
}

fn composed(index: &FieldIndex) -> Option<&Arc<ComposedSegmentReader>> {
    match index {
        FieldIndex::Keyword(k) => k.segment.as_ref(),
        FieldIndex::Number(n) => n.segment.as_ref(),
        FieldIndex::Set(s) => s.segment.as_ref(),
        _ => None,
    }
}

fn scalar_bytes(index: &FieldIndex) -> u64 {
    match index {
        FieldIndex::Keyword(k) => k.bytes,
        FieldIndex::Number(n) => n.bytes,
        FieldIndex::Set(s) => s.bytes,
        _ => unreachable!("scalar plan validated field kind"),
    }
}

/// Current logical byte weight. Raw staged dictionaries lend values directly;
/// no String or set of all members is constructed for an immutable row.
fn cell_bytes(index: &FieldIndex, id: u32, eid_len: usize) -> u64 {
    match index {
        FieldIndex::Keyword(k) => {
            if let Some(value) = k
                .dense_forward
                .get(id as usize)
                .and_then(Option::as_ref)
                .or_else(|| k.forward.get(&id))
            {
                return (value.len() + eid_len) as u64;
            }
            if k.tombstones.contains(id) {
                return 0;
            }
            k.segment
                .as_ref()
                .and_then(|s| s.keyword_at_cow(id))
                .map_or(0, |value| (value.len() + eid_len) as u64)
        }
        FieldIndex::Number(n) => n.number_at(id).map_or(0, |_| (8 + eid_len) as u64),
        FieldIndex::Set(s) => {
            if let Some(values) = s.forward.get(&id) {
                return values
                    .iter()
                    .map(|value| (value.len() + eid_len) as u64)
                    .sum();
            }
            if s.tombstones.contains(id) {
                return 0;
            }
            let Some(view) = s.segment.as_ref() else {
                return 0;
            };
            let Some((true, count)) = view.set_row_member_count(id) else {
                return 0;
            };
            (0..count)
                .map(|member| {
                    view.set_member_at_cow(id, member)
                        .map_or(0, |value| (value.len() + eid_len) as u64)
                })
                .sum()
        }
        _ => unreachable!("scalar plan validated field kind"),
    }
}

fn require(reserved: &mut RecordReservation, bytes: usize) -> Result<()> {
    if reserved.bytes() < bytes {
        reserved
            .wait_grow_to(bytes)
            .map_err(RecordAdmissionError::Capacity)?;
    }
    Ok(())
}

impl Engine {
    /// `false` is an internal routing result for an unsupported command. A real
    /// preparation failure returns an error and never invokes `complete`.
    pub(crate) fn try_apply_committed_scalar(
        &self,
        scanner: &FastIndexScanner<'_>,
        sequence: u64,
        complete: impl FnOnce(&ApplyLease<'_>, Result<ApplyOutcome>),
    ) -> Result<bool> {
        self.try_apply_committed_scalar_with_capacity_owner(
            scanner,
            sequence,
            &mut || bail!("committed layer capacity needs a caller-owned maintainer"),
            complete,
        )
    }

    pub(crate) fn try_apply_committed_scalar_with_capacity_owner(
        &self,
        scanner: &FastIndexScanner<'_>,
        sequence: u64,
        ensure_owner: &mut dyn FnMut() -> Result<()>,
        complete: impl FnOnce(&ApplyLease<'_>, Result<ApplyOutcome>),
    ) -> Result<bool> {
        self.try_apply_committed_fields_with_capacity_owner(
            scanner,
            None,
            sequence,
            ensure_owner,
            complete,
        )
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod timing_tests;
