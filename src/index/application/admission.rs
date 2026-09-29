//! Whole-record admission before the apply boundary.
//!
//! The transport retains the returned reservation across publication. Repricing
//! returns that same ownership, so its raw payload does not become uncharged
//! while the apply worker waits or stages it. The apply guard stays alive until
//! the caller has persisted AOF and advanced its watermark.
//!
//! [`Engine::try_reserve_record`] never returns a record with **zero**
//! reservation. Exact domain pricing that needs context it cannot resolve
//! before publication (`RecordAdmissionError::NeedsPreparation`) still gets
//! the raw+transport floor apply already knows how to charge — see
//! [`Engine::minimal_context_pending_reservation`] — so the publisher never
//! hands its post-publication apply path (the WAL delivery loop's unbounded
//! `wait_reserve_record_ram`, or `begin_admitted_record`'s bounded repricing
//! `wait_grow_to`) a record with nothing reserved to grow from. A Full
//! budget at that minimal floor is refused here, before publication, exactly
//! like a Full at the fully-priced bound.
//!
//! [`Engine::try_reserve_record`]: crate::index::application::engine::Engine::try_reserve_record
//! [`Engine::minimal_context_pending_reservation`]: crate::index::application::engine::Engine::minimal_context_pending_reservation

mod admitted_record;
pub(super) mod capacity;
mod engine_admission;
mod pricing;
mod record_charges;
pub(crate) mod record_reservation;

use crate::index::application::admission::record_reservation::RecordReservation;

use crate::ingest::domain::change_budget::{AdmissionError, RetainedCharge};
use crate::ingest::domain::change_record_cost::text_upper_bound::NormalizeError;
use crate::shared_kernel::capture_barrier::ApplyLease;
use crate::shared_kernel::log_entry::RaftLogEntry;
use std::cell::Cell;

#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum RecordAdmissionError {
    #[error("record preparation failed: {0}")]
    Preparation(String),
    #[error("pending record admission: {0:?}")]
    Capacity(AdmissionError),
    #[error("record preparation needs its durable source: {0:?}")]
    NeedsPreparation(NormalizeError),
    #[error("record memory bound overflow")]
    Overflow,
    #[error("record reservation belongs to another Engine")]
    WrongEngine,
}

/// A failed prepare never owns an apply lease and returns the raw-record
/// reservation. The transport still owns the exact log entry as well.
pub(crate) struct RepriceRecord {
    pub(crate) entry: RaftLogEntry,
    pub(crate) reservation: RecordReservation,
    pub(crate) required: Option<usize>,
    pub(crate) error: RecordAdmissionError,
}

/// The guard is thread-affine because the capture barrier is thread-affine.
/// It cannot be forged or reused for a second record or another Engine.
pub(crate) struct RecordApplyGuard<'a> {
    entry: Option<RaftLogEntry>,
    prepared_text: Option<super::text_preparation::PreparedTextRows>,
    charge: RetainedCharge,
    runtime: u64,
    used: Cell<bool>,
    apply: ApplyLease<'a>,
}

impl RecordApplyGuard<'_> {
    pub(crate) fn entry(&self) -> Option<&RaftLogEntry> {
        self.entry.as_ref()
    }
    pub(crate) fn apply_lease(&self) -> &ApplyLease<'_> {
        &self.apply
    }
}

#[cfg(test)]
mod tests;
