//! A local record's reservation: admitted before it is published, waiting for
//! capacity relief when the budget is full, and repriced at the ordered head
//! before it applies.

use std::sync::atomic::Ordering;

use anyhow::Result;

use crate::ingest::application::write_coordinator::{LocalRecordReservation, WriteCoordinator};
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::AdmissionError;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::storage::{
    Engine, RecordAdmissionError, RecordApplyGuard, RecordReservation, RepriceRecord,
};

pub(super) enum PreparedLocalRecord<'a> {
    Prepared(RecordApplyGuard<'a>),
    /// The committed source needs a larger exact charge than its publication
    /// reservation.  Keep both pieces at the ordered head while a dedicated
    /// blocking apply worker waits for checkpoint relief.
    CapacityBlocked {
        entry: RaftLogEntry,
        reservation: RecordReservation,
        required: usize,
    },
    /// A committed record reached no charged apply guard. The coordinator must
    /// retain its reservation and source at the unresolved head.
    Unresolved {
        reservation: RecordReservation,
        error: anyhow::Error,
    },
}

/// Repricing happens after the WAL record has a sequence, but before its
/// capture-barrier lease. A larger valid record may grow only after the
/// ordered apply worker has retained the source and reservation at its head.
/// If that growth is full, return a blocked state so the worker can wait for
/// checkpoint relief without acquiring an apply lease or allowing later
/// records to pass.
pub(super) fn prepare_local_record<'a>(
    engine: &'a Engine,
    mut entry: RaftLogEntry,
    mut reservation: RecordReservation,
) -> Result<PreparedLocalRecord<'a>> {
    loop {
        match engine.begin_admitted_record(entry, reservation) {
            Ok(guard) => return Ok(PreparedLocalRecord::Prepared(guard)),
            Err(RepriceRecord {
                entry: repriced_entry,
                reservation: mut repriced_reservation,
                required: Some(required),
                error: _,
            }) => match repriced_reservation.try_grow_to(required) {
                Ok(()) => {
                    entry = repriced_entry;
                    reservation = repriced_reservation;
                }
                Err(AdmissionError::Full { .. }) => {
                    return Ok(PreparedLocalRecord::CapacityBlocked {
                        entry: repriced_entry,
                        reservation: repriced_reservation,
                        required,
                    });
                }
                Err(grow_error) => {
                    return Ok(PreparedLocalRecord::Unresolved {
                        reservation: repriced_reservation,
                        error: anyhow::Error::new(RecordAdmissionError::Capacity(grow_error)),
                    });
                }
            },
            Err(RepriceRecord {
                reservation, error, ..
            }) => {
                return Ok(PreparedLocalRecord::Unresolved {
                    reservation,
                    error: anyhow::Error::new(error),
                });
            }
        }
    }
}

impl WriteCoordinator {
    pub(super) async fn take_local_reservation(&self, seq: u64) -> Option<LocalRecordReservation> {
        self.local_reservations.lock().await.remove(&seq)
    }

    /// `try_reserve_record` prices the submitted raw record. Without an AOF,
    /// two extras cover the retained WAL record and an encode `Vec` with up to
    /// two raw bounds of capacity: three total raw bounds. With an AOF, four
    /// extras cover the retained WAL record, delivered record, AOF clone, and
    /// encode `Vec` up to two raw bounds: five total raw bounds.
    /// An unrepresentable bound is refused before the WAL assigns a sequence.
    ///
    /// `Ok(None)` means "publish with no reservation at all" — the WAL
    /// delivery loop's post-publication path then has nothing to grow from
    /// and waits on capacity with no bound. `try_reserve_record` never
    /// returns that gap for a domain pricing failure (unresolved
    /// schema/context maps to at least a minimal raw+transport reservation,
    /// or a capacity refusal here); this `None` arm stays only for an
    /// admission error `PendingChangeCapacity` does not classify at all
    /// (e.g. `RecordAdmissionError::WrongEngine`), which cannot occur for a
    /// same-Engine local submit today.
    #[cfg(test)]
    pub(super) fn try_admit_local_record(
        &self,
        entry: &RaftLogEntry,
    ) -> Result<Option<LocalRecordReservation>> {
        self.try_admit_local_record_once(entry)
            .0
            .map_err(|error| self.finalize_prepublication_error(error))
    }

    /// Return the shared pending revision with a Full result. Only the final
    /// result is counted as backpressure when submit waits for relief.
    fn try_admit_local_record_once(
        &self,
        entry: &RaftLogEntry,
    ) -> (Result<Option<LocalRecordReservation>>, Option<u64>) {
        let raw = match Engine::record_owned_bytes(entry) {
            Ok(raw) => raw,
            Err(RecordAdmissionError::Overflow) => {
                return (
                    Err(anyhow::Error::new(PendingChangeCapacity::Overflow)),
                    None,
                );
            }
            Err(error) => return (Err(error.into()), None),
        };
        let copies = if self.has_local_aof { 4 } else { 2 };
        let Some(extra_owned) = raw.checked_mul(copies) else {
            return (
                Err(anyhow::Error::new(PendingChangeCapacity::Overflow)),
                None,
            );
        };
        match self.engine.try_reserve_record(entry, extra_owned) {
            Ok(reservation) => {
                let (apply, transient) = reservation.split_transport();
                (Ok(Some(LocalRecordReservation { apply, transient })), None)
            }
            // Unknown schema/context preparation is likewise not a reason to
            // discard a valid record; apply returns its original domain outcome.
            Err(error) => {
                let mut revision = None;
                if let RecordAdmissionError::Capacity(AdmissionError::Full {
                    requested,
                    used,
                    hard_limit,
                }) = &error
                {
                    // A local pre-publication refusal does not create a
                    // committed capacity waiter. Start the same independent
                    // checkpoint owner used by committed/replayed records,
                    // otherwise request_pending_checkpoint() only signals a
                    // driver that may not exist and repeated 429s can make
                    // no progress through active or frozen changes.
                    if let Err(owner_error) = self.ensure_capacity_owner() {
                        tracing::warn!(
                            error = %owner_error,
                            "could not start local capacity maintenance"
                        );
                    }
                    revision = self.engine.request_pending_checkpoint_revision();
                    self.trace_admission_refusal(revision, *requested, *used, *hard_limit);
                    self.capacity_relief_requested
                        .store(true, Ordering::Release);
                }
                match PendingChangeCapacity::from_record_prepublication(&error) {
                    Some(pending) => (Err(anyhow::Error::new(pending)), revision),
                    None => (Ok(None), None),
                }
            }
        }
    }

    pub(super) async fn admit_local_record_with_relief(
        &self,
        entry: &RaftLogEntry,
        admission_deadline: tokio::time::Instant,
    ) -> Result<Option<LocalRecordReservation>> {
        let wake = self.engine.checkpoint_wake();
        loop {
            let (result, revision) = self.try_admit_local_record_once(entry);
            match result {
                Ok(reservation) => return Ok(reservation),
                Err(error) => {
                    let Some(revision) = revision else {
                        return Err(self.finalize_prepublication_error(error));
                    };
                    if tokio::time::Instant::now() >= admission_deadline {
                        return Err(self.finalize_prepublication_error(error));
                    }
                    // Observe after this request's hint. If publication
                    // already consumed the revision, retry without sleeping.
                    let observed = wake.epoch();
                    if self
                        .engine
                        .capacity_owner_state()
                        .and_then(|state| state.checkpoint_request_revision)
                        != Some(revision)
                    {
                        continue;
                    }
                    let remaining = admission_deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .unwrap_or_default();
                    if !wake.wait_for_change_async(observed, remaining).await {
                        return Err(self.finalize_prepublication_error(error));
                    }
                    if tokio::time::Instant::now() >= admission_deadline {
                        return Err(self.finalize_prepublication_error(error));
                    }
                }
            }
        }
    }
}
