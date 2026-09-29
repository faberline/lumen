//! What one admitted record owns: its share of the Engine's change budget, the
//! price of a delivery waiting for record RAM, the reservation that carries it
//! from admission through apply, and the transport's temporary copies, split
//! off so repricing never charges them twice.

use std::sync::atomic::{AtomicU64, Ordering};

use super::record_charges::RecordChargeJournal;
use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget, Owner, Reservation};

static NEXT_ENGINE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct EngineChanges {
    pub(in crate::index::application) records: RecordChargeJournal,
    pub(crate) owner: Owner,
    pub(in crate::index::application) budget: ChangeBudget,
    pub(super) id: u64,
}

impl Default for EngineChanges {
    fn default() -> Self {
        let budget = ChangeBudget::process_shared();
        let owner = budget.owner();
        let id = NEXT_ENGINE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .expect("Engine admission identity exhausted");
        Self {
            records: RecordChargeJournal::new(),
            owner,
            budget,
            id,
        }
    }
}

/// An allocation-free price retained while the delivered working copy is
/// released. The committed WAL source remains pinned during the wait.
pub(crate) struct RecordRamRequest {
    pub(super) runtime: u64,
    pub(super) bytes: usize,
    pub(super) extra_owned: usize,
}

/// All sizes are conservative owned-memory bounds. `extra_owned` comes from
/// the transport's raw-record/codec ownership; normalized field data is priced
/// separately from the current collection schema and live coverage.
pub(crate) struct RecordReservation {
    pub(super) runtime: u64,
    pub(super) reservation: Reservation,
    pub(super) extra_owned: usize,
    pub(super) ngram_cost_workspace_bytes: usize,
    pub(super) staged_text: bool,
    pub(super) preparation_bytes: usize,
    pub(super) prepared_text: Option<crate::index::application::text_preparation::PreparedTextRows>,
    pub(super) wait_for_preparation: bool,
}

/// Transport and AOF scratch ownership for a locally published record.
///
/// It comes from the same atomic admission as [`RecordReservation`], but it
/// never enters Engine repricing or source retention. Keeping it separate
/// lets the coordinator carry the temporary copies across WAL publication and
/// AOF flush without making the apply reservation price them again.
pub(crate) struct RecordTransientReservation {
    reservation: Reservation,
}

impl RecordTransientReservation {
    pub(crate) fn bytes(&self) -> usize {
        self.reservation.bytes()
    }
}

impl RecordReservation {
    /// Split transport-owned copies out of an already-admitted record. The
    /// apply half keeps source retention; the returned transient half has no
    /// source bridge and is released only after AOF persistence and completion.
    pub(crate) fn split_transport(mut self) -> (Self, Option<RecordTransientReservation>) {
        let bytes = self.extra_owned;
        if bytes == 0 {
            return (self, None);
        }
        let reservation = self.reservation.split_off(bytes);
        self.extra_owned = 0;
        (self, Some(RecordTransientReservation { reservation }))
    }

    pub(crate) fn source_retention(
        &mut self,
    ) -> crate::ingest::domain::change_budget::SourceRetention {
        self.reservation.source_retention()
    }

    /// Release one transport-owned raw copy after its caller has dropped it.
    /// This does not affect the decoded entry or any normalized change charge.
    /// It must run with no apply lease.
    pub(crate) fn release_transport_bytes(&mut self) -> Result<(), AdmissionError> {
        let released = self.extra_owned;
        if released == 0 {
            return Ok(());
        }
        let remaining = self
            .bytes()
            .checked_sub(released)
            .expect("transport bytes are part of this reservation");
        self.reservation.shrink_to(remaining)?;
        self.extra_owned = 0;
        Ok(())
    }

    pub(in crate::index::application) fn grow_preparation(
        &mut self,
        additional: usize,
    ) -> Result<(), AdmissionError> {
        let overflow = AdmissionError::Oversized {
            requested: usize::MAX,
            hard_limit: crate::ingest::domain::change_budget::HARD_LIMIT,
        };
        let required = self.bytes().checked_add(additional).ok_or(overflow)?;
        if self.wait_for_preparation {
            self.wait_grow_to(required)?;
        } else {
            self.try_grow_to(required)?;
        }
        self.preparation_bytes = self
            .preparation_bytes
            .checked_add(additional)
            .ok_or(overflow)?;
        Ok(())
    }
    /// Release helper workspace only after its temporary owners have dropped.
    /// This runs outside apply and preserves other prepared rows and transport.
    pub(in crate::index::application) fn release_preparation_workspace(
        &mut self,
        bytes: usize,
    ) -> Result<(), AdmissionError> {
        let prepared = self
            .preparation_bytes
            .checked_sub(bytes)
            .expect("released workspace was admitted as preparation");
        let remaining = self
            .bytes()
            .checked_sub(bytes)
            .expect("prepared workspace is part of this reservation");
        self.reservation.shrink_to(remaining)?;
        self.preparation_bytes = prepared;
        Ok(())
    }

    pub(crate) fn bytes(&self) -> usize {
        self.reservation.bytes()
    }

    /// Retain only the prepared borrowed representation. The prepared rows
    /// are held by the caller, so no owned decoder or staging scratch remains.
    pub(in crate::index::application) fn finish_borrowed_preparation(
        &mut self,
        bytes: usize,
    ) -> Result<(), AdmissionError> {
        debug_assert!(self.prepared_text.is_none());
        debug_assert_eq!(self.extra_owned, 0);
        self.reservation.shrink_to(bytes)?;
        self.preparation_bytes = 0;
        Ok(())
    }

    pub(in crate::index::application) fn shrink_borrowed_to(
        &mut self,
        bytes: usize,
    ) -> Result<(), AdmissionError> {
        debug_assert_eq!(self.preparation_bytes, 0);
        self.reservation.shrink_to(bytes)
    }

    /// This must run with no apply lease. Failure retains the original bytes.
    pub(crate) fn try_grow_to(&mut self, required: usize) -> Result<(), AdmissionError> {
        self.reservation.try_grow_to(required)
    }

    pub(crate) fn wait_grow_to(&mut self, required: usize) -> Result<(), AdmissionError> {
        self.reservation.wait_grow_to(required)
    }

    pub(in crate::index::application) fn before_publication(&mut self) {
        self.wait_for_preparation = false;
    }

    pub(super) fn discard_preparation(&mut self) -> Result<(), AdmissionError> {
        // Drop every mmap owner before releasing its reserved representation.
        // Raw entry and transport reservations remain owned by this record.
        drop(self.prepared_text.take());
        let remaining = self
            .bytes()
            .checked_sub(self.preparation_bytes)
            .expect("prepared bytes are part of this reservation");
        self.reservation.shrink_to(remaining)?;
        self.preparation_bytes = 0;
        Ok(())
    }
}
