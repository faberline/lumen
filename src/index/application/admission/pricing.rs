//! Pricing a record: its owned bytes and decode peak, the decoded record
//! repriced in place of its scanning workspace, and the exact memory bound,
//! with the n-gram cost table reserved before it is built.

use crate::index::application::admission::record_reservation::RecordReservation;
use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::index::domain::record_ram::{estimate_record_decode_peak, estimate_record_ram};
use crate::ingest::domain::change_record_cost::RecordEstimate;
use crate::shared_kernel::log_entry::RaftLogEntry;

impl Engine {
    pub(crate) fn record_owned_bytes(entry: &RaftLogEntry) -> Result<usize, RecordAdmissionError> {
        estimate_record_ram(entry).map_err(|_| RecordAdmissionError::Overflow)
    }

    pub(crate) fn record_decode_peak(entry: &RaftLogEntry) -> Result<usize, RecordAdmissionError> {
        estimate_record_decode_peak(entry).map_err(|_| RecordAdmissionError::Overflow)
    }

    /// Replace the scanning workspace price after a bounded wire decoder has
    /// produced the record. The same reservation keeps both the decoded peak
    /// and the transport source alive; this never acquires an apply lease.
    pub(crate) fn price_decoded_record(
        &self,
        entry: &RaftLogEntry,
        reserved: &mut RecordReservation,
        decoded_peak: usize,
        transport_bytes: usize,
        before_publication: bool,
    ) -> Result<(), RecordAdmissionError> {
        if reserved.runtime != self.changes.id {
            return Err(RecordAdmissionError::WrongEngine);
        }
        let raw = Self::record_owned_bytes(entry)?;
        let overhead = decoded_peak
            .checked_sub(raw)
            .ok_or(RecordAdmissionError::Overflow)?;
        reserved.extra_owned = transport_bytes
            .checked_add(overhead)
            .ok_or(RecordAdmissionError::Overflow)?;
        if before_publication {
            self.price_before_publication(entry, reserved)?;
        } else {
            // A foreign committed delivery does not reserve a cost table until
            // begin_admitted_record can return ownership for a capacity wait.
            // Keep any workspace already supplied by local admission.
            let required = decoded_peak
                .checked_add(transport_bytes)
                .and_then(|n| n.checked_add(reserved.ngram_cost_workspace_bytes))
                .ok_or(RecordAdmissionError::Overflow)?;
            reserved
                .reservation
                .shrink_to(required)
                .map_err(RecordAdmissionError::Capacity)?;
        }
        Ok(())
    }

    /// Reserve the fixed cost table before calling its estimator. RAM-only
    /// delivery keeps its old scanner/decoder bound until this point. A failed
    /// growth returns the same record ownership for waiting outside apply.
    pub(super) fn ensure_ngram_cost_workspace(
        &self,
        entry: &RaftLogEntry,
        reserved: &mut RecordReservation,
    ) -> Result<(), (Option<usize>, RecordAdmissionError)> {
        if reserved.staged_text
            || (reserved.ngram_cost_workspace_bytes == 0
                && !self.may_need_default_ngram_workspace(entry))
        {
            return Ok(());
        }
        let workspace = crate::ingest::domain::change_record_cost::ngram_distinct_table::DEFAULT_NGRAM_COST_WORKSPACE_BYTES;
        let minimum = Self::record_owned_bytes(entry)
            .and_then(|raw| {
                raw.checked_add(reserved.extra_owned)
                    .and_then(|n| n.checked_add(reserved.preparation_bytes))
                    .and_then(|n| n.checked_add(workspace))
                    .ok_or(RecordAdmissionError::Overflow)
            })
            .map_err(|error| (None, error))?;
        reserved
            .try_grow_to(minimum)
            .map_err(|error| (Some(minimum), RecordAdmissionError::Capacity(error)))?;
        reserved.ngram_cost_workspace_bytes = workspace;
        Ok(())
    }

    /// Only this reservation-owning path may construct an exact Ngram table.
    /// Its input and table bounds have already been reserved. The table is
    /// destroyed before this helper returns; generic estimators stay unchanged.
    pub(super) fn record_memory_bound_with_workspace(
        &self,
        entry: &RaftLogEntry,
        reserved: &RecordReservation,
        staged_text: bool,
    ) -> Result<usize, RecordAdmissionError> {
        if staged_text || reserved.ngram_cost_workspace_bytes == 0 {
            return self.record_memory_bound(entry, reserved.extra_owned, staged_text);
        }
        let raw = Self::record_owned_bytes(entry)?;
        let floor = raw
            .checked_add(reserved.extra_owned)
            .and_then(|n| n.checked_add(reserved.preparation_bytes))
            .and_then(|n| n.checked_add(reserved.ngram_cost_workspace_bytes))
            .ok_or(RecordAdmissionError::Overflow)?;
        assert!(
            reserved.bytes() >= floor,
            "exact Ngram workspace must be reserved before pricing"
        );
        match self.estimate_record_exact_default_ngram_cost(entry) {
            RecordEstimate::Ready(cost) => cost
                .active
                .checked_add(cost.frozen)
                .and_then(|n| n.checked_add(cost.prepublish))
                .and_then(|n| n.checked_add(raw))
                .and_then(|n| n.checked_add(reserved.extra_owned))
                .ok_or(RecordAdmissionError::Overflow),
            RecordEstimate::Retain { cause } => Err(RecordAdmissionError::NeedsPreparation(cause)),
        }
    }

    /// Finish complete local admission before a transport may publish the WAL.
    /// The table is gone; keep its reserved workspace for the later state recheck.
    pub(super) fn price_before_publication(
        &self,
        entry: &RaftLogEntry,
        reserved: &mut RecordReservation,
    ) -> Result<(), RecordAdmissionError> {
        self.ensure_ngram_cost_workspace(entry, reserved)
            .map_err(|(_, error)| error)?;
        let ordinary = self.record_memory_bound_with_workspace(entry, reserved, false)?;
        let ordinary_with_workspace = ordinary
            .checked_add(reserved.ngram_cost_workspace_bytes)
            .ok_or(RecordAdmissionError::Overflow)?;
        reserved.staged_text =
            ordinary_with_workspace > crate::ingest::domain::change_budget::HARD_LIMIT;
        let final_bound = if reserved.staged_text {
            self.record_memory_bound(entry, reserved.extra_owned, true)?
        } else {
            ordinary
        };
        let required = final_bound
            .checked_add(reserved.ngram_cost_workspace_bytes)
            .ok_or(RecordAdmissionError::Overflow)?;
        reserved
            .try_grow_to(required)
            .map_err(RecordAdmissionError::Capacity)?;
        reserved
            .reservation
            .shrink_to(required)
            .map_err(RecordAdmissionError::Capacity)?;
        Ok(())
    }

    pub(super) fn record_memory_bound(
        &self,
        entry: &RaftLogEntry,
        extra_owned: usize,
        staged_text: bool,
    ) -> Result<usize, RecordAdmissionError> {
        let raw = Self::record_owned_bytes(entry)?;
        if staged_text {
            return self
                .prepared_text_bound(entry)?
                .checked_add(raw)
                .and_then(|n| n.checked_add(extra_owned))
                .ok_or(RecordAdmissionError::Overflow);
        }
        match self.estimate_record_cost(entry) {
            RecordEstimate::Ready(cost) => cost
                .active
                .checked_add(cost.frozen)
                .and_then(|n| n.checked_add(cost.prepublish))
                .and_then(|n| n.checked_add(raw))
                .and_then(|n| n.checked_add(extra_owned))
                .ok_or(RecordAdmissionError::Overflow),
            RecordEstimate::Retain { cause } => Err(RecordAdmissionError::NeedsPreparation(cause)),
        }
    }
}
