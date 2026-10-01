//! The Engine's side of the shared change budget: its capacity owner and
//! waiters, the checkpoint relief it requests and consumes, and the record
//! charges it freezes into a capture and releases once that capture is durably
//! published.

use crate::index::application::admission::record_reservation::RecordReservation;
use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::{BudgetWake, OwnerCapacityState, RetainedCharge};

#[cfg(test)]
use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget};

impl Engine {
    #[cfg(test)]
    pub(crate) fn test_retained_owner_charge(&self, bytes: usize) -> RetainedCharge {
        self.changes
            .owner
            .try_reserve(bytes)
            .unwrap()
            .commit_retained()
            .unwrap()
    }
    /// The caller owns apply and has rechecked its borrowed plan. No source or
    /// codec workspace remains in this reservation, only attached metadata.
    pub(in crate::index::application) fn retain_borrowed_reservation(
        &self,
        reserved: RecordReservation,
    ) -> Result<RetainedCharge, RecordAdmissionError> {
        if reserved.runtime != self.changes.id {
            return Err(RecordAdmissionError::WrongEngine);
        }
        let charge = reserved
            .reservation
            .commit_retained()
            .map_err(RecordAdmissionError::Capacity)?;
        self.changes.records.retain(charge.clone());
        Ok(charge)
    }

    pub(crate) fn has_capacity_waiters(&self) -> bool {
        self.changes.budget.has_capacity_waiters()
    }

    #[cfg(test)]
    pub(crate) fn with_change_budget(budget: ChangeBudget) -> Self {
        let mut engine = Self::new();
        engine.changes.owner = budget.owner();
        engine.changes.budget = budget;
        engine
    }

    pub(crate) fn request_pending_checkpoint(&self) {
        let _ = self.request_pending_checkpoint_revision();
    }

    /// Request relief for this Engine and return the revision atomically with
    /// the request. `None` means it had no active or frozen local work.
    pub(crate) fn request_pending_checkpoint_revision(&self) -> Option<u64> {
        self.changes.owner.request_checkpoint_revision()
    }

    #[cfg(test)]
    pub(crate) fn raw_capacity_state_for_test(
        &self,
    ) -> Result<crate::ingest::domain::change_budget::OwnerRawCapacityState, AdmissionError> {
        self.changes.owner.raw_capacity_state_for_test()
    }

    pub(crate) fn consume_checkpoint_request(&self, revision: u64) {
        self.changes
            .owner
            .consume_checkpoint_request(Some(revision));
    }

    /// Fallback checkpoint maintenance selects only this Engine's work, while
    /// its waiter demand remains process-global to relieve other Engines too.
    pub(crate) fn capacity_owner_state(&self) -> Option<OwnerCapacityState> {
        self.changes.owner.capacity_state().ok()
    }

    pub(crate) fn checkpoint_wake(&self) -> std::sync::Arc<BudgetWake> {
        self.changes.budget.checkpoint_wake()
    }
}

/// One exact captured ownership cut. A background merge has no record cut.
pub(crate) struct RecordCut {
    records: super::record_charges::FrozenRecordCharges,
    budget: crate::ingest::domain::change_budget::FrozenBatch,
}

impl Engine {
    pub(in crate::index::application) fn freeze_record_charges(
        &self,
    ) -> anyhow::Result<std::sync::Arc<RecordCut>> {
        let budget = self
            .changes
            .owner
            .freeze()
            .map_err(|error| anyhow::anyhow!("freeze pending record budget: {error:?}"))?;
        Ok(std::sync::Arc::new(RecordCut {
            records: self.changes.records.freeze(),
            budget,
        }))
    }

    /// Caller has durably published and bound this exact capture. The pending
    /// checkpoint and escaped row handles still retain their payload charges.
    pub(crate) fn acknowledge_record_charges(
        &self,
        capture: &crate::index::application::checkpoint_capture::CheckpointCapture,
    ) -> anyhow::Result<()> {
        if let Some(cut) = &capture.record_cut {
            self.changes
                .owner
                .publish_through(&cut.budget)
                .map_err(|error| anyhow::anyhow!("publish pending record budget: {error:?}"))?;
            self.changes.records.acknowledge_through(&cut.records);
        }
        Ok(())
    }
}
