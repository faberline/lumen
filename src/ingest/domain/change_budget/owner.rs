//! An owner's side of the budget: reserve, request a checkpoint, freeze a
//! batch, publish it, and retire.

use crate::ingest::domain::change_budget::{
    request_revision, AdmissionError, FrozenBatch, Owner, OwnerCapacityState, Reservation,
};

#[cfg(test)]
use crate::ingest::domain::change_budget::OwnerRawCapacityState;

impl Owner {
    #[cfg(test)]
    pub(crate) fn raw_capacity_state_for_test(
        &self,
    ) -> Result<OwnerRawCapacityState, AdmissionError> {
        let state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let owner = state.owners.get(&self.id).ok_or(AdmissionError::Retired)?;
        Ok(OwnerRawCapacityState {
            active: owner.active,
            frozen_batches: owner.frozen.len(),
            resident_active: owner
                .resident
                .get(&owner.current_capture_epoch)
                .copied()
                .unwrap_or_default(),
            resident_frozen: owner
                .resident
                .iter()
                .filter(|(epoch, _)| **epoch != owner.current_capture_epoch)
                .map(|(_, bytes)| *bytes)
                .sum(),
        })
    }

    pub(crate) fn request_checkpoint(&self) -> bool {
        self.request_checkpoint_revision().is_some()
    }

    /// Return this owner's active/frozen checkpoint request revision while
    /// the accounting lock is held. This is the only revision that a local
    /// admission refusal may report.
    pub(crate) fn request_checkpoint_revision(&self) -> Option<u64> {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let Some(owner) = state.owners.get(&self.id) else {
            return None;
        };
        let resident_active = owner
            .resident
            .get(&owner.current_capture_epoch)
            .copied()
            .unwrap_or_default();
        let resident_frozen = owner
            .resident
            .iter()
            .filter(|(epoch, _)| **epoch != owner.current_capture_epoch)
            .any(|(_, bytes)| *bytes != 0);
        if owner.active == 0 && owner.frozen.is_empty() && resident_active == 0 && !resident_frozen
        {
            return None;
        }
        let work_revision = owner.work_revision;
        let previous = owner.checkpoint_request_revision;
        let revision = request_revision(&mut state, work_revision);
        state
            .owners
            .get_mut(&self.id)
            .expect("owner checked above")
            .checkpoint_request_revision = Some(revision);
        if previous != Some(revision) {
            self.budget.0.wake.signal();
        }
        Some(revision)
    }

    pub(crate) fn consume_checkpoint_request(&self, revision: Option<u64>) {
        let Some(revision) = revision else {
            return;
        };
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        if let Some(owner) = state.owners.get_mut(&self.id) {
            if owner.checkpoint_request_revision == Some(revision) {
                owner.checkpoint_request_revision = None;
            }
        }
        if state.checkpoint_request_revision == Some(revision) {
            state.checkpoint_request_revision = None;
        }
        self.budget.0.wake.signal();
    }

    pub fn try_reserve(&self, bytes: usize) -> Result<Reservation, AdmissionError> {
        self.budget.reserve(&self.lifetime, bytes, false)
    }

    /// Wait for admission but keep the bytes reserved until the caller crosses
    /// its apply boundary and explicitly commits this reservation.
    pub fn wait_reserve(&self, bytes: usize) -> Result<Reservation, AdmissionError> {
        self.budget.reserve(&self.lifetime, bytes, true)
    }

    pub(crate) fn work_revision(&self) -> Result<u64, AdmissionError> {
        let state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let owner = state.owners.get(&self.id).ok_or(AdmissionError::Retired)?;
        if owner.retired {
            return Err(AdmissionError::Retired);
        }
        Ok(owner.work_revision)
    }

    /// Read only this owner's publishable work. `ChangeBudget::snapshot` is
    /// process-global and therefore cannot safely select an Engine to save.
    pub(crate) fn capacity_state(&self) -> Result<OwnerCapacityState, AdmissionError> {
        let state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let owner = state.owners.get(&self.id).ok_or(AdmissionError::Retired)?;
        if owner.retired {
            return Err(AdmissionError::Retired);
        }
        let resident_active = owner
            .resident
            .get(&owner.current_capture_epoch)
            .copied()
            .unwrap_or_default();
        let resident_frozen = owner
            .resident
            .iter()
            .filter(|(epoch, _)| **epoch != owner.current_capture_epoch)
            .try_fold(0usize, |total, (_, bytes)| total.checked_add(*bytes))
            .expect("resident frozen overflow");
        let frozen = owner
            .frozen
            .values()
            .copied()
            .try_fold(resident_frozen, |total, bytes| total.checked_add(bytes))
            .expect("frozen overflow");
        Ok(OwnerCapacityState {
            active: owner
                .active
                .checked_add(resident_active)
                .expect("active overflow"),
            frozen,
            work_revision: owner.work_revision,
            checkpoint_request_revision: owner.checkpoint_request_revision,
        })
    }

    /// Move all active work into one durable checkpoint batch.
    pub fn freeze(&self) -> Result<FrozenBatch, AdmissionError> {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let active = {
            let owner = state
                .owners
                .get_mut(&self.id)
                .ok_or(AdmissionError::Retired)?;
            if owner.retired {
                return Err(AdmissionError::Retired);
            }
            let active = std::mem::take(&mut owner.active);
            // Retained payload handles carry their old epoch. Advancing this
            // scalar moves them into the frozen view without visiting them.
            owner.current_capture_epoch = owner
                .current_capture_epoch
                .checked_add(1)
                .expect("capture epoch exhausted");
            active
        };
        state.next_batch = state.next_batch.checked_add(1).expect("batch id exhausted");
        let id = state.next_batch;
        state
            .owners
            .get_mut(&self.id)
            .expect("owner checked above")
            .frozen
            .insert(id, active);
        self.budget.0.wake.signal();
        Ok(FrozenBatch {
            budget: self.budget.clone(),
            owner: self.id,
            _lifetime: self.lifetime.clone(),
            id,
        })
    }

    /// Release only this engine's accounting.  Restore candidates and other
    /// live engines remain charged.
    pub fn retire(&self) {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        if let Some(owner) = state.owners.get_mut(&self.id) {
            owner.reserved = 0;
            owner.committed_ram = 0;
            owner.active = 0;
            owner.frozen.clear();
            // `resident` belongs to retained payload handles. Those handles
            // can outlive this owner, so retirement must leave their bytes
            // visible until the final handle drops.
            owner.retired = true;
            self.budget.0.changed.notify_all();
            self.budget.0.wake.signal();
        }
    }

    /// Publish only this owner's frozen batch.
    pub fn publish_through(&self, batch: &FrozenBatch) -> Result<usize, AdmissionError> {
        if self.id != batch.owner {
            return Err(AdmissionError::WrongOwner);
        }
        self.budget.publish_through(batch)
    }
}
