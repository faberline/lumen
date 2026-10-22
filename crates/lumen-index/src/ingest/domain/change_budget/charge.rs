//! What a committed reservation becomes: an active, RAM or retained charge, and
//! the frozen batch a checkpoint publishes.

use crate::ingest::domain::change_budget::{
    ActiveCharge, AdmissionError, FrozenBatch, RamCharge, RetainedCharge, RetainedChargeInner,
};

impl ActiveCharge {
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn owner_id(&self) -> u64 {
        self.owner
    }
}

impl Drop for ActiveCharge {
    fn drop(&mut self) {
        // Deliberate: a committed mutation outlives its request handle.
        let _ = &self.budget;
    }
}

impl RamCharge {
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Consume this RAM charge only after the unforgeable marker made by the
    /// carrier confirms the same source, epoch, and sequence reached durable
    /// staging after the payload was dropped.
    pub(crate) fn release_after_stage(
        self,
        proof: crate::ingest::domain::change_admission::RamReleased,
    ) -> Result<(), (AdmissionError, RamCharge)> {
        if !proof.matches(self.sequence, self.engine_epoch, &self.source) {
            return Err((AdmissionError::WrongOwner, self));
        }
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let retired = match state.owners.get_mut(&self.owner) {
            Some(owner) if !owner.retired => false,
            _ => true,
        };
        if retired {
            drop(state);
            return Err((AdmissionError::Retired, self));
        }
        let owner = state
            .owners
            .get_mut(&self.owner)
            .expect("live RAM charge owner checked");
        owner.committed_ram = owner
            .committed_ram
            .checked_sub(self.bytes)
            .expect("RAM charge accounting lost");
        self.budget.0.changed.notify_all();
        self.budget.0.wake.signal();
        Ok(())
    }
}

impl Drop for RamCharge {
    fn drop(&mut self) {
        let _ = &self.budget;
    }
}

impl RetainedCharge {
    pub(crate) fn bytes(&self) -> usize {
        self.0.bytes
    }
}

impl Drop for RetainedChargeInner {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        if let Some(owner) = state.owners.get_mut(&self.owner) {
            if self.bytes != 0 {
                let remove_epoch = {
                    let resident = owner
                        .resident
                        .get_mut(&self.capture_epoch)
                        .expect("retained charge accounting lost");
                    *resident = resident
                        .checked_sub(self.bytes)
                        .expect("retained charge accounting lost");
                    *resident == 0
                };
                if remove_epoch {
                    owner.resident.remove(&self.capture_epoch);
                }
            }
            self.budget.0.changed.notify_all();
            self.budget.0.wake.signal();
        }
    }
}

impl FrozenBatch {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn publish(self) -> Result<usize, AdmissionError> {
        self.budget.publish_through(&self)
    }
}
