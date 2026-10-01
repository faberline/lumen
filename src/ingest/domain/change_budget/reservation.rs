//! A reservation: bytes held before an operation runs, resized as the operation
//! learns its cost, then committed or dropped.

use std::sync::{Arc, Mutex};

use crate::ingest::domain::change_budget::{
    snapshot, update_high_water, ActiveCharge, AdmissionError, RamCharge, Reservation,
    RetainedCharge, RetainedChargeInner, SourceRetention, SourceRetentionHeld,
};

impl Reservation {
    /// Divide one already-admitted reservation without changing process
    /// accounting. The original half keeps its source-retention bridge; the
    /// new half is ordinary transient ownership. This is deliberately a pure
    /// handle split: it must not wake capacity waiters or open a second
    /// admission race.
    pub(crate) fn split_off(&mut self, bytes: usize) -> Reservation {
        assert!(
            bytes <= self.bytes,
            "cannot split {bytes} bytes from {} reserved bytes",
            self.bytes
        );
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .expect("split bytes were checked against reservation");
        Reservation {
            budget: self.budget.clone(),
            owner: self.owner,
            lifetime: self.lifetime.clone(),
            bytes,
            committed: false,
            // The source must stay with the apply half. The transient half
            // protects only temporary encode/AOF ownership.
            source_retention: None,
        }
    }

    pub(crate) fn source_retention(&mut self) -> SourceRetention {
        let retained = self
            .source_retention
            .get_or_insert_with(|| Arc::new(Mutex::new(None)));
        SourceRetention(retained.clone())
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Release unused bytes before this reservation becomes a charge.
    ///
    /// This only changes the mutable reservation.  Charges created by a
    /// commit retain their existing lifetime and accounting.
    pub(crate) fn shrink_to(&mut self, required: usize) -> Result<(), AdmissionError> {
        if required > self.bytes {
            return Err(AdmissionError::CannotShrink {
                current: self.bytes,
                requested: required,
            });
        }
        if required == self.bytes {
            return Ok(());
        }

        let released = self
            .bytes
            .checked_sub(required)
            .expect("required is smaller than the reservation");
        let mut state = self.budget.0.state.lock().expect("change budget poisoned");
        let owner = state
            .owners
            .get_mut(&self.owner)
            .ok_or(AdmissionError::Retired)?;
        if owner.retired {
            return Err(AdmissionError::Retired);
        }
        owner.reserved = owner
            .reserved
            .checked_sub(released)
            .expect("reservation accounting lost");
        self.bytes = required;
        drop(state);
        self.budget.0.changed.notify_all();
        self.budget.0.wake.signal();
        Ok(())
    }

    /// Extend this live reservation to the requested total; it never shrinks.
    /// Call before an apply or CaptureBarrier lease. The waiting form may block
    /// until another owner publishes or retires.
    pub(crate) fn try_grow_to(&mut self, required_total: usize) -> Result<(), AdmissionError> {
        self.grow_to(required_total, false)
    }

    /// Like try_grow_to, but waits only for the positive delta.
    /// Call before an apply or CaptureBarrier lease.
    pub(crate) fn wait_grow_to(&mut self, required_total: usize) -> Result<(), AdmissionError> {
        self.grow_to(required_total, true)
    }

    fn grow_to(&mut self, required_total: usize, wait: bool) -> Result<(), AdmissionError> {
        if required_total <= self.bytes {
            return Ok(());
        }
        if required_total > self.budget.0.hard_limit {
            return Err(AdmissionError::Oversized {
                requested: required_total,
                hard_limit: self.budget.0.hard_limit,
            });
        }
        let delta = required_total
            .checked_sub(self.bytes)
            .expect("required total checked above");
        let mut waiting = None;
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        loop {
            let used = snapshot(&state).total;
            let owner = state
                .owners
                .get(&self.owner)
                .ok_or(AdmissionError::Retired)?;
            if owner.retired {
                return Err(AdmissionError::Retired);
            }
            if delta <= self.budget.0.hard_limit - used {
                let owner = state
                    .owners
                    .get_mut(&self.owner)
                    .expect("owner checked above");
                owner.reserved = owner
                    .reserved
                    .checked_add(delta)
                    .expect("reservation accounting overflow");
                self.bytes = required_total;
                update_high_water(&mut state);
                self.budget.0.wake.signal();
                return Ok(());
            }
            if !wait {
                return Err(AdmissionError::Full {
                    requested: required_total,
                    used,
                    hard_limit: self.budget.0.hard_limit,
                });
            }
            waiting.get_or_insert_with(|| self.budget.mark_capacity_waiter());
            state = self
                .budget
                .0
                .changed
                .wait(state)
                .expect("change budget lock poisoned");
        }
    }

    pub fn commit(mut self) -> Result<ActiveCharge, AdmissionError> {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let owner = state
            .owners
            .get_mut(&self.owner)
            .ok_or(AdmissionError::Retired)?;
        if owner.retired {
            return Err(AdmissionError::Retired);
        }
        owner.reserved = owner
            .reserved
            .checked_sub(self.bytes)
            .expect("reservation accounting lost");
        owner.active = owner
            .active
            .checked_add(self.bytes)
            .expect("active accounting overflow");
        state.next_work_revision = state
            .next_work_revision
            .checked_add(1)
            .expect("work revision exhausted");
        let revision = state.next_work_revision;
        state
            .owners
            .get_mut(&self.owner)
            .expect("owner checked above")
            .work_revision = revision;
        self.budget.0.wake.signal();
        self.committed = true;
        Ok(ActiveCharge {
            budget: self.budget.clone(),
            owner: self.owner,
            _lifetime: self.lifetime.clone(),
            bytes: self.bytes,
        })
    }

    /// Commit journal or reclaimer payload bytes that stay resident until the
    /// actual shared payload owner releases its final handle.
    pub(crate) fn commit_retained(mut self) -> Result<RetainedCharge, AdmissionError> {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let capture_epoch = {
            let owner = state
                .owners
                .get_mut(&self.owner)
                .ok_or(AdmissionError::Retired)?;
            if owner.retired {
                return Err(AdmissionError::Retired);
            }
            owner.reserved = owner
                .reserved
                .checked_sub(self.bytes)
                .expect("reservation accounting lost");
            let capture_epoch = owner.current_capture_epoch;
            if self.bytes != 0 {
                let resident = owner.resident.entry(capture_epoch).or_default();
                *resident = resident
                    .checked_add(self.bytes)
                    .expect("resident accounting overflow");
            }
            capture_epoch
        };
        state.next_work_revision = state
            .next_work_revision
            .checked_add(1)
            .expect("work revision exhausted");
        let revision = state.next_work_revision;
        state
            .owners
            .get_mut(&self.owner)
            .expect("owner checked above")
            .work_revision = revision;
        self.budget.0.wake.signal();
        self.committed = true;
        let charge = RetainedCharge(Arc::new(RetainedChargeInner {
            budget: self.budget.clone(),
            owner: self.owner,
            _lifetime: self.lifetime.clone(),
            capture_epoch,
            bytes: self.bytes,
        }));
        if let Some(retention) = self.source_retention.take() {
            *retention.lock().expect("source retention poisoned") =
                Some(SourceRetentionHeld::Retained(charge.clone()));
        }
        Ok(charge)
    }

    /// Commit pre-apply RAM that is eligible only for a verified durable-stage
    /// transition. Applied journal work must use [`Self::commit`] instead.
    pub(crate) fn commit_ram(
        mut self,
        sequence: u64,
        engine_epoch: u64,
        source: crate::ingest::infrastructure::committed_stage::SourceIdentity,
    ) -> Result<RamCharge, AdmissionError> {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let owner = state
            .owners
            .get_mut(&self.owner)
            .ok_or(AdmissionError::Retired)?;
        if owner.retired {
            return Err(AdmissionError::Retired);
        }
        owner.reserved = owner
            .reserved
            .checked_sub(self.bytes)
            .expect("reservation accounting lost");
        owner.committed_ram = owner
            .committed_ram
            .checked_add(self.bytes)
            .expect("committed RAM accounting overflow");
        self.budget.0.wake.signal();
        self.committed = true;
        Ok(RamCharge {
            budget: self.budget.clone(),
            owner: self.owner,
            _lifetime: self.lifetime.clone(),
            bytes: self.bytes,
            sequence,
            engine_epoch,
            source,
        })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Some(retention) = self.source_retention.take() {
            let held = Reservation {
                budget: self.budget.clone(),
                owner: self.owner,
                lifetime: self.lifetime.clone(),
                bytes: self.bytes,
                committed: false,
                source_retention: None,
            };
            *retention.lock().expect("source retention poisoned") =
                Some(SourceRetentionHeld::Reservation(held));
            self.committed = true;
            return;
        }
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        if let Some(owner) = state.owners.get_mut(&self.owner) {
            if !owner.retired {
                owner.reserved = owner
                    .reserved
                    .checked_sub(self.bytes)
                    .expect("reservation accounting lost");
            }
            self.budget.0.changed.notify_all();
            self.budget.0.wake.signal();
        }
    }
}
