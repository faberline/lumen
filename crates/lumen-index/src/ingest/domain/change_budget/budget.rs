//! `ChangeBudget` itself: new owners, snapshots, checkpoint requests, and the
//! reservation every admission starts with.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::ingest::domain::change_budget::{
    request_revision, snapshot, update_high_water, AdmissionError, BudgetWake, CapacityWaiter,
    ChangeBudget, FrozenBatch, Inner, Owner, OwnerLifetime, OwnerState, Reservation, Snapshot,
    State, HARD_LIMIT,
};

impl ChangeBudget {
    pub fn new() -> Self {
        Self::new_inner(HARD_LIMIT)
    }

    fn new_inner(hard_limit: usize) -> Self {
        assert!(hard_limit > 0);
        Self(Arc::new(Inner {
            hard_limit,
            capacity_waiters: AtomicUsize::new(0),
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            wake: Arc::new(BudgetWake {
                epoch: AtomicU64::new(0),
                lock: Mutex::new(()),
                changed: Condvar::new(),
                async_changed: tokio::sync::Notify::new(),
            }),
        }))
    }

    pub fn checkpoint_wake(&self) -> Arc<BudgetWake> {
        self.0.wake.clone()
    }

    #[cfg(test)]
    pub(crate) fn with_hard_limit(hard_limit: usize) -> Self {
        Self::new_inner(hard_limit)
    }

    pub fn owner(&self) -> Owner {
        let mut state = self.0.state.lock().expect("change budget lock poisoned");
        state.next_owner = state.next_owner.checked_add(1).expect("owner id exhausted");
        let id = state.next_owner;
        state.owners.insert(id, OwnerState::default());
        Owner {
            budget: self.clone(),
            id,
            lifetime: Arc::new(OwnerLifetime {
                budget: self.clone(),
                id,
            }),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_with_high_water().0
    }

    /// Read the live pending totals and their lifetime peak under one budget
    /// mutex acquisition. Prometheus rendering needs this pair to avoid
    /// combining totals from different concurrent accounting states.
    pub(crate) fn snapshot_with_high_water(&self) -> (Snapshot, usize) {
        let state = self.0.state.lock().expect("change budget lock poisoned");
        (snapshot(&state), state.high_water_bytes)
    }

    /// Largest successful pending-change total since this budget was created.
    /// Releases and rejected reservations never lower or raise this value.
    pub fn high_water_bytes(&self) -> usize {
        self.snapshot_with_high_water().1
    }

    /// True only while an admitted apply/replay operation waits for capacity.
    /// A coordinator uses this to park future WAL payloads outside the apply
    /// barrier. A pre-publication refusal does not request such work.
    pub(crate) fn has_capacity_waiters(&self) -> bool {
        self.0.capacity_waiters.load(Ordering::Acquire) != 0
    }

    pub(super) fn mark_capacity_waiter(&self) -> CapacityWaiter {
        self.0
            .capacity_waiters
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .expect("capacity waiter count overflow");
        self.0.wake.signal();
        CapacityWaiter(self.clone())
    }

    /// Ask the checkpoint driver to make one early attempt for current
    /// checkpointable work. This never makes a reservation or pre-apply RAM
    /// eligible by itself, and it does not advance the work revision.
    pub fn request_checkpoint(&self) -> bool {
        self.request_checkpoint_revision().is_some()
    }

    /// Return the pending checkpoint request revision from the same lock
    /// acquisition that creates or observes it. Callers that emit a refusal
    /// diagnostic must not take a later snapshot, because another checkpoint
    /// can consume the request between those two operations.
    pub fn request_checkpoint_revision(&self) -> Option<u64> {
        let mut state = self.0.state.lock().expect("change budget lock poisoned");
        let pending = snapshot(&state);
        if pending.active == 0 && pending.frozen == 0 {
            return None;
        }
        let work_revision = state.next_work_revision;
        let previous = state.checkpoint_request_revision;
        let revision = request_revision(&mut state, work_revision);
        if previous != Some(revision) {
            self.0.wake.signal();
        }
        Some(revision)
    }

    pub(super) fn reserve(
        &self,
        lifetime: &Arc<OwnerLifetime>,
        bytes: usize,
        wait: bool,
    ) -> Result<Reservation, AdmissionError> {
        let owner = lifetime.id;
        if bytes > self.0.hard_limit {
            return Err(AdmissionError::Oversized {
                requested: bytes,
                hard_limit: self.0.hard_limit,
            });
        }
        // Declared before the state guard so marker drop never holds that
        // mutex. A spurious wake reuses this marker instead of counting twice.
        let mut waiting = None;
        let mut state = self.0.state.lock().expect("change budget lock poisoned");
        loop {
            let used = snapshot(&state).total;
            let owner_state = state.owners.get(&owner).ok_or(AdmissionError::Retired)?;
            if owner_state.retired {
                return Err(AdmissionError::Retired);
            }
            if bytes <= self.0.hard_limit - used {
                state
                    .owners
                    .get_mut(&owner)
                    .expect("owner checked above")
                    .reserved = state
                    .owners
                    .get(&owner)
                    .expect("owner checked above")
                    .reserved
                    .checked_add(bytes)
                    .expect("reservation accounting overflow");
                update_high_water(&mut state);
                self.0.wake.signal();
                return Ok(Reservation {
                    budget: self.clone(),
                    owner,
                    lifetime: lifetime.clone(),
                    bytes,
                    committed: false,
                    source_retention: None,
                });
            }
            if !wait {
                return Err(AdmissionError::Full {
                    requested: bytes,
                    used,
                    hard_limit: self.0.hard_limit,
                });
            }
            waiting.get_or_insert_with(|| self.mark_capacity_waiter());
            state = self
                .0
                .changed
                .wait(state)
                .expect("change budget lock poisoned");
        }
    }

    /// Publish every frozen batch for this owner through `captured`, inclusive.
    /// Newer frozen batches, active work, and reservations stay charged.
    pub fn publish_through(&self, captured: &FrozenBatch) -> Result<usize, AdmissionError> {
        if !Arc::ptr_eq(&self.0, &captured.budget.0) {
            return Err(AdmissionError::WrongBudget);
        }
        let mut state = self.0.state.lock().expect("change budget lock poisoned");
        let owner = state
            .owners
            .get_mut(&captured.owner)
            .ok_or(AdmissionError::Retired)?;
        let ids: Vec<u64> = owner
            .frozen
            .range(..=captured.id)
            .map(|(&id, _)| id)
            .collect();
        let released = ids
            .into_iter()
            .map(|id| owner.frozen.remove(&id).unwrap())
            .sum();
        self.0.changed.notify_all();
        self.0.wake.signal();
        Ok(released)
    }
}

impl Default for ChangeBudget {
    fn default() -> Self {
        Self::new()
    }
}
