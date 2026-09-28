//! Owner-scoped accounting for durable Lumen changes.
//!
//! A caller reserves bytes before it starts an operation.  A reservation can
//! be cancelled by dropping it, or committed into active change memory.  An
//! active charge is intentionally not released when its original caller is
//! dropped: it remains charged until a checkpoint batch that captured it is
//! published.  Freezing creates such a batch.  Dropping a frozen batch handle
//! represents a failed checkpoint attempt and retains its charge.
//!
//! Admission must happen before a caller takes a future CaptureBarrier.  The
//! blocking admission method waits for publication or owner retirement, while
//! an input larger than the hard limit returns `Oversized` immediately so the
//! caller can choose a spill path.

pub(crate) mod budget;
pub(crate) mod budget_wake;
pub(crate) mod charge;
pub(crate) mod owner;
pub(crate) mod reservation;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

pub const HARD_LIMIT: usize = 256 * 1024 * 1024;

pub const CHECKPOINT_TRIGGER: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// The requested object cannot ever fit and must use a spill path.
    Oversized { requested: usize, hard_limit: usize },
    /// The request fits alone but current reserved, active, and frozen work
    /// leaves insufficient room.  Callers may retry or use blocking admission.
    Full {
        requested: usize,
        used: usize,
        hard_limit: usize,
    },
    /// The owner was retired while an operation still held its handle.
    Retired,
    /// A frozen batch belongs to a different shared budget instance.
    WrongBudget,
    /// The batch belongs to another owner in this shared budget.
    WrongOwner,
    /// A reservation can release bytes but cannot use shrinking to grow.
    CannotShrink { current: usize, requested: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub reserved: usize,
    pub active: usize,
    pub frozen: usize,
    pub total: usize,
    /// Monotonic revision of new checkpointable work.
    pub work_revision: u64,
    /// A blocked-admission request tied to work that was already active or
    /// frozen when the request was made.
    pub checkpoint_request_revision: Option<u64>,
}

impl Snapshot {
    pub fn checkpoint_needed(self) -> bool {
        self.total >= CHECKPOINT_TRIGGER
    }
}

#[derive(Default)]
struct OwnerState {
    reserved: usize,
    // Committed record payloads that have not entered an apply interval.
    // A checkpoint cannot capture these bytes, so freeze must leave them here.
    committed_ram: usize,
    active: usize,
    frozen: BTreeMap<u64, usize>,
    /// Payload ownership retained by a journal or reclaimer. The key is the
    /// capture epoch at which the payload became resident.
    resident: BTreeMap<u64, usize>,
    current_capture_epoch: u64,
    work_revision: u64,
    checkpoint_request_revision: Option<u64>,
    retired: bool,
}

#[derive(Default)]
struct State {
    next_owner: u64,
    next_batch: u64,
    next_work_revision: u64,
    next_checkpoint_request_revision: u64,
    checkpoint_request_revision: Option<u64>,
    high_water_bytes: usize,
    owners: BTreeMap<u64, OwnerState>,
}

fn request_revision(state: &mut State, work_revision: u64) -> u64 {
    if let Some(revision) = state.checkpoint_request_revision {
        return revision;
    }
    let revision = state
        .next_checkpoint_request_revision
        .checked_add(1)
        .expect("checkpoint request revision exhausted")
        .max(work_revision);
    state.next_checkpoint_request_revision = revision;
    state.checkpoint_request_revision = Some(revision);
    revision
}

pub(in crate::ingest) struct Inner {
    hard_limit: usize,
    capacity_waiters: AtomicUsize,
    state: Mutex<State>,
    changed: Condvar,
    wake: Arc<BudgetWake>,
}

/// A checkpoint driver can wait for accounting changes without registering a
/// callback. No user code runs while the accounting mutex is held.
///
/// Mutators take the accounting mutex before this mutex. Waiters release this
/// mutex before they may query accounting, so the reverse order is forbidden.
pub struct BudgetWake {
    epoch: AtomicU64,
    lock: Mutex<()>,
    changed: Condvar,
    async_changed: tokio::sync::Notify,
}

/// Shared process accounting.  Its owners are independent engines or restore
/// candidates; publishing one owner never clears another owner's work.
#[derive(Clone)]
pub struct ChangeBudget(pub(in crate::ingest) Arc<Inner>);

/// A waiter owns this marker only until admission finishes or fails. It does
/// not hold an apply lease, and it does not invoke user code or callbacks.
struct CapacityWaiter(ChangeBudget);

impl Drop for CapacityWaiter {
    fn drop(&mut self) {
        let previous = self.0 .0.capacity_waiters.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "capacity waiter accounting lost");
        self.0 .0.wake.signal();
    }
}

/// Pending checkpointable work for one Engine owner. Process-wide snapshots
/// cannot choose a checkpoint target because they aggregate other Engines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerCapacityState {
    pub(crate) active: usize,
    pub(crate) frozen: usize,
    pub(crate) work_revision: u64,
    pub(crate) checkpoint_request_revision: Option<u64>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct OwnerRawCapacityState {
    pub active: usize,
    pub frozen_batches: usize,
    pub resident_active: usize,
    pub resident_frozen: usize,
}

/// One engine or restore candidate's accounting namespace. Retire only after
/// ordinary reservations and committed RAM have left this owner's live path.
/// Retained payload handles may outlive retirement and stay charged until
/// their actual final drop.
pub struct Owner {
    budget: ChangeBudget,
    id: u64,
    lifetime: Arc<OwnerLifetime>,
}

/// Every handle that can use an owner's namespace keeps this token alive.
/// Ending an Engine cannot remove reservations or payloads that escaped it.
/// The last handle removes only empty accounting; unpublished committed bytes
/// still require publication or explicit retirement, even without a handle.
struct OwnerLifetime {
    budget: ChangeBudget,
    id: u64,
}

impl Drop for OwnerLifetime {
    fn drop(&mut self) {
        let mut state = self
            .budget
            .0
            .state
            .lock()
            .expect("change budget lock poisoned");
        let empty = state.owners.get(&self.id).is_some_and(|owner| {
            owner.reserved == 0
                && owner.committed_ram == 0
                && owner.active == 0
                && owner.frozen.values().all(|&bytes| bytes == 0)
                && owner.resident.values().all(|&bytes| bytes == 0)
        });
        if empty {
            state.owners.remove(&self.id);
        }
    }
}

/// Bytes held before a change is committed.  Dropping releases only this
/// reservation.
pub struct Reservation {
    budget: ChangeBudget,
    owner: u64,
    lifetime: Arc<OwnerLifetime>,
    bytes: usize,
    committed: bool,
    source_retention: Option<Arc<Mutex<Option<SourceRetentionHeld>>>>,
}

enum SourceRetentionHeld {
    Reservation(Reservation),
    Retained(RetainedCharge),
}

/// Keeps one source-backed reservation charged while a failed worker still
/// owns the source but cannot safely resume application.
#[derive(Clone)]
pub(crate) struct SourceRetention(Arc<Mutex<Option<SourceRetentionHeld>>>);

/// A committed charge.  Dropping it deliberately does not release memory.
pub struct ActiveCharge {
    budget: ChangeBudget,
    owner: u64,
    _lifetime: Arc<OwnerLifetime>,
    bytes: usize,
}

/// Pre-apply RAM ownership. Unlike [`ActiveCharge`], this may be released only
/// by the private durable-stage proof emitted after its payload is destroyed.
pub(crate) struct RamCharge {
    budget: ChangeBudget,
    owner: u64,
    _lifetime: Arc<OwnerLifetime>,
    bytes: usize,
    sequence: u64,
    engine_epoch: u64,
    source: crate::committed_stage::SourceIdentity,
}

/// Shared ownership of payload bytes retained outside the checkpoint batch.
/// Cloning this handle shares one accounting entry; only the last drop frees
/// its bytes.
#[derive(Clone)]
pub(crate) struct RetainedCharge(Arc<RetainedChargeInner>);

struct RetainedChargeInner {
    budget: ChangeBudget,
    owner: u64,
    _lifetime: Arc<OwnerLifetime>,
    capture_epoch: u64,
    bytes: usize,
}

/// A frozen checkpoint capture.  Dropping it deliberately retains memory.
pub struct FrozenBatch {
    budget: ChangeBudget,
    owner: u64,
    _lifetime: Arc<OwnerLifetime>,
    id: u64,
}

fn snapshot(state: &State) -> Snapshot {
    let mut out = Snapshot {
        reserved: 0,
        active: 0,
        frozen: 0,
        total: 0,
        work_revision: state.next_work_revision,
        checkpoint_request_revision: state.checkpoint_request_revision,
    };
    for owner in state.owners.values() {
        // Retained payloads remain process-owned after their Engine owner is
        // retired. The current epoch is active; earlier epochs are frozen.
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
        out.active = out
            .active
            .checked_add(resident_active)
            .expect("active overflow");
        out.frozen = out
            .frozen
            .checked_add(resident_frozen)
            .expect("frozen overflow");
        if owner.retired {
            continue;
        }
        out.reserved = out
            .reserved
            .checked_add(owner.reserved)
            .and_then(|bytes| bytes.checked_add(owner.committed_ram))
            .expect("reserved overflow");
        out.active = out
            .active
            .checked_add(owner.active)
            .expect("active overflow");
        let frozen: usize = owner.frozen.values().copied().sum();
        out.frozen = out.frozen.checked_add(frozen).expect("frozen overflow");
    }
    out.total = out
        .reserved
        .checked_add(out.active)
        .and_then(|v| v.checked_add(out.frozen))
        .expect("total overflow");
    out
}

fn update_high_water(state: &mut State) {
    let total = snapshot(state).total;
    state.high_water_bytes = state.high_water_bytes.max(total);
}

#[cfg(test)]
mod tests;
