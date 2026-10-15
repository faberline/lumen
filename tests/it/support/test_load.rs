//! Limits for the `it` cases that load the whole test process.
//!
//! Every live `Engine` charges the one process-wide `ChangeBudget`, so two
//! cases that fill or measure the 256 MiB pending budget in this process
//! cannot overlap: each one sees the other's bytes. Those cases hold
//! [`exclusive_process_budget`]. Cases that rerun themselves in a child process,
//! or drive several checkpointing engines, hold a [`heavy_slot`] so that at most
//! [`HEAVY_SLOTS`] of them run at once.
//!
//! Take the guard in the parent only, after a re-executed child has returned,
//! and keep it until the case ends. The guards are std locks: a panicking case
//! releases them, and the next case recovers the poisoned lock.

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};

/// `TOKIO_WORKER_THREADS` for a spawned `lumen serve`. Production keeps at
/// least four reactor workers (`src/bin/lumen/main.rs`); without this each
/// child would start one per host CPU.
pub const SERVE_WORKER_THREADS: &str = "4";

/// Heavy cases that may run at the same time.
pub const HEAVY_SLOTS: usize = 2;

static FREE_SLOTS: Mutex<usize> = Mutex::new(HEAVY_SLOTS);
static SLOT_RELEASED: Condvar = Condvar::new();
// Taken before a slot, so a case that holds a slot never waits for it.
static PROCESS_BUDGET: Mutex<()> = Mutex::new(());

/// One of the [`HEAVY_SLOTS`] slots, released on drop.
pub struct HeavySlot(());

impl Drop for HeavySlot {
    fn drop(&mut self) {
        *FREE_SLOTS.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        SLOT_RELEASED.notify_one();
    }
}

/// Waits for a free heavy slot.
pub fn heavy_slot() -> HeavySlot {
    let mut free = FREE_SLOTS.lock().unwrap_or_else(PoisonError::into_inner);
    while *free == 0 {
        free = SLOT_RELEASED
            .wait(free)
            .unwrap_or_else(PoisonError::into_inner);
    }
    *free -= 1;
    HeavySlot(())
}

/// Sole use of the process-wide pending budget, plus a heavy slot.
pub struct ProcessBudgetGuard {
    _slot: HeavySlot,
    _budget: MutexGuard<'static, ()>,
}

/// Waits until no other case uses the process-wide pending budget, then for
/// a heavy slot.
pub fn exclusive_process_budget() -> ProcessBudgetGuard {
    let budget = PROCESS_BUDGET
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    ProcessBudgetGuard {
        _slot: heavy_slot(),
        _budget: budget,
    }
}
