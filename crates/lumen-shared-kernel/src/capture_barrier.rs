//! A checkpoint cut excludes whole synchronous apply records. Nested Engine
//! methods reuse the current thread's apply interval. Readers need no lease.

use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Condvar, Mutex};
use std::thread::{self, ThreadId};
use std::time::Instant;

#[derive(Default)]
pub struct CaptureBarrier {
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    owner: Option<ThreadId>,
    depth: usize,
    capturing: bool,
    publication_pins: usize,
    #[cfg(test)]
    publication_wait_observer: Option<std::sync::mpsc::Sender<()>>,
    waiting_captures: usize,
    sequence: Option<u64>,
    epoch: u64,
    apply_revision: u64,
    restore_inhibitions: usize,
    uncertain: bool,
    apply_started: Option<Instant>,
    timing: CaptureTiming,
}

/// Completed lease intervals. Nested apply guards form one record interval.
/// Wait time is deliberately excluded: these values measure how long work
/// holds the apply/capture boundary after admission.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct HoldTiming {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
}

impl HoldTiming {
    fn record(&mut self, started: Instant) {
        let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.count = self.count.saturating_add(1);
        self.total_ns = self.total_ns.saturating_add(elapsed);
        self.max_ns = self.max_ns.max(elapsed);
    }
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureTiming {
    pub apply: HoldTiming,
    pub capture: HoldTiming,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CaptureStamp {
    pub sequence: u64,
    pub epoch: u64,
}

/// A process-local identity for the live mutation state. A cache seal records
/// this under the writer fence and may only be reused while it is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationStamp {
    pub epoch: u64,
    pub apply_revision: u64,
}

pub struct ApplyLease<'a> {
    barrier: &'a CaptureBarrier,
    _thread_affine: PhantomData<Rc<()>>,
}

pub struct CaptureLease<'a> {
    barrier: &'a CaptureBarrier,
    stamp: CaptureStamp,
    started: Instant,
    _thread_affine: PhantomData<Rc<()>>,
}

/// Keeps a validated capture epoch stable while durable pointer I/O runs.
/// Unlike [`CaptureLease`], this does not exclude ordinary apply.
pub struct PublicationPin<'a> {
    barrier: &'a CaptureBarrier,
}

/// Refuses captures while a durable restore owns the interval between
/// publishing CURRENT and activating the replacement. The guard has no
/// apply lease and is Send because it borrows a Sync CaptureBarrier.
pub struct RestoreInhibition<'a> {
    barrier: &'a CaptureBarrier,
}

impl CaptureBarrier {
    fn wait_for_publication_pins<'a>(
        &self,
        mut state: std::sync::MutexGuard<'a, State>,
    ) -> std::sync::MutexGuard<'a, State> {
        while state.publication_pins != 0 {
            #[cfg(test)]
            if let Some(observer) = &state.publication_wait_observer {
                observer
                    .send(())
                    .expect("publication wait observer dropped");
            }
            state = self.changed.wait(state).expect("capture barrier poisoned");
        }
        state
    }
    /// Read the replacement epoch without acquiring an apply or capture lease.
    /// Pending checkpoint ownership uses this only to discard a detached cut
    /// that a completed restore has already made impossible to publish.
    pub fn epoch(&self) -> u64 {
        self.state.lock().expect("capture barrier poisoned").epoch
    }

    /// Preparation observes this while holding the Engine state read lock.
    /// Recheck under apply and state write before publishing detached work.
    /// Capture completion also advances it: checkpoint binding may replace a
    /// reader view without changing the collection's document data version.
    pub fn apply_revision(&self) -> u64 {
        self.state
            .lock()
            .expect("capture barrier poisoned")
            .apply_revision
    }

    /// Read both cache-seal invalidation values while holding one state lock.
    /// Separate reads could combine an old epoch with a new apply revision.
    pub fn mutation_stamp(&self) -> MutationStamp {
        let state = self.state.lock().expect("capture barrier poisoned");
        MutationStamp {
            epoch: state.epoch,
            apply_revision: state.apply_revision,
        }
    }

    pub fn is_uncertain(&self) -> bool {
        self.state
            .lock()
            .expect("capture barrier poisoned")
            .uncertain
    }

    pub fn timing(&self) -> CaptureTiming {
        self.state.lock().expect("capture barrier poisoned").timing
    }

    pub fn apply(&self) -> ApplyLease<'_> {
        self.acquire_apply(false)
    }

    /// Acquire an apply interval. Only a [`RestoreInhibition`] may set
    /// `allow_restore_inhibition`: this prevents ordinary WAL apply from
    /// mutating old live state after durable `CURRENT` publication, while
    /// still letting the restoring task activate its replacement.
    fn acquire_apply(&self, allow_restore_inhibition: bool) -> ApplyLease<'_> {
        let owner = thread::current().id();
        let mut state = self.state.lock().expect("capture barrier poisoned");
        loop {
            if state.owner == Some(owner) {
                state.depth = state.depth.checked_add(1).expect("apply depth overflow");
                break;
            }
            if state.owner.is_none()
                && !state.capturing
                && state.waiting_captures == 0
                && (allow_restore_inhibition || state.restore_inhibitions == 0)
            {
                state.owner = Some(owner);
                state.depth = 1;
                state.apply_started = Some(Instant::now());
                break;
            }
            state = self.changed.wait(state).expect("capture barrier poisoned");
        }
        ApplyLease {
            barrier: self,
            _thread_affine: PhantomData,
        }
    }

    pub fn capture(&self, fallback: u64) -> Result<CaptureLease<'_>, &'static str> {
        let mut state = self.state.lock().expect("capture barrier poisoned");
        assert_ne!(
            state.owner,
            Some(thread::current().id()),
            "capture inside apply interval"
        );
        if state.restore_inhibitions != 0 {
            return Err("checkpoint refused: durable restore is in progress");
        }
        state.waiting_captures = state
            .waiting_captures
            .checked_add(1)
            .expect("capture queue overflow");
        self.changed.notify_all();
        while state.owner.is_some() || state.capturing {
            state = self.changed.wait(state).expect("capture barrier poisoned");
        }
        state.waiting_captures -= 1;
        // A restore can consume the apply lease that we were waiting behind.
        // Recheck before granting this capture so a queued caller cannot cross
        // its publication inhibition.
        if state.restore_inhibitions != 0 {
            self.changed.notify_all();
            return Err("checkpoint refused: durable restore is in progress");
        }
        if state.uncertain {
            self.changed.notify_all();
            return Err("checkpoint refused: durability is uncertain; restart required");
        }
        state.capturing = true;
        let stamp = CaptureStamp {
            sequence: state.sequence.unwrap_or(fallback),
            epoch: state.epoch,
        };
        Ok(CaptureLease {
            barrier: self,
            stamp,
            started: Instant::now(),
            _thread_affine: PhantomData,
        })
    }
}

impl<'a> ApplyLease<'a> {
    pub fn initialize_sequence(&self, sequence: u64) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        if let Some(old) = state.sequence {
            assert_eq!(old, sequence, "applied sequence initialization mismatch");
        }
        state.sequence = Some(sequence);
    }
    pub fn advance_sequence(&self, sequence: u64) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        assert!(
            state.sequence.is_none_or(|old| sequence >= old),
            "applied sequence regressed"
        );
        state.sequence = Some(sequence);
    }
    pub fn replace_epoch(&self) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        state = self.barrier.wait_for_publication_pins(state);
        state.epoch = state.epoch.checked_add(1).expect("engine epoch overflow");
        state.sequence = None;
    }
    pub fn mark_uncertain(&self) {
        self.barrier
            .state
            .lock()
            .expect("capture barrier poisoned")
            .uncertain = true;
    }
    /// Bump the engine epoch while preserving its applied sequence. No capture
    /// can be active because this consumes the apply lease; the returned guard
    /// then releases apply before durable I/O starts.
    pub fn inhibit_checkpoints_for_restore(self) -> RestoreInhibition<'a> {
        let barrier = self.barrier;
        {
            let mut state = barrier.state.lock().expect("capture barrier poisoned");
            state = barrier.wait_for_publication_pins(state);
            state.epoch = state.epoch.checked_add(1).expect("engine epoch overflow");
            state.restore_inhibitions = state
                .restore_inhibitions
                .checked_add(1)
                .expect("restore inhibition overflow");
        }
        drop(self);
        RestoreInhibition { barrier }
    }
}

impl<'a> RestoreInhibition<'a> {
    /// Take the short apply interval that installs the already-prepared
    /// replacement. This is the only path allowed through this inhibition.
    pub fn activation_apply(&self) -> ApplyLease<'_> {
        self.barrier.acquire_apply(true)
    }

    /// Latch uncertainty without acquiring ordinary apply, which is blocked
    /// until this restore inhibition drops.
    pub fn mark_uncertain(&self) {
        self.barrier
            .state
            .lock()
            .expect("capture barrier poisoned")
            .uncertain = true;
    }
}

impl Drop for ApplyLease<'_> {
    fn drop(&mut self) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        assert_eq!(state.owner, Some(thread::current().id()));
        if thread::panicking() {
            state.uncertain = true;
        }
        state.depth -= 1;
        if state.depth == 0 {
            state.apply_revision = state
                .apply_revision
                .checked_add(1)
                .expect("capture barrier preparation revision exhausted");
            let started = state.apply_started.take().expect("apply start missing");
            state.timing.apply.record(started);
            state.owner = None;
            self.barrier.changed.notify_all();
        }
    }
}

impl<'a> CaptureLease<'a> {
    pub fn stamp(&self) -> CaptureStamp {
        self.stamp
    }
    /// Check this capture against the original cut and retain only an epoch
    /// pin for durable publication. Drop this lease before filesystem I/O.
    pub fn publication_pin(
        &self,
        captured: CaptureStamp,
    ) -> Result<PublicationPin<'a>, &'static str> {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        if self.stamp.epoch != captured.epoch {
            return Err("checkpoint refused: engine was restored after capture");
        }
        state.publication_pins = state
            .publication_pins
            .checked_add(1)
            .ok_or("checkpoint publication pin overflow")?;
        Ok(PublicationPin {
            barrier: self.barrier,
        })
    }

    /// Check a short binding capture after durable publication. Later apply
    /// intervals may advance the cut, but only a restore invalidates it.
    pub fn validate_publish(&self, captured: CaptureStamp) -> Result<(), &'static str> {
        if self.stamp.epoch != captured.epoch {
            Err("checkpoint refused: engine was restored after capture")
        } else {
            Ok(())
        }
    }
}

impl Drop for RestoreInhibition<'_> {
    fn drop(&mut self) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        if thread::panicking() {
            state.uncertain = true;
        }
        state.restore_inhibitions = state
            .restore_inhibitions
            .checked_sub(1)
            .expect("restore inhibition underflow");
        self.barrier.changed.notify_all();
    }
}

impl Drop for CaptureLease<'_> {
    fn drop(&mut self) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        assert!(state.capturing);
        if thread::panicking() {
            state.uncertain = true;
        }
        state.timing.capture.record(self.started);
        state.apply_revision = state
            .apply_revision
            .checked_add(1)
            .expect("capture barrier preparation revision exhausted");
        state.capturing = false;
        self.barrier.changed.notify_all();
    }
}

impl Drop for PublicationPin<'_> {
    fn drop(&mut self) {
        let mut state = self.barrier.state.lock().expect("capture barrier poisoned");
        if thread::panicking() {
            state.uncertain = true;
        }
        state.publication_pins = state
            .publication_pins
            .checked_sub(1)
            .expect("checkpoint publication pin underflow");
        self.barrier.changed.notify_all();
    }
}

#[cfg(test)]
mod tests;
