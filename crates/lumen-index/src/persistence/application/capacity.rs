//! Capacity maintenance has its own native worker. Committed apply retains its
//! source and reservation while waiting outside every Engine/apply lock.
//! A weak registry avoids an Engine -> checkpoint sink -> Engine ownership cycle.

pub mod relay_trace;
pub mod worker;

use crate::index::application::engine::Engine;
use crate::persistence::application::capacity::worker::{BudgetRelay, Owner};
use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

use crate::persistence::infrastructure::segment_save_gate as publication_gate;

#[derive(Default)]
pub(crate) struct Registry {
    publication: Arc<crate::persistence::application::capacity::publication_gate::SaveGate>,
    registration: Mutex<Registration>,
    checkpoint_progress: Mutex<Weak<CheckpointProgress>>,
    frozen_windows: AtomicUsize,
}

#[derive(Default)]
struct Registration {
    token: u64,
    endpoint: Weak<Endpoint>,
    configured: bool,
}

/// At most one extra catalog delta may replace a captured scalar prefix. While
/// any cut is frozen, leave a slot for that delta. The marker is installed under
/// capture and lives with frozen ownership, including a failed save's retry.
pub(crate) struct FrozenWindow(Arc<Registry>);

impl Drop for FrozenWindow {
    fn drop(&mut self) {
        self.0.frozen_windows.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub(crate) struct PublicationFence {
    registry: Weak<Registry>,
    token: u64,
    checkpoints: Arc<CheckpointProgress>,
}

/// Periodic and native relief share progress only within one publication
/// owner. A replacement owner gets fresh progress, even for the same Engine.
pub(crate) struct CheckpointProgress {
    execution: Mutex<()>,
    state: Mutex<CheckpointProgressState>,
    active: AtomicBool,
}

#[derive(Default)]
struct CheckpointProgressState {
    relieved: Option<u64>,
    relieved_work: Option<u64>,
    requested: Option<(u64, u64)>,
    published_owner: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
    published_request_work: Option<u64>,
    completed_owner: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
}

impl Default for CheckpointProgress {
    fn default() -> Self {
        Self {
            execution: Mutex::new(()),
            state: Mutex::default(),
            active: AtomicBool::new(true),
        }
    }
}

impl CheckpointProgress {
    pub(crate) fn lock_execution(&self) -> std::sync::MutexGuard<'_, ()> {
        self.execution.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn request_revision(
        &self,
        request: impl FnOnce(Option<u64>) -> Option<(u64, u64)>,
    ) -> Option<u64> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let (revision, work) = request(state.relieved)?;
        // Keep the first demand boundary for this episode. Work arriving
        // after that refusal still earns the normal publication successor.
        if state.requested.map(|(pending, _)| pending) != Some(revision) {
            state.requested = Some((revision, work));
        }
        Some(revision)
    }

    pub(crate) fn has_relieved(&self, revision: u64) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .relieved
            .is_some_and(|completed| completed >= revision)
    }

    pub(crate) fn relieved_capacity(&self) -> Option<(u64, Option<u64>)> {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .relieved
            .map(|revision| (revision, state.relieved_work))
    }

    pub(crate) fn completed_owner_state(
        &self,
    ) -> Option<crate::ingest::domain::change_budget::OwnerCapacityState> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .completed_owner
    }

    pub(crate) fn record_publication(
        &self,
        acknowledge: impl FnOnce() -> Result<
            Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
        >,
    ) -> Result<()> {
        // The caller still owns the capture barrier. Holding this state lock
        // across acknowledgement keeps a newly woken refusal out of the
        // publication it has not received. No apply/IO waits enter here.
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let owner = acknowledge()?;
        state.published_owner = owner;
        state.published_request_work = owner
            .and_then(|owner| owner.checkpoint_request_revision)
            .and_then(|revision| {
                state
                    .requested
                    .filter(|(pending, _)| *pending == revision)
                    .map(|(_, work)| work)
            });
        Ok(())
    }

    pub(crate) fn completed(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        // Capture before diagnostic callbacks and before the async driver can
        // resume. Later applied writes do not belong to this completed IO.
        let owner = state.published_owner;
        state.completed_owner = owner;
        let Some(revision) = owner.and_then(|owner| owner.checkpoint_request_revision) else {
            return false;
        };
        if state
            .relieved
            .is_some_and(|completed| completed >= revision)
        {
            return false;
        }
        state.relieved = Some(revision);
        state.relieved_work = state.published_request_work;
        true
    }
}

#[derive(Debug, thiserror::Error)]
#[error("checkpoint maintenance owner was superseded before CURRENT publication")]
pub(crate) struct Superseded;

pub(crate) struct PublicationPermit {
    _permit: crate::persistence::application::capacity::publication_gate::SavePermit,
}

impl PublicationFence {
    pub(crate) fn checkpoint_progress(&self) -> Arc<CheckpointProgress> {
        self.checkpoints.clone()
    }

    pub(crate) fn acquire(&self) -> Result<PublicationPermit> {
        let registry = self.registry.upgrade().ok_or(Superseded)?;
        let permit = registry.publication.lock_owned();
        let current = registry
            .registration
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .token;
        if current != self.token {
            return Err(Superseded.into());
        }
        Ok(PublicationPermit { _permit: permit })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Work {
    Checkpoint,
    Merge,
}

#[derive(Default)]
struct Requests {
    requested: u64,
    completed: u64,
    checkpoint: bool,
    merge: bool,
    stopped: bool,
    error: Option<String>,
    #[cfg(test)]
    operations: Vec<Work>,
}

pub(crate) struct Endpoint {
    requests: Mutex<Requests>,
    changed: Condvar,
    fence: PublicationFence,
}

impl Endpoint {
    fn stop(&self) {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopped = true;
        self.fence
            .checkpoints
            .active
            .store(false, Ordering::Release);
        self.changed.notify_all();
    }

    fn is_stopped(&self) -> bool {
        self.requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopped
    }

    /// `Ok` means recheck the live view. Another owner may have replaced this
    /// endpoint; no record result or watermark is manufactured here.
    pub(crate) fn wait_for(&self, work: Work) -> Result<()> {
        let mut state = self.requests.lock().unwrap_or_else(|p| p.into_inner());
        if state.stopped {
            return Ok(());
        }
        state.requested = state
            .requested
            .checked_add(1)
            .ok_or_else(|| anyhow!("capacity work revision overflow"))?;
        let revision = state.requested;
        match work {
            Work::Checkpoint => state.checkpoint = true,
            Work::Merge => state.merge = true,
        }
        self.changed.notify_all();
        while !state.stopped && state.completed < revision {
            state = self.changed.wait(state).unwrap_or_else(|p| p.into_inner());
        }
        if !state.stopped {
            if let Some(error) = &state.error {
                return Err(anyhow!(error.clone()));
            }
        }
        Ok(())
    }
}

impl Registry {
    pub(crate) fn checkpoint_progress(&self) -> Option<Arc<CheckpointProgress>> {
        // Admission can publish a request while a caller holds endpoint.requests.
        // Do not acquire either that mutex or registration from this path.
        let progress = self
            .checkpoint_progress
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .upgrade()?;
        progress.active.load(Ordering::Acquire).then_some(progress)
    }

    pub(crate) fn owner(&self) -> Option<Arc<Endpoint>> {
        let owner = self
            .registration
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .endpoint
            .upgrade()?;
        let stopped = owner
            .requests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stopped;
        (!stopped).then_some(owner)
    }

    pub(crate) fn freeze(self: &Arc<Self>) -> FrozenWindow {
        self.frozen_windows.fetch_add(1, Ordering::AcqRel);
        FrozenWindow(self.clone())
    }

    pub(crate) fn append_limit(&self) -> usize {
        crate::storage::infrastructure::composed_segment::MAX_INCREMENTAL_LAYERS
            - usize::from(self.frozen_windows.load(Ordering::Acquire) != 0)
    }
}

/// Caller-owned lazy fallback. Constructor and file IO run before apply; its
/// EngineWatermarkSink does not retain a coordinator or Raft state machine.
#[derive(Default)]
pub struct Fallback {
    owner: Option<Owner>,
    relay: Option<BudgetRelay>,
    endpoint: Option<Arc<Endpoint>>,
    /// The caller's configured store. Only `with_store` sets it, so it decides
    /// `configured` for every owner this fallback starts.
    configured: Option<Arc<SegmentRdbStore>>,
    /// The store the current owner writes to. A temporary spill store lives
    /// here and never becomes `configured`.
    _store: Option<Arc<SegmentRdbStore>>,
}

impl Fallback {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_store(configured: Option<Arc<SegmentRdbStore>>) -> Self {
        Self {
            owner: None,
            relay: None,
            endpoint: None,
            configured,
            _store: None,
        }
    }

    /// Slot form used by callers that pass the configured store on each call.
    /// A slot that is still bound to the live endpoint is kept. Otherwise the
    /// slot is replaced, as before, so `configured` decides the next owner.
    pub(crate) fn ensure(
        slot: &mut Option<Self>,
        engine: &Arc<Engine>,
        configured: Option<Arc<SegmentRdbStore>>,
    ) -> Result<()> {
        use crate::ingest::application::ports::CapacityOwner;
        let bound_live = slot
            .as_ref()
            .is_some_and(|fallback| fallback.is_bound_to_live(engine));
        if !bound_live {
            *slot = Some(Self::with_store(configured));
        }
        slot.as_mut().unwrap().ensure(engine)
    }

    fn is_bound_to_live(&self, engine: &Arc<Engine>) -> bool {
        self.endpoint
            .as_ref()
            .zip(engine.layer_maintenance.owner().as_ref())
            .is_some_and(|(bound, live)| Arc::ptr_eq(bound, live))
    }

    pub fn stop(&mut self) {
        use crate::ingest::application::ports::CapacityOwner;
        CapacityOwner::stop(self);
    }
}

impl crate::ingest::application::ports::CapacityOwner for Fallback {
    fn ensure(&mut self, engine: &Arc<Engine>) -> Result<()> {
        let live_endpoint = engine.layer_maintenance.owner();
        if self
            .endpoint
            .as_ref()
            .zip(live_endpoint.as_ref())
            .is_some_and(|(bound, live)| Arc::ptr_eq(bound, live))
        {
            // The caller already owns the relay for this live endpoint. Do
            // not replace the slot: dropping its old fallback stops that
            // owner while its capacity waiters are still pending.
            return Ok(());
        }
        // A prior fallback can remain after its owner stops. Drop it before
        // creating the replacement so its stopped relay cannot outlive it.
        self.stop();
        if let Some(endpoint) = engine.layer_maintenance.owner() {
            // A configured bootstrap owner may already exist before replay
            // creates its caller-owned fallback. Keep the owner as the sole
            // checkpoint writer, but attach the native relay to that same
            // endpoint so capacity waits can submit work to it.
            let relay = BudgetRelay::start(engine.clone(), endpoint.clone())?;
            self.owner = None;
            self.relay = Some(relay);
            self.endpoint = Some(endpoint);
            return Ok(());
        }
        let is_configured = self.configured.is_some();
        let store = match &self.configured {
            Some(store) => store.clone(),
            None => crate::persistence::infrastructure::spill_directory::temporary_spill_store()?,
        };
        let sink = Arc::new(SegmentCheckpointSink {
            engine: engine.clone(),
            store: store.clone(),
            writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
            aof: None,
        });
        let owner = Owner::start(sink, is_configured)?;
        let endpoint = owner.as_ref().map(Owner::endpoint);
        let relay = endpoint
            .as_ref()
            .map(|endpoint| BudgetRelay::start(engine.clone(), endpoint.clone()))
            .transpose()?;
        self.owner = owner;
        self.relay = relay;
        self.endpoint = endpoint;
        self._store = Some(store);
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.owner.is_some() || self.relay.is_some()
    }

    fn stop(&mut self) {
        if let Some(relay) = &self.relay {
            relay.stop();
        }
        self.relay = None;
        if let Some(owner) = &mut self.owner {
            owner.stop();
        }
        self.owner = None;
    }
}

impl Drop for Fallback {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests;
