//! Capacity maintenance has its own native worker. Committed apply retains its
//! source and reservation while waiting outside every Engine/apply lock.
//! A weak registry avoids an Engine -> checkpoint sink -> Engine ownership cycle.

pub(crate) mod relay_trace;
pub(crate) mod worker;

use crate::persistence::application::capacity::worker::{BudgetRelay, Owner};
use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

#[path = "../infrastructure/segment_save_gate.rs"]
mod publication_gate;

#[derive(Default)]
pub(crate) struct Registry {
    publication: Arc<crate::persistence::application::capacity::publication_gate::SaveGate>,
    registration: Mutex<Registration>,
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
}

#[derive(Debug, thiserror::Error)]
#[error("checkpoint maintenance owner was superseded before CURRENT publication")]
pub(crate) struct Superseded;

pub(crate) struct PublicationPermit {
    _permit: crate::persistence::application::capacity::publication_gate::SavePermit,
}

impl PublicationFence {
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
        crate::persistence::infrastructure::composed_segment::MAX_INCREMENTAL_LAYERS
            - usize::from(self.frozen_windows.load(Ordering::Acquire) != 0)
    }
}

/// Caller-owned lazy fallback. Constructor and file IO run before apply; its
/// EngineWatermarkSink does not retain a coordinator or Raft state machine.
pub(crate) struct Fallback {
    owner: Option<Owner>,
    relay: Option<BudgetRelay>,
    _store: Option<Arc<SegmentRdbStore>>,
}

impl Fallback {
    pub(crate) fn ensure(
        slot: &mut Option<Self>,
        engine: &Arc<Engine>,
        configured: Option<Arc<SegmentRdbStore>>,
    ) -> Result<()> {
        if slot.is_some() && engine.layer_maintenance.owner().is_some() {
            // The caller already owns the relay for this live endpoint.  Do
            // not replace the slot: dropping its old fallback stops that
            // owner while its capacity waiters are still pending.
            return Ok(());
        }
        // A prior fallback can remain after its owner stops. Drop it before
        // creating the replacement so its stopped relay cannot outlive it.
        let _ = slot.take();
        if let Some(endpoint) = engine.layer_maintenance.owner() {
            // A configured bootstrap owner may already exist before replay
            // creates its caller-owned fallback.  Keep the owner as the sole
            // checkpoint writer, but attach the native relay to that same
            // endpoint so capacity waits can submit work to it.
            let relay = BudgetRelay::start(engine.clone(), endpoint)?;
            *slot = Some(Self {
                owner: None,
                relay: Some(relay),
                _store: configured,
            });
            return Ok(());
        }
        let is_configured = configured.is_some();
        let store = match configured {
            Some(store) => store,
            None => crate::persistence::infrastructure::spill_directory::temporary_spill_store()?,
        };
        let sink = Arc::new(SegmentCheckpointSink {
            engine: engine.clone(),
            store: store.clone(),
            writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
            aof: None,
        });
        let owner = Owner::start(sink, is_configured)?;
        let relay = owner
            .as_ref()
            .map(|owner| BudgetRelay::start(engine.clone(), owner.endpoint()))
            .transpose()?;
        *slot = Some(Self {
            owner,
            relay,
            _store: Some(store),
        });
        Ok(())
    }
}

impl Drop for Fallback {
    fn drop(&mut self) {
        if let Some(relay) = &self.relay {
            relay.stop();
        }
        if let Some(owner) = &mut self.owner {
            owner.stop();
        }
    }
}

#[cfg(test)]
mod tests;
