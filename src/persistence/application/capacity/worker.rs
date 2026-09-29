//! The capacity worker: the checkpoint owner's native thread, which runs the
//! checkpoint and merge work requested on its endpoint, and the budget relay,
//! which turns change-budget capacity waits into those requests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};

use crate::persistence::application::capacity::relay_trace::{
    relay_diagnostic_enabled, RelayTrace,
};
use crate::persistence::application::capacity::{Endpoint, PublicationFence, Requests, Work};
use crate::segment_checkpoint::SegmentCheckpointSink;
use crate::storage::Engine;

/// Held by the caller, never by Engine. The worker holds its endpoint and sink
/// only until stop and any already started operation finish.
pub(crate) struct Owner {
    pub(super) endpoint: Arc<Endpoint>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Owner {
    pub(crate) fn start(
        sink: Arc<SegmentCheckpointSink>,
        configured: bool,
    ) -> Result<Option<Self>> {
        let registry = &sink.engine.layer_maintenance;
        let _publication = registry.publication.lock_owned();
        let mut registration = registry
            .registration
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(active) = registration.endpoint.upgrade() {
            if !active
                .requests
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .stopped
                && !configured
            {
                return Ok(None);
            }
        }
        let token = registration
            .token
            .checked_add(1)
            .ok_or_else(|| anyhow!("capacity owner revision overflow"))?;
        let fence = PublicationFence {
            registry: Arc::downgrade(registry),
            token,
        };
        let endpoint = Arc::new(Endpoint {
            requests: Mutex::new(Requests::default()),
            changed: Condvar::new(),
            fence: fence.clone(),
        });
        let worker_endpoint = endpoint.clone();
        let worker_sink = sink.clone();
        let worker = std::thread::Builder::new()
            .name("lumen-layer-capacity".into())
            .spawn(move || run(worker_sink, worker_endpoint))?;
        if let Some(old) = registration.endpoint.upgrade() {
            old.stop();
        }
        registration.token = token;
        registration.endpoint = Arc::downgrade(&endpoint);
        registration.configured = configured;
        Ok(Some(Self {
            endpoint,
            worker: Some(worker),
        }))
    }

    pub(crate) fn fence(&self) -> PublicationFence {
        self.endpoint.fence.clone()
    }

    pub(super) fn endpoint(&self) -> Arc<Endpoint> {
        self.endpoint.clone()
    }

    pub(crate) fn stop(&mut self) {
        self.endpoint.stop();
    }

    pub(crate) fn join(&mut self) -> Result<()> {
        self.stop();
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("capacity worker panicked"))?;
        }
        Ok(())
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        // Never block an async executor on held file IO. The native worker owns
        // the sink/root until completion. Explicit shutdown may join it.
        self.stop();
    }
}

fn run(sink: Arc<SegmentCheckpointSink>, endpoint: Arc<Endpoint>) {
    loop {
        let (revision, checkpoint, merge) = {
            let mut state = endpoint.requests.lock().unwrap_or_else(|p| p.into_inner());
            while !state.stopped && state.completed == state.requested {
                state = endpoint
                    .changed
                    .wait(state)
                    .unwrap_or_else(|p| p.into_inner());
            }
            if state.stopped {
                return;
            }
            let work = (state.requested, state.checkpoint, state.merge);
            state.checkpoint = false;
            state.merge = false;
            work
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            #[cfg(test)]
            {
                let mut state = endpoint.requests.lock().unwrap_or_else(|p| p.into_inner());
                if checkpoint {
                    state.operations.push(Work::Checkpoint);
                }
                if merge {
                    state.operations.push(Work::Merge);
                }
            }
            let store = sink
                .store
                .as_ref()
                .clone()
                .with_publication_fence(endpoint.fence.clone());
            if checkpoint || (merge && !store.has_current_generation()?) {
                sink.checkpoint_sync(&store)?;
            }
            if merge {
                let revision = store.request_capacity_merge(&sink.engine)?;
                store.wait_for_capacity_merge_progress(revision, Duration::from_secs(60))?;
            }
            Ok(())
        }))
        .unwrap_or_else(|_| {
            sink.engine.capture_barrier.apply().mark_uncertain();
            Err(anyhow!(
                "capacity worker panicked; committed source remains retained"
            ))
        });
        let superseded = endpoint.fence.acquire().is_err();
        let retry = result.is_err() && !superseded && !sink.engine.capture_barrier.is_uncertain();
        let mut state = endpoint.requests.lock().unwrap_or_else(|p| p.into_inner());
        if retry && !state.stopped {
            // A failed pre-publication save still owns the frozen cut. Keep the
            // request outstanding and the apply waiter asleep while this same
            // independent worker retries. New requests coalesce into its flags.
            state.checkpoint |= checkpoint;
            state.merge |= merge;
            tracing::warn!(error = %result.as_ref().unwrap_err(), "capacity maintenance will retry with retained committed input");
            let _ = endpoint
                .changed
                .wait_timeout(state, Duration::from_secs(1))
                .unwrap_or_else(|p| p.into_inner());
            continue;
        }
        state.completed = revision;
        state.error = result.as_ref().err().map(|error| format!("{error:#}"));
        // A superseded owner can return without a record error. Its waiters
        // re-read the registry and submit to the replacement owner.
        if superseded {
            state.stopped = true;
        }
        endpoint.changed.notify_all();
        if state.stopped {
            return;
        }
    }
}

/// A fallback needs no Tokio runtime. This native relay only requests the
/// existing `Owner` worker; `run` remains the sole checkpoint/save path.
pub(super) struct BudgetRelay {
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl BudgetRelay {
    pub(super) fn start(engine: Arc<Engine>, endpoint: Arc<Endpoint>) -> Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let wake = engine.checkpoint_wake();
        let worker = std::thread::Builder::new()
            .name("lumen-capacity-relay".into())
            .spawn(move || run_budget_relay(engine, wake, endpoint, worker_stop))?;
        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }

    pub(super) fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

impl Drop for BudgetRelay {
    fn drop(&mut self) {
        // Do not join on an async caller's drop path. Fallback stops the
        // endpoint too, so a relay waiting for owner work wakes and exits.
        self.stop();
        let _ = self.worker.take();
    }
}

fn run_budget_relay(
    engine: Arc<Engine>,
    wake: Arc<crate::ingest::domain::change_budget::BudgetWake>,
    endpoint: Arc<Endpoint>,
    stop: Arc<AtomicBool>,
) {
    let mut observed = wake.epoch();
    let mut last_requested_revision = None;
    loop {
        if stop.load(Ordering::Acquire) || endpoint.is_stopped() {
            return;
        }
        // Work belongs to `engine`, while waiter demand is process-global.
        // A live Engine with publishable work can relieve another Engine's
        // waiter, but an empty fallback never writes for that waiter.
        let Some(state) = engine.capacity_owner_state() else {
            return;
        };
        let demand_revision = state
            .checkpoint_request_revision
            .or_else(|| engine.has_capacity_waiters().then_some(state.work_revision));
        if demand_revision.is_some()
            && (state.active != 0
                || state.frozen != 0
                // Explicit requests may be backed only by resident bytes.
                || state.checkpoint_request_revision.is_some())
            && last_requested_revision != demand_revision
        {
            last_requested_revision = demand_revision;
            // `wait_for` blocks this independent native thread until the one
            // existing owner completes, retries, or is superseded. No Engine,
            // accounting, or apply lock is held here.
            if relay_cycle(&engine, &endpoint, state.checkpoint_request_revision).is_err() {
                return;
            }
            continue;
        }
        // Sample after checking state. A change between the check and this
        // sample is handled immediately; a later change is observed by the
        // BudgetWake predicate before the Condvar can sleep.
        let current = wake.epoch();
        if current != observed {
            observed = current;
            continue;
        }
        let _ = wake.wait_for_change_timeout(observed, Duration::from_millis(50));
        observed = wake.epoch();
    }
}

pub(super) fn relay_cycle(
    engine: &Engine,
    endpoint: &Endpoint,
    request_revision: Option<u64>,
) -> Result<()> {
    // A normal or qualifying relay pays only this opt-in check. It does not
    // sample metrics, time work, or emit a diagnostic record.
    let mut trace = relay_diagnostic_enabled().then(|| {
        let trace = RelayTrace::start(engine, request_revision);
        trace.phase(engine, "relay_started");
        trace
    });

    let checkpoint_started = trace.as_ref().map(|_| Instant::now());
    // Start immediately before the first blocking maintenance operation. The
    // interval excludes optional diagnostic setup and ends after the merge.
    let checkpoint_merge_started = Instant::now();
    if let Err(error) = endpoint.wait_for(Work::Checkpoint) {
        if let Some(trace) = trace.as_mut() {
            trace.checkpoint_ns = checkpoint_started
                .map(|started| u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            trace.checkpoint_result = "error";
        }
        if let Some(trace) = trace {
            trace.terminal(engine, "checkpoint_error", Some(&error));
        }
        return Err(error);
    }
    if let Some(trace) = trace.as_mut() {
        trace.checkpoint_ns = checkpoint_started
            .map(|started| u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        trace.checkpoint_result = "ok";
        trace.phase(engine, "checkpoint_completed");
    }

    // A local capacity refusal needs both publication and compaction. The
    // checkpoint frees only the frozen layer; without the merge, repeated
    // refusals can fill the segment budget again before the next request.
    let merge_started = trace.as_ref().map(|_| Instant::now());
    if let Err(error) = endpoint.wait_for(Work::Merge) {
        if let Some(trace) = trace.as_mut() {
            trace.merge_ns = merge_started
                .map(|started| u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            trace.merge_result = "error";
        }
        if let Some(trace) = trace {
            trace.terminal(engine, "merge_error", Some(&error));
        }
        return Err(error);
    }
    if let Some(trace) = trace.as_mut() {
        trace.merge_ns = merge_started
            .map(|started| u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        trace.merge_result = "ok";
        trace.phase(engine, "merge_completed");
    }
    engine
        .metrics()
        .observe_capacity_relief_checkpoint_merge_cycle(checkpoint_merge_started.elapsed());

    if let Some(request_revision) = request_revision {
        let consume_started = trace.as_ref().map(|_| Instant::now());
        engine.consume_checkpoint_request(request_revision);
        if let Some(trace) = trace.as_mut() {
            trace.consume_ns = consume_started
                .map(|started| u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            trace.consume_result = match engine
                .capacity_owner_state()
                .and_then(|state| state.checkpoint_request_revision)
            {
                None => "consumed",
                Some(current) if current == request_revision => "unchanged",
                Some(_) => "stale",
            };
        }
    }
    if let Some(trace) = trace {
        trace.terminal(engine, "complete", None);
    }
    Ok(())
}
