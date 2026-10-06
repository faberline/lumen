use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::application::capacity::tests::{engine, sink, FailSync};
use crate::persistence::application::capacity::worker::Owner;
use crate::persistence::application::capacity::{Fallback, Work};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[test]
fn capacity_worker_retries_prepublication_failure_without_failing_waiter() {
    let engine = engine();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            dir.path(),
            Arc::new(FailSync(AtomicBool::new(true))),
        )
        .unwrap(),
    );
    let mut owner = Owner::start(sink(engine.clone(), store.clone()), true)
        .unwrap()
        .unwrap();
    let result = owner.endpoint.wait_for(Work::Checkpoint);
    owner.join().unwrap();
    result.expect("a pre-publication capacity failure must retry while the committed waiter retains ownership");
    assert_eq!(store.load_latest().unwrap().unwrap().1, 0);
    assert!(!engine.capture_barrier.is_uncertain());
}

#[test]
fn fallback_replaces_a_stopped_owner_slot() {
    let engine = engine();
    let mut fallback = None;
    Fallback::ensure(&mut fallback, &engine, None).unwrap();
    let old = engine.layer_maintenance.owner().unwrap();
    old.stop();
    assert!(engine.layer_maintenance.owner().is_none());

    Fallback::ensure(&mut fallback, &engine, None).unwrap();
    let replacement = engine.layer_maintenance.owner().unwrap();
    assert!(
        !Arc::ptr_eq(&old, &replacement) && !replacement.is_stopped(),
        "a stopped fallback slot must not suppress a replacement owner"
    );
}

#[test]
fn fallback_ensure_reuses_active_relay_for_one_capacity_request() {
    let engine = engine();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let mut fallback = None;
    Fallback::ensure(&mut fallback, &engine, Some(store)).unwrap();
    let endpoint = engine.layer_maintenance.owner().unwrap();

    Fallback::ensure(&mut fallback, &engine, None).unwrap();
    assert!(Arc::ptr_eq(
        &endpoint,
        &engine.layer_maintenance.owner().unwrap()
    ));

    engine.request_pending_checkpoint();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let request_cleared = engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision)
            .is_none();
        let requests = endpoint.requests.lock().unwrap();
        if request_cleared && requests.completed == 2 {
            break;
        }
        drop(requests);
        std::thread::sleep(Duration::from_millis(10));
    }

    let requests = endpoint.requests.lock().unwrap();
    assert_eq!(requests.requested, 2);
    assert_eq!(requests.completed, 2);
    assert_eq!(requests.operations, [Work::Checkpoint, Work::Merge]);
    drop(requests);
    assert!(engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .is_none());
}

#[test]
fn local_capacity_requests_schedule_two_checkpoint_merge_cycles() {
    let engine = engine();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let mut fallback = None;
    Fallback::ensure(&mut fallback, &engine, Some(store)).unwrap();
    let endpoint = engine.layer_maintenance.owner().unwrap();
    for id in ["one", "two"] {
        while engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision)
            .is_some()
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        engine
            .index(
                "docs",
                serde_json::from_value(serde_json::json!({
                    "items":[{"external_id":id,"field":"kw","value":"next"}]
                }))
                .unwrap(),
            )
            .unwrap();
        let state = engine.capacity_owner_state().unwrap();
        assert!(state.active != 0 || state.frozen != 0);
        let start = endpoint.requests.lock().unwrap().requested;
        engine.request_pending_checkpoint();
        let expected = start + 2;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            let requests = endpoint.requests.lock().unwrap();
            let request_cleared = engine
                .capacity_owner_state()
                .and_then(|state| state.checkpoint_request_revision)
                .is_none();
            if requests.requested >= expected && requests.completed >= expected && request_cleared {
                break;
            }
            drop(requests);
            std::thread::sleep(Duration::from_millis(10));
        }
        let requests = endpoint.requests.lock().unwrap();
        assert!(requests.requested >= expected && requests.completed >= expected);
        drop(requests);
        assert!(engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision)
            .is_none());
    }
    drop(fallback);
}

#[test]
fn resident_only_capacity_request_reaches_checkpoint_and_merge_relay() {
    let engine = engine();
    let raw = engine.raw_capacity_state_for_test().unwrap();
    assert_eq!(raw.active, 0);
    assert_eq!(raw.frozen_batches, 0);
    assert!(raw.resident_active > 0);
    assert_eq!(raw.resident_frozen, 0);
    let dir = tempfile::tempdir().unwrap();
    engine.request_pending_checkpoint();
    assert!(engine
        .capacity_owner_state()
        .unwrap()
        .checkpoint_request_revision
        .is_some());
    let mut fallback = None;
    Fallback::ensure(
        &mut fallback,
        &engine,
        Some(Arc::new(SegmentRdbStore::new(dir.path()).unwrap())),
    )
    .unwrap();
    let endpoint = engine.layer_maintenance.owner().unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let request = engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision);
        let requests = endpoint.requests.lock().unwrap();
        if request.is_none() && requests.requested >= 2 && requests.completed >= 2 {
            break;
        }
        drop(requests);
        std::thread::sleep(Duration::from_millis(10));
    }
    let requests = endpoint.requests.lock().unwrap();
    assert!(requests.requested >= 2 && requests.completed >= 2);
    drop(requests);
    assert!(engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .is_none());
    drop(fallback);
}

#[test]
fn relay_publishes_own_work_for_other_engine_waiter_without_empty_save() {
    const HARD: usize = 64 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine_a = Arc::new(Engine::with_change_budget(budget.clone()));
    engine_a
        .create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({
                "fields": {"kw":{"type":"keyword"}}
            }))
            .unwrap(),
        )
        .unwrap();
    engine_a
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"one","field":"kw","value":"retained"}]
            }))
            .unwrap(),
        )
        .unwrap();
    let engine_b = Arc::new(Engine::with_change_budget(budget.clone()));
    let mut fallback_a = None;
    let mut fallback_b = None;
    Fallback::ensure(&mut fallback_a, &engine_a, None).unwrap();
    Fallback::ensure(&mut fallback_b, &engine_b, None).unwrap();
    let used = budget.snapshot().total;
    assert!(used > 0 && used < HARD, "fixture needs publishable A work");
    let held = budget.owner().try_reserve(HARD - used).unwrap();
    // The budget is full before the request starts. A fast publication may
    // finish before this thread can observe the transient waiter counter.
    assert_eq!(budget.snapshot().total, HARD);
    let request = engine_b.record_ram_request_from_bound(1, 0);
    let (done_tx, done_rx) = mpsc::channel();
    let waiting_engine = engine_b.clone();
    std::thread::spawn(move || {
        done_tx
            .send(waiting_engine.wait_reserve_record_ram(&request).map(drop))
            .unwrap();
    });
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("Engine A fallback must publish work for Engine B waiter")
        .expect("Engine B admission must succeed after Engine A publication");
    assert!(
        engine_a.metrics().segment_checkpoint_completed_total.get() > 0,
        "Engine A owns the only publishable work"
    );
    assert_eq!(
        engine_b.metrics().segment_checkpoint_completed_total.get(),
        0,
        "empty Engine B must not checkpoint for its own capacity wait"
    );
    drop(held);
}

#[test]
fn merge_request_bootstraps_a_fresh_fallback_root_from_live_state() {
    let engine = engine();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let mut owner = Owner::start(sink(engine, store.clone()), false)
        .unwrap()
        .unwrap();
    owner.endpoint.wait_for(Work::Merge).unwrap();
    owner.join().unwrap();
    assert!(
        store.load_latest().unwrap().is_some(),
        "merge capacity on an empty fallback root must first publish the live engine"
    );
}

// Append this test to capacity/tests/capacity_requests.rs.
// Use its existing Engine, ChangeBudget, Arc, Owner, Fallback, Work,
// SegmentRdbStore, Duration, and sink imports.
#[test]
fn fallback_rebinds_relay_after_configured_owner_replacement() {
    use crate::persistence::application::capacity::Endpoint;
    use crate::ingest::domain::change_budget::HARD_LIMIT;
    use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
    use std::thread::{JoinHandle, ThreadId};
    use std::time::Instant;

    #[derive(Debug, Default)]
    struct Cleanup {
        errors: Vec<String>,
        owner_joins: Vec<&'static str>,
        relay_joins: Vec<ThreadId>,
    }

    #[derive(Default)]
    struct Workers {
        old_owner: Option<Owner>,
        configured_owner: Option<Owner>,
        fallback: Option<Fallback>,
        old_relay_worker: Option<JoinHandle<()>>,
    }

    impl Workers {
        fn finish(&mut self) -> Cleanup {
            let mut result = Cleanup::default();
            if let Some(owner) = self.old_owner.as_mut() {
                owner.stop();
            }
            if let Some(owner) = self.configured_owner.as_mut() {
                owner.stop();
            }
            if let Some(fallback) = self.fallback.as_mut() {
                if let Some(owner) = fallback.owner.as_mut() {
                    owner.stop();
                }
                if let Some(relay) = fallback.relay.as_ref() {
                    relay.stop();
                }
            }

            // Stop all endpoints before joining a relay that can wait on one.
            if let Some(owner) = self.old_owner.as_mut() {
                match owner.join() {
                    Ok(()) => result.owner_joins.push("old owner"),
                    Err(error) => result.errors.push(format!("old owner: {error:#}")),
                }
            }
            if let Some(owner) = self.configured_owner.as_mut() {
                match owner.join() {
                    Ok(()) => result.owner_joins.push("configured owner"),
                    Err(error) => result.errors.push(format!("configured owner: {error:#}")),
                }
            }
            if let Some(fallback) = self.fallback.as_mut() {
                if let Some(owner) = fallback.owner.as_mut() {
                    match owner.join() {
                        Ok(()) => result.owner_joins.push("fallback owner"),
                        Err(error) => result.errors.push(format!("fallback owner: {error:#}")),
                    }
                }
            }

            if let Some(worker) = self.old_relay_worker.take() {
                let id = worker.thread().id();
                match worker.join() {
                    Ok(()) => result.relay_joins.push(id),
                    Err(_) => result.errors.push(format!("old relay {id:?} panicked")),
                }
            }
            if let Some(fallback) = self.fallback.as_mut() {
                if let Some(relay) = fallback.relay.as_mut() {
                    if let Some(worker) = relay.take_worker_for_test() {
                        let id = worker.thread().id();
                        match worker.join() {
                            Ok(()) => result.relay_joins.push(id),
                            Err(_) => result.errors.push(format!("retained relay {id:?} panicked")),
                        }
                    }
                }
            }

            let _ = self.fallback.take();
            let _ = self.old_owner.take();
            let _ = self.configured_owner.take();
            result
        }
    }

    impl Drop for Workers {
        fn drop(&mut self) {
            let cleanup = self.finish();
            if !cleanup.errors.is_empty() {
                eprintln!("capacity relay replacement drop cleanup: {cleanup:?}");
            }
        }
    }

    #[derive(Debug)]
    struct Observation {
        registry_address: usize,
        old_endpoint_address: usize,
        configured_endpoint_address: usize,
        old_token: u64,
        configured_token: u64,
        old_relay_id: ThreadId,
        old_owner_joined_before_request: bool,
        old_relay_joined_before_request: bool,
        old_stopped: bool,
        configured_is_current: bool,
        configured_registration: bool,
        stopped: bool,
        work_revision: u64,
        request_revision: u64,
        pending_at_creation: Option<u64>,
        pending_before_cleanup: Option<u64>,
        requested: u64,
        completed: u64,
        checkpoint_flag: bool,
        merge_flag: bool,
        operations: Vec<Work>,
        error: Option<String>,
        checkpoint_completions: u64,
        successful_cycles: u64,
        has_capacity_waiters: bool,
        progress_seen_before_deadline: bool,
    }

    // Declared first. These roots outlive stores, Engine, worker joins,
    // and the final assertions on both success and failure.
    let old_root = tempfile::tempdir().expect("old fixture root");
    let configured_root = tempfile::tempdir().expect("configured fixture root");
    assert_eq!(HARD_LIMIT, 268_435_456);
    let budget = ChangeBudget::with_hard_limit(HARD_LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let old_store = Arc::new(SegmentRdbStore::new(old_root.path()).expect("old fixture store"));
    let configured_store = Arc::new(
        SegmentRdbStore::new(configured_root.path()).expect("configured fixture store"),
    );
    let mut workers = Workers::default();

    // Returned setup errors and panics reach the final assertions only
    // after explicit cleanup. Drop is a second cleanup path.
    let run = catch_unwind(AssertUnwindSafe(|| -> anyhow::Result<Observation> {
        engine.create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({
                "fields": {"kw": {"type": "keyword"}}
            }))?,
        )?;
        engine.index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items": [{"external_id": "one", "field": "kw", "value": "retained"}]
            }))?,
        )?;
        let work = engine
            .capacity_owner_state()
            .ok_or_else(|| anyhow::anyhow!("fixture has no capacity owner state"))?;
        anyhow::ensure!(
            work.active != 0 || work.frozen != 0,
            "fixture must have nonzero local publishable work"
        );
        anyhow::ensure!(
            work.checkpoint_request_revision.is_none()
                && budget.snapshot().checkpoint_request_revision.is_none()
                && budget.snapshot().total < crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER
                && !engine.has_capacity_waiters(),
            "fixture must have no request, waiter, or trigger-sized work"
        );
        anyhow::ensure!(
            engine.metrics().segment_checkpoint_completed_total.get() == 0
                && engine.metrics().segment_capacity_relief_checkpoint_merge_seconds_count.get() == 0
                && engine.layer_maintenance.owner().is_none(),
            "fixture must have no earlier maintenance"
        );

        // Retain A externally. A relay spawn error cannot detach its owner.
        workers.old_owner = Owner::start(sink(engine.clone(), old_store.clone()), false)?;
        let old_endpoint: Arc<Endpoint> = workers
            .old_owner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("fixture failed to create owner A"))?
            .endpoint();
        Fallback::ensure(&mut workers.fallback, &engine, None)?;
        let old_relay_worker = {
            let fallback = workers
                .fallback
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("fallback A is absent"))?;
            anyhow::ensure!(
                fallback.owner.is_none(),
                "fallback A must attach to the externally retained owner"
            );
            fallback
                .relay
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("relay A is absent"))?
                .take_worker_for_test()
                .ok_or_else(|| anyhow::anyhow!("relay A has no native handle"))?
        };
        let old_relay_id = old_relay_worker.thread().id();
        workers.old_relay_worker = Some(old_relay_worker);
        let old_token = {
            let registration = engine.layer_maintenance.registration.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            anyhow::ensure!(
                !registration.configured
                    && registration.endpoint.upgrade().is_some_and(|current| Arc::ptr_eq(&current, &old_endpoint)),
                "registry A must be the unconfigured live owner"
            );
            registration.token
        };
        {
            let state = old_endpoint.requests.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            anyhow::ensure!(
                !state.stopped && state.requested == 0 && state.completed == 0
                    && state.operations.is_empty() && state.error.is_none()
                    && !state.checkpoint && !state.merge,
                "owner A must be idle before replacement"
            );
        }

        workers.configured_owner =
            Owner::start(sink(engine.clone(), configured_store.clone()), true)?;
        let configured_endpoint: Arc<Endpoint> = workers
            .configured_owner
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("fixture failed to create configured owner B"))?
            .endpoint();
        let configured_token = {
            let registration = engine.layer_maintenance.registration.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            anyhow::ensure!(
                registration.configured
                    && registration.endpoint.upgrade().is_some_and(|current| Arc::ptr_eq(&current, &configured_endpoint)),
                "registry must hold configured owner B"
            );
            registration.token
        };
        anyhow::ensure!(
            !Arc::ptr_eq(&old_endpoint, &configured_endpoint)
                && old_endpoint.is_stopped()
                && !configured_endpoint.is_stopped()
                && configured_token == old_token + 1,
            "configured owner B must replace and stop owner A"
        );

        workers.old_owner.as_mut()
            .ok_or_else(|| anyhow::anyhow!("owner A handle was lost"))?
            .join()?;
        workers.old_relay_worker.take()
            .ok_or_else(|| anyhow::anyhow!("relay A handle was lost"))?
            .join()
            .map_err(|_| anyhow::anyhow!("relay A panicked before the request"))?;
        anyhow::ensure!(
            engine.capacity_owner_state().is_some_and(|state| state.checkpoint_request_revision.is_none())
                && !engine.has_capacity_waiters(),
            "old joins must precede the only controlled request"
        );

        // This is the original populated slot. A's relay has exited and
        // has been joined. Neither join changed the registry's live B.
        Fallback::ensure(&mut workers.fallback, &engine, None)?;
        anyhow::ensure!(
            engine.layer_maintenance.owner().is_some_and(|current| Arc::ptr_eq(&current, &configured_endpoint))
                && !configured_endpoint.is_stopped()
                && workers.fallback.as_ref().is_some_and(|fallback| fallback.owner.is_none() && fallback.relay.is_some()),
            "ensure must preserve configured B and retain a fallback relay"
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = configured_endpoint.requests.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        anyhow::ensure!(
            !state.stopped && state.requested == 0 && state.completed == 0
                && state.operations.is_empty() && state.error.is_none()
                && !state.checkpoint && !state.merge,
            "owner B must be idle before the controlled request"
        );
        // Holding B.requests prevents the relay's first wait_for call.
        // The returned budget revision is not a WAL sequence.
        let request_revision = engine.request_pending_checkpoint_revision()
            .ok_or_else(|| anyhow::anyhow!("local work did not create request r"))?;
        let pending_at_creation = engine.capacity_owner_state()
            .ok_or_else(|| anyhow::anyhow!("owner state was lost when creating r"))?
            .checkpoint_request_revision;
        anyhow::ensure!(
            pending_at_creation == Some(request_revision),
            "r must be pending before B can receive the first operation"
        );
        while state.completed < 2 && !state.stopped && state.error.is_none() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            state = configured_endpoint.changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        let endpoint_cycle_completed =
            state.requested == 2 && state.completed == 2 && state.error.is_none();
        drop(state);

        // Request consumption notifies BudgetWake, not B.changed.
        // Sample its epoch before reading the predicate to avoid a lost wake.
        let wake = engine.checkpoint_wake();
        let (pending_before_cleanup, progress_seen_before_deadline) = loop {
            let observed = wake.epoch();
            let pending = engine.capacity_owner_state()
                .ok_or_else(|| anyhow::anyhow!("owner state was lost during observation"))?
                .checkpoint_request_revision;
            let now = Instant::now();
            if pending.is_none() || now >= deadline {
                break (pending, endpoint_cycle_completed && pending.is_none() && now < deadline);
            }
            let _ = wake.wait_for_change_timeout(
                observed,
                deadline.saturating_duration_since(now),
            );
        };
        let (requested, completed, checkpoint_flag, merge_flag, stopped, operations, error) = {
            let state = configured_endpoint.requests.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                state.requested, state.completed, state.checkpoint, state.merge,
                state.stopped, state.operations.clone(), state.error.clone(),
            )
        };
        let (configured_is_current, configured_registration) = {
            let registration = engine.layer_maintenance.registration.lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (
                registration.token == configured_token
                    && registration.endpoint.upgrade().is_some_and(|current| Arc::ptr_eq(&current, &configured_endpoint)),
                registration.configured,
            )
        };
        Ok(Observation {
            registry_address: Arc::as_ptr(&engine.layer_maintenance) as usize,
            old_endpoint_address: Arc::as_ptr(&old_endpoint) as usize,
            configured_endpoint_address: Arc::as_ptr(&configured_endpoint) as usize,
            old_token,
            configured_token,
            old_relay_id,
            old_owner_joined_before_request: true,
            old_relay_joined_before_request: true,
            old_stopped: old_endpoint.is_stopped(),
            configured_is_current,
            configured_registration,
            stopped,
            work_revision: work.work_revision,
            request_revision,
            pending_at_creation,
            pending_before_cleanup,
            requested,
            completed,
            checkpoint_flag,
            merge_flag,
            operations,
            error,
            checkpoint_completions: engine.metrics().segment_checkpoint_completed_total.get(),
            successful_cycles: engine.metrics().segment_capacity_relief_checkpoint_merge_seconds_count.get(),
            has_capacity_waiters: engine.has_capacity_waiters(),
            progress_seen_before_deadline,
        })
    }));

    let cleanup = workers.finish();
    let cleanup_complete = workers.old_owner.is_none()
        && workers.configured_owner.is_none()
        && workers.fallback.is_none()
        && workers.old_relay_worker.is_none();
    let observed = match run {
        Err(payload) => {
            eprintln!("fixture panic after cleanup: {cleanup:?}; complete={cleanup_complete}");
            resume_unwind(payload);
        }
        Ok(Err(error)) => panic!(
            "fixture/control setup error after cleanup: {error:#}; cleanup={cleanup:?}; complete={cleanup_complete}"
        ),
        Ok(Ok(observed)) => observed,
    };
    eprintln!(
        "capacity relay replacement observation={observed:?}; cleanup={cleanup:?}; complete={cleanup_complete}"
    );
    assert!(cleanup_complete && cleanup.errors.is_empty(), "cleanup failed: {cleanup:?}");
    assert!(
        observed.old_owner_joined_before_request && observed.old_relay_joined_before_request
            && observed.old_stopped && observed.configured_is_current
            && observed.configured_registration && !observed.stopped
            && !observed.has_capacity_waiters
            && observed.pending_at_creation == Some(observed.request_revision),
        "replacement control failed before cleanup: {observed:?}"
    );
    assert!(
        observed.requested == 2 && observed.completed == 2
            && observed.operations == [Work::Checkpoint, Work::Merge]
            && !observed.checkpoint_flag && !observed.merge_flag && observed.error.is_none()
            && observed.pending_before_cleanup.is_none()
            && observed.checkpoint_completions == 1 && observed.successful_cycles == 1
            && observed.progress_seen_before_deadline,
        "relay-progress oracle: B must complete exactly one Checkpoint then Merge cycle and consume r after success; {observed:?}"
    );
}
