use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::application::capacity::tests::{engine, sink, FailSync};
use crate::persistence::application::capacity::worker::Owner;
use crate::persistence::application::capacity::{Fallback, Work};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;

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
