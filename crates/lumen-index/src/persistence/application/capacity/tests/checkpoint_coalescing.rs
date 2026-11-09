use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use crate::persistence::application::capacity::tests::{sink, FailSync, HoldSync, Release};
use crate::persistence::application::capacity::worker::{relay_cycle, Owner};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

// These tests pin one owner's request revision. A process-shared budget
// lets unrelated parallel tests legitimately replace its global revision.
// Give only these new fixtures independent real accounting at the same limit.
fn engine() -> Arc<crate::index::application::engine::Engine> {
    use crate::index::application::engine::Engine;
    use crate::ingest::domain::change_budget::ChangeBudget;
    let engine = Arc::new(Engine::with_change_budget(ChangeBudget::new()));
    engine
        .create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({
                "fields": {"kw":{"type":"keyword"}}
            }))
            .unwrap(),
        )
        .unwrap();
    engine
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"one","field":"kw","value":"retained"}]
            }))
            .unwrap(),
        )
        .unwrap();
    engine
}

#[test]
fn capacity_request_during_publication_reuses_the_owner_checkpoint() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Release(Some(release_tx));
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            root.path(),
            Arc::new(HoldSync {
                entered: Mutex::new(Some(entered_tx)),
                release: Mutex::new(release_rx),
            }),
        )
        .unwrap(),
    );
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let fenced = store.as_ref().clone().with_publication_fence(owner.fence());
    let completed_before = engine.metrics().segment_checkpoint_completed_total.get();
    let publication = std::thread::spawn(move || sink.checkpoint_sync(&fenced));
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let revision = engine
        .request_pending_checkpoint_revision()
        .expect("the held frozen cut must own the capacity request");
    let endpoint = owner.endpoint();
    let relay_endpoint = endpoint.clone();
    let relay_engine = engine.clone();
    let relay =
        std::thread::spawn(move || relay_cycle(&relay_engine, &relay_endpoint, Some(revision)));
    let deadline = Instant::now() + Duration::from_secs(5);
    let queued = loop {
        if endpoint.requests.lock().unwrap().requested != 0 {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };
    // Always release and join both real operations before asserting. A failed
    // regression must not leave an IO hold or detached owner behind.
    drop(release);
    let published = publication.join().unwrap();
    let relieved = relay.join().unwrap();
    owner.join().unwrap();
    assert!(
        queued,
        "relay must queue while the existing SyncFile is held"
    );
    published.unwrap();
    relieved.unwrap();
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        completed_before + 1,
        "the relay must reuse the one publication already in progress"
    );
    assert!(engine
        .capacity_owner_state()
        .unwrap()
        .checkpoint_request_revision
        .is_none());
}

#[test]
fn completed_relief_does_not_hide_new_work_before_the_old_merge() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let fenced = store.as_ref().clone().with_publication_fence(owner.fence());
    let first = engine.request_pending_checkpoint_revision().unwrap();
    sink.checkpoint_sync(&fenced).unwrap();
    assert_eq!(
        engine
            .capacity_owner_state()
            .unwrap()
            .checkpoint_request_revision,
        Some(first),
        "publication must not consume the relay's outstanding merge request"
    );
    engine
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"two","field":"kw","value":"new work"}]
            }))
            .unwrap(),
        )
        .unwrap();
    let next = engine.request_pending_checkpoint_revision().unwrap();
    assert!(
        next > first,
        "a new refusal cannot reuse already served relief"
    );
    engine.consume_checkpoint_request(first);
    assert_eq!(
        engine
            .capacity_owner_state()
            .unwrap()
            .checkpoint_request_revision,
        Some(next),
        "late consumption of the old request must preserve its successor"
    );
    let before = engine.metrics().segment_checkpoint_completed_total.get();
    relay_cycle(&engine, &owner.endpoint(), Some(next)).unwrap();
    owner.join().unwrap();
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        before + 1
    );
    assert_eq!(
        store
            .load_latest()
            .unwrap()
            .unwrap()
            .0
            .stats("docs")
            .unwrap()
            .documents_indexed,
        2
    );
}

#[test]
fn failed_publication_cannot_mark_a_request_as_relieved() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            root.path(),
            Arc::new(FailSync(AtomicBool::new(true))),
        )
        .unwrap(),
    );
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let fence = owner.fence();
    let fenced = store.as_ref().clone().with_publication_fence(fence.clone());
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    assert!(sink.checkpoint_sync(&fenced).is_err());
    assert!(!fence.checkpoint_progress().has_relieved(revision));
    assert_eq!(engine.request_pending_checkpoint_revision(), Some(revision));
    assert!(store.load_latest().unwrap().is_none());
    sink.checkpoint_sync(&fenced).unwrap();
    assert!(fence.checkpoint_progress().has_relieved(revision));
    relay_cycle(&engine, &owner.endpoint(), Some(revision)).unwrap();
    owner.join().unwrap();
}

#[test]
fn replacement_owner_cannot_reuse_an_old_roots_relief() {
    let engine = engine();
    let old_root = tempfile::tempdir().unwrap();
    let old_store = Arc::new(SegmentRdbStore::new(old_root.path()).unwrap());
    let old_sink = sink(engine.clone(), old_store.clone());
    let mut old = Owner::start(old_sink.clone(), true).unwrap().unwrap();
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    old_sink
        .checkpoint_sync(
            &old_store
                .as_ref()
                .clone()
                .with_publication_fence(old.fence()),
        )
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut replacement = Owner::start(sink(engine.clone(), store.clone()), true)
        .unwrap()
        .unwrap();
    assert!(!replacement
        .fence()
        .checkpoint_progress()
        .has_relieved(revision));
    let before = engine.metrics().segment_checkpoint_completed_total.get();
    relay_cycle(&engine, &replacement.endpoint(), Some(revision)).unwrap();
    replacement.join().unwrap();
    old.join().unwrap();
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        before + 1
    );
    assert_eq!(
        store
            .load_latest()
            .unwrap()
            .unwrap()
            .0
            .stats("docs")
            .unwrap()
            .documents_indexed,
        1
    );
}

#[test]
fn relief_keeps_the_first_owner_work_boundary_for_the_request() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let work = engine.capacity_owner_state().unwrap().work_revision;
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    engine
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"later","field":"kw","value":"new work"}]
            }))
            .unwrap(),
        )
        .unwrap();
    let later = engine.capacity_owner_state().unwrap().work_revision;
    assert!(later > work);
    assert_eq!(engine.request_pending_checkpoint_revision(), Some(revision));
    let fence = owner.fence();
    sink.checkpoint_sync(&store.as_ref().clone().with_publication_fence(fence.clone()))
        .unwrap();
    assert_eq!(
        fence.checkpoint_progress().relieved_capacity(),
        Some((revision, Some(work))),
        "later work in the same pending request must retain its successor eligibility"
    );
    owner.join().unwrap();
}

#[test]
fn relief_records_the_owner_clock_when_request_revisions_run_ahead() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let work = engine.capacity_owner_state().unwrap().work_revision;
    let mut revision = engine.request_pending_checkpoint_revision().unwrap();
    for _ in 0..4 {
        engine.consume_checkpoint_request(revision);
        revision = engine.request_pending_checkpoint_revision().unwrap();
    }
    assert!(revision > work, "fixture must separate the two clocks");
    assert_eq!(engine.capacity_owner_state().unwrap().work_revision, work);
    let fence = owner.fence();
    sink.checkpoint_sync(&store.as_ref().clone().with_publication_fence(fence.clone()))
        .unwrap();
    assert_eq!(
        fence.checkpoint_progress().relieved_capacity(),
        Some((revision, Some(work)))
    );
    owner.join().unwrap();
}

#[test]
fn failed_publication_does_not_certify_a_work_boundary() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            root.path(),
            Arc::new(FailSync(AtomicBool::new(true))),
        )
        .unwrap(),
    );
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let work = engine.capacity_owner_state().unwrap().work_revision;
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    let fence = owner.fence();
    let fenced = store.as_ref().clone().with_publication_fence(fence.clone());
    assert!(sink.checkpoint_sync(&fenced).is_err());
    assert_eq!(fence.checkpoint_progress().relieved_capacity(), None);
    sink.checkpoint_sync(&fenced).unwrap();
    assert_eq!(
        fence.checkpoint_progress().relieved_capacity(),
        Some((revision, Some(work)))
    );
    owner.join().unwrap();
}

#[test]
fn published_owner_snapshot_does_not_move_with_later_applied_work() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    let expected_work = engine.capacity_owner_state().unwrap().work_revision;
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    let fence = owner.fence();
    sink.checkpoint_sync(&store.as_ref().clone().with_publication_fence(fence.clone()))
        .unwrap();
    let published = fence.checkpoint_progress().completed_owner_state().unwrap();
    assert_eq!(published.checkpoint_request_revision, Some(revision));
    assert_eq!(published.work_revision, expected_work);
    engine
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"after","field":"kw","value":"after publication"}]
            }))
            .unwrap(),
        )
        .unwrap();
    let live = engine.capacity_owner_state().unwrap();
    assert!(live.work_revision > published.work_revision);
    assert_ne!(live, published);
    assert_eq!(
        fence.checkpoint_progress().completed_owner_state(),
        Some(published),
        "async bookkeeping must see the actual publication boundary, not a later live snapshot"
    );
    owner.join().unwrap();
}

#[test]
fn failed_publication_has_no_completed_owner_snapshot() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            root.path(),
            Arc::new(FailSync(AtomicBool::new(true))),
        )
        .unwrap(),
    );
    let sink = sink(engine.clone(), store.clone());
    let mut owner = Owner::start(sink.clone(), true).unwrap().unwrap();
    engine.request_pending_checkpoint_revision().unwrap();
    let fence = owner.fence();
    let fenced = store.as_ref().clone().with_publication_fence(fence.clone());
    assert!(sink.checkpoint_sync(&fenced).is_err());
    assert_eq!(fence.checkpoint_progress().completed_owner_state(), None);
    sink.checkpoint_sync(&fenced).unwrap();
    assert!(fence
        .checkpoint_progress()
        .completed_owner_state()
        .is_some());
    owner.join().unwrap();
}
