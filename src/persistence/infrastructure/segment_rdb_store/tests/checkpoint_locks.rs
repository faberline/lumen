use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    index_kw, index_kw_in, kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use std::sync::{Arc, Mutex};
use storage_durable::FailureInjector;

#[test]
fn checkpoint_file_writes_do_not_hold_the_live_state_lock() {
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    engine.create_collection("idle", kw_schema()).unwrap();
    index_kw(&engine, "doc", "value");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer_engine = engine.clone();
    let writer = std::thread::spawn(move || {
        crate::index::infrastructure::checkpoint_fs::CHECKPOINT_WRITE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            }));
        });
        store.save(&writer_engine, 1).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (query_tx, query_rx) = mpsc::channel();
    let query = std::thread::spawn(move || {
        let request = serde_json::from_value(serde_json::json!({
            "query":{"term":{"field":"email","value":"absent"}}
        }))
        .unwrap();
        query_tx.send(engine.search("idle", request)).unwrap();
    });
    let during_write = query_rx.recv_timeout(Duration::from_millis(250));
    // Release and join before asserting, including on the expected red.
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    query.join().unwrap();
    assert!(
        during_write.is_ok(),
        "idle query blocked by checkpoint file I/O"
    );
    assert!(during_write.unwrap().is_ok());
}

#[test]
fn concurrent_first_base_keeps_an_attachable_overlay_for_the_next_checkpoint() {
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "base", "base");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer_store = store.clone();
    let writer_engine = engine.clone();
    let writer = std::thread::spawn(move || {
        crate::index::infrastructure::checkpoint_fs::CHECKPOINT_WRITE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            }));
        });
        writer_store.save(&writer_engine, 1).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    index_kw(&engine, "later", "later");
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    store.save(&engine, 2).unwrap();
    let request: crate::shared_kernel::types::search::SearchRequest =
        serde_json::from_value(serde_json::json!({
            "query":{"term":{"field":"email","value":"later"}}
        }))
        .unwrap();
    assert_eq!(engine.search("u", request.clone()).unwrap().total, 1);
    let (cold, _) = store.load_latest().unwrap().unwrap();
    assert_eq!(cold.search("u", request).unwrap().total, 1);
}

#[test]
fn first_base_publication_preserves_newer_field_update_and_deletion() {
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    let keep: crate::shared_kernel::types::schema::FieldSpec =
        serde_json::from_value(serde_json::json!({"type":"keyword"})).unwrap();
    engine.add_field("u", "keep", keep).unwrap();
    for id in ["deleted", "updated"] {
        index_kw(&engine, id, "old");
        engine.index("u", serde_json::from_value(serde_json::json!({"items":[{"external_id":id,"field":"keep","value":"present"}]})).unwrap()).unwrap();
    }
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer_store = store.clone();
    let writer_engine = engine.clone();
    let writer = std::thread::spawn(move || {
        crate::index::infrastructure::checkpoint_fs::CHECKPOINT_WRITE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            }));
        });
        writer_store.save(&writer_engine, 1).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    engine.delete("u", "deleted", Some("email")).unwrap();
    index_kw(&engine, "updated", "new");
    let search = |engine: &Engine, value: &str| {
        engine
            .search(
                "u",
                serde_json::from_value(
                    serde_json::json!({"query":{"term":{"field":"email","value":value}}}),
                )
                .unwrap(),
            )
            .unwrap()
            .total
    };
    assert_eq!(search(&engine, "old"), 0);
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    let uncached: crate::shared_kernel::types::search::SearchRequest = serde_json::from_value(
        serde_json::json!({"query":{"term":{"field":"email","value":"old"}},"limit":17}),
    )
    .unwrap();
    assert_eq!(
        engine.search("u", uncached).unwrap().total,
        0,
        "first base must not resurrect a newer update or field deletion"
    );
    assert_eq!(search(&engine, "new"), 1);
    store.save(&engine, 2).unwrap();
    let (cold, _) = store.load_latest().unwrap().unwrap();
    assert_eq!(search(&cold, "old"), 0);
    assert_eq!(search(&cold, "new"), 1);
}

#[test]
fn first_vector_base_publication_releases_only_acknowledged_payloads() {
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine
        .create_collection(
            "v",
            serde_json::from_value(serde_json::json!({"fields":{
                "vec":{"type":"vector","dim":2,"metric":"l2","backend":"flat-cpu"}
            }}))
            .unwrap(),
        )
        .unwrap();
    let index = |eid: &str, value: f32| {
        engine
            .index(
                "v",
                serde_json::from_value(serde_json::json!({
                    "items":[{"external_id":eid,"field":"vec","value":[value,value]}]
                }))
                .unwrap(),
            )
            .unwrap()
    };
    index("ack", 0.0);
    index("updated", 1.0);
    index("deleted", 2.0);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer_store = store.clone();
    let writer_engine = engine.clone();
    let writer = std::thread::spawn(move || {
        crate::index::infrastructure::checkpoint_fs::CHECKPOINT_WRITE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            }));
        });
        writer_store.save(&writer_engine, 1).unwrap();
    });
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    index("updated", 3.0);
    index("new", 4.0);
    engine.delete("v", "deleted", Some("vec")).unwrap();
    let logical_snapshot = |engine: &Engine| {
        let mut value = serde_json::to_value(engine.snapshot().unwrap()).unwrap();
        let field = &mut value["collections"]["v"]["fields"]["vec"];
        field.as_object_mut().unwrap().remove("bytes");
        field["vectors"]
            .as_array_mut()
            .unwrap()
            .sort_by(|a, b| a[0].as_str().cmp(&b[0].as_str()));
        value
    };
    let reference = logical_snapshot(&engine);
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(
        engine.segment_field_probe("v", "vec").unwrap().0,
        2,
        "first vector base publication must release acknowledged payloads while preserving newer writes"
    );
    assert_eq!(logical_snapshot(&engine), reference);
    store.save(&engine, 2).unwrap();
    assert_eq!(engine.segment_field_probe("v", "vec").unwrap().0, 0);
    let (cold, _) = store.load_latest().unwrap().unwrap();
    assert_eq!(logical_snapshot(&cold), reference);
    assert_eq!(cold.segment_field_probe("v", "vec").unwrap().0, 0);
}

#[test]
fn checkpoint_file_sync_does_not_hold_the_apply_barrier() {
    use std::sync::mpsc;
    use std::time::Duration;
    struct HoldSync(Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>);
    impl FailureInjector for HoldSync {
        fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
            if point.step == storage_durable::CommitStep::SyncFile {
                if let Some((entered, release)) = self.0.lock().unwrap().take() {
                    entered.send(()).unwrap();
                    release.recv_timeout(Duration::from_secs(3)).unwrap();
                }
            }
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(HoldSync(Mutex::new(Some((entered_tx, release_rx))))),
    )
    .unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    let saving = engine.clone();
    let save = std::thread::spawn(move || store.save(&saving, 1));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (written_tx, written_rx) = mpsc::channel();
    let applying = engine.clone();
    let write = std::thread::spawn(move || {
        index_kw(&applying, "during-sync", "new");
        written_tx.send(()).unwrap();
    });
    let during_sync = written_rx.recv_timeout(Duration::from_millis(250));
    release_tx.send(()).unwrap();
    save.join().unwrap().unwrap();
    write.join().unwrap();
    assert!(
        during_sync.is_ok(),
        "apply blocked by checkpoint file fsync"
    );
}

#[test]
fn flat_checkpoint_syncs_payload_and_generation_root_for_one_changed_collection() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountDirectorySync(Arc<AtomicUsize>);
    impl FailureInjector for CountDirectorySync {
        fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
            if point.step == storage_durable::CommitStep::SyncDirectory {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let syncs = Arc::new(AtomicUsize::new(0));
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(CountDirectorySync(syncs.clone())),
    )
    .unwrap();
    let engine = Arc::new(Engine::new());
    for collection in ["c0", "c1", "c2", "c3", "c4"] {
        engine.create_collection(collection, kw_schema()).unwrap();
    }
    index_kw_in(&engine, "c0", "c0", "first");
    store.save(&engine, 1).unwrap();
    syncs.store(0, Ordering::Relaxed);

    index_kw_in(&engine, "c0", "c0", "second");
    store.save(&engine, 2).unwrap();

    // v3 stores all durable payload files under one flat directory. The
    // commit still syncs that directory and the generation root, but it
    // no longer creates or syncs one directory per unchanged collection.
    assert!(
        syncs.load(Ordering::Relaxed) >= 2,
        "checkpoint must sync payload data and the generation root"
    );
}

#[test]
fn restore_during_checkpoint_sync_rejects_the_old_epoch_before_current() {
    use std::sync::mpsc;
    use std::time::Duration;
    struct HoldSync(Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>);
    impl FailureInjector for HoldSync {
        fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
            if point.step == storage_durable::CommitStep::SyncFile {
                if let Some((entered, release)) = self.0.lock().unwrap().take() {
                    entered.send(()).unwrap();
                    release.recv_timeout(Duration::from_secs(3)).unwrap();
                }
            }
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(HoldSync(Mutex::new(Some((entered_tx, release_rx))))),
    )
    .unwrap();
    let original = std::fs::read(dir.path().join("CURRENT")).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    let saving = engine.clone();
    let save = std::thread::spawn(move || store.save(&saving, 1));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (restored_tx, restored_rx) = mpsc::channel();
    let restoring = engine.clone();
    let restore = std::thread::spawn(move || {
        let replacement = Engine::new();
        replacement
            .create_collection("replacement", kw_schema())
            .unwrap();
        restoring.restore(replacement.snapshot().unwrap()).unwrap();
        restored_tx.send(()).unwrap();
    });
    let during_sync = restored_rx.recv_timeout(Duration::from_millis(250));
    release_tx.send(()).unwrap();
    let result = save.join().unwrap();
    restore.join().unwrap();
    assert!(during_sync.is_ok(), "restore blocked by file fsync");
    let error = result.expect_err("old engine epoch must not publish");
    assert!(format!("{error:#}").contains("restored after capture"));
    assert_eq!(std::fs::read(dir.path().join("CURRENT")).unwrap(), original);
    assert_eq!(engine.list_collections().unwrap(), vec!["replacement"]);
}
#[test]
fn checkpoint_validation_does_not_rebuild_collections() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    crate::index::domain::collection::segments::CHECKPOINT_COLLECTION_OPENS
        .with(|count| count.set(0));
    store.save_required(&engine, 51).unwrap();
    store.save_required(&engine, 52).unwrap();
    assert_eq!(
        crate::index::domain::collection::segments::CHECKPOINT_COLLECTION_OPENS
            .with(|count| count.get()),
        0,
        "checkpoint validation must inspect files without rebuilding an Engine"
    );
}
