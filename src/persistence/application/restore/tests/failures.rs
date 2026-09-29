use std::io;
use std::sync::Arc;

use storage_durable::CommitStep;

use crate::api::RestoreSink;
use crate::ingest::application::write_coordinator::errors::{RestartRequired, StorageFullError};
use crate::persistence::application::restore::tests::{
    assert_collections, current_bytes, engine_with, replacement_snapshot, setup, FailAt, WATERMARK,
};
use crate::persistence::application::restore::{
    RestoreNotCommitted, RestoreUnavailable, SegmentRestoreSink, UnavailableRestoreSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::SnapshotV1;

#[tokio::test]
async fn invalid_snapshot_does_not_wait_on_gate_or_change_state() {
    let fixture = setup(None);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store.clone(),
        fixture.writer,
        fixture.aof,
    )
    .unwrap();
    let gate = sink.gate.clone();
    let _held = gate.shared().await.unwrap();
    let invalid = SnapshotV1 {
        version: 999,
        collections: Default::default(),
    };

    tokio::time::timeout(std::time::Duration::from_secs(1), sink.restore(invalid))
        .await
        .expect("invalid snapshot must fail before waiting for exclusive gate")
        .expect_err("invalid snapshot must fail");
    assert_eq!(current_bytes(fixture.dir.path()), fixture.old_current);
    assert_collections(fixture.live.as_ref(), &["old"]);
    assert!(!sink.gate.is_restart_required());
}

#[tokio::test]
async fn precommit_preserves_old_current_and_live_state() {
    let injector = Arc::new(FailAt::default());
    let fixture = setup(Some(injector.clone()));
    injector.arm(CommitStep::WriteCurrentTemp, io::ErrorKind::Other);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store,
        fixture.writer,
        fixture.aof,
    )
    .unwrap();

    let err = sink.restore(replacement_snapshot()).await.unwrap_err();
    assert!(err
        .chain()
        .any(|e| e.downcast_ref::<RestoreNotCommitted>().is_some()));
    assert_eq!(current_bytes(fixture.dir.path()), fixture.old_current);
    assert_collections(fixture.live.as_ref(), &["old"]);
    assert!(!sink.gate.is_restart_required());
    assert!(!fixture.live.metrics().is_storage_degraded());
}

#[tokio::test]
async fn precommit_storage_full_is_degraded_without_moving_current() {
    let injector = Arc::new(FailAt::default());
    let fixture = setup(Some(injector.clone()));
    injector.arm(CommitStep::WriteCurrentTemp, io::ErrorKind::StorageFull);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store,
        fixture.writer,
        fixture.aof,
    )
    .unwrap();

    let err = sink.restore(replacement_snapshot()).await.unwrap_err();
    assert!(err
        .chain()
        .any(|e| e.downcast_ref::<StorageFullError>().is_some()));
    assert_eq!(current_bytes(fixture.dir.path()), fixture.old_current);
    assert_collections(fixture.live.as_ref(), &["old"]);
    assert!(!sink.gate.is_restart_required());
    assert!(fixture.live.metrics().is_storage_degraded());
}

#[tokio::test]
async fn commit_uncertain_latches_and_fresh_store_follows_new_current() {
    let injector = Arc::new(FailAt::default());
    let fixture = setup(Some(injector.clone()));
    injector.arm(CommitStep::SyncRootAfterCurrent, io::ErrorKind::Other);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store,
        fixture.writer,
        fixture.aof,
    )
    .unwrap();

    let err = sink.restore(replacement_snapshot()).await.unwrap_err();
    assert!(err
        .chain()
        .any(|e| e.downcast_ref::<RestartRequired>().is_some()));
    assert!(sink.gate.is_restart_required());
    assert_collections(fixture.live.as_ref(), &["old"]);

    let fresh = SegmentRdbStore::new(fixture.dir.path()).unwrap();
    let loaded = fresh
        .load_current_generation()
        .unwrap()
        .expect("new CURRENT after uncertain final sync");
    assert_eq!(loaded.sequence, WATERMARK);
    assert_ne!(loaded.name, fixture.old_name);
    assert_collections(loaded.engine.as_ref(), &["restored"]);
}

#[tokio::test]
async fn reload_integrity_failure_latches_and_preserves_old_live_state() {
    let fixture = setup(None);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store,
        fixture.writer,
        fixture.aof,
    )
    .unwrap();
    sink.force_reload_integrity_mismatch();
    let err = sink.restore(replacement_snapshot()).await.unwrap_err();
    assert!(err
        .chain()
        .any(|e| e.downcast_ref::<RestartRequired>().is_some()));
    assert!(sink.gate.is_restart_required());
    assert_collections(fixture.live.as_ref(), &["old"]);

    let fresh = SegmentRdbStore::new(fixture.dir.path()).unwrap();
    let loaded = fresh.load_current_generation().unwrap().unwrap();
    assert_collections(loaded.engine.as_ref(), &["restored"]);
}

#[tokio::test]
async fn save_task_panic_latches_and_preserves_old_live_state() {
    let injector = Arc::new(FailAt::default());
    let fixture = setup(Some(injector.clone()));
    injector.panic_at(CommitStep::WriteCurrentTemp);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store,
        fixture.writer,
        fixture.aof,
    )
    .unwrap();

    let err = sink.restore(replacement_snapshot()).await.unwrap_err();
    assert!(err
        .chain()
        .any(|e| e.downcast_ref::<RestartRequired>().is_some()));
    assert!(sink.gate.is_restart_required());
    assert_eq!(current_bytes(fixture.dir.path()), fixture.old_current);
    assert_collections(fixture.live.as_ref(), &["old"]);
}

#[tokio::test]
async fn activation_failure_latches_with_new_current_and_old_live_state() {
    let fixture = setup(None);
    let sink = SegmentRestoreSink::new(
        fixture.live.clone(),
        fixture.store,
        fixture.writer,
        fixture.aof,
    )
    .unwrap();
    sink.force_activation_failure();
    let err = sink.restore(replacement_snapshot()).await.unwrap_err();
    assert!(err
        .chain()
        .any(|e| e.downcast_ref::<RestartRequired>().is_some()));
    assert!(sink.gate.is_restart_required());
    assert_collections(fixture.live.as_ref(), &["old"]);

    let fresh = SegmentRdbStore::new(fixture.dir.path()).unwrap();
    let loaded = fresh.load_current_generation().unwrap().unwrap();
    assert_collections(loaded.engine.as_ref(), &["restored"]);
}

#[tokio::test]
async fn unavailable_sink_returns_typed_error_without_mutating_live_state() {
    let live = engine_with("old");
    let sink = UnavailableRestoreSink::new("non-embedded topology is unsupported");

    let error = sink
        .restore(replacement_snapshot())
        .await
        .expect_err("unavailable restore must fail closed");
    assert!(error.downcast_ref::<RestoreUnavailable>().is_some());
    assert_collections(live.as_ref(), &["old"]);
}
