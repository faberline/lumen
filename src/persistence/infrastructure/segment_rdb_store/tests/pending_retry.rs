use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::CheckpointDiagnosticContext;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::records::parse_revision_name;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    current_generation, has_keyword, index_kw, kw_schema, DiagnosticEnvironment,
    DiagnosticTraceWriter,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use storage_durable::FailureInjector;

struct FailInheritedPayload {
    relative_path: PathBuf,
    hits: std::sync::atomic::AtomicUsize,
}

impl FailureInjector for FailInheritedPayload {
    fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
        if point.step == storage_durable::CommitStep::SyncFile
            && point.relative_path == self.relative_path
        {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(std::io::Error::other("retained payload must be synced"));
        }
        Ok(())
    }
}

#[test]
fn required_save_syncs_a_retained_payload_that_ordinary_save_skips() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "first", "value");
    let initial = SegmentRdbStore::new(dir.path()).unwrap();
    initial.save(&engine, 1).unwrap();
    let initial_name = current_generation(&initial);
    let manifest = read_generation_manifest(&dir.path().join(initial_name.as_str())).unwrap();
    let retained = PathBuf::from(manifest.collections[0].segments[0].path.clone());
    let injector = Arc::new(FailInheritedPayload {
        relative_path: retained,
        hits: std::sync::atomic::AtomicUsize::new(0),
    });
    let store = SegmentRdbStore::new_with_failure_injector(dir.path(), injector.clone()).unwrap();

    index_kw(&engine, "second", "value-two");
    store.save(&engine, 2).unwrap();
    assert_ne!(current_generation(&store), initial_name);
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        2
    );
    assert_eq!(
        injector.hits.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "ordinary current-derived save must skip a registered retained payload"
    );
    assert!(
        store.save_required(&engine, 3).is_err(),
        "restore/import save must keep generic full-file durability"
    );
    assert_eq!(
        injector.hits.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "required generic save must sync the retained payload"
    );
}

struct FailOnce(Mutex<Option<storage_durable::CommitStep>>);

impl FailureInjector for FailOnce {
    fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
        let mut armed = self.0.lock().unwrap();
        if *armed == Some(point.step) {
            *armed = None;
            return Err(std::io::Error::other("one pending-checkpoint failure"));
        }
        Ok(())
    }
}

fn pending_retry_preserves_cut_after_failure(step: storage_durable::CommitStep) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "doc", "old");
    SegmentRdbStore::new(dir.path())
        .unwrap()
        .save(&engine, 1)
        .unwrap();

    index_kw(&engine, "doc", "captured");
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(FailOnce(Mutex::new(Some(step)))),
    )
    .unwrap();
    assert!(store.save(&engine, 2).is_err());
    index_kw(&engine, "doc", "newer");
    store.save(&engine, 2).unwrap();
    let (replayed, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 2);
    assert!(has_keyword(&replayed, "captured"));
    assert!(!has_keyword(&replayed, "newer"));

    store.save(&engine, 3).unwrap();
    let (later, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 3);
    assert!(has_keyword(&later, "newer"));
}

#[test]
fn pending_frozen_syncfile_retry_keeps_captured_payload() {
    pending_retry_preserves_cut_after_failure(storage_durable::CommitStep::SyncFile);
}

#[test]
fn pending_frozen_renamecurrent_retry_keeps_captured_payload() {
    pending_retry_preserves_cut_after_failure(storage_durable::CommitStep::RenameCurrent);
}

#[test]
fn pending_retry_then_requested_newer_cut_publishes_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "doc", "old");
    SegmentRdbStore::new(dir.path())
        .unwrap()
        .save(&engine, 1)
        .unwrap();
    index_kw(&engine, "doc", "captured");
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(FailOnce(Mutex::new(Some(
            storage_durable::CommitStep::SyncFile,
        )))),
    )
    .unwrap();
    assert!(store.save(&engine, 2).is_err());
    let retained = store
        .pending_frozen_identity()
        .expect("actual frozen payload retained");
    index_kw(&engine, "doc", "newer");
    assert_eq!(store.save_with_sequence(&engine, 3).unwrap(), 3);
    assert!(store.pending_frozen_identity().is_none());
    assert!(store
        .generation_entries()
        .unwrap()
        .iter()
        .any(|(name, _)| parse_revision_name(name.as_str()).is_some_and(|(seq, _)| seq == 2)));
    assert_ne!(retained, 0);
    let (latest, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 3);
    assert!(has_keyword(&latest, "newer"));
}

#[test]
fn diagnostic_reused_cut_then_fresh_successor_uses_ordered_passes() {
    use tracing_subscriber::prelude::*;

    let _environment = DiagnosticEnvironment::set(true);
    let writer = DiagnosticTraceWriter::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(writer.clone()),
    );
    let _guard = tracing::subscriber::set_default(subscriber);
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "doc", "old");
    SegmentRdbStore::new(dir.path())
        .unwrap()
        .save(&engine, 1)
        .unwrap();
    index_kw(&engine, "doc", "captured");
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(FailOnce(Mutex::new(Some(
            storage_durable::CommitStep::SyncFile,
        )))),
    )
    .unwrap();
    assert!(store.save(&engine, 2).is_err());
    index_kw(&engine, "doc", "newer");
    let context = CheckpointDiagnosticContext::new("periodic", Some(81));
    assert_eq!(
        store
            .save_with_sequence_diagnostic_context(&engine, 3, Some(context))
            .unwrap(),
        3
    );
    drop(_guard);

    let phases: Vec<_> = writer
        .records()
        .into_iter()
        .filter(|record| record["fields"]["event"] == "segment_checkpoint_diagnostic_phase")
        .filter(|record| {
            matches!(
                record["fields"]["phase"].as_str(),
                Some("freeze_completed" | "publish_completed")
            )
        })
        .collect();
    assert_eq!(
        phases
            .iter()
            .map(|record| record["fields"]["phase"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "freeze_completed",
            "publish_completed",
            "freeze_completed",
            "publish_completed",
        ]
    );
    assert_eq!(phases[0]["fields"]["checkpoint_pass"], 1);
    assert_eq!(phases[0]["fields"]["frozen_cut_reused"], true);
    assert_eq!(phases[0]["fields"]["frozen_cut_bytes"], 0);
    assert_eq!(phases[1]["fields"]["checkpoint_pass"], 1);
    assert_eq!(phases[2]["fields"]["checkpoint_pass"], 2);
    assert_eq!(phases[2]["fields"]["frozen_cut_reused"], false);
    assert!(phases[2]["fields"]["frozen_cut_bytes"].as_u64().is_some());
    assert_eq!(phases[3]["fields"]["checkpoint_pass"], 2);
}

#[test]
fn restored_epoch_discards_old_pending_before_foreign_engine_can_save() {
    let dir = tempfile::tempdir().unwrap();
    let old = Arc::new(Engine::new());
    old.create_collection("u", kw_schema()).unwrap();
    index_kw(&old, "doc", "old");
    SegmentRdbStore::new(dir.path())
        .unwrap()
        .save(&old, 1)
        .unwrap();
    index_kw(&old, "doc", "captured");
    let store = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(FailOnce(Mutex::new(Some(
            storage_durable::CommitStep::SyncFile,
        )))),
    )
    .unwrap();
    assert!(store.save(&old, 2).is_err());
    let retained = store
        .pending_frozen_identity()
        .expect("pending frozen payload");

    let replacement = Engine::new();
    replacement.create_collection("u", kw_schema()).unwrap();
    index_kw(&replacement, "doc", "restored");
    old.restore(replacement.snapshot().unwrap()).unwrap();
    let foreign = Arc::new(Engine::new());
    foreign.create_collection("u", kw_schema()).unwrap();
    index_kw(&foreign, "doc", "foreign");
    store.save(&foreign, 3).unwrap();
    assert!(store.pending_frozen_identity().is_none());
    assert_ne!(retained, 0);
    let (latest, _) = store.load_latest().unwrap().unwrap();
    assert!(has_keyword(&latest, "foreign"));
    assert!(!has_keyword(&latest, "captured"));
}
