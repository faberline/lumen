use crate::persistence::infrastructure::segment_rdb_store::startup::SegmentStartupDecision;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    index_kw, install_unpointed_generation, kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use std::sync::{Arc, Mutex};
use storage_durable::{FailureInjector, GenerationName};

#[test]
fn save_then_load_round_trips_at_seq() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();

    let src = Arc::new(Engine::new());
    src.create_collection("u", kw_schema()).unwrap();
    index_kw(&src, "u1", "a@x.com");
    store.save(&src, 42).unwrap();

    let (eng, seq) = store.load_latest().unwrap().expect("a checkpoint");
    assert_eq!(seq, 42);
    assert_eq!(eng.stats("u").unwrap().documents_indexed, 1);
}

#[test]
fn reopen_replaces_the_complete_previous_collection_set() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    store.save(&source, 10).unwrap();
    let target = Arc::new(Engine::new());
    target.create_collection("obsolete", kw_schema()).unwrap();
    assert_eq!(store.reopen_into(&target).unwrap(), Some(10));
    assert_eq!(target.list_collections().unwrap(), vec!["u"]);
}

#[test]
fn adopts_exact_0428_generation_once_and_writes_exact_current() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42");
    std::fs::create_dir(&legacy).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(!dir.path().join("CURRENT").exists());
    let loaded = Arc::new(Engine::new());
    let outcome = store.reopen_into_with_outcome(&loaded).unwrap();
    assert_eq!(outcome.decision, SegmentStartupDecision::AdoptedLegacy0428);
    assert_eq!(outcome.checkpoint_sequence, Some(42));
    assert_eq!(
        outcome.generation.as_ref().map(GenerationName::as_str),
        Some("gen-42")
    );
    assert_eq!(loaded.stats("u").unwrap().documents_indexed, 1);
    assert_eq!(
        std::fs::read(dir.path().join("CURRENT")).unwrap(),
        b"generation:gen-42\n"
    );

    let restarted = SegmentRdbStore::new(dir.path()).unwrap();
    assert_eq!(restarted.load_latest().unwrap().unwrap().1, 42);
}

#[test]
fn adopts_empty_0428_generation_without_losing_sequence() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("gen-42")).unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let (_, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 42);
    assert_eq!(
        std::fs::read(dir.path().join("CURRENT")).unwrap(),
        b"generation:gen-42\n"
    );
}

#[test]
fn corrupt_highest_legacy_generation_blocks_fallback_and_adoption() {
    let dir = tempfile::tempdir().unwrap();
    let valid = dir.path().join("gen-42");
    std::fs::create_dir(&valid).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&valid, 42).unwrap();
    let corrupt = dir.path().join("gen-99");
    std::fs::create_dir(&corrupt).unwrap();
    std::fs::write(corrupt.join("not-a-collection"), b"corrupt").unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(store.load_latest().is_err());
    assert!(!dir.path().join("CURRENT").exists());
}

#[cfg(unix)]
#[test]
fn legacy_nested_symlink_blocks_adoption() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42");
    std::fs::create_dir(&legacy).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside.path(), b"outside").unwrap();
    let collection = std::fs::read_dir(&legacy)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.is_dir())
        .unwrap();
    symlink(outside.path(), collection.join("nested-link")).unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(store.load_latest().is_err());
    assert!(!dir.path().join("CURRENT").exists());
}

#[cfg(unix)]
#[test]
fn parseable_legacy_aside_symlink_blocks_empty_initialization() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    symlink(target.path(), dir.path().join("gen-7.old")).unwrap();

    assert!(SegmentRdbStore::new(dir.path()).is_err());
    assert!(!dir.path().join("CURRENT").exists());
}

#[test]
fn empty_new_generation_preserves_sequence_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    store.save(&Arc::new(Engine::new()), 42).unwrap();

    let restarted = SegmentRdbStore::new(dir.path()).unwrap();
    let (_, sequence) = restarted.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 42);
}

#[test]
fn lower_sequence_save_is_a_noop() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 9).unwrap();
    let current_before = std::fs::read(dir.path().join("CURRENT")).unwrap();

    index_kw(&engine, "u2", "b@x.com");
    store.save(&engine, 8).unwrap();

    assert_eq!(
        std::fs::read(dir.path().join("CURRENT")).unwrap(),
        current_before
    );
    let (loaded, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 9);
    assert_eq!(loaded.stats("u").unwrap().documents_indexed, 1);
}

#[test]
fn required_lower_sequence_rejects_without_changing_current() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    store.save(&engine, 9).unwrap();
    let before = std::fs::read(dir.path().join("CURRENT")).unwrap();

    let error = store.save_required(&engine, 8).unwrap_err();
    assert!(error.to_string().contains("below CURRENT sequence 9"));
    assert_eq!(std::fs::read(dir.path().join("CURRENT")).unwrap(), before);
}

#[test]
fn required_same_sequence_creates_distinct_revision_and_exact_loader_matches() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    let first = store.save_required(&engine, 9).unwrap();
    index_kw(&engine, "u2", "b@x.com");
    let second = store.save_required(&engine, 9).unwrap();
    assert_ne!(first, second);

    let loaded = store.load_current_generation().unwrap().unwrap();
    assert_eq!(loaded.name, second);
    assert_eq!(loaded.sequence, 9);
    assert_eq!(loaded.engine.stats("u").unwrap().documents_indexed, 2);
}

#[test]
fn exact_loader_never_selects_unpointed_higher_generation() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    let current = store.save_required(&engine, 9).unwrap();
    let unpointed = install_unpointed_generation(&store, &engine, 99, Some(&current));

    let loaded = store.load_current_generation().unwrap().unwrap();
    assert_eq!(loaded.name, current);
    assert_eq!(loaded.sequence, 9);
    assert_ne!(loaded.name, unpointed);
}

#[test]
fn exact_loader_never_adopts_a_legacy_generation_when_current_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42");
    std::fs::create_dir(&legacy).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(!dir.path().join("CURRENT").exists());
    assert!(store.load_current_generation().is_err());
    assert!(
        !dir.path().join("CURRENT").exists(),
        "exact restore reload must not perform the 0.4.28 startup adoption"
    );
}

#[test]
fn injected_store_commit_is_deterministic() {
    #[derive(Default)]
    struct FailRenameCurrent(Mutex<Vec<storage_durable::FailurePoint>>);

    impl FailureInjector for FailRenameCurrent {
        fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
            self.0.lock().unwrap().push(point.clone());
            if point.step == storage_durable::CommitStep::RenameCurrent {
                return Err(std::io::Error::other("injected rename failure"));
            }
            Ok(())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    SegmentRdbStore::new(dir.path()).unwrap();
    let injector = Arc::new(FailRenameCurrent::default());
    let store = SegmentRdbStore::new_with_failure_injector(dir.path(), injector.clone()).unwrap();
    let error = store
        .save_required(&Arc::new(Engine::new()), 1)
        .unwrap_err();
    assert!(error.to_string().contains("activate segment generation"));
    assert!(matches!(store.load_current_generation().unwrap(), None));
    assert!(injector
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|point| point.step == storage_durable::CommitStep::RenameCurrent));
}

#[test]
fn load_latest_picks_highest_seq() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let e = Arc::new(Engine::new());
    e.create_collection("u", kw_schema()).unwrap();
    index_kw(&e, "u1", "a@x.com");
    for seq in [10u64, 5, 99, 50] {
        store.save(&e, seq).unwrap();
    }
    assert_eq!(store.load_latest().unwrap().unwrap().1, 99);
}

#[test]
fn prune_keeps_newest() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let e = Arc::new(Engine::new());
    e.create_collection("u", kw_schema()).unwrap();
    index_kw(&e, "u1", "a@x.com");
    for seq in 1..=5u64 {
        store.save(&e, seq).unwrap();
    }
    let removed = store.prune(2).unwrap();
    assert_eq!(removed, 3);
    assert_eq!(store.generation_seqs().unwrap(), vec![4, 5]);
    assert_eq!(store.load_latest().unwrap().unwrap().1, 5);
}
