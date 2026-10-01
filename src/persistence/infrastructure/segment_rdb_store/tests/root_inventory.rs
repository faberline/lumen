use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::startup::SegmentStartupDecision;
use crate::persistence::infrastructure::segment_rdb_store::tests::{index_kw, kw_schema};
use crate::persistence::infrastructure::segment_rdb_store::{
    SegmentRdbStore, AOF_COMPACT_TEMP_FILE, AOF_FILE, CONTAINER_VOLUME_SEED_FILE, CURRENT_FILE,
    EXT_FILESYSTEM_METADATA_DIR, HNSW_GRAPH_CACHE_DIR,
};
use std::path::Path;
use std::sync::Arc;

#[cfg(unix)]
#[test]
fn graph_cache_without_current_cannot_initialize_or_clean_a_root() {
    for kind in 0..3 {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let cache = root.path().join(HNSW_GRAPH_CACHE_DIR);
        match kind {
            0 => std::fs::create_dir(&cache).unwrap(),
            1 => std::fs::write(&cache, b"damaged cache").unwrap(),
            _ => std::os::unix::fs::symlink(outside.path(), &cache).unwrap(),
        }
        let partial = root.path().join(".gen-7.tmp");
        std::fs::create_dir(&partial).unwrap();
        std::fs::write(partial.join("sentinel"), b"keep until authority is known").unwrap();
        let result = SegmentRdbStore::new(root.path());
        assert!(
            result.is_err(),
            "a cache cannot authorize empty initialization without CURRENT (kind {kind})"
        );
        assert!(!root.path().join("CURRENT").exists());
        assert_eq!(
            std::fs::read(partial.join("sentinel")).unwrap(),
            b"keep until authority is known"
        );
        assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    }
}

#[test]
fn graph_cache_cleanup_does_not_remove_unrelated_root_entries() {
    let root = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(root.path()).unwrap();
    let cache = root.path().join(HNSW_GRAPH_CACHE_DIR);
    let obsolete = cache.join("a".repeat(64));
    std::fs::create_dir_all(&obsolete).unwrap();
    std::fs::write(obsolete.join("graph.hnsw.graph"), b"obsolete cache").unwrap();
    let retained = cache.join("unknown-entry");
    std::fs::create_dir(&retained).unwrap();
    std::fs::write(retained.join("sentinel"), b"keep").unwrap();
    assert_eq!(
        store
            .save_hnsw_graph_caches(&crate::index::application::engine::Engine::new())
            .unwrap(),
        0
    );
    assert!(!obsolete.exists());
    assert_eq!(std::fs::read(retained.join("sentinel")).unwrap(), b"keep");
    assert!(root.path().join("CURRENT").is_file());
}

#[test]
fn torn_staging_dir_is_ignored_and_swept() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let e = Arc::new(Engine::new());
    e.create_collection("u", kw_schema()).unwrap();
    index_kw(&e, "u1", "a@x.com");
    store.save(&e, 7).unwrap();

    // Simulate a crash mid-stage: a leftover `.gen-<seq>.tmp` dir.
    std::fs::create_dir_all(dir.path().join(".gen-9.tmp")).unwrap();
    // load_latest still returns the good committed generation, not the torn one.
    assert_eq!(store.load_latest().unwrap().unwrap().1, 7);
    // A subsequent save sweeps the torn staging dir.
    store.save(&e, 8).unwrap();
    assert!(!dir.path().join(".gen-9.tmp").exists());
    assert_eq!(store.load_latest().unwrap().unwrap().1, 8);
}

#[test]
fn abandoned_durable_staging_is_swept_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let _first = SegmentRdbStore::new(dir.path()).unwrap();
    let staging = dir.path().join(".stage-gen-9-rev-99");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("partial"), b"partial").unwrap();

    let _reopened = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(!staging.exists());
}

#[test]
fn unrelated_legacy_like_directory_is_not_swept() {
    let dir = tempfile::tempdir().unwrap();
    let unrelated = dir.path().join(".gen-user.tmp");
    std::fs::create_dir(&unrelated).unwrap();
    std::fs::write(unrelated.join("owned-by-user"), b"keep").unwrap();

    assert!(SegmentRdbStore::new(dir.path()).is_err());
    assert!(
        !dir.path().join("CURRENT").exists(),
        "an unrecognized non-empty root must not become an empty store"
    );
    assert!(unrelated.is_dir());
    assert_eq!(
        std::fs::read(unrelated.join("owned-by-user")).unwrap(),
        b"keep"
    );
}

#[test]
fn mixed_legacy_and_unknown_root_fails_before_legacy_adoption() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42");
    std::fs::create_dir(&legacy).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();
    std::fs::create_dir(dir.path().join("foreign-layout")).unwrap();

    assert!(SegmentRdbStore::new(dir.path()).is_err());
    assert!(
        !dir.path().join("CURRENT").exists(),
        "unknown content must block legacy adoption before it writes CURRENT"
    );
    assert!(legacy.is_dir());
    assert!(dir.path().join("foreign-layout").is_dir());
}

#[test]
fn unknown_entry_beside_valid_current_fails_before_any_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let first = SegmentRdbStore::new(dir.path()).unwrap();
    let current = std::fs::read(dir.path().join("CURRENT")).unwrap();
    drop(first);
    let foreign = dir.path().join("foreign-layout");
    std::fs::create_dir(&foreign).unwrap();

    assert!(SegmentRdbStore::new(dir.path()).is_err());
    assert_eq!(std::fs::read(dir.path().join("CURRENT")).unwrap(), current);
    assert!(foreign.is_dir());
}

#[test]
fn unpointed_revision_without_current_has_a_specific_fail_closed_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("gen-7-rev-1")).unwrap();

    let error = SegmentRdbStore::new(dir.path()).unwrap_err();
    assert!(error.to_string().contains("unpointed revision generation"));
    assert!(!dir.path().join("CURRENT").exists());
}

#[test]
fn aof_only_root_remains_a_supported_empty_checkpoint_baseline() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("aof.log"), b"").unwrap();
    std::fs::write(dir.path().join("aof.log.compact.tmp"), b"").unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let outcome = store
        .reopen_into_with_outcome(&Arc::new(Engine::new()))
        .unwrap();
    assert_eq!(
        outcome.decision,
        SegmentStartupDecision::RecoveredUncommittedEmpty
    );
    assert_eq!(outcome.checkpoint_sequence, None);
    assert_eq!(
        std::fs::read(dir.path().join("CURRENT")).unwrap(),
        b"empty\n"
    );
}

#[test]
fn compact_aof_temp_without_aof_is_rejected_before_current_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let compact = dir.path().join(AOF_COMPACT_TEMP_FILE);
    let bytes = b"uncommitted compact output";
    std::fs::write(&compact, bytes).unwrap();

    let error = SegmentRdbStore::new(dir.path()).unwrap_err();
    assert!(error
        .to_string()
        .contains("aof.log.compact.tmp requires regular aof.log beside it"));
    assert!(!dir.path().join(CURRENT_FILE).exists());
    assert_eq!(std::fs::read(compact).unwrap(), bytes);
}

#[test]
fn invalid_root_inventory_lists_every_child_name_and_kind_in_sorted_order() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(AOF_FILE)).unwrap();
    std::fs::write(dir.path().join("alpha-foreign"), b"foreign").unwrap();
    std::fs::create_dir(dir.path().join("zeta-foreign")).unwrap();

    let error = SegmentRdbStore::new(dir.path()).unwrap_err();
    let rendered = error.to_string();
    assert!(rendered.contains(
        "invalid segment checkpoint root inventory [alpha-foreign (regular file), aof.log (directory), zeta-foreign (directory)]"
    ));
    assert!(rendered.contains("checkpoint root entry must be a regular file"));
    assert!(
        rendered.contains("unrecognized non-empty segment checkpoint root entry `alpha-foreign`")
    );
    assert!(
        rendered.contains("unrecognized non-empty segment checkpoint root entry `zeta-foreign`")
    );
    assert!(!dir.path().join(CURRENT_FILE).exists());
}

#[test]
fn current_empty_remains_authoritative_over_an_unpointed_revision() {
    let dir = tempfile::tempdir().unwrap();
    let _first = SegmentRdbStore::new(dir.path()).unwrap();
    let current = std::fs::read(dir.path().join(CURRENT_FILE)).unwrap();
    std::fs::create_dir(dir.path().join("gen-7-rev-1")).unwrap();

    let reopened = SegmentRdbStore::new(dir.path()).unwrap();
    let outcome = reopened
        .reopen_into_with_outcome(&Arc::new(Engine::new()))
        .unwrap();
    assert_eq!(
        outcome.decision,
        SegmentStartupDecision::RestoredCurrentEmpty
    );
    assert_eq!(
        std::fs::read(dir.path().join(CURRENT_FILE)).unwrap(),
        current
    );
}

#[test]
fn genuinely_empty_root_reports_initialization_once_then_current_empty() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(CONTAINER_VOLUME_SEED_FILE), b"").unwrap();
    let first = SegmentRdbStore::new(dir.path()).unwrap();
    assert_eq!(
        first
            .reopen_into_with_outcome(&Arc::new(Engine::new()))
            .unwrap()
            .decision,
        SegmentStartupDecision::InitializedEmptyRoot
    );

    let reopened = SegmentRdbStore::new(dir.path()).unwrap();
    assert_eq!(
        reopened
            .reopen_into_with_outcome(&Arc::new(Engine::new()))
            .unwrap()
            .decision,
        SegmentStartupDecision::RestoredCurrentEmpty
    );
}

#[test]
fn empty_lost_found_is_accepted_beside_supported_root_layouts() {
    fn create_lost_found(root: &Path) {
        let lost_found = root.join(EXT_FILESYSTEM_METADATA_DIR);
        std::fs::create_dir(&lost_found).unwrap();
        assert!(std::fs::read_dir(&lost_found).unwrap().next().is_none());
    }

    let empty = tempfile::tempdir().unwrap();
    create_lost_found(empty.path());
    let store = SegmentRdbStore::new(empty.path()).unwrap();
    assert_eq!(
        store
            .reopen_into_with_outcome(&Arc::new(Engine::new()))
            .unwrap()
            .decision,
        SegmentStartupDecision::InitializedEmptyRoot
    );
    assert_eq!(
        std::fs::read(empty.path().join(CURRENT_FILE)).unwrap(),
        b"empty\n"
    );

    let aof = tempfile::tempdir().unwrap();
    create_lost_found(aof.path());
    std::fs::write(aof.path().join(AOF_FILE), b"").unwrap();
    let store = SegmentRdbStore::new(aof.path()).unwrap();
    assert_eq!(
        store
            .reopen_into_with_outcome(&Arc::new(Engine::new()))
            .unwrap()
            .decision,
        SegmentStartupDecision::RecoveredUncommittedEmpty
    );

    let legacy = tempfile::tempdir().unwrap();
    create_lost_found(legacy.path());
    let legacy_generation = legacy.path().join("gen-42");
    std::fs::create_dir(&legacy_generation).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy_generation, 42).unwrap();
    let store = SegmentRdbStore::new(legacy.path()).unwrap();
    assert_eq!(
        store
            .reopen_into_with_outcome(&Arc::new(Engine::new()))
            .unwrap()
            .decision,
        SegmentStartupDecision::AdoptedLegacy0428
    );

    let current = tempfile::tempdir().unwrap();
    create_lost_found(current.path());
    let first = SegmentRdbStore::new(current.path()).unwrap();
    drop(first);
    let reopened = SegmentRdbStore::new(current.path()).unwrap();
    assert_eq!(
        reopened
            .reopen_into_with_outcome(&Arc::new(Engine::new()))
            .unwrap()
            .decision,
        SegmentStartupDecision::RestoredCurrentEmpty
    );

    for root in [empty.path(), aof.path(), legacy.path(), current.path()] {
        let lost_found = root.join(EXT_FILESYSTEM_METADATA_DIR);
        let metadata = std::fs::symlink_metadata(&lost_found).unwrap();
        assert!(metadata.is_dir() && !metadata.file_type().is_symlink());
        assert!(std::fs::read_dir(lost_found).unwrap().next().is_none());
    }
}

#[test]
fn legacy_aside_is_recovered_and_reported_before_adoption() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42.old");
    std::fs::create_dir(&legacy).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let outcome = store
        .reopen_into_with_outcome(&Arc::new(Engine::new()))
        .unwrap();
    assert_eq!(outcome.decision, SegmentStartupDecision::AdoptedLegacy0428);
    assert!(outcome.recovered_legacy_aside);
    assert!(dir.path().join("gen-42").is_dir());
    assert!(!dir.path().join("gen-42.old").exists());
}

#[cfg(unix)]
#[test]
fn unknown_root_symlink_fails_before_current_is_written() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), dir.path().join("foreign-layout")).unwrap();

    assert!(SegmentRdbStore::new(dir.path()).is_err());
    assert!(!dir.path().join("CURRENT").exists());
    assert!(dir.path().join("foreign-layout").is_symlink());
}
