use crate::persistence::domain::generation_manifest::SegmentGenerationManifest;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, write_generation_manifest,
};
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    current_generation, first_segment_file, index_kw, index_kw_in, install_unpointed_generation,
    kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, SegmentRdbStore, GENERATION_MANIFEST_FILE, GENERATION_MANIFEST_SCHEMA_VERSION,
};
use crate::storage::Engine;
use std::sync::Arc;
use storage_durable::CurrentTarget;

#[test]
fn staged_corruption_is_rejected_before_current_moves() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    let (revision, staged) = store.begin_next_generation(7).unwrap();
    let staging_path = staged.path().to_path_buf();
    engine.flush_to_segments(&staging_path, 7).unwrap();
    write_generation_manifest(
        &staging_path,
        &SegmentGenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            checkpoint_sequence: 7,
            revision,
            previous: None,
            next_collection_generation: 1,
            collections: Vec::new(),
        },
    )
    .unwrap();
    std::fs::write(first_segment_file(&staging_path), b"corrupt").unwrap();
    let record = GenerationRecord {
        name: staged.generation().clone(),
        path: staging_path,
        sequence: 7,
        revision,
        legacy: false,
        previous: None,
    };

    assert!(store.validate_record(&record).is_err());
    assert_eq!(
        store.generations.read_current().unwrap(),
        CurrentTarget::Empty
    );
}

#[test]
fn staged_manifest_must_parse_and_match_the_validated_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    let (revision, staged) = store.begin_next_generation(7).unwrap();
    let staging_path = staged.path().to_path_buf();
    engine.flush_to_segments(&staging_path, 7).unwrap();
    let record = GenerationRecord {
        name: staged.generation().clone(),
        path: staging_path.clone(),
        sequence: 7,
        revision,
        legacy: false,
        previous: None,
    };

    std::fs::write(staging_path.join(GENERATION_MANIFEST_FILE), b"{\"").unwrap();
    assert!(store.validate_record(&record).is_err());

    write_generation_manifest(
        &staging_path,
        &SegmentGenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            checkpoint_sequence: 8,
            revision,
            previous: None,
            next_collection_generation: 1,
            collections: Vec::new(),
        },
    )
    .unwrap();
    assert!(store.validate_record(&record).is_err());
    assert_eq!(
        store.generations.read_current().unwrap(),
        CurrentTarget::Empty
    );
}

/// Keep the historical test name because the release gate calls it by
/// exact name. The 0.4.29 model no longer moves the predecessor aside.
/// It installs a complete immutable replacement first. `CURRENT` remains
/// the sole commit point, so a crash before that pointer rename must reopen
/// the predecessor and ignore the complete replacement.
#[test]
fn same_seq_resave_crash_between_aside_and_commit_recovers_predecessor() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();

    // The predecessor: a first successful save at seq 7.
    let engine_a = Arc::new(Engine::new());
    engine_a.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine_a, "a1", "a1@x.com");
    store.save(&engine_a, 7).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().1, 7);
    let predecessor = current_generation(&store);

    // Prepare the complete replacement and perform the generation rename.
    // Do not change CURRENT. This is the exact pre-commit crash state.
    let engine_b = Arc::new(Engine::new());
    engine_b.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine_b, "b1", "b1@x.com");
    index_kw(&engine_b, "b2", "b2@x.com");
    let replacement = install_unpointed_generation(&store, &engine_b, 7, Some(&predecessor));
    assert!(store.generations.generation_path(&replacement).is_dir());
    assert_eq!(current_generation(&store), predecessor);

    // Cold start from scratch, as a restarted pod does.
    let cold_store = SegmentRdbStore::new(dir.path()).unwrap();
    let (reloaded, seq) = cold_store
        .load_latest()
        .unwrap()
        .expect("a complete generation survives the crash window");
    assert_eq!(seq, 7);
    assert_eq!(
        reloaded.stats("u").unwrap().documents_indexed,
        1,
        "recovered generation must be the predecessor (1 doc), not the \
             never-committed replacement (2 docs) or nothing"
    );

    // A normal same-sequence save activates a new immutable revision.
    cold_store.save(&engine_b, 7).unwrap();
    assert_ne!(current_generation(&cold_store), replacement);
    assert_eq!(
        cold_store
            .load_latest()
            .unwrap()
            .unwrap()
            .0
            .stats("u")
            .unwrap()
            .documents_indexed,
        2
    );
}

#[test]
fn complete_unpointed_higher_generation_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let active_engine = Arc::new(Engine::new());
    active_engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&active_engine, "u1", "a@x.com");
    store.save(&active_engine, 7).unwrap();
    let active = current_generation(&store);

    let later_engine = Arc::new(Engine::new());
    later_engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&later_engine, "u1", "a@x.com");
    index_kw(&later_engine, "u2", "b@x.com");
    install_unpointed_generation(&store, &later_engine, 99, Some(&active));

    let restarted = SegmentRdbStore::new(dir.path()).unwrap();
    let (loaded, sequence) = restarted.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 7);
    assert_eq!(loaded.stats("u").unwrap().documents_indexed, 1);
    assert_eq!(current_generation(&restarted), active);
}

#[test]
fn corrupt_current_manifest_fails_without_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 7).unwrap();
    store.save(&engine, 8).unwrap();
    let current = current_generation(&store);
    let current_path = store.generations.generation_path(&current);
    let mut manifest = read_generation_manifest(&current_path).unwrap();
    manifest.checkpoint_sequence = 999;
    write_generation_manifest(&current_path, &manifest).unwrap();

    let restarted = SegmentRdbStore::new(dir.path()).unwrap();
    assert!(restarted.load_latest().is_err());
    assert_eq!(current_generation(&restarted), current);
}

#[test]
fn unsupported_predecessor_name_blocks_prune_without_deleting_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 1).unwrap();
    store.save(&engine, 2).unwrap();
    let current = current_generation(&store);
    let current_path = store.generations.generation_path(&current);
    let mut manifest = read_generation_manifest(&current_path).unwrap();
    let predecessor = manifest.previous.clone().unwrap();
    manifest.previous = Some("missing-safe-name".to_owned());
    write_generation_manifest(&current_path, &manifest).unwrap();

    assert!(store.prune(1).is_err());
    assert!(current_path.is_dir());
    assert!(dir.path().join(predecessor).is_dir());
}

#[test]
fn missing_future_predecessor_blocks_prune_without_deleting_history() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 1).unwrap();
    store.save(&engine, 2).unwrap();
    let current = current_generation(&store);
    let current_path = store.generations.generation_path(&current);
    let mut manifest = read_generation_manifest(&current_path).unwrap();
    let predecessor = manifest.previous.clone().unwrap();
    manifest.previous = Some("gen-999-rev-1".to_owned());
    write_generation_manifest(&current_path, &manifest).unwrap();

    assert!(store.prune(1).is_err());
    assert!(current_path.is_dir());
    assert!(dir.path().join(predecessor).is_dir());
}

#[test]
fn missing_older_predecessor_makes_prune_conservative() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 1).unwrap();
    store.save(&engine, 2).unwrap();
    let current = current_generation(&store);
    let current_path = store.generations.generation_path(&current);
    let mut manifest = read_generation_manifest(&current_path).unwrap();
    let real_predecessor = manifest.previous.clone().unwrap();
    manifest.previous = Some("gen-0-rev-0".to_owned());
    write_generation_manifest(&current_path, &manifest).unwrap();

    assert_eq!(store.prune(1).unwrap(), 0);
    assert!(current_path.is_dir());
    assert!(dir.path().join(real_predecessor).is_dir());
}

#[test]
fn full_reopen_validation_leaves_target_engine_unchanged_on_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42");
    std::fs::create_dir(&legacy).unwrap();
    let source = Arc::new(Engine::new());
    source.create_collection("a", kw_schema()).unwrap();
    source.create_collection("z", kw_schema()).unwrap();
    index_kw_in(&source, "a", "a1", "a@x.com");
    index_kw_in(&source, "z", "z1", "z@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();
    std::fs::write(first_segment_file(&legacy.join("7a")), b"corrupt").unwrap();

    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let target = Arc::new(Engine::new());
    target.create_collection("existing", kw_schema()).unwrap();
    index_kw_in(&target, "existing", "e1", "existing@x.com");

    assert!(store.reopen_into(&target).is_err());
    assert_eq!(
        target.stats("existing").unwrap().documents_indexed,
        1,
        "validation failure must not replace or partly extend the caller engine"
    );
    assert!(target.stats("a").is_err());
    assert!(target.stats("z").is_err());
    assert!(!dir.path().join("CURRENT").exists());
}

#[test]
fn malformed_unpointed_revision_does_not_block_save_or_prune() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 1).unwrap();

    let malformed = dir.path().join("gen-200-rev-999");
    std::fs::create_dir(&malformed).unwrap();
    std::fs::write(malformed.join(GENERATION_MANIFEST_FILE), b"not-json").unwrap();

    store.save(&engine, 2).unwrap();
    assert_eq!(store.load_latest().unwrap().unwrap().1, 2);
    assert_eq!(store.prune(1).unwrap(), 2);
    assert!(!malformed.exists());
    assert_eq!(store.generation_seqs().unwrap(), vec![2]);
}

#[test]
fn prune_follows_active_chain_and_removes_aborted_revision() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 1).unwrap();
    store.save(&engine, 2).unwrap();
    let active = current_generation(&store);
    let aborted = install_unpointed_generation(&store, &engine, 200, Some(&active));
    let aborted_path = store.generations.generation_path(&aborted);
    store.save(&engine, 3).unwrap();

    assert_eq!(store.prune(2).unwrap(), 2);
    assert!(!aborted_path.exists());
    assert_eq!(store.generation_seqs().unwrap(), vec![2, 3]);
    assert_eq!(store.load_latest().unwrap().unwrap().1, 3);
}

#[cfg(unix)]
#[test]
fn parseable_generation_symlink_fails_closed() {
    use std::os::unix::fs::symlink;

    let dir = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    symlink(target.path(), dir.path().join("gen-99")).unwrap();

    assert!(SegmentRdbStore::new(dir.path()).is_err());
}
