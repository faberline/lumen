use crate::persistence::domain::generation_manifest::SegmentKind;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    current_generation, has_keyword, index_kw, index_kw_in, kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::{SegmentRdbStore, StagingSelection};
use crate::storage::Engine;
use std::sync::Arc;

#[test]
fn background_scratch_does_not_consume_published_revision() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "one", "base");
    store.save(&engine, 1).unwrap();

    let scratch = store.begin_background_merge_stage(1).unwrap();
    assert_eq!(scratch.generation().as_str(), "gen-1-rev-0");
    let (revision, publication) = store
        .begin_next_generation_selected(1, StagingSelection::CurrentIfDurable)
        .unwrap();
    assert_eq!(revision, 2);

    drop(publication);
    drop(scratch);
}

#[test]
fn background_worker_drains_compaction_requests_for_every_ready_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    for name in ["u", "v"] {
        engine.create_collection(name, kw_schema()).unwrap();
        index_kw_in(&engine, name, "one", "base");
    }
    store.save(&engine, 1).unwrap();
    for round in 1..=4 {
        for name in ["u", "v"] {
            index_kw_in(&engine, name, "one", &format!("round-{round}"));
        }
        store.save(&engine, round + 1).unwrap();
    }
    store
        .wait_for_merges(std::time::Duration::from_secs(10))
        .unwrap();
    // The bounded pairwise scheduler publishes one collection per job.
    // Request the next job after the first publication so the tied
    // collection receives its own pairwise compaction turn.
    store.request_merge(&engine).unwrap();
    store
        .wait_for_merges(std::time::Duration::from_secs(10))
        .unwrap();
    let manifest =
        read_generation_manifest(&dir.path().join(current_generation(&store).as_str())).unwrap();
    for collection in &manifest.collections {
        let deltas = collection
            .segments
            .iter()
            .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
            .count();
        assert!(
            deltas < 4,
            "every ready field must receive compaction work before the worker becomes idle: {} still has {deltas}",
            collection.collection_id
        );
    }
}

#[test]
fn loaded_checkpoint_files_stay_until_its_last_reader_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "one", "base");
    store.save(&engine, 1).unwrap();
    let loaded = store.load_current_generation().unwrap().unwrap();
    let protected = dir.path().join(loaded.name.as_str());
    // The loaded Engine can outlive every handle used to open its store.
    drop(store);
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    for round in 1..=4 {
        index_kw(&engine, "one", &format!("round-{round}"));
        store.save(&engine, round + 1).unwrap();
    }
    store
        .wait_for_merges(std::time::Duration::from_secs(10))
        .unwrap();
    store.prune(1).unwrap();
    assert!(
        protected.exists(),
        "a live cold reader must pin its checkpoint files"
    );
    assert!(has_keyword(&loaded.engine, "base"));
    drop(loaded);
    store.prune(1).unwrap();
    assert!(
        !protected.exists(),
        "a proved old generation can be reclaimed once its reader is gone"
    );
}
