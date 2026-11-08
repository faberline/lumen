use crate::index::application::engine::Engine;
use crate::persistence::domain::generation_manifest::{SegmentKind, SegmentRole};
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    current_generation, index_kw, index_kw_in, kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    SegmentRdbStore, StagingSelection, GENERATION_MANIFEST_FILE,
};
use std::sync::Arc;
use storage_durable::FailureInjector;

#[test]
fn merge_checkpoint_telemetry_separates_transient_delta_and_merged_base() {
    let actual_dir = tempfile::tempdir().unwrap();
    let control_dir = tempfile::tempdir().unwrap();
    let actual = SegmentRdbStore::new(actual_dir.path()).unwrap();
    let control = SegmentRdbStore::new(control_dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    let reference = Arc::new(Engine::new());
    for (store, state) in [(&actual, &engine), (&control, &reference)] {
        state.create_collection("u", kw_schema()).unwrap();
        for (id, value) in [
            ("one", "base-one"),
            ("two", "base-two"),
            ("three", "base-three"),
        ] {
            index_kw(state, id, value);
        }
        // Both controls need the same populated base before four measured
        // updates. This also exercises upgrade from snapshot state.
        state.restore(state.snapshot().unwrap()).unwrap();
        store.save(state, 1).unwrap();
    }
    // A single-delta control writes the same final record at the same cut.
    // Its physical payload supplies an oracle independent of the counter.
    index_kw(&reference, "one", "round-4");
    control.save(&reference, 5).unwrap();
    let control_path = control_dir
        .path()
        .join(current_generation(&control).as_str());
    let control_manifest = read_generation_manifest(&control_path).unwrap();
    let delta = control_manifest.collections[0]
        .segments
        .iter()
        .find(|segment| matches!(segment.kind, SegmentKind::Delta))
        .unwrap();
    let fresh_delta_bytes = std::fs::metadata(control_path.join(&delta.path))
        .unwrap()
        .len()
        + std::fs::metadata(control_path.join(&delta.local_rows.as_ref().unwrap().path))
            .unwrap()
            .len();
    for round in 1..=3 {
        index_kw(&engine, "one", &format!("round-{round}"));
        actual.save(&engine, round + 1).unwrap();
    }
    let checkpoint_before = engine.metrics().segment_checkpoint_bytes_total.get();
    let merge_before = engine.metrics().segment_merge_write_bytes_total.get();
    let merge_count_before = engine.metrics().segment_merge_completed_total.get();
    index_kw(&engine, "one", "round-4");
    let checkpoint_name = actual
        .save_inner(&engine, 5, false, StagingSelection::CurrentIfDurable)
        .unwrap();
    actual
        .wait_for_merges(std::time::Duration::from_secs(10))
        .unwrap();
    let current = actual_dir.path().join(current_generation(&actual).as_str());
    let manifest = read_generation_manifest(&current).unwrap();
    let base = manifest.collections[0]
        .segments
        .iter()
        .find(|segment| matches!(segment.role, SegmentRole::Field))
        .unwrap();
    assert!(matches!(base.kind, SegmentKind::Base));
    let merged_bytes = std::fs::metadata(current.join(&base.path)).unwrap().len()
        + std::fs::metadata(current.join(&base.local_rows.as_ref().unwrap().path))
            .unwrap()
            .len();
    assert_ne!(
        fresh_delta_bytes, merged_bytes,
        "oracle must distinguish fresh input from merged output"
    );
    let manifest_bytes = std::fs::metadata(
        actual_dir
            .path()
            .join(checkpoint_name.as_str())
            .join(GENERATION_MANIFEST_FILE),
    )
    .unwrap()
    .len();
    assert_eq!(
        engine.metrics().segment_checkpoint_bytes_total.get() - checkpoint_before,
        fresh_delta_bytes + manifest_bytes,
        "checkpoint counter must include its fresh delta and its own published manifest"
    );
    assert_eq!(
        engine.metrics().segment_merge_write_bytes_total.get() - merge_before,
        merged_bytes,
        "merge counter must contain the merged output only"
    );
    assert_eq!(
        engine.metrics().segment_merge_completed_total.get() - merge_count_before,
        1
    );
}

#[test]
fn checkpoint_telemetry_counts_only_durable_publications_and_new_file_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "first", "value");
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    assert_eq!(engine.metrics().segment_checkpoint_completed_total.get(), 0);
    store.save(&engine, 1).unwrap();
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        1,
        "real durable checkpoint must produce one telemetry completion"
    );
    let first_bytes = engine.metrics().segment_checkpoint_bytes_total.get();
    assert!(first_bytes > 0);
    assert!(engine.metrics().segment_disk_bytes.get() >= first_bytes);
    assert_eq!(
        engine.metrics().segment_merge_completed_total.get(),
        0,
        "checkpoint without a merge cannot manufacture merge evidence"
    );
    store.save(&engine, 2).unwrap();
    let linked_bytes = engine.metrics().segment_checkpoint_bytes_total.get() - first_bytes;
    assert!(
        linked_bytes > 0 && linked_bytes < first_bytes,
        "an unchanged checkpoint writes its manifest, not every linked payload again"
    );
    struct FailAt(storage_durable::CommitStep);
    impl FailureInjector for FailAt {
        fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
            if point.step == self.0 {
                return Err(std::io::Error::other("telemetry injected failure"));
            }
            Ok(())
        }
    }
    let failing = SegmentRdbStore::new_with_failure_injector(
        dir.path(),
        Arc::new(FailAt(storage_durable::CommitStep::SyncFile)),
    )
    .unwrap();
    index_kw(&engine, "second", "new");
    assert!(failing.save(&engine, 3).is_err());
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        2,
        "failed checkpoint cannot manufacture durable completion evidence"
    );
}

#[test]
fn save_path_file_entries_inspected_does_not_grow_with_idle_collections() {
    use crate::persistence::infrastructure::segment_rdb_store::telemetry::{
        inspected_file_entries, reset_inspected_file_entries,
    };

    fn run_case(idle_count: usize) -> u64 {
        let dir = tempfile::tempdir().unwrap();
        let store = SegmentRdbStore::new(dir.path()).unwrap();
        let engine = Arc::new(Engine::new());
        engine.create_collection("u", kw_schema()).unwrap();
        index_kw(&engine, "changed_0", "val");
        for i in 0..idle_count {
            let name = format!("idle_{i}");
            engine.create_collection(&name, kw_schema()).unwrap();
            index_kw_in(&engine, &name, &format!("idle_item_{i}"), "val");
        }
        store.save(&engine, 1).unwrap();

        reset_inspected_file_entries();
        index_kw(&engine, "changed_1", "val_2");
        store.save(&engine, 2).unwrap();

        inspected_file_entries()
    }

    let inspected_1 = run_case(1);
    let inspected_50 = run_case(50);
    assert!(
        inspected_50 <= inspected_1,
        "save path file entries inspected must not grow with idle collections: N=1 was {inspected_1}, N=50 was {inspected_50}"
    );
}

#[test]
fn incremental_disk_bytes_matches_full_generation_walk_after_saves_and_prunes() {
    use crate::persistence::infrastructure::segment_rdb_store::telemetry::generation_disk_bytes;

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "doc1", "val1");

    for seq in 1..=8u64 {
        index_kw(&engine, &format!("doc_{seq}"), &format!("val_{seq}"));
        store.save(&engine, seq).unwrap();
        if seq % 2 == 0 {
            store.prune(3).unwrap();
        }
        let incremental = store.disk_bytes().unwrap();
        let fresh_walk = generation_disk_bytes(dir.path()).unwrap();
        assert_eq!(
            incremental, fresh_walk,
            "incremental disk bytes must match full walk at seq {seq}"
        );
    }
}

fn generation_dir_count(store: &SegmentRdbStore) -> usize {
    store.generation_entries().unwrap().len()
}

#[test]
fn failed_prune_marks_inventory_stale_and_rebuilds_on_next_disk_bytes() {
    use crate::persistence::infrastructure::segment_rdb_store::telemetry::{
        generation_disk_bytes, inject_prune_failure_after,
    };

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "doc1", "val1");

    for seq in 1..=5u64 {
        index_kw(&engine, &format!("doc_{seq}"), &format!("val_{seq}"));
        store.save(&engine, seq).unwrap();
    }

    // Fail after one generation is really removed. The inventory then holds a
    // removal it has not applied, so only the stale mark can make it correct.
    // A directory mode cannot force this failure: root ignores it.
    // Background merges publish generations too. Let them finish, so the
    // count below sees only this prune.
    store
        .wait_for_merges(std::time::Duration::from_secs(10))
        .unwrap();
    let before = generation_dir_count(&store);
    inject_prune_failure_after(1);
    let prune_result = store.prune(3);
    assert!(
        prune_result.is_err(),
        "the injected prune failure must surface"
    );
    assert_eq!(
        before - generation_dir_count(&store),
        1,
        "exactly one generation is removed before the injected failure"
    );

    let rebuilt_bytes = store.disk_bytes().unwrap();
    let expected = generation_disk_bytes(dir.path()).unwrap();
    assert_eq!(
        rebuilt_bytes, expected,
        "rebuilt disk bytes after failed prune must equal fresh generation walk"
    );
}

#[test]
fn reopened_store_keeps_idle_collections_reusable_across_saves_and_prunes() {
    use crate::persistence::infrastructure::segment_rdb_store::telemetry::generation_disk_bytes;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    engine.create_collection("idle", kw_schema()).unwrap();
    index_kw(&engine, "doc1", "val1");
    index_kw_in(&engine, "idle", "idle_item", "val");
    {
        let store = SegmentRdbStore::new(dir.path()).unwrap();
        store.save(&engine, 1).unwrap();
        index_kw(&engine, "doc2", "val2");
        store.save(&engine, 2).unwrap();
    }
    // Dropping the first store releases the shared inventory. The reopened
    // store therefore starts from a full walk, whose keys must let the idle
    // collection be reused by the next save.
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    index_kw(&engine, "doc3", "val3");
    store.save(&engine, 3).unwrap();
    store.prune(2).unwrap();
    index_kw(&engine, "doc4", "val4");
    store.save(&engine, 4).unwrap();
    assert_eq!(
        store.disk_bytes().unwrap(),
        generation_disk_bytes(dir.path()).unwrap(),
        "incremental disk bytes after reopen, saves and prune must match a full walk"
    );
}
