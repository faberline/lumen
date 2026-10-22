use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::tests::{index_kw, kw_schema};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use std::sync::Arc;

/// #1389 AC1: a `reshard:apply` batch applied to a target shard, and a
/// `reshard:evict` on a source shard, both survive a cold start from a
/// checkpoint written after those mutations — independent of any
/// periodic-snapshot cadence, closing the restart gap `#1387`'s embedded
/// persistence left open for reshard's direct-state-mutation admin verbs
/// (`Engine::apply_reshard_batch` / `Engine::evict_not_owned`, added by
/// `#1380`). This is the engine-level half of `#1389`'s proof; the
/// driver-level half (cutover cannot fire before every touched shard's
/// checkpoint completes) lives in `tests/it/reshard_driver_e2e.rs`.
#[test]
fn reshard_apply_and_evict_survive_checkpoint_and_cold_start() {
    use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();

    // Target shard: receives a reshard:apply batch on top of its own
    // pre-existing data — mirrors what a shard actually looks like
    // mid-migration.
    let target = Arc::new(Engine::new());
    target.create_collection("u", kw_schema()).unwrap();
    index_kw(&target, "t-existing", "existing@x.com");

    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "migrated-1", "migrated1@x.com");
    let batch = source.snapshot().unwrap();
    let apply_outcome = target.apply_reshard_batch(batch, None).unwrap();
    assert_eq!(apply_outcome.documents_upserted, 1);
    assert_eq!(target.stats("u").unwrap().documents_indexed, 2);

    // Source shard: post-cutover eviction of the bucket that just moved
    // off of it, under a 2-shard map where bucket 0 now belongs to shard
    // 1 (mirrors `reshard_evict_removes_only_moved_bucket_docs`).
    let source_after_cutover = Arc::new(Engine::new());
    source_after_cutover
        .create_collection("u", kw_schema())
        .unwrap();
    let ids: Vec<String> = (0..8).map(|i| format!("s-{i:02}")).collect();
    for id in &ids {
        index_kw(&source_after_cutover, id, &format!("{id}@x.com"));
    }
    let mut assignments = vec![0u32; 4];
    assignments[0] = 1;
    let new_map = VirtualBucketShardMap::new(1, assignments, 2).unwrap();
    let evict_outcome = source_after_cutover.evict_not_owned(&new_map, 0).unwrap();
    assert!(evict_outcome.documents_evicted > 0);
    let remaining_before_checkpoint = source_after_cutover.stats("u").unwrap().documents_indexed;
    assert!(remaining_before_checkpoint < ids.len() as u64);

    // Checkpoint both post-mutation states, exactly like
    // `checkpoint_touched_shards` (#1389) drives per shard before
    // cutover — this is the synchronous, awaited durability step, not a
    // background snapshot the driver has no visibility into.
    store.save(&target, 100).unwrap();
    let target_docs_before_drop = target.stats("u").unwrap().documents_indexed;
    drop(target);

    let store2 = SegmentRdbStore::new(dir.path().join("source")).unwrap();
    store2.save(&source_after_cutover, 100).unwrap();
    drop(source_after_cutover);

    // Cold start: reload from the checkpoint alone, as a restarted pod
    // would (WAL replay from `seq + 1` is orthogonal to this proof —
    // there are no un-checkpointed writes here).
    let (reloaded_target, seq) = store.load_latest().unwrap().expect("target checkpoint");
    assert_eq!(seq, 100);
    assert_eq!(
        reloaded_target.stats("u").unwrap().documents_indexed,
        target_docs_before_drop
    );

    let (reloaded_source, seq2) = store2.load_latest().unwrap().expect("source checkpoint");
    assert_eq!(seq2, 100);
    assert_eq!(
        reloaded_source.stats("u").unwrap().documents_indexed,
        remaining_before_checkpoint
    );
}

/// #1397 AC1: `POST /admin/checkpoint` (the checkpoint sink) and the
/// periodic snapshotter share one `SegmentRdbStore` and can both fire at
/// an unchanged `applied_seq` (reshard apply/evict mutate engine state
/// without advancing `applied_seq`, so this is a routine, not a rare,
/// interleaving). Loop the interleaving many rounds with several
/// concurrent `save` callers per round: every round must cold-start to a
/// complete engine, never a torn one — proving `save_lock` actually
/// prevents `sweep_staging`/`rename` races rather than merely narrowing
/// them.
#[test]
fn separately_opened_same_root_stores_hold_one_owned_save_permit() {
    let dir = tempfile::tempdir().unwrap();
    let first = SegmentRdbStore::new(dir.path()).unwrap();
    let second = SegmentRdbStore::new(dir.path()).unwrap();
    let permit = first.save_gate.lock_owned();
    let second_gate = second.save_gate.clone();
    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        attempt_tx.send(()).unwrap();
        let _permit = second_gate.lock_owned();
        done_tx.send(()).unwrap();
    });
    attempt_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let acquired_early = done_rx
        .recv_timeout(std::time::Duration::from_millis(50))
        .is_ok();
    drop(permit);
    if !acquired_early {
        done_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
    }
    worker.join().unwrap();
    assert!(
        !acquired_early,
        "a second same-root owner acquired before the first permit dropped"
    );
}

#[test]
fn concurrent_saves_at_same_seq_never_produce_torn_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    for i in 0..20 {
        index_kw(&engine, &format!("u{i:02}"), &format!("u{i:02}@x.com"));
    }
    let expected_docs = engine.stats("u").unwrap().documents_indexed;

    for round in 0..50u64 {
        // Same `up_to_seq` across every concurrent caller this round,
        // mirroring a quiet cutover where `applied_seq` hasn't moved.
        let seq = round;
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let store = store.clone();
                let engine = engine.clone();
                std::thread::spawn(move || store.save(&engine, seq))
            })
            .collect();
        for h in handles {
            h.join().unwrap().unwrap();
        }

        // Cold-start from scratch after the interleaving: the committed
        // generation must always be complete and loadable, never torn.
        let (reloaded, loaded_seq) = store.load_latest().unwrap().expect("a checkpoint");
        assert_eq!(loaded_seq, seq);
        assert_eq!(
            reloaded.stats("u").unwrap().documents_indexed,
            expected_docs,
            "round {round}: cold start after concurrent saves must be complete"
        );
    }
}

#[test]
fn independently_opened_handles_share_checkpoint_preparation_lock() {
    let dir = tempfile::tempdir().unwrap();
    let first = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let second = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");

    for sequence in 1..=10 {
        let left = {
            let store = first.clone();
            let engine = engine.clone();
            std::thread::spawn(move || store.save(&engine, sequence))
        };
        let right = {
            let store = second.clone();
            let engine = engine.clone();
            std::thread::spawn(move || store.save(&engine, sequence))
        };
        left.join().unwrap().unwrap();
        right.join().unwrap().unwrap();
    }

    assert_eq!(first.load_latest().unwrap().unwrap().1, 10);
    assert!(!std::fs::read_dir(dir.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(".stage-"))
    }));
}
