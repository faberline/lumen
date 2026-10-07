use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::index::application::engine::Engine;
use crate::persistence::application::ports::checkpoint_sink::{
    CheckpointSink, HnswCacheDurability, HnswCacheSealUnavailable,
};
use crate::persistence::application::segment_checkpoint_sink::SegmentCheckpointSink;
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::log_entry::RaftLogEntry;

#[tokio::test]
async fn shutdown_graph_cache_preserves_current_and_durable_aof_tail() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let tail = root.path().join("aof.log");
    let aof = Arc::new(Mutex::new(AofWriter::open(&tail).unwrap()));
    let writer =
        crate::ingest::application::write_coordinator::WriteCoordinator::start_from_with_aof(
            Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
            engine.clone(),
            0,
            aof.clone(),
        );
    writer
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({
                "fields": {"v": {"type": "vector", "dim": 3, "metric": "l2", "backend": "hnsw-cpu"}}
            }))
            .unwrap(),
        })
        .await
        .unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine,
        store,
        writer: writer.clone(),
        aof: Some(aof.clone()),
    });
    sink.checkpoint_now().await.unwrap();
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    writer
        .submit(RaftLogEntry::Index {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({"items": [{
                "external_id": "tail", "field": "v", "value": [1.0, 2.0, 3.0]
            }]}))
            .unwrap(),
        })
        .await
        .unwrap();
    aof.lock().unwrap().sync().unwrap();
    let before = std::fs::read(&tail).unwrap();
    assert!(!before.is_empty());
    assert_eq!(sink.save_shutdown_graph_cache().await.unwrap(), 1);
    assert_eq!(
        std::fs::read(root.path().join("CURRENT")).unwrap(),
        current,
        "shutdown cache must not rewrite CURRENT when a durable AOF exists"
    );
    assert_eq!(
        std::fs::read(tail).unwrap(),
        before,
        "cache must not trim the authoritative tail"
    );
}

#[tokio::test]
async fn sealed_hnsw_cache_is_reused_only_until_the_next_mutation() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let tail = root.path().join("aof.log");
    let aof = Arc::new(Mutex::new(AofWriter::open(&tail).unwrap()));
    let writer =
        crate::ingest::application::write_coordinator::WriteCoordinator::start_from_with_aof(
            Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
            engine.clone(),
            0,
            aof.clone(),
        );
    writer
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({
                "fields": {"v": {"type": "vector", "dim": 3, "metric": "l2", "backend": "hnsw-cpu"}}
            }))
            .unwrap(),
        })
        .await
        .unwrap();
    writer
        .submit(RaftLogEntry::Index {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({"items": [{
                "external_id": "one", "field": "v", "value": [1.0, 2.0, 3.0]
            }]}))
            .unwrap(),
        })
        .await
        .unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store,
        writer: writer.clone(),
        aof: Some(aof),
    });

    let receipt = sink.clone().seal_hnsw_graph_cache().await.unwrap();
    assert_eq!(receipt.cache_fields, 1);
    assert_eq!(receipt.durability, HnswCacheDurability::AofSynced);
    assert!(sink.has_current_hnsw_cache_seal());
    assert_eq!(sink.clone().save_shutdown_graph_cache().await.unwrap(), 0);

    writer
        .submit(RaftLogEntry::Index {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({"items": [{
                "external_id": "two", "field": "v", "value": [4.0, 5.0, 6.0]
            }]}))
            .unwrap(),
        })
        .await
        .unwrap();
    assert!(!sink.has_current_hnsw_cache_seal());
}

#[tokio::test]
async fn failed_hnsw_cache_seal_clears_a_prior_marker() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let writer = crate::ingest::application::write_coordinator::WriteCoordinator::start(
        Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
        engine.clone(),
    );
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(SegmentRdbStore::new(root.path()).unwrap()),
        writer,
        aof: None,
    });
    sink.record_hnsw_cache_seal(engine.capture_barrier.mutation_stamp())
        .unwrap();
    assert!(sink.has_current_hnsw_cache_seal());

    let error = sink.clone().seal_hnsw_graph_cache().await.unwrap_err();
    assert!(error.downcast_ref::<HnswCacheSealUnavailable>().is_some());
    assert!(!sink.has_current_hnsw_cache_seal());
}

#[tokio::test]
async fn sealed_hnsw_cache_is_invalidated_by_direct_reshard_and_restore_calls() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let tail = root.path().join("aof.log");
    let aof = Arc::new(Mutex::new(AofWriter::open(&tail).unwrap()));
    let writer =
        crate::ingest::application::write_coordinator::WriteCoordinator::start_from_with_aof(
            Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
            engine.clone(),
            0,
            aof.clone(),
        );
    writer
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({
                "fields": {"v": {"type": "vector", "dim": 3, "metric": "l2", "backend": "hnsw-cpu"}}
            }))
            .unwrap(),
        })
        .await
        .unwrap();
    writer
        .submit(RaftLogEntry::Index {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({"items": [{
                "external_id": "one", "field": "v", "value": [1.0, 2.0, 3.0]
            }]}))
            .unwrap(),
        })
        .await
        .unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store,
        writer: writer.clone(),
        aof: Some(aof),
    });
    let applied_seq = writer.applied_seq();

    let seal = |sink: Arc<SegmentCheckpointSink>| async move {
        assert_eq!(
            sink.clone()
                .seal_hnsw_graph_cache()
                .await
                .unwrap()
                .cache_fields,
            1
        );
        assert!(sink.has_current_hnsw_cache_seal());
    };

    seal(sink.clone()).await;
    assert!(engine
        .apply_reshard_batch(
            crate::index::infrastructure::snapshot_v1::SnapshotV1 {
                version: 0,
                collections: BTreeMap::new(),
            },
            None,
        )
        .is_err());
    assert_eq!(writer.applied_seq(), applied_seq);
    assert!(!sink.has_current_hnsw_cache_seal());

    seal(sink.clone()).await;
    assert!(engine
        .apply_reshard_prune_chunk(crate::sharding::domain::prune_chunk::ReshardPruneChunk {
            to_map_version: 1,
            bucket: 0,
            virtual_bucket_count: 1,
            collection_id: "v".into(),
            chunk_index: 0,
            total_chunks: 0,
            keep_ids: BTreeSet::new(),
        })
        .is_err());
    assert_eq!(writer.applied_seq(), applied_seq);
    assert!(!sink.has_current_hnsw_cache_seal());

    seal(sink.clone()).await;
    let one_shard =
        crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap::balanced(1, 1, 1)
            .unwrap();
    assert_eq!(
        engine
            .evict_not_owned(&one_shard, 0)
            .unwrap()
            .documents_evicted,
        0
    );
    assert_eq!(writer.applied_seq(), applied_seq);
    assert!(!sink.has_current_hnsw_cache_seal());

    seal(sink.clone()).await;
    engine.restore(engine.snapshot().unwrap()).unwrap();
    assert_eq!(writer.applied_seq(), applied_seq);
    assert!(!sink.has_current_hnsw_cache_seal());
    assert_eq!(sink.save_shutdown_graph_cache().await.unwrap(), 1);
}

#[tokio::test]
async fn shutdown_cache_wait_for_inflight_writes_can_be_cancelled() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let writer = crate::ingest::application::write_coordinator::WriteCoordinator::start(
        Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
        engine.clone(),
    );
    let gate = writer.mutation_gate();
    let in_flight = gate.shared().await.unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine,
        store: Arc::new(SegmentRdbStore::new(root.path()).unwrap()),
        writer,
        aof: None,
    });
    assert!(tokio::time::timeout(
        Duration::from_millis(25),
        sink.clone().save_shutdown_graph_cache()
    )
    .await
    .is_err());
    assert!(!root.path().join("hnsw-graph-cache").exists());
    drop(in_flight);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), sink.save_shutdown_graph_cache())
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn completed_checkpoint_before_cache_publication_keeps_seal_valid() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let tail = root.path().join("aof.log");
    let aof = Arc::new(Mutex::new(AofWriter::open(&tail).unwrap()));
    let writer =
        crate::ingest::application::write_coordinator::WriteCoordinator::start_from_with_aof(
            Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
            engine.clone(),
            0,
            aof.clone(),
        );
    writer
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({
                "fields": {"v": {"type": "vector", "dim": 3, "metric": "l2", "backend": "hnsw-cpu"}}
            }))
            .unwrap(),
        })
        .await
        .unwrap();
    writer
        .submit(RaftLogEntry::Index {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({"items": [{
                "external_id": "one", "field": "v", "value": [1.0, 2.0, 3.0]
            }]}))
            .unwrap(),
        })
        .await
        .unwrap();
    aof.lock().unwrap().sync().unwrap();
    let durable_tail = std::fs::read(&tail).unwrap();
    let sequence = writer.applied_seq();
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer,
        aof: Some(aof),
    });
    let observed = Arc::new(Mutex::new(None));
    let observed_checkpoint = observed.clone();
    let checkpoint_engine = engine.clone();
    let checkpoint_store = store.clone();
    sink.set_before_cache_publication_hook(move || {
        let before = checkpoint_engine.capture_barrier.mutation_stamp();
        std::thread::spawn(move || {
            checkpoint_store.save(&checkpoint_engine, sequence).unwrap();
            let after = checkpoint_engine.capture_barrier.mutation_stamp();
            assert_eq!(before.epoch, after.epoch);
            assert!(after.apply_revision > before.apply_revision);
            *observed_checkpoint.lock().unwrap() = Some(after);
        })
        .join()
        .unwrap();
    });
    let result = sink.clone().seal_hnsw_graph_cache().await;
    if let Err(error) = &result {
        assert!(
            error
                .downcast_ref::<crate::persistence::application::ports::checkpoint_sink::HnswCacheSealInvalidated>()
                .is_none(),
            "background checkpoint alone returned HnswCacheSealInvalidated: {error}"
        );
    }
    let receipt = result.unwrap();
    let after_checkpoint = observed.lock().unwrap().unwrap();
    assert_eq!(receipt.cache_fields, 1);
    assert_eq!(receipt.durability, HnswCacheDurability::AofSynced);
    assert_eq!(receipt.mutation_epoch, after_checkpoint.epoch);
    assert_eq!(
        receipt.mutation_apply_revision,
        after_checkpoint.apply_revision
    );
    assert!(sink.has_current_hnsw_cache_seal());
    assert_eq!(sink.clone().save_shutdown_graph_cache().await.unwrap(), 0);
    assert_eq!(std::fs::read(&tail).unwrap(), durable_tail);
    let (cold, restored_sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(restored_sequence, sequence);
    assert!(cold.has_hnsw_graphs().unwrap());
}

#[tokio::test]
async fn cache_seal_without_aof_commits_checkpoint_before_store_publication() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let writer = crate::ingest::application::write_coordinator::WriteCoordinator::start(
        Arc::new(crate::ingest::infrastructure::wal::mem_wal::MemWal::new()),
        engine.clone(),
    );
    writer
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({
                "fields": {"v": {"type": "vector", "dim": 3, "metric": "l2", "backend": "hnsw-cpu"}}
            }))
            .unwrap(),
        })
        .await
        .unwrap();
    writer
        .submit(RaftLogEntry::Index {
            collection_id: "v".into(),
            req: serde_json::from_value(serde_json::json!({"items": [{
                "external_id": "one", "field": "v", "value": [1.0, 2.0, 3.0]
            }]}))
            .unwrap(),
        })
        .await
        .unwrap();
    let sequence = writer.applied_seq();
    let sink = Arc::new(SegmentCheckpointSink {
        engine,
        store: store.clone(),
        writer,
        aof: None,
    });
    let receipt =
        tokio::time::timeout(Duration::from_secs(5), sink.clone().seal_hnsw_graph_cache())
            .await
            .expect("the fallback checkpoint must not wait on its own store gate")
            .unwrap();
    assert_eq!(receipt.cache_fields, 1);
    assert_eq!(receipt.durability, HnswCacheDurability::CheckpointCommitted);
    assert!(sink.has_current_hnsw_cache_seal());
    let current = std::fs::read(root.path().join("CURRENT")).unwrap();
    assert_eq!(sink.save_shutdown_graph_cache().await.unwrap(), 0);
    assert_eq!(std::fs::read(root.path().join("CURRENT")).unwrap(), current);
    let (cold, restored_sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(restored_sequence, sequence);
    assert!(cold.has_hnsw_graphs().unwrap());
}
