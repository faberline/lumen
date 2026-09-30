use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use crate::index::application::engine::Engine;
use crate::persistence::application::segment_checkpoint_sink::SegmentCheckpointSink;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

use crate::ingest::application::write_coordinator::WriteSink;
use crate::persistence::application::ports::checkpoint_sink::CheckpointSink;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

struct FailOnce(Mutex<Option<storage_durable::CommitStep>>);

impl storage_durable::FailureInjector for FailOnce {
    fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
        let mut armed = self.0.lock().unwrap();
        if *armed == Some(point.step) {
            *armed = None;
            return Err(std::io::Error::other("injected pending spill failure"));
        }
        Ok(())
    }
}

struct Watermark(AtomicU64);

#[async_trait::async_trait]
impl WriteSink for Watermark {
    async fn submit(
        &self,
        _: crate::shared_kernel::log_entry::RaftLogEntry,
    ) -> Result<crate::index::application::engine::raft_dispatch::ApplyOutcome> {
        anyhow::bail!("checkpoint test has no publisher")
    }
    fn applied_seq(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn spill_keyword_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "email".to_owned(),
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

fn admitted_keyword(engine: &Engine, external_id: &str, value: &str) {
    let entry = RaftLogEntry::Index {
        collection_id: "captured".to_owned(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: external_id.to_owned(),
                field: "email".to_owned(),
                value: FieldValue::String(value.to_owned()),
                version: None,
            }],
            request_id: None,
        },
    };
    let reservation = engine.try_reserve_record(&entry, 0).unwrap();
    let mut guard = engine
        .begin_admitted_record(entry, reservation)
        .unwrap_or_else(|failure| panic!("admit fixture: {:?}", failure.error));
    engine.apply_prepared_raft_entry(&mut guard).unwrap();
}

fn contains_keyword(engine: &Engine, value: &str) -> bool {
    let request = serde_json::from_value(serde_json::json!({
        "query": {"term": {"field": "email", "value": value}},
        "limit": 10
    }))
    .unwrap();
    !engine.search("captured", request).unwrap().hits.is_empty()
}

#[tokio::test]
async fn checkpoint_sink_disk_gauge_matches_post_prune_files() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection(
            "u",
            serde_json::from_value(serde_json::json!({
                "fields": { "email": { "type": "keyword" } }
            }))
            .unwrap(),
        )
        .unwrap();
    let writer = Arc::new(Watermark(AtomicU64::new(0)));
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: writer.clone(),
        aof: None,
    };
    for sequence in 1..=6 {
        writer.0.store(sequence, Ordering::SeqCst);
        assert!(sink.checkpoint_now().await.unwrap());
    }
    let generations = std::fs::read_dir(root.path())
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("gen-"))
        .count();
    assert_eq!(
        generations, 3,
        "fixture must actually prune retained generations"
    );
    assert_eq!(
        engine.metrics().segment_disk_bytes.get(),
        store.disk_bytes().unwrap(),
        "checkpoint sink must refresh disk usage after pruning"
    );
}

async fn wait_for_checkpoint_count(engine: &Engine, expected: u64) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while engine.metrics().segment_checkpoint_completed_total.get() < expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual checkpoint driver made no progress");
}

mod driver;

mod driver_lifecycle;

mod hnsw_cache;

mod pending_spill;
