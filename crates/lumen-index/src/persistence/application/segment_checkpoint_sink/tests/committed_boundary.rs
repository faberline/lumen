use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use tokio::sync::Semaphore;

use super::{contains_keyword, spill_keyword_schema};
use crate::index::application::engine::Engine;
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::wal_log::{WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::persistence::application::ports::checkpoint_sink::CheckpointSink;
use crate::persistence::application::segment_checkpoint_sink::SegmentCheckpointSink;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::log_entry::RaftLogEntry;

struct HeldDeliveryWal {
    inner: MemWal,
    release: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl WalLog for HeldDeliveryWal {
    async fn publish(&self, record: WalRecord) -> Result<u64> {
        self.inner.publish(record).await
    }

    async fn subscribe(&self, from_seq: u64) -> Result<WalStream> {
        let release = self.release.clone();
        Ok(Box::pin(self.inner.subscribe(from_seq).await?.then(
            move |item| {
                let release = release.clone();
                async move {
                    release.acquire().await.unwrap().forget();
                    item
                }
            },
        )))
    }

    async fn latest_seq(&self) -> Result<u64> {
        self.inner.latest_seq().await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_checkpoint_persists_the_committed_boundary_after_relief() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    let release = Arc::new(Semaphore::new(0));
    let wal = Arc::new(HeldDeliveryWal {
        inner: MemWal::new(),
        release: release.clone(),
    });
    let writer = WriteCoordinator::start_from(wal.clone(), engine.clone(), 0);
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: writer.clone(),
        aof: None,
    });
    let sequence = wal.publish(WalRecord::new(RaftLogEntry::Index {
        collection_id: "captured".into(),
        req: serde_json::from_value(serde_json::json!({"items":[{"external_id":"committed","field":"email","value":"durable"}]})).unwrap(),
    })).await.unwrap();
    assert_eq!(
        writer.applied_seq(),
        0,
        "the committed delivery is deliberately held before apply"
    );
    let mut checkpoint = tokio::spawn(async move { sink.checkpoint_now().await });
    // A real initial save must run before waiting for the blocked record. In
    // production this save releases the capacity that apply needs to proceed.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !store.has_current_generation().unwrap() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("initial relief checkpoint must publish before delivery is released");
    let premature = tokio::time::timeout(Duration::from_millis(100), &mut checkpoint).await;
    // A later committed record must not move this request's fixed boundary.
    // Keep that second delivery held, so chasing the newest WAL head hangs.
    let later = wal.publish(WalRecord::new(RaftLogEntry::Index {
        collection_id: "captured".into(),
        req: serde_json::from_value(serde_json::json!({"items":[{"external_id":"later","field":"email","value":"later"}]})).unwrap(),
    })).await.unwrap();
    assert!(later > sequence);
    // Always unblock the real apply loop before reporting an old-path failure.
    release.add_permits(1);
    assert!(
        premature.is_err(),
        "manual success must not precede its committed boundary: {premature:?}"
    );
    assert!(tokio::time::timeout(Duration::from_secs(5), checkpoint)
        .await
        .unwrap()
        .unwrap()
        .unwrap());
    assert_eq!(writer.applied_seq(), sequence);
    assert!(contains_keyword(&engine, "durable"));
    let cold = store.load_current_generation().unwrap().unwrap();
    assert!(
        cold.sequence >= sequence,
        "CURRENT must cover the fixed committed boundary"
    );
    assert!(
        contains_keyword(&cold.engine, "durable"),
        "cold reopen must contain the committed record without another caller checkpoint"
    );
}

struct UnavailableBoundary;

#[async_trait::async_trait]
impl crate::ingest::application::write_coordinator::WriteSink for UnavailableBoundary {
    async fn submit(
        &self,
        _: RaftLogEntry,
    ) -> Result<crate::index::application::engine::raft_dispatch::ApplyOutcome> {
        anyhow::bail!("no publisher")
    }
    fn applied_seq(&self) -> u64 {
        0
    }
    async fn checkpoint_target_seq(&self) -> Result<u64> {
        anyhow::bail!("committed boundary unavailable")
    }
}

#[tokio::test]
async fn unavailable_committed_boundary_cannot_start_a_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let sink = SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(SegmentRdbStore::new(root.path()).unwrap()),
        writer: Arc::new(UnavailableBoundary),
        aof: None,
    };
    let error = sink.checkpoint_now().await.unwrap_err();
    assert!(error.to_string().contains("committed boundary unavailable"));
    assert!(sink.store.load_current_generation().unwrap().is_none());
    assert_eq!(engine.metrics().segment_checkpoint_completed_total.get(), 0);
}

struct FailedApply;

#[async_trait::async_trait]
impl crate::ingest::application::write_coordinator::WriteSink for FailedApply {
    async fn submit(
        &self,
        _: RaftLogEntry,
    ) -> Result<crate::index::application::engine::raft_dispatch::ApplyOutcome> {
        anyhow::bail!("no publisher")
    }
    fn applied_seq(&self) -> u64 {
        0
    }
    async fn checkpoint_target_seq(&self) -> Result<u64> {
        Ok(1)
    }
    fn restart_required(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn failed_committed_apply_cannot_report_a_durable_boundary() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: Arc::new(FailedApply),
        aof: None,
    };
    let error = sink.checkpoint_now().await.unwrap_err();
    assert!(error
        .downcast_ref::<crate::ingest::application::write_coordinator::errors::RestartRequired>()
        .is_some());
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        1,
        "relief saves before waiting; failed apply must not fake a completed boundary"
    );
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        0
    );
}
