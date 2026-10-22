use std::collections::BTreeMap as Map;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use tokio::sync::Notify;

use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::{WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    schema::{CreateCollectionRequest, FieldSpec, FieldType, VectorBackend, VectorMetric},
};

#[derive(Clone)]
struct ControlledWal {
    record: Arc<Mutex<Option<WalRecord>>>,
    latest: Arc<AtomicU64>,
    published: Arc<Notify>,
    observed_publish: Arc<Notify>,
    delivered: Arc<Notify>,
    release_publish: Arc<Notify>,
    release_delivery: Arc<Notify>,
    mode: Arc<AtomicU8>,
}

impl ControlledWal {
    const PAUSE_PUBLISH: u8 = 1;
    const PAUSE_DELIVERY: u8 = 2;

    fn paused(mode: u8) -> Arc<Self> {
        Arc::new(Self {
            record: Arc::new(Mutex::new(None)),
            latest: Arc::new(AtomicU64::new(0)),
            published: Arc::new(Notify::new()),
            observed_publish: Arc::new(Notify::new()),
            delivered: Arc::new(Notify::new()),
            release_publish: Arc::new(Notify::new()),
            release_delivery: Arc::new(Notify::new()),
            mode: Arc::new(AtomicU8::new(mode)),
        })
    }
}

#[async_trait::async_trait]
impl WalLog for ControlledWal {
    async fn publish(&self, record: WalRecord) -> Result<u64> {
        *self.record.lock().unwrap() = Some(record);
        self.latest.store(1, Ordering::Release);
        self.published.notify_one();
        self.observed_publish.notify_one();
        if self.mode.load(Ordering::Acquire) & Self::PAUSE_PUBLISH != 0 {
            self.release_publish.notified().await;
        }
        Ok(1)
    }

    async fn subscribe(&self, _from_seq: u64) -> Result<WalStream> {
        let wal = self.clone();
        Ok(Box::pin(futures::stream::unfold(false, move |delivered| {
            let wal = wal.clone();
            async move {
                if delivered {
                    return futures::future::pending().await;
                }
                wal.published.notified().await;
                if wal.mode.load(Ordering::Acquire) & Self::PAUSE_DELIVERY != 0 {
                    wal.release_delivery.notified().await;
                }
                let record = wal.record.lock().unwrap().clone()?;
                wal.delivered.notify_one();
                Some((Ok((1, record)), true))
            }
        })))
    }

    async fn latest_seq(&self) -> Result<u64> {
        Ok(self.latest.load(Ordering::Acquire))
    }
}

fn keyword_schema() -> CreateCollectionRequest {
    let mut fields = Map::new();
    fields.insert(
        "email".to_string(),
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

fn hnsw_schema() -> CreateCollectionRequest {
    let mut fields = Map::new();
    fields.insert(
        "embedding".to_owned(),
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(2),
            metric: Some(VectorMetric::L2),
            backend: Some(VectorBackend::HnswCpu),
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

fn admitted_index_entry() -> RaftLogEntry {
    RaftLogEntry::Index {
        collection_id: "u".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "u1".into(),
                field: "email".into(),
                value: FieldValue::String("v".into()),
                version: None,
            }],
            request_id: None,
        },
    }
}

async fn wait_for_reserved(budget: &ChangeBudget) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("local published record must retain its reservation");
}

mod aof;
mod capacity_refusal;
mod capacity_relief;
mod local_reservation;
mod replay;
mod reprice;
mod submit;
mod write_phase;
