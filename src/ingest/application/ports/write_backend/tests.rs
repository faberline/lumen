use std::sync::Arc;

use crate::index::application::engine::Engine;
use crate::ingest::application::ports::write_backend::{LocalWriteBackend, WriteBackend};
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::types::document::BatchUnindexDocsRequest;

#[tokio::test]
async fn invalid_batch_unindex_never_publishes_or_applies() {
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let writer = WriteCoordinator::start(wal.clone(), engine.clone());
    let backend = LocalWriteBackend {
        writer: writer.clone(),
    };

    let result = backend
        .unindex_docs(
            "documents".to_string(),
            BatchUnindexDocsRequest {
                external_ids: Vec::new(),
            },
        )
        .await;

    assert!(result.is_err(), "an empty batch must be rejected");
    assert_eq!(
        wal.latest_seq().await.expect("read WAL sequence"),
        0,
        "invalid direct calls must not publish a WAL record"
    );
    assert_eq!(
        writer.applied_seq(),
        0,
        "invalid direct calls must not reach Engine apply"
    );
    assert!(
        engine
            .snapshot()
            .expect("read engine snapshot")
            .collections
            .is_empty(),
        "invalid direct calls must not mutate engine state"
    );
}
