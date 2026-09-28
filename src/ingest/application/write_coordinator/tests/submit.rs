use std::sync::Arc;

use crate::ingest::application::write_coordinator::errors::RestartRequired;
use crate::ingest::application::write_coordinator::tests::keyword_schema;
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::storage::{ApplyOutcome, Engine};

#[tokio::test]
async fn submit_creates_then_indexes_and_outcome_is_routed_back() {
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine.clone());

    let created = coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();
    match created {
        ApplyOutcome::Created(r) => {
            assert_eq!(r.version, 1);
            assert_eq!(r.fields_count, 1);
        }
        other => panic!("expected Created, got {other:?}"),
    }

    let indexed = coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "u1".into(),
                    field: "email".into(),
                    value: FieldValue::String("a@x.com".into()),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap();
    match indexed {
        ApplyOutcome::Indexed(r) => assert_eq!(r.indexed, 1),
        other => panic!("expected Indexed, got {other:?}"),
    }

    // The write is visible via a direct engine read (read-your-write).
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 1);
}

/// #4326: `lumen_coordinator_apply_seconds`/`lumen_coordinator_apply_items_total`
/// must observe exactly once per admitted local `submit`, under the
/// `index` kind with `items_total` equal to the submitted doc count —
/// the per-record apply cost a 500k-doc perf probe reads from
/// `GET /metrics`.
#[tokio::test]
async fn submit_of_index_record_observes_coordinator_apply_histogram() {
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine.clone());

    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();

    assert_eq!(
        engine
            .metrics()
            .render()
            .matches("lumen_coordinator_apply_seconds_count{kind=\"index\"} 0")
            .count(),
        1,
        "index kind must still emit its zero row before any index submit"
    );

    coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: "u1".into(),
                        field: "email".into(),
                        value: FieldValue::String("a@x.com".into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: "u2".into(),
                        field: "email".into(),
                        value: FieldValue::String("b@x.com".into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        })
        .await
        .unwrap();

    let out = engine.metrics().render();
    assert!(
        out.contains("lumen_coordinator_apply_seconds_count{kind=\"index\"} 1"),
        "expected exactly one observed index apply in:\n{out}"
    );
    assert!(
        out.contains("lumen_coordinator_apply_items_total{kind=\"index\"} 2"),
        "expected items_total to equal the submitted doc count in:\n{out}"
    );
    for stage in [
        "admission_to_mutation_gate",
        "publish_to_apply_start",
        "apply_to_waiter",
    ] {
        assert!(
            out.contains(&format!(
                "lumen_coordinator_stage_seconds_count{{kind=\"index\",stage=\"{stage}\"}} 1"
            )),
            "expected one {stage} observation in:\n{out}"
        );
    }
}

#[tokio::test]
async fn exclusive_mutation_fence_blocks_publish_until_released() {
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine);
    let fence = coord.fence_mutations().await.unwrap();

    let mut pending = {
        let coord = coord.clone();
        tokio::spawn(async move {
            coord
                .submit(RaftLogEntry::CreateCollection {
                    collection_id: "u".into(),
                    req: keyword_schema(),
                })
                .await
        })
    };

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut pending)
            .await
            .is_err(),
        "a submit must remain blocked while the exclusive fence is held"
    );
    assert_eq!(coord.applied_seq(), 0, "blocked submit must not publish");

    drop(fence);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), pending)
        .await
        .expect("submit must resume after the fence is released")
        .expect("submit task must not panic")
        .expect("submit must succeed");
    assert!(matches!(outcome, ApplyOutcome::Created(_)));
    assert_eq!(coord.applied_seq(), 1);
}

#[tokio::test]
async fn cancelled_submit_keeps_fence_closed_until_sequence_completes() {
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine);

    // Model the exact post-publish state directly: the caller owns a
    // waiter, while the completion table owns the mutation permit for the
    // published sequence. Cancelling the caller drops only the receiver.
    let permit = coord.mutation_gate.shared().await.unwrap();
    let waiter = coord.register_waiter(1, permit).unwrap();
    drop(waiter);

    let mut fence = Box::pin(coord.fence_mutations());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut fence)
            .await
            .is_err(),
        "caller cancellation must not open the fence before apply completion"
    );

    coord.complete(1, Err(anyhow::anyhow!("synthetic apply failure")));
    let _guard = tokio::time::timeout(std::time::Duration::from_secs(5), fence)
        .await
        .expect("the fence must open after the sequence completes")
        .expect("the process must not require restart");
    assert_eq!(coord.applied_seq(), 1);
}

#[tokio::test]
async fn restart_required_rejects_mutations_and_never_clears_itself() {
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine);

    coord.require_restart();
    assert!(coord.is_restart_required());
    assert!(coord.mutation_gate().is_restart_required());

    let error = coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .expect_err("restart latch must reject new writes");
    assert!(error.downcast_ref::<RestartRequired>().is_some(), "{error}");
    assert!(coord.fence_mutations().await.is_err());
    assert!(coord.checkpoint_permit().await.is_err());
    assert!(coord.is_restart_required());
    assert_eq!(coord.applied_seq(), 0, "rejected write must not publish");
}

#[tokio::test]
async fn submit_propagates_apply_error_with_type() {
    use crate::storage::StorageError;
    let engine = Arc::new(Engine::new());
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine.clone());

    // Index into a collection that doesn't exist → CollectionNotFound,
    // and the error must survive routing (downcast still works).
    let err = coord
        .submit(RaftLogEntry::Index {
            collection_id: "ghost".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "x".into(),
                    field: "email".into(),
                    value: FieldValue::String("a@x.com".into()),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap_err();
    assert!(
        err.downcast_ref::<StorageError>()
            .map(|e| matches!(e, StorageError::CollectionNotFound(_)))
            .unwrap_or(false),
        "StorageError must survive coordinator routing, got: {err}"
    );
    assert_eq!(coord.applied_seq(), 1);
    assert!(matches!(
        coord
            .submit(RaftLogEntry::CreateCollection {
                collection_id: "u".into(),
                req: keyword_schema(),
            })
            .await
            .unwrap(),
        ApplyOutcome::Created(_)
    ));
    assert_eq!(coord.applied_seq(), 2);
}
