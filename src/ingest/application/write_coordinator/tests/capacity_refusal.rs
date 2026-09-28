use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::ingest::application::write_coordinator::capacity::claim_diagnostic_refusal_revision;
use crate::ingest::application::write_coordinator::tests::{
    admitted_index_entry, hnsw_schema, keyword_schema,
};
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::storage::{ApplyOutcome, Engine};

#[tokio::test]
async fn full_local_admission_refuses_before_wal_publication() {
    let entry = admitted_index_entry();
    let calibration_budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let calibration = Engine::with_change_budget(calibration_budget);
    calibration
        .create_collection("u", keyword_schema())
        .unwrap();
    let base = calibration.try_reserve_record(&entry, 0).unwrap().bytes();
    let raw = Engine::record_owned_bytes(&entry).unwrap();
    let requested = calibration
        .try_reserve_record(&entry, raw.checked_mul(2).unwrap())
        .unwrap()
        .bytes();
    assert!(requested > base);

    let hard_limit = requested.checked_mul(2).unwrap();
    let budget = ChangeBudget::with_hard_limit(hard_limit);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let remaining = requested.checked_mul(2).unwrap() - budget.snapshot().total;
    let _held = engine.try_reserve_record(&entry, remaining - base).unwrap();
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal.clone(), engine);
    let before = coord.engine.metrics().segment_backpressure_total.get();

    let error = coord.submit(entry).await.unwrap_err();
    assert!(error.downcast_ref::<PendingChangeCapacity>().is_some());
    assert_eq!(
        coord.engine.metrics().segment_backpressure_total.get(),
        before + 1,
        "one final pre-publication refusal increments once"
    );
    assert_eq!(wal.latest_seq().await.unwrap(), 0);
    assert_eq!(coord.applied_seq(), 0);
    assert!(
        budget.high_water_bytes() <= hard_limit,
        "a refused record must not push high water above the hard limit"
    );
}

#[tokio::test]
async fn full_local_refusal_emits_one_numeric_diagnostic_without_wal_publish() {
    use crate::segment_rdb::{DiagnosticCapture, NumericCanonicalEvent as Event};

    let capture = DiagnosticCapture::new();
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal.clone(), engine);
    coord.set_diagnostic_capture(capture.token());
    let filler = budget.owner();
    let _held = filler
        .try_reserve(1024 * 1024 - budget.snapshot().total)
        .unwrap();
    for _ in 0..2 {
        let error = coord.submit(admitted_index_entry()).await.unwrap_err();
        assert!(error.downcast_ref::<PendingChangeCapacity>().is_some());
    }
    assert_eq!(wal.latest_seq().await.unwrap(), 0);
    let numeric = capture.drain();
    assert_eq!(
        numeric.len(),
        1,
        "real repeated Full refusals share one request revision"
    );
    assert!(
        matches!(numeric[0], Event::Refusal { revision: 1.., present: true, requested: 1.., hard_limit, .. } if hard_limit == 1024 * 1024)
    );
}

#[tokio::test]
async fn foreign_budget_full_without_local_work_has_unlinked_numeric_refusal() {
    use crate::segment_rdb::{DiagnosticCapture, NumericCanonicalEvent as Event};

    let capture = DiagnosticCapture::new();
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal.clone(), engine);
    coord.set_diagnostic_capture(capture.token());
    let foreign = budget.owner();
    let _held = foreign.try_reserve(1024 * 1024).unwrap();
    let error = coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "new".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<PendingChangeCapacity>().is_some());
    assert_eq!(wal.latest_seq().await.unwrap(), 0);
    let events = capture.drain();
    assert_eq!(events.len(), 1);
    assert!(
        matches!(events[0], Event::Refusal { revision: 0, present: false, requested: 1.., hard_limit, .. } if hard_limit == 1024 * 1024)
    );
}

#[test]
fn diagnostic_admission_refusal_claims_each_request_revision_once() {
    let last_seen = AtomicU64::new(0);
    assert!(claim_diagnostic_refusal_revision(&last_seen, 17));
    assert!(
        !claim_diagnostic_refusal_revision(&last_seen, 17),
        "two Full refusals for one pending request must produce one event"
    );
    assert!(claim_diagnostic_refusal_revision(&last_seen, 18));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hnsw_capacity_refusal_trace_keeps_applied_source_until_relief() {
    const LIMIT: usize = 4 * 1024 * 1024;
    let budget = ChangeBudget::with_hard_limit(LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let wal = Arc::new(MemWal::new());
    let _slow = wal.subscribe_admitted(0).await.unwrap();
    let coord = WriteCoordinator::start(wal.clone(), engine.clone());

    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "hnsw".into(),
            req: hnsw_schema(),
        })
        .await
        .unwrap();
    coord
        .submit(RaftLogEntry::Index {
            collection_id: "hnsw".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "one".into(),
                    field: "embedding".into(),
                    value: FieldValue::Vector(vec![1.0, 2.0]),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap();
    let before = budget.snapshot().total;
    let filler_owner = budget.owner();
    let filler = filler_owner.try_reserve(LIMIT - before).unwrap();
    let checkpoint_before = engine.metrics().segment_checkpoint_completed_total.get();
    let merge_before = engine.metrics().segment_merge_completed_total.get();

    let refused = match coord.try_admit_local_record(&RaftLogEntry::CreateCollection {
        collection_id: "refused".into(),
        req: hnsw_schema(),
    }) {
        Err(error) => error,
        Ok(_) => panic!("forced Full pre-WAL admission must refuse"),
    };
    assert!(refused.downcast_ref::<PendingChangeCapacity>().is_some());
    assert_eq!(wal.latest_seq().await.unwrap(), 2);
    assert_eq!(coord.applied_seq(), 2);

    // The refusal starts the relief owner. It must complete checkpoint
    // publication while the filler remains held. A base-only one-item
    // HNSW fixture has no merge candidate, so merge stays unchanged.
    let staged = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while engine.metrics().segment_checkpoint_completed_total.get() <= checkpoint_before
            || budget.snapshot().total == LIMIT
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        staged.is_ok(),
        "capacity relief must stage the applied source and checkpoint"
    );
    assert_eq!(
        engine.metrics().segment_merge_completed_total.get(),
        merge_before,
        "a base-only one-item HNSW fixture must not claim merge completion"
    );
    let top_up_owner = budget.owner();
    let top_up = top_up_owner
        .try_reserve(LIMIT - budget.snapshot().total)
        .expect("restore the forced-full condition for the negative control");
    let second_refused = match coord.try_admit_local_record(&RaftLogEntry::CreateCollection {
        collection_id: "refused".into(),
        req: hnsw_schema(),
    }) {
        Err(error) => error,
        Ok(_) => panic!("restored Full pre-WAL admission must refuse"),
    };
    assert!(second_refused
        .downcast_ref::<PendingChangeCapacity>()
        .is_some());
    assert_eq!(wal.latest_seq().await.unwrap(), 2);
    assert_eq!(coord.applied_seq(), 2);
    assert!(
        engine.stats("refused").is_err(),
        "refused pre-WAL input must not create a collection"
    );
    drop(filler);
    drop(top_up);

    let retry = coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "refused".into(),
            req: hnsw_schema(),
        })
        .await
        .expect("capacity relief must admit the retry after filler release");
    assert!(matches!(retry, ApplyOutcome::Created(_)));
    assert_eq!(wal.latest_seq().await.unwrap(), 3);
    assert_eq!(coord.applied_seq(), 3);

    let lock_before = engine
        .metrics()
        .engine_state_write_lock_wait_seconds_count
        .get();
    let hnsw_before = engine.metrics().hnsw_add_seconds_count.get();
    coord
        .submit(RaftLogEntry::Index {
            collection_id: "hnsw".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "two".into(),
                    field: "embedding".into(),
                    value: FieldValue::Vector(vec![3.0, 4.0]),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .expect("post-relief HNSW index must apply");
    let lock_delta = engine
        .metrics()
        .engine_state_write_lock_wait_seconds_count
        .get()
        - lock_before;
    let hnsw_delta = engine.metrics().hnsw_add_seconds_count.get() - hnsw_before;
    assert!(hnsw_delta > 0, "post-relief index must record an HNSW add");
    assert!(
        lock_delta <= hnsw_delta,
        "state-lock waits ({lock_delta}) must not exceed HNSW adds ({hnsw_delta})"
    );
    assert_eq!(
        coord.applied_seq(),
        4,
        "post-relief HNSW index must publish after the successful retry"
    );
    assert_eq!(engine.stats("hnsw").unwrap().documents_indexed, 2);
    assert!(!coord.is_restart_required());
}

#[tokio::test]
async fn local_success_and_domain_error_do_not_count_segment_backpressure() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget));
    engine.create_collection("u", keyword_schema()).unwrap();
    let coord = WriteCoordinator::start(Arc::new(MemWal::new()), engine.clone());
    let before = engine.metrics().segment_backpressure_total.get();

    coord.submit(admitted_index_entry()).await.unwrap();
    let mut invalid = admitted_index_entry();
    let RaftLogEntry::Index { req, .. } = &mut invalid else {
        unreachable!("the admitted fixture is an Index request")
    };
    req.items[0].field = "missing-field".to_owned();
    let error = coord.submit(invalid).await.unwrap_err();

    assert!(error.downcast_ref::<PendingChangeCapacity>().is_none());
    assert_eq!(
        engine.metrics().segment_backpressure_total.get(),
        before,
        "successful and domain-error applies are not pre-publication capacity refusals"
    );
}

#[tokio::test]
async fn local_capacity_refusal_starts_checkpoint_owner_without_committed_waiter() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let coord = WriteCoordinator::start(Arc::new(MemWal::new()), engine);
    let filler_owner = budget.owner();
    let _filler = filler_owner
        .try_reserve(1024 * 1024 - budget.snapshot().total)
        .unwrap();

    let error = match coord.try_admit_local_record(&admitted_index_entry()) {
        Ok(_) => panic!("full local admission must be refused"),
        Err(error) => error,
    };
    assert!(
        error.downcast_ref::<PendingChangeCapacity>().is_some(),
        "a full local admission remains a typed retryable refusal"
    );
    assert!(
        coord
            .layer_capacity_owner
            .lock()
            .expect("capacity owner poisoned")
            .is_some(),
        "a local refusal must create independent checkpoint maintenance"
    );
}
