use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;

use crate::ingest::application::write_coordinator::tests::keyword_schema;
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::{WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::storage::{ApplyOutcome, Engine};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_relief_stages_applied_sources_pinned_by_a_slow_subscriber() {
    check_slow_subscriber_capacity_relief(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_refusal_stages_applied_sources_without_a_waiting_apply() {
    check_slow_subscriber_capacity_relief(false).await;
}

async fn check_slow_subscriber_capacity_relief(wait_for_capacity: bool) {
    const LIMIT: usize = 4 * 1024 * 1024;
    let budget = ChangeBudget::with_hard_limit(LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let wal = Arc::new(MemWal::new());
    let _slow = wal.subscribe_admitted(0).await.unwrap();
    let coord = WriteCoordinator::start(wal.clone(), engine.clone());
    coord
        .submit(RaftLogEntry::CreateCollection {
            collection_id: "u".into(),
            req: keyword_schema(),
        })
        .await
        .unwrap();
    coord
        .submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "retained".into(),
                    field: "email".into(),
                    value: FieldValue::String("x".repeat(32 * 1024)),
                    version: None,
                }],
                request_id: None,
            },
        })
        .await
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store =
        crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(dir.path())
            .unwrap();
    store.save(&engine, coord.applied_seq()).unwrap();
    let pinned = budget.snapshot().total;
    assert!(
        pinned >= 32 * 1024,
        "checkpoint must retain the slow subscriber's raw source charge"
    );
    let filler_owner = budget.owner();
    let filler = filler_owner.try_reserve(LIMIT - pinned).unwrap();
    if wait_for_capacity {
        let waiting_owner = budget.owner();
        let mut waiting = tokio::task::spawn_blocking(move || waiting_owner.wait_reserve(1));
        let progressed =
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut waiting).await;
        let staged_without_releasing_filler = progressed.is_ok();
        // Always join the blocking waiter, including the failed progress case.
        drop(filler);
        if let Ok(result) = progressed {
            drop(result.unwrap().unwrap());
        } else {
            drop(waiting.await.unwrap().unwrap());
        }
        assert!(
            staged_without_releasing_filler,
            "capacity relief must stage an applied source held by a slow subscriber"
        );
    } else {
        let error = match coord.try_admit_local_record(&RaftLogEntry::CreateCollection {
            collection_id: "refused".into(),
            req: keyword_schema(),
        }) {
            Err(error) => error,
            Ok(_) => panic!("forced Full pre-WAL admission must refuse"),
        };
        assert!(error.downcast_ref::<PendingChangeCapacity>().is_some());
        assert_eq!(coord.applied_seq(), 2);
        assert!(!budget.has_capacity_waiters());
        let relieved = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while budget.snapshot().total == LIMIT {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        drop(filler);
        assert!(
            relieved.is_ok(),
            "pre-publication capacity refusal must stage pinned sources without a waiting apply"
        );
        assert!(engine.stats("refused").is_err());
    }
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 1);
    assert!(!coord.is_restart_required());
}

/// A door refusal (HTTP 429) sets `capacity_relief_requested` with no
/// committed apply blocked on capacity. That alone must not let the
/// relief task steal the ledger reservation of a different, already
/// admitted local record that is simply waiting its turn to apply.
/// Stealing it forces the apply loop to re-reserve from scratch and can
/// park it in `wait_reserve_record_ram` until checkpoint frees bytes —
/// exactly the > 5s perf-run stall this test guards against.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_relief_leaves_a_fresh_local_reservation_when_no_apply_is_waiting() {
    struct DeferredAdmittedWal {
        inner: MemWal,
        released: Arc<std::sync::atomic::AtomicBool>,
        changed: Arc<tokio::sync::Notify>,
    }
    #[async_trait::async_trait]
    impl WalLog for DeferredAdmittedWal {
        async fn publish(&self, record: WalRecord) -> Result<u64> {
            self.inner.publish(record).await
        }
        async fn latest_seq(&self) -> Result<u64> {
            self.inner.latest_seq().await
        }
        async fn stage_source(
            &self,
            seq: u64,
        ) -> Result<Option<crate::ingest::infrastructure::wal::delivery::WalSourceRelease>>
        {
            self.inner.stage_source(seq).await
        }
        async fn subscribe(&self, from: u64) -> Result<WalStream> {
            self.inner.subscribe(from).await
        }
        async fn subscribe_admitted(
            &self,
            from: u64,
        ) -> Result<crate::ingest::domain::wal_log::WalAdmissionStream> {
            let stream = self.inner.subscribe_admitted(from).await?;
            let state = (stream, self.released.clone(), self.changed.clone());
            Ok(Box::pin(futures::stream::unfold(
                state,
                |(mut stream, released, changed)| async move {
                    loop {
                        let wake = changed.notified();
                        if released.load(Ordering::Acquire) {
                            break;
                        }
                        wake.await;
                    }
                    stream
                        .next()
                        .await
                        .map(|record| (record, (stream, released, changed)))
                },
            )))
        }
    }

    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = Arc::new(DeferredAdmittedWal {
        inner: MemWal::new(),
        released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        changed: Arc::new(tokio::sync::Notify::new()),
    });
    let coord = WriteCoordinator::start(wal.clone(), engine.clone());

    let entry = RaftLogEntry::Index {
        collection_id: "u".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "admitted".into(),
                field: "email".into(),
                value: FieldValue::String("x".repeat(4096)),
                version: None,
            }],
            request_id: None,
        },
    };
    let mut submitted = tokio::spawn({
        let coord = coord.clone();
        async move { coord.submit(entry).await }
    });

    // Wait until the door-admitted record's reservation lands in the
    // pending ledger, while the apply loop stays deferred and cannot
    // take it yet.
    let seq = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if let Some(seq) = coord.local_reservations.lock().await.keys().next().copied() {
                return seq;
            }
            assert!(
                !submitted.is_finished(),
                "submit finished before installing its reservation"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the admitted local reservation must appear in the pending ledger");

    // Simulate a door refusal that requested relief with no committed
    // apply blocked on capacity.
    coord
        .capacity_relief_requested
        .store(true, Ordering::Release);
    assert!(!engine.has_capacity_waiters());

    // Give the relief task (25ms tick) several chances to (mis)fire.
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;

    assert!(
        coord.local_reservations.lock().await.contains_key(&seq),
        "a door refusal alone must not steal an already-admitted, unapplied local reservation"
    );

    // Release the deferred apply loop and confirm the record still
    // applies cleanly, without going through a capacity wait.
    wal.released.store(true, Ordering::Release);
    wal.changed.notify_waiters();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), &mut submitted)
        .await
        .expect("submit must complete once the apply loop is released")
        .expect("submit task must not panic")
        .expect("submit must apply successfully");
    assert!(matches!(outcome, ApplyOutcome::Indexed(_)));
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 1);
}
