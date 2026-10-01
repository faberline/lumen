use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::index::application::engine::{raft_dispatch::ApplyOutcome, Engine};
use crate::ingest::application::write_coordinator::errors::{RestartRequired, SubmitStalled};
use crate::ingest::application::write_coordinator::tests::keyword_schema;
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::WalLog;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

/// #1486 AC1/AC2: an engine "restored" to a non-zero watermark (mirrors
/// `serve()`'s `MemWal::starting_at(start_seq)` + `start_from(engine,
/// start_seq)` pairing, whatever the restore source — segment checkpoint,
/// AOF-tail replay, or CBOR RDB) accepts its first subsequent write
/// immediately (no waiter leak) and that write is durable + reflected in
/// stats/metrics, not stranded behind a stale watermark.
#[tokio::test]
async fn restore_seeds_wal_above_watermark_first_write_completes_promptly() {
    let engine = Arc::new(Engine::new());
    // Pre-restore state: schema already present (as a real checkpoint
    // restore would leave it), engine otherwise fresh.
    engine.create_collection("u", keyword_schema()).unwrap();

    const RESTORED_WATERMARK: u64 = 5;
    let wal = Arc::new(MemWal::starting_at(RESTORED_WATERMARK));
    let coord = WriteCoordinator::start_from(wal, engine.clone(), RESTORED_WATERMARK);
    assert_eq!(coord.applied_seq(), RESTORED_WATERMARK);

    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        coord.submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "post-restore-1".into(),
                    field: "email".into(),
                    value: FieldValue::String("fresh@x.com".into()),
                    version: None,
                }],
                request_id: None,
            },
        }),
    )
    .await
    .expect("first post-restore write must complete promptly, not hang (#1486)")
    .expect("first post-restore write must succeed");

    match outcome {
        ApplyOutcome::Indexed(r) => assert_eq!(r.indexed, 1),
        other => panic!("expected Indexed, got {other:?}"),
    }

    // Durable + reflected in read-your-write state.
    let stats = engine.stats("u").unwrap();
    assert_eq!(
        stats.documents_indexed, 1,
        "the fresh post-restore doc must be counted"
    );
    assert!(
        stats.last_indexed_at.is_some(),
        "last_indexed_at must advance for a genuinely-applied post-restore write"
    );

    // Searchable: the apply loop actually folded the write into the
    // engine (not silently dropped by the dedup guard).
    assert!(
        engine.metrics().index_writes_total.get() >= 1,
        "lumen_index_writes_total must advance for a genuinely-applied post-restore write"
    );
    assert!(
        engine.metrics().index_bytes_total.get() > 0,
        "lumen_index_bytes_total must advance for a genuinely-applied post-restore write"
    );

    // The WAL's own sequence domain is strictly above the restored
    // watermark, and the coordinator's applied head advanced past it.
    assert!(coord.applied_seq() > RESTORED_WATERMARK);
}

/// #1486 documents the defect class R1 fixes: pairing a non-zero
/// `start_from` watermark with an UNSEEDED `MemWal::new()` (base 0) — the
/// pre-fix `serve()` wiring. The stale sequence must fail promptly without
/// applying. `MemWal::starting_at` remains required for the first write to
/// succeed after a restore.
#[tokio::test]
async fn unseeded_wal_after_restore_fails_without_applying() {
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", keyword_schema()).unwrap();

    const RESTORED_WATERMARK: u64 = 5;
    // The bug: base-0 WAL paired with a watermark seeded from a restore.
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start_from(wal, engine.clone(), RESTORED_WATERMARK);

    let error = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        coord.submit(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: "post-restore-1".into(),
                    field: "email".into(),
                    value: FieldValue::String("fresh@x.com".into()),
                    version: None,
                }],
                request_id: None,
            },
        }),
    )
    .await
    .expect("stale sequence must fail promptly")
    .expect_err("an unseeded WAL must not report a successful write");
    assert!(
        error.downcast_ref::<SubmitStalled>().is_some(),
        "an unseeded WAL must report the stale sequence as SubmitStalled: {error}"
    );
    // Never actually applied — the read side agrees with the error.
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 0);
}

#[tokio::test]
async fn full_raw_delivery_replays_the_same_head_before_later_records() {
    use crate::ingest::domain::change_budget::ChangeBudget;
    use crate::ingest::domain::wal_log::{WalLog, WalStream};

    struct ObservedWal {
        inner: MemWal,
        subscriptions: AtomicU64,
        subscribed: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl WalLog for ObservedWal {
        async fn publish(&self, record: WalRecord) -> Result<u64> {
            self.inner.publish(record).await
        }
        async fn subscribe(&self, from: u64) -> Result<WalStream> {
            let stream = self.inner.subscribe(from).await?;
            self.subscriptions.fetch_add(1, Ordering::Release);
            self.subscribed.notify_one();
            Ok(stream)
        }
        async fn latest_seq(&self) -> Result<u64> {
            self.inner.latest_seq().await
        }
    }

    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let blocking_owner = budget.owner();
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    // Keep this fixture reserved-only. A fallback owner may validly publish
    // real active schema work, which would make a synthetic Full assertion
    // race with maintenance rather than exercise the pinned raw head.
    let schema_dir = tempfile::tempdir().unwrap();
    let schema_store = crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(
        schema_dir.path(),
    )
    .unwrap();
    schema_store.save(&engine, 0).unwrap();
    assert_eq!(
        budget.snapshot().active,
        0,
        "schema checkpoint must freeze fixture work"
    );
    let blocking = blocking_owner
        .try_reserve(1024 * 1024 - budget.snapshot().total)
        .unwrap();
    let wal = Arc::new(ObservedWal {
        inner: MemWal::new(),
        subscriptions: AtomicU64::new(0),
        subscribed: tokio::sync::Notify::new(),
    });
    let record = |id: &str| {
        WalRecord::new(RaftLogEntry::Index {
            collection_id: "u".into(),
            req: IndexRequest {
                items: vec![IndexItem {
                    external_id: id.into(),
                    field: "email".into(),
                    value: FieldValue::String(format!("{id}@example.test")),
                    version: None,
                }],
                request_id: None,
            },
        })
    };
    assert_eq!(wal.publish(record("first")).await.unwrap(), 1);
    let dir = tempfile::tempdir().unwrap();
    let aof_path = dir.path().join("aof.log");
    let aof = Arc::new(Mutex::new(
        crate::persistence::infrastructure::aof::aof_writer::AofWriter::open(&aof_path).unwrap(),
    ));
    let coord = WriteCoordinator::start_from_with_aof(wal.clone(), engine.clone(), 0, aof);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while wal.subscriptions.load(Ordering::Acquire) < 2 {
            wal.subscribed.notified().await;
        }
    })
    .await
    .expect("full raw admission must pin a replay subscription");
    assert!(
        coord
            .layer_capacity_owner
            .lock()
            .expect("capacity owner poisoned")
            .is_some(),
        "committed native raw admission must start independent capacity maintenance before waiting",
    );
    assert_eq!(wal.publish(record("second")).await.unwrap(), 2);
    assert_eq!(coord.applied_seq(), 0, "no unowned working copy may apply");
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 0);
    assert_eq!(budget.snapshot().total, 1024 * 1024);
    // The raw delivery was destroyed before the blocking reserve. Dropping
    // the real competing reservation supplies space without an apply lease.
    drop(blocking);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while coord.applied_seq() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the original head and its successor must both apply");
    assert!(!coord.is_restart_required());
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 2);
    let mut persisted = Vec::new();
    crate::persistence::infrastructure::aof::replay::AofReader::replay(&aof_path, 0, |seq, rec| {
        let RaftLogEntry::Index { req, .. } = rec.entry else {
            panic!("unexpected AOF operation")
        };
        persisted.push((seq, req.items[0].external_id.clone()));
    })
    .unwrap();
    assert_eq!(persisted, vec![(1, "first".into()), (2, "second".into())]);
}

/// #1486 R2: `submit()` is bounded by `SUBMIT_TIMEOUT`, so even a
/// completely stalled apply (nothing ever calls `complete`) surfaces as
/// a distinct, retryable `SubmitStalled` error rather than an infinite
/// hang. Exercises `complete_stale` too: the dedup guard's stale-skip
/// path releases a waiter with `SubmitStalled`, not a plain hang.
#[tokio::test]
async fn dedup_guard_completes_stranded_waiter_as_submit_stalled() {
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal, engine.clone());

    // Register a waiter directly for a sequence at/below `applied`
    // (0 at start) — exactly what the apply loop's dedup guard would
    // see on a stale-redelivery, and route it through the same
    // `complete_stale` the guard calls.
    let permit = coord.mutation_gate.shared().await.unwrap();
    let rx = coord
        .register_waiter(0, permit)
        .expect("register waiter for seq 0");
    coord.complete_stale(0);
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(2), rx)
        .await
        .expect("complete_stale must resolve the waiter promptly, not hang")
        .expect("oneshot must not be dropped without a send");
    let err = outcome.expect_err("a dedup-skipped sequence must not report a fake success");
    assert!(
        err.downcast_ref::<SubmitStalled>().is_some(),
        "expected SubmitStalled, got: {err}"
    );
}

#[tokio::test]
async fn unresolved_head_result_survives_waiter_registration_race() {
    let coord = WriteCoordinator::start(Arc::new(MemWal::new()), Arc::new(Engine::new()));
    coord.fail_unresolved(
        1,
        Err(anyhow::Error::new(RestartRequired(
            "committed source is unresolved".into(),
        ))),
        None,
    );

    let permit = coord.mutation_gate.shared().await.unwrap();
    let receiver = coord
        .register_waiter(1, permit)
        .expect("late waiter must receive the unresolved head result");
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), receiver)
        .await
        .expect("unresolved head result must not wait for submit timeout")
        .expect("unresolved head result must be sent")
        .expect_err("unresolved head cannot report success");
    assert!(error.downcast_ref::<RestartRequired>().is_some(), "{error}");
    assert_eq!(coord.applied_seq(), 0);
}

#[tokio::test]
async fn unbounded_committed_delivery_latches_at_the_last_applied_head() {
    let engine = Arc::new(Engine::with_change_budget(ChangeBudget::with_hard_limit(1)));
    let wal = Arc::new(MemWal::new());
    for collection_id in ["first", "second"] {
        wal.publish(WalRecord::new(RaftLogEntry::CreateCollection {
            collection_id: collection_id.into(),
            req: keyword_schema(),
        }))
        .await
        .unwrap();
    }

    let coord = WriteCoordinator::start(wal, engine.clone());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !coord.is_restart_required() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an unbounded committed delivery must require restart");

    assert_eq!(coord.applied_seq(), 0);
    assert!(engine.list_collections().unwrap().is_empty());
}
