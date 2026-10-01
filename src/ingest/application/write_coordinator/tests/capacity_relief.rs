use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;

use crate::index::application::engine::{raft_dispatch::ApplyOutcome, Engine};
use crate::ingest::application::write_coordinator::tests::keyword_schema;
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::{WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

#[tokio::test]
async fn local_admission_observes_capacity_released_before_wait_at_same_revision() {
    use std::sync::{mpsc, Mutex};
    use std::time::Duration;

    use storage_durable::{CommitStep, FailureInjector, FailurePoint};
    use tracing::field::{Field, Visit};
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::Layer;

    use crate::ingest::application::write_coordinator::tests::admitted_index_entry;
    use crate::ingest::domain::change_budget::Reservation;
    use crate::persistence::application::ports::checkpoint_sink::CheckpointSink;
    use crate::persistence::application::segment_checkpoint_sink::{
        EngineWatermarkSink, SegmentCheckpointSink,
    };
    use crate::persistence::infrastructure::segment_rdb_store::diagnostic::DiagnosticCapture;
    use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

    struct HoldSync {
        entered: Mutex<Option<mpsc::Sender<()>>>,
        release: Mutex<mpsc::Receiver<()>>,
    }

    impl FailureInjector for HoldSync {
        fn check(&self, point: &FailurePoint) -> std::io::Result<()> {
            if point.step == CommitStep::SyncFile {
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    entered.send(()).unwrap();
                    self.release.lock().unwrap().recv().unwrap();
                }
            }
            Ok(())
        }
    }

    struct Release(Option<mpsc::Sender<()>>);

    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[derive(Default)]
    struct RefusalFields(serde_json::Map<String, serde_json::Value>);

    impl Visit for RefusalFields {
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().into(), value.into());
        }

        fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
    }

    struct ReleaseAtRefusal {
        budget: ChangeBudget,
        held: Mutex<Option<Reservation>>,
        observed: Arc<Mutex<Option<(RefusalFields, usize)>>>,
    }

    impl<S: Subscriber> Layer<S> for ReleaseAtRefusal {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            let mut fields = RefusalFields::default();
            event.record(&mut fields);
            if fields.0.get("event").and_then(serde_json::Value::as_str)
                != Some("segment_capacity_admission_refusal")
            {
                return;
            }
            let held = self.held.lock().unwrap().take();
            if held.is_some() {
                // The real refusal has returned Full. Release synchronously
                // before the admission loop can register its asynchronous wait.
                drop(held);
                *self.observed.lock().unwrap() = Some((fields, self.budget.snapshot().total));
            }
        }
    }

    const LIMIT: usize = 1024 * 1024;
    let budget = ChangeBudget::with_hard_limit(LIMIT);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal.clone(), engine.clone());
    let capture = DiagnosticCapture::new();
    coord.set_diagnostic_capture(capture.token());

    // Hold the real save after capture. Its owner remains registered and
    // cannot publish or consume the checkpoint request during admission.
    let dir = tempfile::tempdir().unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release = Release(Some(release_tx));
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(
            SegmentRdbStore::new_with_failure_injector(
                dir.path(),
                Arc::new(HoldSync {
                    entered: Mutex::new(Some(entered_tx)),
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        ),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    let checkpoint = tokio::spawn(async move { sink.checkpoint_now().await });
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .expect("checkpoint must reach the real SyncFile hold");
    let revision = engine.request_pending_checkpoint_revision().unwrap();
    let filler = budget
        .owner()
        .try_reserve(LIMIT - budget.snapshot().total)
        .unwrap();
    let observed = Arc::new(Mutex::new(None));
    let subscriber = tracing_subscriber::registry().with(ReleaseAtRefusal {
        budget: budget.clone(),
        held: Mutex::new(Some(filler)),
        observed: observed.clone(),
    });
    let subscriber_guard = tracing::subscriber::set_default(subscriber);
    let entry = admitted_index_entry();
    let admitted = coord
        .admit_local_record_with_relief(
            &entry,
            tokio::time::Instant::now() + Duration::from_millis(250),
        )
        .await;
    drop(subscriber_guard);

    let (refusal, pending_after_release) = observed.lock().unwrap().take().unwrap();
    assert_eq!(refusal.0["capacity_request_revision"], revision);
    assert_eq!(refusal.0["capacity_request_present"], true);
    assert_eq!(refusal.0["hard_limit_bytes"], LIMIT);
    let used = refusal.0["used_bytes"].as_u64().unwrap() as usize;
    let requested = refusal.0["requested_bytes"].as_u64().unwrap() as usize;
    assert!(used + requested > LIMIT, "the first attempt must be Full");
    assert!(pending_after_release < used);
    assert!(pending_after_release + requested <= LIMIT);
    assert_eq!(
        engine
            .capacity_owner_state()
            .unwrap()
            .checkpoint_request_revision,
        Some(revision),
        "capacity release must leave the same checkpoint request pending"
    );
    assert_eq!(wal.latest_seq().await.unwrap(), 0);
    assert_eq!(coord.applied_seq(), 0);
    assert_eq!(engine.stats("u").unwrap().documents_indexed, 0);
    assert!(budget.high_water_bytes() <= LIMIT);

    // Join the held checkpoint before checking the admission outcome. This
    // also keeps the failing regression's cleanup bounded and complete.
    drop(release);
    assert!(tokio::time::timeout(Duration::from_secs(5), checkpoint)
        .await
        .unwrap()
        .unwrap()
        .unwrap());
    assert!(
        admitted.is_ok(),
        "capacity released before waiting must admit at the same pending revision: {:?}",
        admitted.err()
    );
}

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
