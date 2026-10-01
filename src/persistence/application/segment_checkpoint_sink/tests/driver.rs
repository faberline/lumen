use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::application::ports::checkpoint_sink::CheckpointSink;
use crate::persistence::application::segment_checkpoint_sink::tests::{
    admitted_keyword, contains_keyword, spill_keyword_schema, wait_for_checkpoint_count, FailOnce,
};
use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::domain::checkpoint_schedule::CheckpointSchedule;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[test]
fn periodic_success_bookkeeping_does_not_consume_capacity_request() {
    let budget = crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER * 2,
    );
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({
                "fields": {"kw":{"type":"keyword"}}
            }))
            .unwrap(),
        )
        .unwrap();
    engine
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"one","field":"kw","value":"retained"}]
            }))
            .unwrap(),
        )
        .unwrap();
    let _held = budget
        .owner()
        .try_reserve(crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER)
        .unwrap()
        .commit_retained()
        .unwrap();
    engine.request_pending_checkpoint();
    let request_revision = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .expect("capacity request revision");
    let root = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(root.path()).unwrap();
    let sink = SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(store),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    };
    sink.checkpoint_sync(&sink.store).unwrap();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(1), Instant::now());
    let before = engine.capacity_owner_state();
    SegmentCheckpointSink::complete_periodic_checkpoint(
        &mut schedule,
        budget.snapshot(),
        before,
        engine.capacity_owner_state(),
    );
    assert!(engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .is_some_and(|revision| revision == request_revision));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_driver_budget_notice_has_one_ordered_numeric_attempt() {
    use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
        DiagnosticCapture, NumericCanonicalEvent as Event,
    };

    let capture = DiagnosticCapture::new();
    let budget = ChangeBudget::new();
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "kept", "kept");
    let root = tempfile::tempdir().unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(SegmentRdbStore::new(root.path()).unwrap()),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    let (mut driver, idle) =
        sink.spawn_driver_with_capture(Duration::from_secs(3600), budget.clone(), capture.token());
    tokio::time::timeout(Duration::from_secs(3), idle)
        .await
        .unwrap()
        .unwrap();
    assert!(capture.drain().is_empty());
    let filler = budget.owner();
    let _held = filler
        .try_reserve(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER - budget.snapshot().total,
        )
        .unwrap()
        .commit_retained()
        .unwrap();
    wait_for_checkpoint_count(&engine, 1).await;
    driver.shutdown().await.unwrap();
    let events = capture.drain();
    let (attempt, pending) = events
        .iter()
        .find_map(|event| match event {
            Event::Selected {
                attempt_id,
                reason: "threshold",
                source: "budget_notice",
                pending_total,
            } => Some((*attempt_id, *pending_total)),
            _ => None,
        })
        .expect("real driver must select threshold after a budget notice");
    assert!(pending >= crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER);
    let phases: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Phase {
                attempt_id,
                phase,
                pass,
                reused,
                frozen_bytes,
            } if *attempt_id == attempt => Some((*phase, *pass, *reused, *frozen_bytes)),
            _ => None,
        })
        .collect();
    assert_eq!(
        phases.iter().map(|item| item.0).collect::<Vec<_>>(),
        [
            "checkpoint_started",
            "freeze_completed",
            "publish_completed",
            "terminal",
        ]
    );
    assert_eq!(phases[1].1, 1);
    assert!(!phases[1].2);
    assert!(phases[1].3 > 0, "the real local cut must report its bytes");
    assert_eq!(phases[2].1, 1);
    assert!(
        events
            .iter()
            .filter(|event| matches!(event, Event::Selected { .. }))
            .count()
            == 1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_driver_zero_period_reports_deadline_tick() {
    use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
        DiagnosticCapture, NumericCanonicalEvent as Event,
    };

    let capture = DiagnosticCapture::new();
    let budget = ChangeBudget::new();
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let root = tempfile::tempdir().unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(SegmentRdbStore::new(root.path()).unwrap()),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    let (mut driver, _) = sink.spawn_driver_with_capture(Duration::ZERO, budget, capture.token());
    wait_for_checkpoint_count(&engine, 1).await;
    driver.shutdown().await.unwrap();
    let events = capture.drain();
    assert!(matches!(
        events.first(),
        Some(Event::Selected {
            reason: "deadline",
            source: "initial_sample",
            ..
        })
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        Event::Phase {
            phase: "terminal",
            ..
        }
    )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_driver_reports_successor_after_blocked_first_publication() {
    use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
        DiagnosticCapture, NumericCanonicalEvent as Event,
    };

    struct BlockFirstWrite {
        armed: AtomicBool,
        entered: std::sync::mpsc::Sender<()>,
        released: (Mutex<bool>, std::sync::Condvar),
    }
    impl storage_durable::FailureInjector for BlockFirstWrite {
        fn check(&self, point: &storage_durable::FailurePoint) -> std::io::Result<()> {
            if point.step == storage_durable::CommitStep::SyncFile
                && self.armed.swap(false, Ordering::AcqRel)
            {
                self.entered.send(()).unwrap();
                let (lock, wake) = &self.released;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            }
            Ok(())
        }
    }
    struct Release(Arc<BlockFirstWrite>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.released.0.lock().unwrap() = true;
            self.0.released.1.notify_all();
        }
    }

    let capture = DiagnosticCapture::new();
    let budget = ChangeBudget::new();
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "kept", "kept");
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let block = Arc::new(BlockFirstWrite {
        armed: AtomicBool::new(true),
        entered: entered_tx,
        released: (Mutex::new(false), std::sync::Condvar::new()),
    });
    let release = Release(block.clone());
    let root = tempfile::tempdir().unwrap();
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: Arc::new(SegmentRdbStore::new_with_failure_injector(root.path(), block).unwrap()),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    let (mut driver, idle) =
        sink.spawn_driver_with_capture(Duration::from_secs(3600), budget.clone(), capture.token());
    tokio::time::timeout(Duration::from_secs(3), idle)
        .await
        .unwrap()
        .unwrap();
    let foreign = budget.owner();
    let first_charge = foreign
        .try_reserve(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER - budget.snapshot().total,
        )
        .unwrap()
        .commit_retained()
        .unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .expect("first real publication must reach SyncFile");
    drop(first_charge);
    let local_active = engine.capacity_owner_state().unwrap().active;
    let _late_charge = engine.test_retained_owner_charge(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER - local_active,
    );
    drop(release);
    wait_for_checkpoint_count(&engine, 2).await;
    driver.shutdown().await.unwrap();
    let events = capture.drain();
    let selected: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Selected {
                attempt_id,
                reason,
                source,
                ..
            } => Some((*attempt_id, *reason, *source)),
            _ => None,
        })
        .collect();
    assert_eq!(
        selected.len(),
        2,
        "one blocked save must schedule one successor"
    );
    assert_eq!(selected[0].1, "threshold");
    assert_eq!(selected[0].2, "budget_notice");
    assert_eq!(selected[1].1, "successor");
    assert_eq!(selected[1].2, "budget_notice");
    assert_ne!(selected[0].0, selected[1].0);
    for (attempt, _, _) in selected {
        assert!(events.iter().any(|event| matches!(event, Event::Phase { attempt_id, phase: "terminal", .. } if *attempt_id == attempt)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_driver_retries_frozen_pass_then_captures_fresh_pass() {
    use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
        DiagnosticCapture, NumericCanonicalEvent as Event,
    };

    let capture = DiagnosticCapture::new();
    let budget = ChangeBudget::with_hard_limit(8 * 1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "old", "old");
    let root = tempfile::tempdir().unwrap();
    SegmentRdbStore::new(root.path())
        .unwrap()
        .save_with_sequence(&engine, 1)
        .unwrap();
    let apply = engine.capture_barrier.apply();
    apply.initialize_sequence(1);
    drop(apply);
    admitted_keyword(&engine, "frozen", "frozen");
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            root.path(),
            Arc::new(FailOnce(Mutex::new(Some(
                storage_durable::CommitStep::SyncFile,
            )))),
        )
        .unwrap(),
    );
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    assert!(sink.checkpoint_now().await.is_err());
    admitted_keyword(&engine, "fresh", "fresh");
    let apply = engine.capture_barrier.apply();
    apply.advance_sequence(2);
    drop(apply);
    let completed_before = engine.metrics().segment_checkpoint_completed_total.get();
    let (mut driver, _) = sink.spawn_driver_with_capture(Duration::ZERO, budget, capture.token());
    wait_for_checkpoint_count(&engine, completed_before + 1).await;
    driver.shutdown().await.unwrap();
    let events = capture.drain();
    let attempt = events
        .iter()
        .find_map(|event| match event {
            Event::Selected { attempt_id, .. } => Some(*attempt_id),
            _ => None,
        })
        .unwrap();
    let freezes: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::Phase {
                attempt_id,
                phase: "freeze_completed",
                pass,
                reused,
                frozen_bytes,
            } if *attempt_id == attempt => Some((*pass, *reused, *frozen_bytes)),
            _ => None,
        })
        .collect();
    assert_eq!(freezes.len(), 2);
    assert_eq!((freezes[0].0, freezes[0].1), (1, true));
    assert_eq!((freezes[1].0, freezes[1].1), (2, false));
    assert_eq!(freezes[0].2, 0, "a reused cut has no newly frozen bytes");
    assert!(
        freezes[1].2 > 0,
        "the fresh pass freezes the newer local change"
    );
    assert!(events.iter().any(|event| matches!(event,
        Event::Phase { attempt_id, phase: "publish_completed", pass: 2, .. } if *attempt_id == attempt)));
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 2);
    assert!(contains_keyword(&cold, "fresh"));
}
