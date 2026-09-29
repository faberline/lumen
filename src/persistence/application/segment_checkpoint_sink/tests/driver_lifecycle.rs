use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::index::application::engine::Engine;
use crate::ingest::application::write_coordinator::WriteSink;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::application::segment_checkpoint_sink::driver::spawn_budget_waiter;
use crate::persistence::application::segment_checkpoint_sink::tests::{
    admitted_keyword, contains_keyword, spill_keyword_schema, wait_for_checkpoint_count, Watermark,
};
use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_waits_for_the_started_checkpoint_write() {
    struct HoldWrite {
        armed: AtomicBool,
        entered: std::sync::mpsc::Sender<()>,
        released: (Mutex<bool>, std::sync::Condvar),
    }
    impl storage_durable::FailureInjector for HoldWrite {
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
    struct ReleaseWrite(Arc<HoldWrite>);
    impl Drop for ReleaseWrite {
        fn drop(&mut self) {
            *self.0.released.0.lock().unwrap() = true;
            self.0.released.1.notify_all();
        }
    }
    let root = tempfile::tempdir().unwrap();
    let budget = ChangeBudget::with_hard_limit(8 * 1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "kept", "kept");
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let hold = Arc::new(HoldWrite {
        armed: AtomicBool::new(false),
        entered: entered_tx,
        released: (Mutex::new(false), std::sync::Condvar::new()),
    });
    let store =
        Arc::new(SegmentRdbStore::new_with_failure_injector(root.path(), hold.clone()).unwrap());
    // Establish a durable predecessor first, then create a real changed
    // checkpoint payload. This makes the injected SyncFile pause belong
    // to an actual SegmentRdbStore write.
    store.save(&engine, 1).unwrap();
    admitted_keyword(&engine, "after-base", "after-base");
    let successor_apply = engine.capture_barrier.apply();
    successor_apply.initialize_sequence(2);
    drop(successor_apply);
    let release = ReleaseWrite(hold.clone());
    hold.armed.store(true, Ordering::Release);
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    // Force the periodic checkpoint path to observe the real write. The
    // request remains a wake hint; the zero period makes the first
    // scheduled attempt deterministic for this ownership test.
    let mut driver = sink.spawn_driver_with_budget(Duration::ZERO, budget);
    engine.request_pending_checkpoint();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .expect("checkpoint must reach the real write pause");
    assert_eq!(engine.metrics().segment_checkpoint_started_total.get(), 1);
    assert_eq!(engine.metrics().segment_checkpoint_failed_total.get(), 0);
    assert_eq!(engine.metrics().segment_checkpoint_in_flight.get(), 1);
    let stop = driver.stop.clone();
    let (finished_tx, mut finished_rx) = oneshot::channel();
    let stopping = tokio::spawn(async move {
        let result = driver.shutdown().await;
        let _ = finished_tx.send(result);
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !stop.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown must request stop");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut finished_rx)
            .await
            .is_err(),
        "graceful shutdown must not finish while its checkpoint write is paused"
    );
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), finished_rx)
        .await
        .expect("shutdown must finish after write release")
        .unwrap()
        .unwrap();
    stopping.await.unwrap();
    assert_eq!(engine.metrics().segment_checkpoint_started_total.get(), 1);
    assert_eq!(engine.metrics().segment_checkpoint_failed_total.get(), 0);
    assert_eq!(engine.metrics().segment_checkpoint_in_flight.get(), 0);
    let (cold, _) = store
        .load_latest()
        .unwrap()
        .expect("started checkpoint must finish");
    assert!(contains_keyword(&cold, "kept"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budget_wake_does_not_repeat_high_checkpoint_for_each_small_change() {
    let root = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let writer = Arc::new(Watermark(AtomicU64::new(7)));
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: writer.clone(),
        aof: None,
    });
    // This unit drives the accounting input directly. Runtime Engine
    // admission has its own HTTP contract; no large payload is allocated here.
    let budget = ChangeBudget::new();
    let owner = budget.owner();
    owner
        .try_reserve(crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER)
        .unwrap()
        .commit()
        .unwrap();
    let driver = sink.spawn_driver_with_budget(Duration::from_secs(3600), budget);
    wait_for_checkpoint_count(&engine, 1).await;
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        7
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        1,
        "unchanged high work must not spin checkpoints"
    );
    owner.try_reserve(1).unwrap().commit().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        engine.metrics().segment_checkpoint_completed_total.get(),
        1,
        "small work under the same high-water window must not recheckpoint"
    );
    assert_eq!(writer.applied_seq(), 7);
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        7
    );
    drop(driver);
    owner.retire();
}

#[test]
fn waiter_observes_change_between_subscription_and_thread_start() {
    let budget = ChangeBudget::new();
    let wake = budget.checkpoint_wake();
    let observed = wake.epoch();
    let owner = budget.owner();
    let _reservation = owner.try_reserve(1).unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    let stop = Arc::new(AtomicBool::new(false));
    let waiter = spawn_budget_waiter(wake, sender, stop.clone(), observed);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    let got_notice = loop {
        if receiver.try_recv().is_ok() {
            break true;
        }
        if std::time::Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };
    stop.store(true, Ordering::Release);
    waiter.join().unwrap();
    assert!(
        got_notice,
        "a change after subscription must not be swallowed by late thread start"
    );
}
