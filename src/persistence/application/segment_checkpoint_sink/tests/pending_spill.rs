use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use crate::api::CheckpointSink;
use crate::ingest::application::write_coordinator::WriteSink;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::application::segment_checkpoint_sink::pending_spill::PendingChangeSpill;
use crate::persistence::application::segment_checkpoint_sink::tests::{
    admitted_keyword, contains_keyword, spill_keyword_schema, wait_for_checkpoint_count, FailOnce,
};
use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::aof::aof_writer::AofWriter;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::persistence::infrastructure::spill_directory::SpillDirectory;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::storage::Engine;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_private_spill_keeps_root_while_blocking_save_owns_store() {
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

    let budget = ChangeBudget::with_hard_limit(8 * 1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "kept", "kept");
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let hold = Arc::new(HoldWrite {
        armed: AtomicBool::new(true),
        entered: entered_tx,
        released: (Mutex::new(false), std::sync::Condvar::new()),
    });
    let release = ReleaseWrite(hold.clone());
    let root = Arc::new(SpillDirectory::create().unwrap());
    let root_path = root.path.clone();
    let sink = {
        let store = Arc::new(
            SegmentRdbStore::new_with_failure_injector(&root.path, hold)
                .unwrap()
                .with_root_guard(root.clone()),
        );
        Arc::new(SegmentCheckpointSink {
            engine: engine.clone(),
            store,
            writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
            aof: None,
        })
    };
    let running_store = Arc::downgrade(&sink.store);
    let driver = sink
        .clone()
        // This test verifies shutdown ownership after a write starts. A
        // zero period starts the periodic path immediately; request
        // revisions intentionally do not bypass the high-water schedule.
        .spawn_driver_with_budget(Duration::ZERO, budget);
    let spill = PendingChangeSpill {
        sink,
        driver: Some(driver),
        temporary_root: Some(root),
    };
    engine.request_pending_checkpoint();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(Duration::from_secs(5)))
        .await
        .unwrap()
        .expect("save must reach the injected blocking write");

    drop(spill);
    assert!(
        root_path.exists(),
        "the blocking store clone pins its private root"
    );
    // The final store Arc's strong count reaches zero before its destructor
    // necessarily finishes dropping `root_guard` on the blocking worker.
    // One deadline covers both that drain and final root reclamation.
    let completion_deadline = Instant::now() + Duration::from_secs(5);
    drop(release);
    tokio::time::timeout_at(completion_deadline, async {
        while running_store.strong_count() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted driver must let the started blocking save finish and release its store");
    assert!(contains_keyword(&engine, "kept"));
    assert!(
        root_path.join("CURRENT").is_file(),
        "blocking save must finish durable publication after driver drop"
    );
    assert!(
        root_path.exists(),
        "the live engine pins installed reader files"
    );
    drop(engine);
    tokio::time::timeout_at(completion_deadline, async {
        while root_path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("last store/engine owner must reclaim its private root within the shared deadline");
    assert!(
        !root_path.exists(),
        "last store/engine owner releases only its root"
    );
}

#[test]
fn bootstrap_watermark_reads_engine_capture_sequence() {
    let engine = Arc::new(Engine::new());
    let apply = engine.capture_barrier.apply();
    apply.initialize_sequence(41);
    drop(apply);
    assert_eq!(EngineWatermarkSink::new(engine).applied_seq(), 41);
}

#[tokio::test]
async fn private_spill_root_outlives_handle_store_and_live_cold_engines() {
    let engine = Arc::new(Engine::new());
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "kept", "kept");
    let mut spill =
        PendingChangeSpill::temporary(engine.clone(), Duration::from_secs(3600)).unwrap();
    let root = spill.temporary_root.as_ref().unwrap().path.clone();
    assert!(spill.sink.checkpoint_now().await.unwrap());
    let escaped = spill.store().clone();
    let (cold, _) = escaped.load_latest().unwrap().unwrap();
    spill.stop_bootstrap().await.unwrap();
    drop(spill);
    assert!(root.exists(), "escaped store must retain the private root");
    assert!(contains_keyword(&engine, "kept"));
    assert!(contains_keyword(&cold, "kept"));
    drop(escaped);
    assert!(root.exists(), "live engines must retain their reader paths");
    drop(engine);
    assert!(
        root.exists(),
        "cold engine must retain its installed root guard"
    );
    drop(cold);
    assert!(
        !root.exists(),
        "last reader/store drop must reclaim private root"
    );
}

#[tokio::test]
async fn failed_bootstrap_spill_retries_the_frozen_cut() {
    let root = tempfile::tempdir().unwrap();
    let budget = ChangeBudget::with_hard_limit(8 * 1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine
        .create_collection("captured", spill_keyword_schema())
        .unwrap();
    admitted_keyword(&engine, "old", "old");
    let baseline = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    baseline.save_with_sequence(&engine, 1).unwrap();
    let apply = engine.capture_barrier.apply();
    apply.initialize_sequence(1);
    drop(apply);
    admitted_keyword(&engine, "captured", "captured");
    let frozen_before_failure = budget.snapshot().active;
    assert!(
        frozen_before_failure > 0,
        "admitted captured row must own pending bytes"
    );
    let store = Arc::new(
        SegmentRdbStore::new_with_failure_injector(
            root.path(),
            Arc::new(FailOnce(Mutex::new(Some(
                storage_durable::CommitStep::SyncFile,
            )))),
        )
        .unwrap(),
    );
    let sink = SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    };
    assert!(sink.checkpoint_now().await.is_err());
    assert_eq!(engine.metrics().segment_checkpoint_started_total.get(), 1);
    assert_eq!(engine.metrics().segment_checkpoint_failed_total.get(), 1);
    assert_eq!(engine.metrics().segment_checkpoint_in_flight.get(), 0);
    assert!(
        budget.snapshot().frozen >= frozen_before_failure,
        "failed save must retain the captured charge"
    );
    admitted_keyword(&engine, "newer", "newer");
    assert!(sink.checkpoint_now().await.unwrap());
    assert_eq!(engine.metrics().segment_checkpoint_started_total.get(), 2);
    assert_eq!(engine.metrics().segment_checkpoint_failed_total.get(), 1);
    assert_eq!(engine.metrics().segment_checkpoint_in_flight.get(), 0);
    let (replayed, sequence) = sink.store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 1);
    assert!(contains_keyword(&replayed, "captured"));
    assert!(!contains_keyword(&replayed, "newer"));
    assert!(
        budget.snapshot().active > 0,
        "newer post-failure row remains charged"
    );
}

#[tokio::test]
async fn bootstrap_driver_high_budget_uses_capture_sequence_without_coordinator() {
    let budget =
        ChangeBudget::with_hard_limit(crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER + 1);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let apply = engine.capture_barrier.apply();
    apply.initialize_sequence(23);
    drop(apply);
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let sink = Arc::new(SegmentCheckpointSink {
        engine: engine.clone(),
        store: store.clone(),
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        aof: None,
    });
    let owner = budget.owner();
    let _charge = owner
        .try_reserve(crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER)
        .unwrap()
        .commit_retained()
        .unwrap();
    // Start the write through the periodic path immediately. Capacity
    // request revisions remain wake hints and do not bypass scheduling.
    let mut driver = sink.spawn_driver_with_budget(Duration::ZERO, budget);
    wait_for_checkpoint_count(&engine, 1).await;
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        23
    );
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn configured_replay_spill_checkpoints_but_never_trims_aof_tail() {
    let root = tempfile::tempdir().unwrap();
    let tail = root.path().join("aof.log");
    let mut aof = AofWriter::open(&tail).unwrap();
    aof.append(
        7,
        &WalRecord::new(RaftLogEntry::DropCollection {
            collection_id: "sentinel".to_owned(),
            force: false,
        }),
    )
    .unwrap();
    aof.sync().unwrap();
    let before = std::fs::read(&tail).unwrap();
    let engine = Arc::new(Engine::new());
    let apply = engine.capture_barrier.apply();
    apply.initialize_sequence(7);
    drop(apply);
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut spill =
        PendingChangeSpill::configured_replay(engine, store.clone(), Duration::from_secs(3600));
    assert!(spill.temporary_root.is_none());
    assert!(spill.sink.aof.is_none());
    assert!(spill.sink.checkpoint_now().await.unwrap());
    assert_eq!(
        store.load_current_generation().unwrap().unwrap().sequence,
        7
    );
    assert_eq!(
        std::fs::read(&tail).unwrap(),
        before,
        "bootstrap save must not trim replay tail"
    );
    spill.stop_bootstrap().await.unwrap();
    assert!(root.path().exists());
}
