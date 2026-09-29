use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::tests::{
    capacity_schema, current_manifest, field_delta_count, has_capacity_value, index_fields,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    MergeObserver, MergePhase, SegmentRdbStore,
};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

const CASE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default)]
struct BlockingMergeState {
    before_encode: u64,
    before_publish: u64,
    after_publish: u64,
    release_first_encode: bool,
    release_second_publish: bool,
}

#[derive(Default)]
struct BlockingMergeObserver {
    state: Mutex<BlockingMergeState>,
    changed: Condvar,
}

struct ObserverRelease(Arc<BlockingMergeObserver>);

impl Drop for ObserverRelease {
    fn drop(&mut self) {
        self.0.release_all();
    }
}

impl BlockingMergeObserver {
    fn wait_until(&self, predicate: impl Fn(&BlockingMergeState) -> bool, message: &str) {
        let deadline = Instant::now() + CASE_TIMEOUT;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while !predicate(&state) {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "{message}");
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }

    fn release_first_encode(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.release_first_encode = true;
        self.changed.notify_all();
    }

    fn release_all(&self) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.release_first_encode = true;
        state.release_second_publish = true;
        self.changed.notify_all();
    }

    fn held_observations(&self) -> (u64, u64, u64) {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        (
            state.before_encode,
            state.before_publish,
            state.after_publish,
        )
    }
}

impl MergeObserver for BlockingMergeObserver {
    fn observe(&self, phase: MergePhase) -> std::io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match phase {
            MergePhase::BeforeEncode => {
                state.before_encode += 1;
                self.changed.notify_all();
                if state.before_encode == 1 {
                    while !state.release_first_encode {
                        state = self.changed.wait(state).unwrap_or_else(|p| p.into_inner());
                    }
                }
            }
            MergePhase::BeforePublish => {
                state.before_publish += 1;
                self.changed.notify_all();
                if state.before_publish >= 2 {
                    while !state.release_second_publish {
                        state = self.changed.wait(state).unwrap_or_else(|p| p.into_inner());
                    }
                }
            }
            MergePhase::AfterPublish => {
                state.after_publish += 1;
                self.changed.notify_all();
            }
        }
        Ok(())
    }
}

fn capacity_progress_case_runs_in_its_own_process(test_name: &str) -> bool {
    const CHILD: &str = "LUMEN_CHECKPOINT_CAPACITY_UNIT_CHILD";
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        return false;
    }
    // This case intentionally parks the process-wide merge worker. A child
    // keeps that pause from blocking unrelated, parallel library tests.
    let mut output = tempfile::tempfile().unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD, "1")
        .stdout(std::process::Stdio::from(output.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(output.try_clone().unwrap()))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(120);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("could not observe isolated checkpoint capacity case: {error}");
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("isolated checkpoint capacity case timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    use std::io::{Read as _, Seek as _};
    output.rewind().unwrap();
    let mut diagnostic = String::new();
    output
        .take(64 * 1024)
        .read_to_string(&mut diagnostic)
        .unwrap();
    assert!(
        status.success(),
        "isolated checkpoint capacity case failed: {diagnostic}"
    );
    assert!(
        diagnostic.contains("1 passed; 0 failed"),
        "isolated checkpoint capacity case did not execute exactly its test: {diagnostic}"
    );
    true
}

#[test]
fn checkpoint_capacity_wait_yields_after_its_field_merge_while_follow_up_merge_runs() {
    if capacity_progress_case_runs_in_its_own_process(
        "persistence::application::background_merge::tests::capacity_progress::checkpoint_capacity_wait_yields_after_its_field_merge_while_follow_up_merge_runs",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let observer = Arc::new(BlockingMergeObserver::default());
    let store = SegmentRdbStore::with_merge_observer(directory.path(), observer.clone()).unwrap();
    let engine = Arc::new(Engine::new());
    let _release_on_drop = ObserverRelease(observer.clone());
    engine.create_collection("u", capacity_schema()).unwrap();
    // Publish an empty base: first-save indexed rows already form a delta.
    store.save_required(&engine, 1).unwrap();

    // `z_unrelated` never receives a write, so it never becomes an
    // eligible delta field even though it shares "u" with `a_capacity`:
    // one job now compacts every eligible field of its selected
    // collection, so leaving `z_unrelated` ineligible (rather than merely
    // shallower) is what keeps it untouched by the merge below.
    for sequence in 2..=17 {
        index_fields(&engine, &format!("a-{sequence}"), None);
        store.save_required(&engine, sequence).unwrap();
        if sequence == 5 {
            observer.wait_until(
                |state| state.before_encode >= 1,
                "first background merge must pause before encode",
            );
        }
    }
    let setup = current_manifest(&store);
    assert_eq!(field_delta_count(&setup, "a_capacity"), 16);
    assert_eq!(field_delta_count(&setup, "z_unrelated"), 0);

    let baseline_wait_entries = store.background.test_wait_entries();
    index_fields(&engine, "a-target", None);
    let (target_tx, target_rx) = mpsc::channel();
    let target_store = store.clone();
    let target_engine = engine.clone();
    let target = std::thread::spawn(move || {
        let _ = target_tx.send(target_store.save_required(&target_engine, 18));
    });
    store
        .background
        .wait_for_test_wait_entry_after(baseline_wait_entries, CASE_TIMEOUT)
        .expect("target checkpoint must reach the existing root-wide capacity wait");

    observer.release_first_encode();
    observer.wait_until(
        |state| state.before_publish >= 2 && state.after_publish >= 1,
        "first merge must publish before the follow-up merge pauses",
    );
    let held = observer.held_observations();
    assert!(
        held.0 >= 2,
        "independent eligible fields must reach the bounded parallel encode stage"
    );
    let after_first_merge = current_manifest(&store);

    let held_result = target_rx.recv_timeout(CASE_TIMEOUT);
    let completed_while_follow_up_merge_is_held = held_result.is_ok();
    let held_checkpoint =
        matches!(&held_result, Ok(Ok(_))).then(|| (current_manifest(&store), store.load_latest()));
    observer.release_all();
    let target_result = match held_result {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => target_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("target checkpoint must finish after cleanup release"),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("target checkpoint thread stopped before returning a result")
        }
    };
    target
        .join()
        .expect("target checkpoint thread must not panic");
    target_result.expect("target checkpoint must publish after root work drains");
    store
        .wait_for_merges(Duration::from_secs(10))
        .expect("background merge worker must drain before fixture teardown");
    assert!(held.0 >= 2 && held.1 >= 2 && held.2 >= 1);
    // The blocking field must be drained by the merge the waiting
    // checkpoint resumed on: the unrelated field is never eligible, so it
    // stays untouched regardless of how many eligible fields one job
    // compacts.
    assert!(field_delta_count(&after_first_merge, "a_capacity") < 16);
    assert_eq!(field_delta_count(&after_first_merge, "z_unrelated"), 0);
    if let Some((manifest, cold_result)) = held_checkpoint {
        assert_eq!(manifest.checkpoint_sequence, 18);
        for field in ["a_capacity", "z_unrelated"] {
            assert!(field_delta_count(&manifest, field) <= 16);
        }
        let (cold, sequence) = cold_result.unwrap().unwrap();
        assert_eq!(sequence, 18);
        assert!(has_capacity_value(&cold, "a-target"));
    }
    let final_manifest = current_manifest(&store);
    assert_eq!(final_manifest.checkpoint_sequence, 18);
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 18);
    assert!(has_capacity_value(&cold, "a-target"));

    assert!(
        completed_while_follow_up_merge_is_held,
        "checkpoint did not resume after capacity-field merge while follow-up root merge remained queued"
    );
}

#[test]
fn capacity_owner_merge_completes_after_capacity_progress_while_follow_up_merge_is_held() {
    if capacity_progress_case_runs_in_its_own_process(
        "persistence::application::background_merge::tests::capacity_progress::capacity_owner_merge_completes_after_capacity_progress_while_follow_up_merge_is_held",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let observer = Arc::new(BlockingMergeObserver::default());
    let store =
        Arc::new(SegmentRdbStore::with_merge_observer(directory.path(), observer.clone()).unwrap());
    let engine = Arc::new(Engine::new());
    let _release_on_drop = ObserverRelease(observer.clone());
    engine.create_collection("u", capacity_schema()).unwrap();
    store.save_required(&engine, 1).unwrap();
    for sequence in 2..=17 {
        index_fields(&engine, &format!("a-{sequence}"), None);
        store.save_required(&engine, sequence).unwrap();
        if sequence == 5 {
            observer.wait_until(
                |state| state.before_encode >= 1,
                "first background merge must pause before encode",
            );
        }
    }

    let mut fallback = None;
    crate::persistence::application::capacity::Fallback::ensure(
        &mut fallback,
        &engine,
        Some(store.clone()),
    )
    .unwrap();
    let endpoint = engine.layer_maintenance.owner().unwrap();
    let baseline_wait_entries = store.background.test_wait_entries();
    let (merge_tx, merge_rx) = mpsc::channel();
    let merge_endpoint = endpoint.clone();
    let merge = std::thread::spawn(move || {
        let _ = merge_tx
            .send(merge_endpoint.wait_for(crate::persistence::application::capacity::Work::Merge));
    });
    store
        .background
        .wait_for_test_wait_entry_after(baseline_wait_entries, CASE_TIMEOUT)
        .expect("capacity owner must enter its merge wait");

    observer.release_first_encode();
    observer.wait_until(
        |state| state.before_publish >= 2 && state.after_publish >= 1,
        "first merge must publish before the follow-up merge pauses",
    );
    let held_result = merge_rx.recv_timeout(CASE_TIMEOUT);
    let completed_while_follow_up_merge_is_held = held_result.is_ok();
    observer.release_all();
    let merge_result = match held_result {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => merge_rx
            .recv_timeout(CASE_TIMEOUT)
            .expect("capacity owner merge must finish after cleanup release"),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("capacity owner merge thread stopped before returning a result")
        }
    };
    merge
        .join()
        .expect("capacity owner merge thread must not panic");
    merge_result.expect("capacity owner merge must not fail");
    store
        .wait_for_merges(Duration::from_secs(10))
        .expect("background merge worker must drain before fixture teardown");
    assert!(
        completed_while_follow_up_merge_is_held,
        "capacity owner waited for an unrelated follow-up root merge after capacity progress"
    );
    drop(fallback);
}
