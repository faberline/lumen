use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::{
    CapacityWait, MergeOutcome, RootWork, WorkState,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use std::sync::Arc;
use std::time::{Duration, Instant};

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

#[test]
fn capacity_wait_returns_after_a_published_merge() {
    let work = Arc::new(RootWork::default());
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.queued = true;
    }
    let waiting = work.clone();
    let waiter = std::thread::spawn(move || {
        waiting.wait_for_capacity_progress_after(0, Instant::now() + TEST_TIMEOUT)
    });
    work.wait_for_test_wait_entry_after(0, TEST_TIMEOUT)
        .unwrap();
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.published_revision = 1;
        state.queued = false;
        work.changed.notify_all();
    }
    assert_eq!(waiter.join().unwrap().unwrap(), CapacityWait::Published);
}

#[test]
fn capacity_wait_ignores_stale_error_while_retry_is_queued() {
    let work = Arc::new(RootWork::default());
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.queued = true;
        state.error = Some("stale failed merge".into());
    }
    let waiting = work.clone();
    let waiter = std::thread::spawn(move || {
        waiting.wait_for_capacity_progress_after(0, Instant::now() + TEST_TIMEOUT)
    });
    work.wait_for_test_wait_entry_after(0, TEST_TIMEOUT)
        .unwrap();
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.published_revision = 1;
        state.queued = false;
        state.error = None;
        work.changed.notify_all();
    }
    assert_eq!(waiter.join().unwrap().unwrap(), CapacityWait::Published);
}

#[test]
fn capacity_wait_reports_terminal_error_before_published_revision() {
    let work = RootWork::default();
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.published_revision = 1;
        state.error = Some("terminal merge failure".into());
    }
    let error = work
        .wait_for_capacity_progress_after(0, Instant::now() + TEST_TIMEOUT)
        .expect_err("terminal error must take precedence over an older publication");
    assert!(format!("{error:#}").contains("terminal merge failure"));
}

#[test]
fn capacity_wait_deadline_survives_a_spurious_wake() {
    let work = Arc::new(RootWork::default());
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.running = true;
    }
    let deadline = Instant::now() + Duration::from_millis(50);
    let waiting = work.clone();
    let waiter = std::thread::spawn(move || waiting.wait_for_capacity_progress_after(0, deadline));
    work.wait_for_test_wait_entry_after(0, TEST_TIMEOUT)
        .unwrap();
    work.changed.notify_all();
    let error = waiter
        .join()
        .expect("deadline waiter must not panic")
        .expect_err("unchanged work must not turn a spurious wake into progress");
    assert!(format!("{error:#}").contains("capacity wait timed out"));
}

#[test]
fn capacity_wait_observes_ready_state_even_when_scheduled_after_deadline() {
    let work = RootWork::default();
    let expired = Instant::now() - Duration::from_secs(1);
    assert_eq!(
        work.wait_for_capacity_progress_after(0, expired).unwrap(),
        CapacityWait::Idle
    );
    work.state.lock().unwrap().published_revision = 1;
    assert_eq!(
        work.wait_for_capacity_progress_after(0, expired).unwrap(),
        CapacityWait::Published
    );
}

#[test]
fn capacity_request_rechecks_the_idle_revision_before_rejecting() {
    let mut state = WorkState::default();
    state.published_revision = 7;
    let deadline = Some(Instant::now() + TEST_TIMEOUT);
    assert!(state
        .validate_capacity_retry(Some(7), deadline)
        .unwrap_err()
        .to_string()
        .contains("no capacity progress"));
    state.published_revision = 8;
    state.validate_capacity_retry(Some(7), deadline).unwrap();
}

#[test]
fn stale_retry_is_allowed_to_retake_capacity_request_without_self_requeue() {
    let work = RootWork::default();
    {
        let mut state = work.state.lock().unwrap();
        state.running = true;
    }
    let mut result = Ok(MergeOutcome::RetryableStale);
    assert!(!work.finish_job(&mut result, true));
    {
        let state = work.state.lock().unwrap();
        assert!(!state.queued);
        assert!(state.retryable_stale);
    }
    let mut state = work.state.lock().unwrap();
    state
        .validate_capacity_retry(Some(0), Some(Instant::now() + TEST_TIMEOUT))
        .unwrap();
    state.retryable_stale = false;
    assert!(state
        .validate_capacity_retry(Some(0), Some(Instant::now() + TEST_TIMEOUT))
        .is_err());
}

#[test]
fn a_new_publication_cannot_restart_an_expired_capacity_deadline() {
    let mut state = WorkState::default();
    state.published_revision = 8;
    let expired = Some(Instant::now() - Duration::from_secs(1));
    assert!(state
        .validate_capacity_retry(Some(7), expired)
        .unwrap_err()
        .to_string()
        .contains("capacity wait timed out"));
    assert!(state
        .validate_capacity_retry(None, expired)
        .unwrap_err()
        .to_string()
        .contains("capacity wait timed out"));
}

#[test]
fn an_in_progress_capacity_retry_keeps_a_new_terminal_failure_visible() {
    let mut state = WorkState::default();
    state.published_revision = 8;
    state.error = Some("new terminal failure".into());
    let expired = Some(Instant::now() - Duration::from_secs(1));
    assert!(state
        .validate_capacity_retry(Some(7), expired)
        .unwrap_err()
        .to_string()
        .contains("new terminal failure"));
    // A later independent checkpoint may request the existing normal retry.
    state.validate_capacity_retry(None, None).unwrap();
}

#[test]
fn capacity_wait_reports_idle_without_spinning() {
    let work = RootWork::default();
    assert_eq!(
        work.wait_for_capacity_progress_after(0, Instant::now() + TEST_TIMEOUT)
            .unwrap(),
        CapacityWait::Idle
    );
}

#[test]
fn publication_revision_overflow_stops_requested_requeue() {
    let work = RootWork::default();
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.running = true;
        state.requested = true;
        state.published_revision = u64::MAX;
    }
    let mut result = Ok(MergeOutcome::Published);
    assert!(!work.finish_job(&mut result, true));
    assert!(result.is_err());
    {
        let state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.publication_revision_overflowed);
        assert!(!state.running && !state.queued && !state.requested);
        assert!(state.error.as_deref().unwrap().contains("overflowed"));
    }
    {
        let mut state = work.state.lock().unwrap_or_else(|p| p.into_inner());
        state.running = true;
        state.requested = true;
    }
    let mut later = Ok(MergeOutcome::NoEligibleWork);
    assert!(!work.finish_job(&mut later, true));
    let state = work.state.lock().unwrap_or_else(|p| p.into_inner());
    assert!(state.publication_revision_overflowed);
    assert!(!state.running && !state.queued && !state.requested);
    assert!(state.error.as_deref().unwrap().contains("overflowed"));
}

#[test]
fn publication_revision_overflow_rejects_later_request() {
    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let engine = Arc::new(Engine::new());
    {
        let mut state = store
            .background
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.publication_revision_overflowed = true;
        state.error = Some("background segment merge publication revision overflowed".into());
    }
    let error = store
        .request_merge_with_revision(&engine)
        .expect_err("a terminal publication revision overflow must reject a new request");
    assert!(format!("{error:#}").contains("overflowed"));
    let state = store
        .background
        .state
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    assert!(state.publication_revision_overflowed);
    assert!(state.error.as_deref().unwrap().contains("overflowed"));
}
