use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::ingest::domain::change_budget::ChangeBudget;

#[test]
fn wake_epoch_advances_on_reservation_cancel() {
    let budget = ChangeBudget::with_hard_limit(16);
    let wake = budget.checkpoint_wake();
    let epoch = wake.epoch();
    let reservation = budget.owner().try_reserve(3).unwrap();
    drop(reservation);
    assert!(wake.epoch() > epoch);
}

#[test]
fn wake_observes_signal_before_waiter_sleeps() {
    let budget = ChangeBudget::with_hard_limit(16);
    let wake = budget.checkpoint_wake();
    let observed = wake.epoch();
    wake.signal();
    assert!(wake.wait_for_change_timeout(observed, Duration::from_millis(1)));
}

#[tokio::test]
async fn shared_checkpoint_revision_wakes_async_waiter_once_on_completion() {
    let budget = ChangeBudget::with_hard_limit(64);
    let owner = budget.owner();
    let _charge = owner.try_reserve(16).unwrap().commit().unwrap();
    let wake = budget.checkpoint_wake();
    let revision = owner.request_checkpoint_revision().unwrap();
    let observed = wake.epoch();
    assert_eq!(owner.request_checkpoint_revision(), Some(revision));
    assert_eq!(
        wake.epoch(),
        observed,
        "same revision must not wake peers in a loop"
    );
    owner.consume_checkpoint_request(Some(revision));
    assert!(
        wake.wait_for_change_async(observed, Duration::from_millis(1))
            .await
    );
}

#[test]
fn wake_timeout_is_bounded_without_a_signal() {
    let budget = ChangeBudget::with_hard_limit(16);
    let wake = budget.checkpoint_wake();
    let started = Instant::now();
    assert!(!wake.wait_for_change_timeout(wake.epoch(), Duration::from_millis(10)));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn signal_waits_for_the_predicate_mutex_before_notifying() {
    let budget = ChangeBudget::with_hard_limit(16);
    let wake = budget.checkpoint_wake();
    let guard = wake.lock.lock().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let signal = wake.clone();
    std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        signal.signal();
        done_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(guard);
    done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
}
