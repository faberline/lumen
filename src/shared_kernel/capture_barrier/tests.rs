use super::*;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const LIMIT: Duration = Duration::from_secs(2);

#[test]
fn timing_counts_one_completed_apply_interval_for_nested_guards() {
    let barrier = CaptureBarrier::default();
    let outer = barrier.apply();
    let inner = barrier.apply();
    assert_eq!(barrier.timing().apply.count, 0);
    drop(outer);
    assert_eq!(barrier.timing().apply.count, 0);
    drop(inner);
    let timing = barrier.timing();
    assert_eq!(timing.apply.count, 1);
    assert_eq!(timing.apply.total_ns, timing.apply.max_ns);
    assert_eq!(timing.capture, HoldTiming::default());
}

#[test]
fn timing_records_only_granted_completed_captures() {
    let barrier = CaptureBarrier::default();
    let capture = barrier.capture(0).unwrap();
    assert_eq!(barrier.timing().capture.count, 0);
    drop(capture);
    assert_eq!(barrier.timing().capture.count, 1);
    let inhibition = barrier.apply().inhibit_checkpoints_for_restore();
    assert!(barrier.capture(0).is_err());
    assert_eq!(barrier.timing().capture.count, 1);
    drop(inhibition);
    drop(barrier.capture(0).unwrap());
    let timing = barrier.timing();
    assert_eq!(timing.capture.count, 2);
    assert!(timing.capture.total_ns >= timing.capture.max_ns);
}

#[test]
fn mutation_stamp_reads_epoch_and_apply_revision_together() {
    let barrier = CaptureBarrier::default();
    assert_eq!(
        barrier.mutation_stamp(),
        MutationStamp {
            epoch: 0,
            apply_revision: 0,
        }
    );
    drop(barrier.apply());
    assert_eq!(
        barrier.mutation_stamp(),
        MutationStamp {
            epoch: 0,
            apply_revision: 1,
        }
    );
    barrier.apply().replace_epoch();
    assert_eq!(
        barrier.mutation_stamp(),
        MutationStamp {
            epoch: 1,
            apply_revision: 2,
        }
    );
}

#[test]
fn nested_apply_finishes_a_record_before_queued_capture() {
    let barrier = Arc::new(CaptureBarrier::default());
    let apply = barrier.apply();
    apply.initialize_sequence(8);
    let other = barrier.clone();
    let (tx, rx) = mpsc::channel();
    let capture = thread::spawn(move || tx.send(other.capture(0).unwrap().stamp()).unwrap());
    // Wait for the actual queued capture, not merely a thread start.
    let mut state = barrier.state.lock().unwrap();
    while state.waiting_captures == 0 {
        let (next, timeout) = barrier.changed.wait_timeout(state, LIMIT).unwrap();
        assert!(!timeout.timed_out(), "capture never queued");
        state = next;
    }
    drop(state);
    let nested = barrier.apply();
    nested.advance_sequence(9);
    drop(nested);
    assert!(rx.try_recv().is_err(), "capture crossed unfinished record");
    drop(apply);
    assert_eq!(rx.recv_timeout(LIMIT).unwrap().sequence, 9);
    capture.join().unwrap();
}

#[test]
fn capture_excludes_new_apply() {
    let barrier = Arc::new(CaptureBarrier::default());
    let capture = barrier.capture(17).unwrap();
    let other = barrier.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let apply = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _apply = other.apply();
        done_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(LIMIT).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(capture);
    done_rx.recv_timeout(LIMIT).unwrap();
    apply.join().unwrap();
}

#[test]
fn ordinary_apply_keeps_epoch_and_advances_capture_cut() {
    let barrier = CaptureBarrier::default();
    let old = barrier.capture(4).unwrap().stamp();
    barrier.apply().advance_sequence(6);
    let publication = barrier.capture(99).unwrap();
    assert_eq!(publication.stamp().sequence, 6);
    assert!(publication.validate_publish(old).is_ok());
}

#[test]
fn uncertain_apply_blocks_capture_even_after_restore() {
    let barrier = CaptureBarrier::default();
    let apply = barrier.apply();
    apply.mark_uncertain();
    apply.replace_epoch();
    drop(apply);
    assert!(barrier.capture(4).is_err());
}

#[test]
fn panic_in_apply_latches_uncertainty_before_lease_opens() {
    let barrier = Arc::new(CaptureBarrier::default());
    let other = barrier.clone();
    assert!(thread::spawn(move || {
        let _apply = other.apply();
        panic!("partial apply");
    })
    .join()
    .is_err());
    assert!(barrier.capture(4).is_err());
}

#[test]
fn preparation_revision_changes_only_after_complete_mutation_intervals() {
    let barrier = CaptureBarrier::default();
    assert_eq!(barrier.apply_revision(), 0);
    let outer = barrier.apply();
    let nested = barrier.apply();
    drop(nested);
    assert_eq!(
        barrier.apply_revision(),
        0,
        "nested calls must not invalidate their own record"
    );
    drop(outer);
    assert_eq!(
        barrier.apply_revision(),
        1,
        "empty metadata mutation intervals must invalidate detached preparation"
    );
    let capture = barrier.capture(0).unwrap();
    assert_eq!(barrier.apply_revision(), 1);
    drop(capture);
    assert_eq!(
        barrier.apply_revision(),
        2,
        "checkpoint binding can replace immutable readers without changing document version"
    );
}

mod publication_pin;

mod restore_inhibition;
