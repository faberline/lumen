use std::sync::{mpsc, Arc};
use std::thread::{self};
use std::time::Duration;

use crate::shared_kernel::capture_barrier::tests::LIMIT;
use crate::shared_kernel::capture_barrier::{CaptureBarrier, RestoreInhibition};

#[test]
fn queued_capture_refuses_when_held_apply_becomes_restore_inhibition() {
    let barrier = Arc::new(CaptureBarrier::default());
    let apply = barrier.apply();
    apply.initialize_sequence(8);
    let other = barrier.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let capture = thread::spawn(move || {
        let result = other.capture(0).map(|_| ()).map_err(str::to_owned);
        result_tx.send(result).unwrap();
    });
    let mut state = barrier.state.lock().unwrap();
    while state.waiting_captures == 0 {
        let (next, timeout) = barrier.changed.wait_timeout(state, LIMIT).unwrap();
        assert!(!timeout.timed_out(), "capture never queued");
        state = next;
    }
    drop(state);

    let inhibition = apply.inhibit_checkpoints_for_restore();
    assert_eq!(
        result_rx.recv_timeout(LIMIT).unwrap().err().unwrap(),
        "checkpoint refused: durable restore is in progress"
    );
    capture.join().unwrap();
    drop(inhibition);
}

#[test]
fn restore_inhibition_keeps_apply_open_preserves_sequence_and_clears_on_drop() {
    fn assert_send<T: Send>() {}
    assert_send::<RestoreInhibition<'_>>();

    let barrier = CaptureBarrier::default();
    let initial = barrier.apply();
    initial.initialize_sequence(8);
    let inhibition = initial.inhibit_checkpoints_for_restore();
    let apply = inhibition.activation_apply();
    apply.advance_sequence(9);
    drop(apply);
    assert_eq!(
        barrier.capture(0).err().unwrap(),
        "checkpoint refused: durable restore is in progress"
    );
    drop(inhibition);
    assert_eq!(barrier.capture(0).unwrap().stamp().sequence, 9);
}

#[test]
fn restore_inhibition_blocks_ordinary_apply_but_allows_its_activation_lease() {
    let barrier = Arc::new(CaptureBarrier::default());
    let inhibition = barrier.apply().inhibit_checkpoints_for_restore();
    let other = barrier.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _apply = other.apply();
        done_tx.send(()).unwrap();
    });
    started_rx.recv_timeout(LIMIT).unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
        "ordinary apply crossed a durable restore inhibition"
    );

    let activation = inhibition.activation_apply();
    let nested = barrier.apply();
    drop(nested);
    drop(activation);
    assert!(
        done_rx.recv_timeout(Duration::from_millis(20)).is_err(),
        "dropping only the activation lease reopened ordinary apply"
    );
    drop(inhibition);
    done_rx.recv_timeout(LIMIT).unwrap();
    waiter.join().unwrap();
}

#[test]
fn restore_inhibition_invalidates_prior_capture_stamp() {
    let barrier = CaptureBarrier::default();
    barrier.apply().initialize_sequence(8);
    let old = barrier.capture(0).unwrap().stamp();
    let inhibition = barrier.apply().inhibit_checkpoints_for_restore();
    drop(inhibition);
    let publication = barrier.capture(0).unwrap();
    assert!(publication.validate_publish(old).is_err());
}

#[test]
fn panic_in_restore_inhibition_latches_uncertainty_before_reopening_capture() {
    let barrier = Arc::new(CaptureBarrier::default());
    let other = barrier.clone();
    assert!(thread::spawn(move || {
        let _inhibition = other.apply().inhibit_checkpoints_for_restore();
        panic!("durable restore panicked");
    })
    .join()
    .is_err());
    assert_eq!(
        barrier.capture(0).err().unwrap(),
        "checkpoint refused: durability is uncertain; restart required"
    );
}
