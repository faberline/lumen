use std::sync::{mpsc, Arc};
use std::thread::{self};

use crate::shared_kernel::capture_barrier::tests::LIMIT;
use crate::shared_kernel::capture_barrier::{CaptureBarrier, PublicationPin};

#[test]
fn publication_rejects_stamp_from_before_restore() {
    let barrier = CaptureBarrier::default();
    let old = barrier.capture(4).unwrap().stamp();
    barrier.apply().replace_epoch();
    let publication = barrier.capture(5).unwrap();
    assert!(publication.validate_publish(old).is_err());
}

#[test]
fn publication_pin_allows_apply_and_wakes_epoch_replacement() {
    let barrier = Arc::new(CaptureBarrier::default());
    let initialization = barrier.apply();
    initialization.initialize_sequence(9);
    drop(initialization);
    let capture = barrier.capture(4).unwrap();
    let pin = capture.publication_pin(capture.stamp()).unwrap();
    drop(capture);

    let apply_barrier = barrier.clone();
    let (apply_tx, apply_rx) = mpsc::channel();
    let apply = thread::spawn(move || {
        let _apply = apply_barrier.apply();
        apply_tx.send(()).unwrap();
    });
    apply_rx.recv_timeout(LIMIT).unwrap();
    apply.join().unwrap();

    let (wait_tx, wait_rx) = mpsc::channel();
    barrier.state.lock().unwrap().publication_wait_observer = Some(wait_tx);
    let replacement_barrier = barrier.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let replacement = thread::spawn(move || {
        let apply = replacement_barrier.apply();
        apply.replace_epoch();
        done_tx.send(()).unwrap();
    });
    wait_rx.recv_timeout(LIMIT).unwrap();
    let state = barrier.state.lock().unwrap();
    assert_eq!(
        state.epoch, 0,
        "epoch changed before the publication pin dropped"
    );
    assert_eq!(state.sequence, Some(9));
    assert!(
        done_rx.try_recv().is_err(),
        "epoch replacement crossed a live publication pin"
    );
    drop(state);
    drop(pin);
    done_rx.recv_timeout(LIMIT).unwrap();
    replacement.join().unwrap();
    let state = barrier.state.lock().unwrap();
    assert_eq!(state.epoch, 1);
    assert_eq!(
        state.sequence, None,
        "epoch replacement must clear the sequence"
    );
    assert_eq!(state.publication_pins, 0);
}

#[test]
fn publication_pin_blocks_restore_inhibition_until_drop() {
    let barrier = Arc::new(CaptureBarrier::default());
    let initialization = barrier.apply();
    initialization.initialize_sequence(9);
    drop(initialization);
    let capture = barrier.capture(4).unwrap();
    let pin = capture.publication_pin(capture.stamp()).unwrap();
    drop(capture);
    let (wait_tx, wait_rx) = mpsc::channel();
    barrier.state.lock().unwrap().publication_wait_observer = Some(wait_tx);
    let other = barrier.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let restore = thread::spawn(move || {
        let apply = other.apply();
        let inhibition = apply.inhibit_checkpoints_for_restore();
        done_tx.send(()).unwrap();
        drop(inhibition);
    });
    wait_rx.recv_timeout(LIMIT).unwrap();
    let state = barrier.state.lock().unwrap();
    assert_eq!(
        state.epoch, 0,
        "restore changed epoch before the publication pin dropped"
    );
    assert_eq!(state.restore_inhibitions, 0);
    assert_eq!(
        state.sequence,
        Some(9),
        "restore inhibition must preserve sequence"
    );
    assert!(
        done_rx.try_recv().is_err(),
        "restore inhibition crossed a live publication pin"
    );
    drop(state);
    drop(pin);
    done_rx.recv_timeout(LIMIT).unwrap();
    restore.join().unwrap();
    let state = barrier.state.lock().unwrap();
    assert_eq!(state.epoch, 1);
    assert_eq!(state.restore_inhibitions, 0);
    assert_eq!(state.sequence, Some(9));
    assert_eq!(state.publication_pins, 0);
}

#[test]
fn publication_pin_rejects_a_stamp_invalidated_before_conversion() {
    let barrier = CaptureBarrier::default();
    let capture = barrier.capture(4).unwrap();
    let stamp = capture.stamp();
    drop(capture);
    barrier.apply().replace_epoch();
    let publication = barrier.capture(5).unwrap();
    assert!(publication.publication_pin(stamp).is_err());
}

#[test]
fn panicking_publication_pin_latches_uncertainty() {
    fn assert_send<T: Send>() {}
    assert_send::<PublicationPin<'_>>();

    let barrier = Arc::new(CaptureBarrier::default());
    let capture = barrier.capture(4).unwrap();
    let pin = capture.publication_pin(capture.stamp()).unwrap();
    drop(capture);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _pin = pin;
        panic!("pointer publication panicked");
    }))
    .is_err());
    assert!(barrier.is_uncertain());
    assert_eq!(barrier.state.lock().unwrap().publication_pins, 0);
}

#[test]
fn publication_pin_overflow_refuses_without_changing_the_count() {
    let barrier = CaptureBarrier::default();
    let capture = barrier.capture(4).unwrap();
    {
        let mut state = barrier.state.lock().unwrap();
        state.publication_pins = usize::MAX;
    }
    assert_eq!(
        capture.publication_pin(capture.stamp()).err(),
        Some("checkpoint publication pin overflow")
    );
    assert_eq!(barrier.state.lock().unwrap().publication_pins, usize::MAX);
    barrier.state.lock().unwrap().publication_pins = 0;
}
