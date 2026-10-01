use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget, Snapshot};

#[test]
fn cancelled_reservation_releases_only_reserved_bytes() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let reservation = owner.try_reserve(7).unwrap();
    assert_eq!(budget.snapshot().reserved, 7);
    drop(reservation);
    assert_eq!(
        budget.snapshot(),
        Snapshot {
            reserved: 0,
            active: 0,
            frozen: 0,
            total: 0,
            work_revision: 0,
            checkpoint_request_revision: None,
        }
    );
}

#[test]
fn failed_growth_keeps_the_original_reservation() {
    let budget = ChangeBudget::with_hard_limit(10);
    let owner = budget.owner();
    let other = budget.owner();
    let mut reservation = owner.try_reserve(4).unwrap();
    let _full = other.try_reserve(6).unwrap();
    assert_eq!(
        reservation.try_grow_to(5),
        Err(AdmissionError::Full {
            requested: 5,
            used: 10,
            hard_limit: 10
        })
    );
    assert_eq!(reservation.bytes(), 4);
    assert_eq!(budget.snapshot().reserved, 10);
}

#[test]
fn growth_charges_only_the_delta_not_a_second_reservation() {
    let budget = ChangeBudget::with_hard_limit(10);
    let owner = budget.owner();
    let other = budget.owner();
    let mut reservation = owner.try_reserve(4).unwrap();
    let _other = other.try_reserve(4).unwrap();
    reservation.try_grow_to(6).unwrap();
    assert_eq!(reservation.bytes(), 6);
    assert_eq!(budget.snapshot().reserved, 10);
}

#[test]
fn waiting_growth_wakes_when_only_its_delta_becomes_available() {
    let budget = ChangeBudget::with_hard_limit(10);
    let owner = Arc::new(budget.owner());
    let other = budget.owner();
    let release_owner = budget.owner();
    let reservation = owner.try_reserve(4).unwrap();
    let _other = other.try_reserve(4).unwrap();
    let charge = release_owner.try_reserve(2).unwrap().commit().unwrap();
    let frozen = release_owner.freeze().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reservation = reservation;
        started_tx.send(()).unwrap();
        let result = reservation.wait_grow_to(6).map(|()| reservation);
        result_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(charge);
    assert_eq!(frozen.publish().unwrap(), 2);
    let grown = result_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap()
        .unwrap();
    assert_eq!(grown.bytes(), 6);
    assert_eq!(budget.snapshot().reserved, 10);
}

#[test]
fn grown_reservation_commits_and_drops_exactly_once() {
    let budget = ChangeBudget::with_hard_limit(12);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(3).unwrap();
    reservation.try_grow_to(7).unwrap();
    let charge = reservation.commit().unwrap();
    assert_eq!(budget.snapshot().reserved, 0);
    assert_eq!(budget.snapshot().active, 7);
    drop(charge);
    assert_eq!(budget.snapshot().active, 7);
}

#[test]
fn oversized_growth_retains_old_reservation() {
    let budget = ChangeBudget::with_hard_limit(8);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(3).unwrap();
    assert_eq!(
        reservation.try_grow_to(9),
        Err(AdmissionError::Oversized {
            requested: 9,
            hard_limit: 8
        })
    );
    assert_eq!(reservation.bytes(), 3);
    assert_eq!(budget.snapshot().reserved, 3);
}

#[test]
fn shrinking_reservation_wakes_a_competing_waiter_without_lowering_high_water() {
    let budget = ChangeBudget::with_hard_limit(20);
    let owner = budget.owner();
    let waiter_owner = Arc::new(budget.owner());
    let mut reservation = owner.try_reserve(16).unwrap();
    assert_eq!(budget.high_water_bytes(), 16);

    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = waiter_owner.clone();
    std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx.send(waiter.wait_reserve(8)).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(20)).is_err());

    reservation.shrink_to(12).unwrap();
    assert_eq!(reservation.bytes(), 12);
    // Shrinking wakes the other thread. Its eight bytes may already be
    // reserved here, so observe the process total at the channel handoff.

    let competing = result_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("shrink must wake capacity waiter")
        .unwrap();
    assert_eq!(budget.snapshot().reserved, 20);
    assert_eq!(budget.high_water_bytes(), 20);
    drop(competing);
    assert_eq!(budget.snapshot().reserved, 12);
    drop(reservation);
    assert_eq!(budget.snapshot().reserved, 0);
    assert_eq!(budget.high_water_bytes(), 20);
}

#[test]
fn shrinking_reservation_preserves_other_owner_and_drop_releases_only_remainder() {
    let budget = ChangeBudget::with_hard_limit(20);
    let first = budget.owner();
    let second = budget.owner();
    let mut reservation = first.try_reserve(12).unwrap();
    let other = second.try_reserve(4).unwrap();

    reservation.shrink_to(5).unwrap();
    assert_eq!(budget.snapshot().reserved, 9);
    assert_eq!(budget.high_water_bytes(), 16);
    drop(reservation);
    assert_eq!(budget.snapshot().reserved, 4);
    drop(other);
    assert_eq!(budget.snapshot().reserved, 0);
    assert_eq!(budget.high_water_bytes(), 16);
}

#[test]
fn shrinking_refuses_a_larger_total_without_changing_the_reservation() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(5).unwrap();
    assert_eq!(
        reservation.shrink_to(6),
        Err(AdmissionError::CannotShrink {
            current: 5,
            requested: 6,
        })
    );
    assert_eq!(reservation.bytes(), 5);
    assert_eq!(budget.snapshot().reserved, 5);
}
