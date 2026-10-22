use super::*;
use std::time::{Duration, Instant};

fn owner_count(budget: &ChangeBudget) -> usize {
    budget.0.state.lock().unwrap().owners.len()
}

#[test]
fn source_retention_guard_keeps_a_panicking_reservation_charged() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(7).unwrap();
    let guard = reservation.source_retention();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _reservation = reservation;
        panic!("injected apply-worker panic");
    }));
    assert!(result.is_err());
    assert_eq!(budget.snapshot().reserved, 7);
    drop(guard);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn source_retention_guard_releases_the_final_reserved_bytes_on_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(7).unwrap();
    let guard = reservation.source_retention();
    drop(reservation);
    assert_eq!(budget.snapshot().reserved, 7);
    drop(guard);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn source_retention_guard_bridges_commit_before_journal_adoption() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(7).unwrap();
    let guard = reservation.source_retention();
    let charge = reservation.commit_retained().unwrap();
    drop(charge);
    assert_eq!(budget.snapshot().total, 7);
    drop(guard);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn source_retention_commit_after_guard_drop_does_not_deadlock() {
    let budget = ChangeBudget::with_hard_limit(16);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let owner = budget.owner();
        let mut reservation = owner.try_reserve(7).unwrap();
        let guard = reservation.source_retention();
        drop(guard);
        let charge = reservation.commit_retained().unwrap();
        drop(charge);
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_ok(),
        "commit_retained must not drop a retained charge while holding the budget lock"
    );
}

#[test]
fn source_retention_guard_uses_the_reservation_final_size() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(4).unwrap();
    let guard = reservation.source_retention();
    reservation.try_grow_to(9).unwrap();
    reservation.shrink_to(6).unwrap();
    drop(reservation);
    assert_eq!(budget.snapshot().reserved, 6);
    drop(guard);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn split_off_keeps_owner_total_and_source_retention_on_apply_half() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let mut apply = owner.try_reserve(10).unwrap();
    let guard = apply.source_retention();
    let wake = budget.checkpoint_wake();
    let epoch = wake.epoch();
    let transient = apply.split_off(4);

    assert_eq!(apply.bytes(), 6);
    assert_eq!(transient.bytes(), 4);
    assert_eq!(budget.snapshot().reserved, 10);
    assert_eq!(
        wake.epoch(),
        epoch,
        "a handle-only split must not wake capacity waiters"
    );

    // The transient half has no source bridge and releases independently.
    drop(transient);
    assert_eq!(budget.snapshot().reserved, 6);
    drop(apply);
    assert_eq!(
        budget.snapshot().reserved,
        6,
        "source retention must stay on the apply half"
    );
    drop(guard);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn reservation_without_source_retention_still_releases_on_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let reservation = owner.try_reserve(7).unwrap();
    drop(reservation);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn blocking_admission_announces_pressure_only_until_capacity_is_acquired() {
    let budget = ChangeBudget::with_hard_limit(8);
    let owner = Arc::new(budget.owner());
    let occupied = owner.try_reserve(8).unwrap();
    assert!(owner.try_reserve(1).is_err());
    assert!(!budget.has_capacity_waiters());
    let worker = std::thread::spawn({
        let owner = owner.clone();
        move || owner.wait_reserve(1).unwrap()
    });
    let deadline = Instant::now() + Duration::from_millis(200);
    while !budget.has_capacity_waiters() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let reported = budget.has_capacity_waiters();
    // Always unblock before asserting, including against a broken signal.
    drop(occupied);
    let admitted = worker.join().unwrap();
    assert!(!budget.has_capacity_waiters());
    drop(admitted);
    assert!(
        reported,
        "blocked admission must announce capacity pressure"
    );
}

#[test]
fn blocking_growth_announces_pressure_without_an_apply_lease() {
    let budget = ChangeBudget::with_hard_limit(8);
    let owner = budget.owner();
    let mut growing = owner.try_reserve(1).unwrap();
    let occupied = owner.try_reserve(7).unwrap();
    let worker = std::thread::spawn(move || {
        growing.wait_grow_to(8).unwrap();
        growing
    });
    let deadline = Instant::now() + Duration::from_millis(200);
    while !budget.has_capacity_waiters() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let reported = budget.has_capacity_waiters();
    drop(occupied);
    let admitted = worker.join().unwrap();
    assert_eq!(admitted.bytes(), 8);
    assert!(!budget.has_capacity_waiters());
    drop(admitted);
    assert!(
        reported,
        "blocked repricing must announce capacity pressure"
    );
}

#[test]
fn ended_owner_namespaces_do_not_accumulate_in_a_live_process() {
    let budget = ChangeBudget::with_hard_limit(16);
    let _live = budget.owner();
    for _ in 0..4096 {
        let ended = budget.owner();
        let retained = ended.try_reserve(7).unwrap().commit_retained().unwrap();
        drop(ended);
        assert_eq!(budget.snapshot().total, 7);
        drop(retained);
    }
    assert_eq!(budget.snapshot().total, 0);
    assert_eq!(owner_count(&budget), 1);
}

#[test]
fn escaped_reservations_keep_the_namespace_until_their_last_handle_drops() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let reserved = owner.try_reserve(7).unwrap();
    let empty = owner.try_reserve(0).unwrap();
    drop(owner);
    assert_eq!(budget.snapshot().reserved, 7);
    let retained = reserved.commit_retained().unwrap();
    assert_eq!(budget.snapshot().active, 7);
    drop(retained);
    assert_eq!(budget.snapshot().total, 0);
    assert_eq!(owner_count(&budget), 1);
    let retained_empty = empty.commit_retained().unwrap();
    drop(retained_empty);
    assert_eq!(owner_count(&budget), 0);
}

#[test]
fn escaped_frozen_batch_can_publish_after_owner_drop_and_then_be_reclaimed() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let active = owner.try_reserve(7).unwrap().commit().unwrap();
    let batch = owner.freeze().unwrap();
    drop(owner);
    drop(active);
    assert_eq!(budget.snapshot().frozen, 7);
    assert_eq!(batch.publish().unwrap(), 7);
    assert_eq!(budget.snapshot().total, 0);
    assert_eq!(owner_count(&budget), 0);
}

#[test]
fn dropping_all_handles_cannot_forgive_unpublished_committed_bytes() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let charge = owner.try_reserve(7).unwrap().commit().unwrap();
    drop(owner);
    drop(charge);
    assert_eq!(budget.snapshot().active, 7);
    assert_eq!(owner_count(&budget), 1);
}

mod limit_and_revision;

mod publication;

mod reservation_size;

mod retained;

mod wake;
