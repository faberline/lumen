use std::sync::{mpsc, Arc};

use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget, CHECKPOINT_TRIGGER};

#[test]
fn atomic_limit_rejects_before_overflow_and_soft_trigger_is_visible() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let _a = owner.try_reserve(10).unwrap();
    assert!(matches!(
        owner.try_reserve(7),
        Err(AdmissionError::Full {
            requested: 7,
            used: 10,
            hard_limit: 16
        })
    ));
    assert!(matches!(
        owner.try_reserve(17),
        Err(AdmissionError::Oversized {
            requested: 17,
            hard_limit: 16
        })
    ));
    assert!(!budget.snapshot().checkpoint_needed());
    let normal = ChangeBudget::new();
    let normal_owner = normal.owner();
    let _trigger = normal_owner.try_reserve(CHECKPOINT_TRIGGER).unwrap();
    assert!(normal.snapshot().checkpoint_needed());
}

#[test]
fn high_water_keeps_the_largest_successful_pending_change_total() {
    let budget = ChangeBudget::with_hard_limit(32);
    let owner = budget.owner();
    let mut reservation = owner.try_reserve(8).unwrap();
    assert_eq!(budget.high_water_bytes(), 8);

    reservation.try_grow_to(12).unwrap();
    assert_eq!(budget.high_water_bytes(), 12);
    assert!(matches!(
        owner.try_reserve(21),
        Err(AdmissionError::Full {
            requested: 21,
            used: 12,
            hard_limit: 32,
        })
    ));
    assert_eq!(budget.high_water_bytes(), 12);

    let charge = reservation.commit().unwrap();
    let frozen = owner.freeze().unwrap();
    drop(charge);
    assert_eq!(budget.snapshot().frozen, 12);
    assert_eq!(frozen.publish().unwrap(), 12);
    assert_eq!(budget.snapshot().total, 0);
    assert_eq!(budget.high_water_bytes(), 12);
}

#[test]
fn high_water_is_race_safe_for_concurrent_reservations() {
    let budget = Arc::new(ChangeBudget::with_hard_limit(128));
    let gate = Arc::new(std::sync::Barrier::new(9));
    let (tx, rx) = mpsc::channel();
    let mut workers = Vec::new();
    for _ in 0..8 {
        let budget = budget.clone();
        let gate = gate.clone();
        let tx = tx.clone();
        workers.push(std::thread::spawn(move || {
            let owner = budget.owner();
            gate.wait();
            tx.send(owner.try_reserve(8).unwrap()).unwrap();
        }));
    }
    drop(tx);
    gate.wait();
    let reservations: Vec<_> = rx.into_iter().collect();
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(budget.snapshot().total, 64);
    assert_eq!(budget.high_water_bytes(), 64);
    drop(reservations);
    assert_eq!(budget.snapshot().total, 0);
    assert_eq!(budget.high_water_bytes(), 64);
}

#[test]
fn work_revision_tracks_applied_work_not_reservations_or_checkpoint_progress() {
    let budget = ChangeBudget::with_hard_limit(64);
    let one = budget.owner();
    let two = budget.owner();
    let reservation = one.try_reserve(16).unwrap();
    assert_eq!(budget.snapshot().work_revision, 0);
    reservation.commit().unwrap();
    assert_eq!(one.work_revision().unwrap(), 1);
    assert_eq!(two.work_revision().unwrap(), 0);
    let frozen = one.freeze().unwrap();
    one.publish_through(&frozen).unwrap();
    assert_eq!(budget.snapshot().work_revision, 1);
    drop(two.try_reserve(8).unwrap());
    assert_eq!(budget.snapshot().work_revision, 1);
    two.try_reserve(4).unwrap().commit().unwrap();
    assert_eq!(budget.snapshot().work_revision, 2);
    assert_eq!(one.work_revision().unwrap(), 1);
    assert_eq!(two.work_revision().unwrap(), 2);
    two.retire();
    assert_eq!(budget.snapshot().work_revision, 2);
    assert_eq!(two.work_revision(), Err(AdmissionError::Retired));
}

#[test]
fn checkpoint_request_requires_applied_work_and_keeps_its_revision() {
    let budget = ChangeBudget::with_hard_limit(64);
    assert!(!budget.request_checkpoint());
    assert_eq!(budget.snapshot().checkpoint_request_revision, None);

    let owner = budget.owner();
    let _charge = owner.try_reserve(16).unwrap().commit().unwrap();
    assert_eq!(budget.snapshot().work_revision, 1);
    assert!(budget.request_checkpoint());
    assert_eq!(budget.snapshot().checkpoint_request_revision, Some(1));

    // A blocked waiter can repeat its hint, but it must not manufacture
    // a fresh work revision and bypass checkpoint completion suppression.
    assert!(budget.request_checkpoint());
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.work_revision, 1);
    assert_eq!(snapshot.checkpoint_request_revision, Some(1));
}

#[test]
fn owner_checkpoint_request_advances_after_consumption_without_new_work() {
    let budget = ChangeBudget::with_hard_limit(64);
    let owner = budget.owner();
    let _charge = owner.try_reserve(16).unwrap().commit().unwrap();

    assert!(owner.request_checkpoint());
    let first = owner.capacity_state().unwrap().checkpoint_request_revision;
    owner.consume_checkpoint_request(first);
    assert_eq!(budget.snapshot().checkpoint_request_revision, None);

    assert!(owner.request_checkpoint());
    let second = owner.capacity_state().unwrap().checkpoint_request_revision;
    assert!(second > first);
}

#[test]
fn owner_checkpoint_request_returns_revision_before_an_immediate_consume() {
    let budget = ChangeBudget::with_hard_limit(64);
    let owner = budget.owner();
    let _charge = owner.try_reserve(16).unwrap().commit().unwrap();

    let revision = owner
        .request_checkpoint_revision()
        .expect("active local work must get a request revision");
    owner.consume_checkpoint_request(Some(revision));
    assert_eq!(
        owner.capacity_state().unwrap().checkpoint_request_revision,
        None
    );
    assert_eq!(revision, 1, "caller retains the atomic returned revision");
}
