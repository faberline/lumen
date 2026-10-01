use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget};

#[test]
fn retained_charge_clones_share_one_resident_entry_and_one_revision() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let charge = owner.try_reserve(7).unwrap().commit_retained().unwrap();
    assert_eq!(charge.bytes(), 7);
    assert_eq!(budget.snapshot().active, 7);
    assert_eq!(budget.snapshot().work_revision, 1);

    let clone = charge.clone();
    assert_eq!(budget.snapshot().total, 7);
    drop(charge);
    assert_eq!(budget.snapshot().total, 7);
    assert_eq!(budget.snapshot().work_revision, 1);
    drop(clone);
    assert_eq!(budget.snapshot().total, 0);
    assert_eq!(budget.snapshot().work_revision, 1);
}

#[test]
fn publication_does_not_free_retained_payload_from_a_frozen_epoch() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let charge = owner.try_reserve(7).unwrap().commit_retained().unwrap();
    let frozen = owner.freeze().unwrap();
    assert_eq!(budget.snapshot().active, 0);
    assert_eq!(budget.snapshot().frozen, 7);
    assert_eq!(frozen.publish().unwrap(), 0);
    assert_eq!(budget.snapshot().total, 7);
    drop(charge);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn dropping_an_older_retained_epoch_keeps_later_active_payload_charged() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let old = owner.try_reserve(5).unwrap().commit_retained().unwrap();
    let _frozen = owner.freeze().unwrap();
    let current = owner.try_reserve(6).unwrap().commit_retained().unwrap();
    assert_eq!(budget.snapshot().active, 6);
    assert_eq!(budget.snapshot().frozen, 5);
    drop(old);
    assert_eq!(budget.snapshot().active, 6);
    assert_eq!(budget.snapshot().frozen, 0);
    assert_eq!(budget.snapshot().total, 6);
    drop(current);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn retiring_an_owner_preserves_retained_payload_until_its_last_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let charge = owner.try_reserve(7).unwrap().commit_retained().unwrap();
    owner.retire();
    assert_eq!(budget.snapshot().total, 7);
    assert!(matches!(owner.try_reserve(1), Err(AdmissionError::Retired)));
    drop(charge);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn blocked_admission_wakes_only_after_the_last_retained_handle_drops() {
    let budget = ChangeBudget::with_hard_limit(8);
    let owner = Arc::new(budget.owner());
    let charge = owner.try_reserve(8).unwrap().commit_retained().unwrap();
    let clone = charge.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = owner.clone();
    std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx.send(waiter.wait_reserve(1).map(|_| ())).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(charge);
    assert!(result_rx.recv_timeout(Duration::from_millis(20)).is_err());
    drop(clone);
    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(())
    );
}

#[test]
fn retained_commit_advances_revision_once_and_never_on_clone_or_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let charge = owner.try_reserve(3).unwrap().commit_retained().unwrap();
    assert_eq!(owner.work_revision().unwrap(), 1);
    let clone = charge.clone();
    drop(clone);
    drop(charge);
    assert_eq!(owner.work_revision().unwrap(), 1);
    let _next = owner.try_reserve(2).unwrap().commit_retained().unwrap();
    assert_eq!(owner.work_revision().unwrap(), 2);
}
