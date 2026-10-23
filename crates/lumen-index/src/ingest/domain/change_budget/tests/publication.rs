use std::sync::{mpsc, Arc};
use std::time::Duration;

use crate::ingest::domain::change_budget::{
    AdmissionError, ChangeBudget, OwnerCapacityState, Reservation, Snapshot,
};

#[test]
fn committed_charge_survives_request_handle_drop() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let charge = owner.try_reserve(7).unwrap().commit().unwrap();
    drop(charge);
    assert_eq!(budget.snapshot().active, 7);
    assert!(matches!(
        owner.try_reserve(10),
        Err(AdmissionError::Full {
            requested: 10,
            used: 7,
            hard_limit: 16
        })
    ));
}

#[test]
fn failed_freeze_handle_keeps_committed_bytes() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let _charge = owner.try_reserve(7).unwrap().commit().unwrap();
    let frozen = owner.freeze().unwrap();
    drop(frozen);
    assert_eq!(
        budget.snapshot(),
        Snapshot {
            reserved: 0,
            active: 0,
            frozen: 7,
            total: 7,
            work_revision: 1,
            checkpoint_request_revision: None,
        }
    );
}

#[test]
fn publication_through_batch_preserves_newer_work() {
    let budget = ChangeBudget::with_hard_limit(32);
    let owner = budget.owner();
    let _old = owner.try_reserve(5).unwrap().commit().unwrap();
    let first = owner.freeze().unwrap();
    let _new_active = owner.try_reserve(6).unwrap().commit().unwrap();
    let second = owner.freeze().unwrap();
    let _reserved = owner.try_reserve(3).unwrap();
    assert_eq!(budget.publish_through(&first).unwrap(), 5);
    assert_eq!(
        budget.snapshot(),
        Snapshot {
            reserved: 3,
            active: 0,
            frozen: 6,
            total: 9,
            work_revision: 2,
            checkpoint_request_revision: None,
        }
    );
    assert_eq!(budget.publish_through(&second).unwrap(), 6);
}

#[test]
fn blocked_committed_admission_wakes_after_publish() {
    let budget = ChangeBudget::with_hard_limit(8);
    let owner = Arc::new(budget.owner());
    let _charge = owner.try_reserve(8).unwrap().commit().unwrap();
    let frozen = owner.freeze().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = owner.clone();
    std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx
            .send(
                waiter
                    .wait_reserve(1)
                    .and_then(Reservation::commit)
                    .map(|_| ()),
            )
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(20)).is_err());
    assert_eq!(frozen.publish().unwrap(), 8);
    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Ok(())
    );
}

#[test]
fn blocked_admission_wakes_with_retired_when_its_owner_is_retired() {
    let budget = ChangeBudget::with_hard_limit(8);
    let owner = Arc::new(budget.owner());
    let _charge = owner.try_reserve(8).unwrap().commit().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = owner.clone();
    std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx
            .send(
                waiter
                    .wait_reserve(1)
                    .and_then(Reservation::commit)
                    .map(|_| ()),
            )
            .unwrap();
    });
    started_rx.recv().unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(20)).is_err());
    owner.retire();
    assert_eq!(
        result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        Err(AdmissionError::Retired)
    );
}

#[test]
fn a_frozen_batch_from_another_budget_cannot_release_matching_ids() {
    let left = ChangeBudget::with_hard_limit(16);
    let right = ChangeBudget::with_hard_limit(16);
    let left_owner = left.owner();
    let right_owner = right.owner();
    let _left_charge = left_owner.try_reserve(5).unwrap().commit().unwrap();
    let _right_charge = right_owner.try_reserve(7).unwrap().commit().unwrap();
    let left_batch = left_owner.freeze().unwrap();
    let right_batch = right_owner.freeze().unwrap();
    assert_eq!(
        left.publish_through(&right_batch),
        Err(AdmissionError::WrongBudget)
    );
    assert_eq!(left.snapshot().frozen, 5);
    assert_eq!(right.snapshot().frozen, 7);
    assert_eq!(left.publish_through(&left_batch).unwrap(), 5);
}

#[test]
fn retiring_one_owner_does_not_clear_another() {
    let budget = ChangeBudget::with_hard_limit(32);
    let live = budget.owner();
    let restore = budget.owner();
    let _live = live.try_reserve(7).unwrap().commit().unwrap();
    let _restore = restore.try_reserve(9).unwrap().commit().unwrap();
    restore.retire();
    assert_eq!(
        budget.snapshot(),
        Snapshot {
            reserved: 0,
            active: 7,
            frozen: 0,
            total: 7,
            work_revision: 2,
            checkpoint_request_revision: None,
        }
    );
    assert!(matches!(
        live.try_reserve(26),
        Err(AdmissionError::Full {
            requested: 26,
            used: 7,
            hard_limit: 32
        })
    ));
}

#[test]
fn waiting_reservation_survives_freeze_and_publish_until_commit() {
    let budget = ChangeBudget::with_hard_limit(16);
    let owner = budget.owner();
    let reserved = owner.wait_reserve(5).unwrap();
    assert_eq!(budget.snapshot().reserved, 5);
    let empty = owner.freeze().unwrap();
    assert_eq!(empty.publish().unwrap(), 0);
    assert_eq!(budget.snapshot().reserved, 5);
    let _charge = reserved.commit().unwrap();
    let frozen = owner.freeze().unwrap();
    assert_eq!(frozen.publish().unwrap(), 5);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn process_registry_shares_live_default_budget() {
    let first = ChangeBudget::process_shared();
    let second = ChangeBudget::process_shared();
    assert!(
        Arc::ptr_eq(&first.0, &second.0),
        "live callers must share one process budget"
    );
    let owner = first.owner();
    let reserved = owner.try_reserve(3).unwrap();
    // Other Engine tests use this same process-wide budget in parallel.
    // Observe this owner through the second handle instead of requiring
    // every unrelated reservation in the process to be absent.
    {
        let state = second.0.state.lock().unwrap();
        assert_eq!(state.owners.get(&owner.id).unwrap().reserved, 3);
    }
    drop(reserved);
    let state = second.0.state.lock().unwrap();
    assert_eq!(state.owners.get(&owner.id).unwrap().reserved, 0);
}

#[test]
fn owner_cannot_publish_another_owners_batch() {
    let budget = ChangeBudget::with_hard_limit(16);
    let one = budget.owner();
    let two = budget.owner();
    let _charge = one.try_reserve(3).unwrap().commit().unwrap();
    let batch = one.freeze().unwrap();
    assert_eq!(two.publish_through(&batch), Err(AdmissionError::WrongOwner));
    assert_eq!(one.publish_through(&batch).unwrap(), 3);
}

#[test]
fn owner_capacity_state_selects_only_its_publishable_work() {
    let budget = ChangeBudget::with_hard_limit(64);
    let engine_a = budget.owner();
    let engine_b = budget.owner();
    let a_active = engine_a.try_reserve(9).unwrap().commit().unwrap();
    let b_reservation = engine_b.try_reserve(7).unwrap();

    assert_eq!(
        engine_b.capacity_state().unwrap(),
        OwnerCapacityState {
            active: 0,
            frozen: 0,
            work_revision: 0,
            checkpoint_request_revision: None,
        },
        "another Engine's active work and local reservations are not publishable here"
    );
    let frozen = engine_a.freeze().unwrap();
    assert_eq!(
        engine_a.capacity_state().unwrap(),
        OwnerCapacityState {
            active: 0,
            frozen: 9,
            work_revision: 1,
            checkpoint_request_revision: None,
        }
    );
    drop(b_reservation);
    frozen.publish().unwrap();
    drop(a_active);
}
