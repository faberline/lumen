use std::sync::Arc;

use crate::ingest::application::write_coordinator::local_record::{
    prepare_local_record, PreparedLocalRecord,
};
use crate::ingest::application::write_coordinator::tests::{admitted_index_entry, keyword_schema};
use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget};
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::{index::application::engine::Engine, storage::RecordAdmissionError};

#[tokio::test]
async fn no_guard_reprice_retains_charge_without_mutating_state_or_watermark() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let first = Engine::with_change_budget(budget.clone());
    let second = Arc::new(Engine::with_change_budget(budget));
    first.create_collection("u", keyword_schema()).unwrap();
    second.create_collection("u", keyword_schema()).unwrap();
    let coord = WriteCoordinator::start(Arc::new(MemWal::new()), second.clone());
    let entry = admitted_index_entry();
    let reservation = first.try_reserve_record(&entry, 0).unwrap();
    let bytes = reservation.bytes();

    let result = prepare_local_record(second.as_ref(), entry, reservation)
        .expect("wrong-engine reprice must return its original reservation");
    match result {
        PreparedLocalRecord::Prepared(_) => {
            panic!("wrong-engine reprice cannot acquire an apply guard")
        }
        PreparedLocalRecord::Unresolved { reservation, error } => {
            assert_eq!(
                reservation.bytes(),
                bytes,
                "no-guard failure must retain reservation bytes"
            );
            assert!(
                matches!(
                    error.downcast_ref::<RecordAdmissionError>(),
                    Some(RecordAdmissionError::WrongEngine)
                ),
                "wrong-engine no-guard failure must keep its typed admission error: {error:#}"
            );
        }
        PreparedLocalRecord::CapacityBlocked { .. } => {
            panic!("wrong-engine reprice must fail before capacity blocking")
        }
    }
    assert_eq!(
        second.stats("u").unwrap().documents_indexed,
        0,
        "preparation failure must not mutate collection state"
    );
    assert_eq!(
        coord.applied_seq(),
        0,
        "preparation failure must not advance the coordinator watermark"
    );
}

#[test]
fn fitting_published_reprice_does_not_start_capacity_owner() {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let active = Engine::with_change_budget(budget.clone());
    active.create_collection("u", keyword_schema()).unwrap();
    let entry = admitted_index_entry();

    // Build real stale state. The reservation predates a restore, so the
    // next begin must reprice it; the 1 MiB budget leaves room for growth.
    let initial = active.try_reserve_record(&entry, 0).unwrap();
    let initial = match prepare_local_record(&active, entry.clone(), initial).unwrap() {
        PreparedLocalRecord::Prepared(mut guard) => {
            active.apply_prepared_raft_entry(&mut guard).unwrap();
            drop(guard);
            active.freeze_checkpoint_collections(None).unwrap()
        }
        PreparedLocalRecord::Unresolved { .. } => {
            panic!("initial fitting record must be prepared")
        }
        PreparedLocalRecord::CapacityBlocked { .. } => {
            panic!("initial fitting record must not block")
        }
    };
    let replacement = Engine::with_change_budget(budget.clone());
    replacement
        .create_collection("u", keyword_schema())
        .unwrap();
    let reservation = active.try_reserve_record(&entry, 0).unwrap();
    active.restore(replacement.snapshot().unwrap()).unwrap();
    let before = budget.snapshot().total;
    let prepared = prepare_local_record(&active, entry, reservation)
        .expect("stale fitting reprice must grow without fallback maintenance");
    assert!(
        budget.snapshot().total > before,
        "fixture must exercise a real fitting reprice growth"
    );
    match prepared {
        PreparedLocalRecord::Prepared(mut guard) => {
            active.apply_prepared_raft_entry(&mut guard).unwrap();
            drop(guard);
        }
        PreparedLocalRecord::Unresolved { .. } => {
            panic!("fitting reprice must not use legacy fallback")
        }
        PreparedLocalRecord::CapacityBlocked { .. } => {
            panic!("fitting reprice must not block")
        }
    }
    drop(initial);
}

#[test]
fn full_published_reprice_returns_capacity_blocked_without_waiting() {
    let limit = 1024 * 1024;
    let budget = ChangeBudget::with_hard_limit(limit);
    let active = Engine::with_change_budget(budget.clone());
    active.create_collection("u", keyword_schema()).unwrap();
    let entry = admitted_index_entry();

    let initial = active.try_reserve_record(&entry, 0).unwrap();
    let initial = match prepare_local_record(&active, entry.clone(), initial).unwrap() {
        PreparedLocalRecord::Prepared(mut guard) => {
            active.apply_prepared_raft_entry(&mut guard).unwrap();
            drop(guard);
            active.freeze_checkpoint_collections(None).unwrap()
        }
        PreparedLocalRecord::Unresolved { .. } => {
            panic!("initial record must be prepared")
        }
        PreparedLocalRecord::CapacityBlocked { .. } => {
            panic!("initial record must not block")
        }
    };
    let replacement = Engine::with_change_budget(budget.clone());
    replacement
        .create_collection("u", keyword_schema())
        .unwrap();
    let reservation = active.try_reserve_record(&entry, 0).unwrap();
    let reserved_bytes = reservation.bytes();
    active.restore(replacement.snapshot().unwrap()).unwrap();

    let filler_owner = budget.owner();
    let remaining = limit - budget.snapshot().total;
    let _filler = filler_owner.try_reserve(remaining).unwrap();
    let result = prepare_local_record(&active, entry, reservation).unwrap();
    match result {
        PreparedLocalRecord::Prepared(_) => {
            panic!("a full post-WAL reprice must not acquire an apply guard")
        }
        PreparedLocalRecord::CapacityBlocked {
            reservation,
            required,
            ..
        } => {
            assert_eq!(reservation.bytes(), reserved_bytes);
            assert!(required > reservation.bytes());
            assert!(
                matches!(
                    active.try_reserve_record(&admitted_index_entry(), 0),
                    Err(RecordAdmissionError::Capacity(AdmissionError::Full { .. }))
                ),
                "a later record must not pass the unresolved retained head"
            );
        }
        PreparedLocalRecord::Unresolved { .. } => {
            panic!("capacity Full must retain a retryable blocked state")
        }
    }
    drop(initial);
}

#[test]
fn full_published_reprice_waits_outside_apply_lease_then_applies_after_relief() {
    let limit = 1024 * 1024;
    let budget = ChangeBudget::with_hard_limit(limit);
    let active = Engine::with_change_budget(budget.clone());
    active.create_collection("u", keyword_schema()).unwrap();
    let entry = admitted_index_entry();

    let initial = active.try_reserve_record(&entry, 0).unwrap();
    let initial = match prepare_local_record(&active, entry.clone(), initial).unwrap() {
        PreparedLocalRecord::Prepared(mut guard) => {
            active.apply_prepared_raft_entry(&mut guard).unwrap();
            drop(guard);
            active.freeze_checkpoint_collections(None).unwrap()
        }
        _ => panic!("initial record must be prepared"),
    };
    let replacement = Engine::with_change_budget(budget.clone());
    replacement
        .create_collection("u", keyword_schema())
        .unwrap();
    let reservation = active.try_reserve_record(&entry, 0).unwrap();
    active.restore(replacement.snapshot().unwrap()).unwrap();
    let filler_owner = budget.owner();
    let filler = filler_owner
        .try_reserve(limit - budget.snapshot().total)
        .unwrap();
    let blocked = prepare_local_record(&active, entry, reservation).unwrap();
    let PreparedLocalRecord::CapacityBlocked {
        entry,
        mut reservation,
        required,
    } = blocked
    else {
        panic!("full reprice must retain a blocked head")
    };

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        reservation
            .wait_grow_to(required)
            .map(|()| (entry, reservation))
    });
    started_rx.recv().unwrap();
    for _ in 0..10_000 {
        if budget.has_capacity_waiters() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        budget.has_capacity_waiters(),
        "the blocked committed head must wait on the budget without an apply lease"
    );
    assert!(matches!(
        active.try_reserve_record(&admitted_index_entry(), 0),
        Err(RecordAdmissionError::Capacity(AdmissionError::Full { .. }))
    ));

    drop(filler);
    let (entry, reservation) = waiter.join().unwrap().unwrap();
    let prepared = prepare_local_record(&active, entry, reservation).unwrap();
    match prepared {
        PreparedLocalRecord::Prepared(mut guard) => {
            active.apply_prepared_raft_entry(&mut guard).unwrap();
        }
        _ => panic!("capacity relief must let the retained head acquire its apply guard"),
    }
    assert_eq!(active.stats("u").unwrap().documents_indexed, 1);
    drop(initial);
}
