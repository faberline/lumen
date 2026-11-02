use std::time::Duration;

use tokio::time::Instant;

use crate::ingest::domain::change_budget::{OwnerCapacityState, Snapshot, CHECKPOINT_TRIGGER};
use crate::persistence::domain::checkpoint_schedule::{
    CheckpointSchedule, CheckpointSelectionReason,
};

#[test]
fn capacity_request_during_publication_is_not_scheduled_twice() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = OwnerCapacityState {
        active: CHECKPOINT_TRIGGER,
        frozen: 0,
        work_revision: 10,
        checkpoint_request_revision: None,
    };
    let initial = Snapshot {
        reserved: 0,
        active: before.active,
        frozen: 0,
        total: before.active,
        work_revision: 10,
        checkpoint_request_revision: None,
    };
    assert_eq!(
        schedule.select_attempt(now, initial, Some(before)),
        Some(CheckpointSelectionReason::Threshold)
    );
    // A local refusal arrives while that publication is held. Its requests
    // reserve transport memory after relief, but new publishable owner work
    // is still below the unchanged checkpoint trigger.
    let after = OwnerCapacityState {
        active: CHECKPOINT_TRIGGER / 2,
        work_revision: 11,
        checkpoint_request_revision: Some(11),
        ..before
    };
    let pending = Snapshot {
        reserved: CHECKPOINT_TRIGGER / 2,
        active: after.active,
        total: CHECKPOINT_TRIGGER,
        work_revision: 11,
        checkpoint_request_revision: Some(11),
        ..initial
    };
    // The fenced owner records actual relief. Budget consumption still waits
    // for the relay's merge; scheduler bookkeeping cannot invent this fact.
    schedule.observe_relieved_request(Some(11));
    schedule.completed_success(now, pending, Some(before), Some(after));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(after)),
        None,
        "one completed publication must not schedule its already-pending relief again"
    );
}

#[test]
fn a_new_unrelieved_request_after_publication_still_runs() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    schedule.observe_relieved_request(Some(10));
    let owner = OwnerCapacityState {
        active: CHECKPOINT_TRIGGER / 2,
        frozen: 0,
        work_revision: 11,
        checkpoint_request_revision: Some(11),
    };
    let pending = Snapshot {
        reserved: CHECKPOINT_TRIGGER / 2,
        active: owner.active,
        frozen: 0,
        total: CHECKPOINT_TRIGGER,
        work_revision: 11,
        checkpoint_request_revision: Some(11),
    };
    schedule.completed_success(now, pending, Some(owner), Some(owner));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(owner)),
        Some(CheckpointSelectionReason::CapacityRequest),
        "completion bookkeeping must not swallow a request the owner has not served"
    );
}

#[test]
fn relieved_capacity_demand_does_not_schedule_its_own_high_work_again() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = OwnerCapacityState {
        active: 92_151_760,
        frozen: 0,
        work_revision: 6,
        checkpoint_request_revision: None,
    };
    let initial = Snapshot {
        reserved: 48_075_012,
        active: before.active,
        frozen: 0,
        total: 140_226_772,
        work_revision: 6,
        checkpoint_request_revision: None,
    };
    assert_eq!(
        schedule.select_attempt(now, initial, Some(before)),
        Some(CheckpointSelectionReason::Threshold)
    );
    // Actual HTTP trace: a refusal names work12 while the first frozen
    // cut is held. Its one publication releases both waiting admissions.
    // The old active prefix is still high; it is not a second refusal.
    let after = OwnerCapacityState {
        active: 138_227_640,
        frozen: 0,
        work_revision: 12,
        checkpoint_request_revision: Some(12),
    };
    let pending = Snapshot {
        reserved: 96_150_024,
        active: after.active,
        frozen: 0,
        total: 234_377_664,
        work_revision: 12,
        checkpoint_request_revision: Some(12),
    };
    schedule.observe_relieved_capacity(Some((12, Some(12))));
    schedule.completed_success(now, pending, Some(before), Some(after));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(after)),
        None,
        "served capacity work must not race the same waiting HTTP admissions with another publication"
    );
}

fn owner_work(active: usize, work: u64, request: u64) -> OwnerCapacityState {
    OwnerCapacityState {
        active,
        frozen: 0,
        work_revision: work,
        checkpoint_request_revision: Some(request),
    }
}

fn pending_work(owner: OwnerCapacityState) -> Snapshot {
    Snapshot {
        reserved: 0,
        active: owner.active,
        frozen: 0,
        total: owner.active,
        work_revision: owner.work_revision,
        checkpoint_request_revision: owner.checkpoint_request_revision,
    }
}

#[test]
fn work_after_the_capacity_request_still_gets_a_successor() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = owner_work(CHECKPOINT_TRIGGER, 6, 12);
    let after = owner_work(CHECKPOINT_TRIGGER + 1, 14, 12);
    let pending = pending_work(after);
    schedule.observe_relieved_capacity(Some((12, Some(12))));
    schedule.completed_success(now, pending, Some(before), Some(after));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(after)),
        Some(CheckpointSelectionReason::Successor),
        "fresh work after the served refusal retains the original immediate successor"
    );
    schedule.completed_success(now, pending, Some(after), Some(after));
    assert_eq!(schedule.select_attempt(now, pending, Some(after)), None);
}

#[test]
fn a_larger_request_clock_cannot_hide_new_owner_work() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = owner_work(CHECKPOINT_TRIGGER, 6, 9000);
    let after = owner_work(CHECKPOINT_TRIGGER + 1, 14, 9000);
    let pending = pending_work(after);
    schedule.observe_relieved_capacity(Some((9000, Some(12))));
    schedule.completed_success(now, pending, Some(before), Some(after));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(after)),
        Some(CheckpointSelectionReason::Successor)
    );
}

#[test]
fn unrelieved_new_request_retries_even_without_new_owner_work() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let owner = owner_work(CHECKPOINT_TRIGGER, 12, 9001);
    let pending = pending_work(owner);
    schedule.observe_relieved_capacity(Some((9000, Some(12))));
    schedule.completed_success(now, pending, Some(owner), Some(owner));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(owner)),
        Some(CheckpointSelectionReason::CapacityRequest),
        "a new real refusal must still make progress without any new applied work"
    );
}

#[test]
fn drained_owner_keeps_its_next_crossing_after_capacity_relief() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = owner_work(CHECKPOINT_TRIGGER, 6, 12);
    let drained = owner_work(CHECKPOINT_TRIGGER / 2, 12, 12);
    schedule.observe_relieved_capacity(Some((12, Some(12))));
    schedule.completed_success(now, pending_work(drained), Some(before), Some(drained));
    assert_eq!(
        schedule.select_attempt(now, pending_work(drained), Some(drained)),
        None
    );
    let crossed = owner_work(CHECKPOINT_TRIGGER, 13, 12);
    assert_eq!(
        schedule.select_attempt(now, pending_work(crossed), Some(crossed)),
        Some(CheckpointSelectionReason::Successor)
    );
}

#[test]
fn unknown_owner_boundary_cannot_suppress_a_successor() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = owner_work(CHECKPOINT_TRIGGER, 6, 9000);
    let after = owner_work(CHECKPOINT_TRIGGER + 1, 14, 9000);
    let pending = pending_work(after);
    schedule.observe_relieved_capacity(Some((9000, None)));
    schedule.completed_success(now, pending, Some(before), Some(after));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(after)),
        Some(CheckpointSelectionReason::Successor),
        "an unproven work boundary preserves the existing scheduler behavior"
    );
}

#[test]
fn external_relief_consumes_an_already_armed_old_work_successor() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = owner_work(CHECKPOINT_TRIGGER, 6, 9000);
    let after = owner_work(CHECKPOINT_TRIGGER + 1, 12, 9000);
    let pending = pending_work(after);
    schedule.completed_success(now, pending, Some(before), Some(after));
    // The native owner serves this demand before the periodic task resumes.
    schedule.observe_relieved_capacity(Some((9000, Some(12))));
    assert_eq!(
        schedule.select_attempt(now, pending, Some(after)),
        None,
        "an already armed periodic successor must respect real native capacity relief"
    );
}
