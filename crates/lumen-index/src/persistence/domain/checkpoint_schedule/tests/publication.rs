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
