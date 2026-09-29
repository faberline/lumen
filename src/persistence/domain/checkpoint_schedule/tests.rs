use std::time::Duration;

use tokio::time::Instant;

use crate::ingest::domain::change_budget::Snapshot;
use crate::persistence::domain::checkpoint_schedule::{
    CheckpointSchedule, CheckpointSelectionReason, CHECKPOINT_REARM_THRESHOLD,
};

fn snapshot(
    total: usize,
    work_revision: u64,
    checkpoint_request_revision: Option<u64>,
) -> Snapshot {
    Snapshot {
        reserved: 0,
        active: total,
        frozen: 0,
        total,
        work_revision,
        checkpoint_request_revision,
    }
}

#[test]
fn initial_high_work_runs_before_the_first_deadline() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    assert!(schedule.should_attempt(
        now,
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            1,
            None
        ),
        1
    ));
}

#[test]
fn scheduler_selection_reports_the_real_branch_reason() {
    let now = Instant::now();
    let high = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        1,
        None,
    );

    let mut threshold = CheckpointSchedule::new(Duration::from_secs(30), now);
    assert_eq!(
        threshold.select_attempt(now, high, None),
        Some(CheckpointSelectionReason::Threshold)
    );

    let mut deadline = CheckpointSchedule::new(Duration::from_secs(30), now);
    assert_eq!(
        deadline.select_attempt(now + Duration::from_secs(30), snapshot(0, 1, None), None),
        Some(CheckpointSelectionReason::Deadline)
    );

    let mut successor = CheckpointSchedule::new(Duration::from_secs(30), now);
    successor.immediate_successor = Some(1);
    assert_eq!(
        successor.select_attempt(
            now,
            high,
            Some(crate::ingest::domain::change_budget::OwnerCapacityState {
                active: crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
                frozen: 0,
                work_revision: 1,
                checkpoint_request_revision: None,
            }),
        ),
        Some(CheckpointSelectionReason::Successor)
    );

    let mut successor_with_request = CheckpointSchedule::new(Duration::from_secs(30), now);
    successor_with_request.immediate_successor = Some(1);
    assert_eq!(
        successor_with_request.select_attempt(
            now,
            snapshot(
                crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
                1,
                Some(7)
            ),
            Some(crate::ingest::domain::change_budget::OwnerCapacityState {
                active: crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
                frozen: 0,
                work_revision: 1,
                checkpoint_request_revision: None,
            }),
        ),
        Some(CheckpointSelectionReason::Successor)
    );
    assert_eq!(successor_with_request.last_request_revision, Some(7));
    assert!(!successor_with_request.early_attempted);

    let mut capacity_request = CheckpointSchedule::new(Duration::from_secs(30), now);
    assert_eq!(
        capacity_request.select_attempt(now, snapshot(high.total, 1, Some(7)), None),
        Some(CheckpointSelectionReason::CapacityRequest)
    );
}

#[test]
fn high_work_does_not_recheckpoint_for_every_new_revision() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    let high = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        1,
        None,
    );
    assert!(schedule.should_attempt(now, high, 1));
    schedule.completed(now + Duration::from_secs(2), high);
    // New work remains covered by the completed high-water checkpoint.
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(2),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            2,
            None
        ),
        2,
    ));
    assert!(!schedule.should_attempt(now + Duration::from_secs(2), snapshot(0, 2, None), 2,));
    assert!(schedule.should_attempt(
        now + Duration::from_secs(2),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            3,
            None
        ),
        3,
    ));
}

#[test]
fn transient_dip_below_trigger_does_not_rearm_high_work() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    let high = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        1,
        None,
    );
    assert!(schedule.should_attempt(now, high, 1));
    schedule.completed(now + Duration::from_secs(1), high);
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(1),
        snapshot(CHECKPOINT_REARM_THRESHOLD + 1, 2, None),
        2,
    ));
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(1),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            3,
            None
        ),
        3,
    ));
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(1),
        snapshot(CHECKPOINT_REARM_THRESHOLD - 1, 4, None),
        4,
    ));
    assert!(schedule.should_attempt(now + Duration::from_secs(1), high, 5,));
}

#[test]
fn newer_request_revision_bypasses_high_water_period_once() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    let first = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        10,
        Some(10),
    );
    assert!(schedule.should_attempt(now, first, 10));
    schedule.completed(now + Duration::from_secs(1), first);

    // A newer blocked-admission request proves that the prior checkpoint
    // did not create enough headroom. It must trigger one more attempt
    // without waiting for the normal period.
    assert!(schedule.should_attempt(
        now + Duration::from_secs(1),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            11,
            Some(11)
        ),
        11,
    ));
    schedule.completed(
        now + Duration::from_secs(2),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            11,
            Some(11),
        ),
    );
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(31),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            12,
            Some(11)
        ),
        12,
    ));
    schedule.completed(
        now + Duration::from_secs(32),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            12,
            Some(11),
        ),
    );
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(32),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            13,
            Some(11)
        ),
        13,
    ));

    // A real drain re-arms immediate capacity relief for the next crossing.
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(2),
        snapshot(CHECKPOINT_REARM_THRESHOLD - 1, 13, None),
        13,
    ));
    assert!(schedule.should_attempt(
        now + Duration::from_secs(2),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            14,
            Some(14)
        ),
        14,
    ));
}

mod completion;
