use std::time::Duration;

use tokio::time::Instant;

use crate::ingest::domain::change_budget::Snapshot;
use crate::persistence::domain::checkpoint_schedule::tests::snapshot;
use crate::persistence::domain::checkpoint_schedule::CheckpointSchedule;

#[test]
fn successful_high_checkpoint_immediately_retries_new_publishable_work() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        10,
        Some(10),
    );
    assert!(schedule.should_attempt(now, before, 10));
    schedule.completed_success(
        now + Duration::from_secs(1),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER + 1,
            11,
            Some(11),
        ),
        Some(crate::ingest::domain::change_budget::OwnerCapacityState {
            active: 0,
            frozen: 0,
            work_revision: 10,
            checkpoint_request_revision: None,
        }),
        Some(crate::ingest::domain::change_budget::OwnerCapacityState {
            active: crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER + 1,
            frozen: 0,
            work_revision: 11,
            checkpoint_request_revision: Some(11),
        }),
    );
    let successor_owner = Some(crate::ingest::domain::change_budget::OwnerCapacityState {
        active: crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER + 1,
        frozen: 0,
        work_revision: 11,
        checkpoint_request_revision: Some(11),
    });
    assert!(schedule.take_successor(successor_owner));
    assert!(!schedule.take_successor(successor_owner));
}

#[test]
fn successful_successors_keep_new_high_work_runnable_without_a_refusal() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let high = crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER + 1;
    assert!(schedule.should_attempt(now, snapshot(high, 10, None), 10));

    for revision in 11..=13 {
        let before = crate::ingest::domain::change_budget::OwnerCapacityState {
            active: high,
            frozen: 0,
            work_revision: revision - 1,
            checkpoint_request_revision: None,
        };
        let after = crate::ingest::domain::change_budget::OwnerCapacityState {
            work_revision: revision,
            ..before
        };
        schedule.completed_success(
            now + Duration::from_secs(revision - 10),
            snapshot(high, revision, None),
            Some(before),
            Some(after),
        );
        assert!(
            schedule.take_successor(Some(after)),
            "new high work after publication {revision} must not wait for a 429 to request relief"
        );
        assert!(
            !schedule.take_successor(Some(after)),
            "one publication must arm only one successor"
        );
    }

    // A high process total is not enough: unchanged owner work must never
    // form a busy loop, even after a chain of successful publications.
    let unchanged = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: high,
        frozen: 0,
        work_revision: 13,
        checkpoint_request_revision: None,
    };
    schedule.completed_success(
        now + Duration::from_secs(4),
        snapshot(high, 13, None),
        Some(unchanged),
        Some(unchanged),
    );
    assert!(!schedule.take_successor(Some(unchanged)));
    assert!(!schedule.should_attempt(now + Duration::from_secs(4), snapshot(high, 13, None), 13,));
}

#[test]
fn successful_checkpoint_below_trigger_rearms_next_crossing() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        10,
        Some(10),
    );
    assert!(schedule.should_attempt(now, before, 10));
    schedule.completed_success(
        now + Duration::from_secs(1),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER - 1,
            10,
            Some(10),
        ),
        Some(crate::ingest::domain::change_budget::OwnerCapacityState {
            active: 0,
            frozen: 0,
            work_revision: 10,
            checkpoint_request_revision: None,
        }),
        Some(crate::ingest::domain::change_budget::OwnerCapacityState {
            active: 0,
            frozen: 0,
            work_revision: 10,
            checkpoint_request_revision: Some(10),
        }),
    );
    assert!(schedule.should_attempt(
        now + Duration::from_secs(1),
        snapshot(
            crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            11,
            Some(11)
        ),
        11,
    ));
}

#[test]
fn new_owner_work_crossing_after_publication_is_not_hidden_by_reservations() {
    let now = Instant::now();
    let trigger = crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER;
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    assert!(schedule.should_attempt(now, snapshot(trigger, 10, None), 10));
    let before = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: trigger,
        frozen: 0,
        work_revision: 10,
        checkpoint_request_revision: None,
    };
    let after = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: trigger * 3 / 4,
        work_revision: 11,
        ..before
    };
    // A successful publication left newer owner work below the trigger.
    // In-flight request reservations keep the process total above it.
    let pending = Snapshot {
        reserved: trigger / 2,
        active: after.active,
        frozen: 0,
        total: trigger * 5 / 4,
        work_revision: 11,
        checkpoint_request_revision: None,
    };
    schedule.completed_success(now, pending, Some(before), Some(after));
    assert!(
        !schedule.take_successor(Some(after)),
        "reserved work is not a capture target"
    );
    assert!(
        !schedule.should_attempt(now, pending, 11),
        "do not publish just for reservations"
    );

    let crossed = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: trigger,
        work_revision: 12,
        ..after
    };
    assert!(
        schedule.take_successor(Some(crossed)),
        "new publishable owner work must cross the unchanged trigger without waiting for a refusal or the normal period"
    );
    assert!(
        !schedule.take_successor(Some(crossed)),
        "one publication permits only one successor"
    );
}

#[test]
fn reservations_after_a_complete_owner_drain_do_not_hide_new_work() {
    let now = Instant::now();
    let trigger = crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER;
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(3600), now);
    let before = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: trigger,
        frozen: 0,
        work_revision: 10,
        checkpoint_request_revision: None,
    };
    let drained = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: 0,
        ..before
    };
    let reserved = Snapshot {
        reserved: trigger,
        active: 0,
        frozen: 0,
        total: trigger,
        work_revision: 10,
        checkpoint_request_revision: None,
    };
    schedule.completed_success(now, reserved, Some(before), Some(drained));
    for _ in 0..3 {
        assert!(!schedule.take_successor(Some(drained)));
        assert!(!schedule.should_attempt(now, reserved, 10));
    }
    let new_work = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: trigger,
        work_revision: 11,
        ..drained
    };
    assert!(
        schedule.take_successor(Some(new_work)),
        "the owner's next crossing must remain observable after a complete drain"
    );
    assert!(!schedule.take_successor(Some(new_work)));
}

#[test]
fn failed_publication_cancels_a_deferred_successor() {
    let now = Instant::now();
    let trigger = crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER;
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    schedule.immediate_successor = Some(10);
    schedule.completed(now, snapshot(trigger, 11, None));
    let owner = crate::ingest::domain::change_budget::OwnerCapacityState {
        active: trigger,
        frozen: 0,
        work_revision: 11,
        checkpoint_request_revision: None,
    };
    assert!(
        !schedule.take_successor(Some(owner)),
        "a failed attempt must retain its existing periodic backoff"
    );
    assert!(!schedule.should_attempt(now, snapshot(trigger, 11, None), 11));
    assert!(schedule.should_attempt(
        now + Duration::from_secs(30),
        snapshot(trigger, 11, None),
        11
    ));
}

#[test]
fn missing_or_replaced_owner_cancels_a_deferred_successor() {
    let now = Instant::now();
    let trigger = crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER;
    for owner in [
        None,
        Some(crate::ingest::domain::change_budget::OwnerCapacityState {
            active: trigger,
            frozen: 0,
            work_revision: 9,
            checkpoint_request_revision: None,
        }),
    ] {
        let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
        schedule.immediate_successor = Some(10);
        assert!(!schedule.take_successor(owner));
        assert_eq!(schedule.immediate_successor, None);
    }
}

#[test]
fn unchanged_high_failure_waits_for_completion_relative_period() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(10), now);
    let high = snapshot(
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        1,
        None,
    );
    schedule.completed(now + Duration::from_secs(3), high);
    assert!(!schedule.should_attempt(now + Duration::from_secs(12), high, 1));
    assert!(schedule.should_attempt(now + Duration::from_secs(13), high, 1));
}

#[test]
fn blocked_admission_request_is_consumed_once() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    let requested = snapshot(100 * 1024 * 1024, 7, Some(7));
    assert!(!schedule.should_attempt(now, requested, 7));
    schedule.completed(now + Duration::from_secs(1), requested);
    assert!(!schedule.should_attempt(now + Duration::from_secs(1), requested, 7));
    assert!(!schedule.should_attempt(
        now + Duration::from_secs(1),
        snapshot(100 * 1024 * 1024, 8, Some(7)),
        8,
    ));
}

#[test]
fn no_work_has_no_checkpoint_request_attempt() {
    let now = Instant::now();
    let mut schedule = CheckpointSchedule::new(Duration::from_secs(30), now);
    assert!(!schedule.should_attempt(now, snapshot(0, 0, None), 0));
}
