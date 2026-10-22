//! The periodic checkpoint schedule: which of a high-water crossing, the
//! deadline, a successful publication's successor or a newer capacity request
//! starts the next checkpoint, and how each completed attempt re-arms them.

use std::time::Duration;

use tokio::time::Instant;

use crate::ingest::domain::change_budget::Snapshot;

/// A high-water crossing schedules one immediate checkpoint. Each successful
/// publication can schedule one successor when new owner work arrived during
/// the checkpoint and remains above the trigger. A drained owner also retains
/// one successor for its next active-byte crossing, even if reservations keep
/// the process total high. Unchanged high work waits for the normal
/// post-completion period. New capacity requests can also schedule an immediate
/// attempt without waiting for the pressure to drain.
pub(in crate::persistence) struct CheckpointSchedule {
    period: Duration,
    pub(in crate::persistence) next_deadline: Instant,
    early_attempted: bool,
    immediate_successor: Option<u64>,
    /// The latest explicit capacity request that has been scheduled. A newer
    /// request must be allowed to bypass the pressure-epoch coalescing: the
    /// earlier checkpoint may have completed while the process remained near
    /// its hard limit.
    last_request_revision: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::persistence) enum CheckpointSelectionReason {
    Threshold,
    Deadline,
    Successor,
    CapacityRequest,
}

impl CheckpointSelectionReason {
    pub(in crate::persistence) fn label(self) -> &'static str {
        match self {
            Self::Threshold => "threshold",
            Self::Deadline => "deadline",
            Self::Successor => "successor",
            Self::CapacityRequest => "capacity_request",
        }
    }
}

// Do not re-arm on a brief dip below the 128 MiB trigger. A real drain must
// leave enough headroom before the next crossing can schedule an immediate
// checkpoint. Successful publications and new capacity requests are separate
// reasons to retry while the process remains above the threshold.
const CHECKPOINT_REARM_THRESHOLD: usize =
    crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER / 2;

impl CheckpointSchedule {
    pub(in crate::persistence) fn new(period: Duration, now: Instant) -> Self {
        Self {
            period,
            next_deadline: now + period,
            early_attempted: false,
            immediate_successor: None,
            last_request_revision: None,
        }
    }

    fn should_attempt(&mut self, now: Instant, pending: Snapshot, _work_revision: u64) -> bool {
        self.select_ordinary_attempt(now, pending).is_some()
    }

    /// Select the actual scheduler branch. This is deliberately the one
    /// stateful decision point, so diagnostics cannot describe a different
    /// branch from the one that starts a checkpoint.
    pub(in crate::persistence) fn select_attempt(
        &mut self,
        now: Instant,
        pending: Snapshot,
        owner: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
    ) -> Option<CheckpointSelectionReason> {
        // Preserve the established order exactly: even when a successor
        // selects this iteration, the ordinary selector must run to consume
        // a capacity request and update the pressure epoch for the next one.
        let successor = self.take_successor(owner);
        let ordinary = self.select_ordinary_attempt(now, pending);
        if successor {
            return Some(CheckpointSelectionReason::Successor);
        }
        ordinary
    }

    fn select_ordinary_attempt(
        &mut self,
        now: Instant,
        pending: Snapshot,
    ) -> Option<CheckpointSelectionReason> {
        let newer_capacity_request = pending
            .checkpoint_request_revision
            .filter(|revision| {
                self.last_request_revision
                    .is_none_or(|last| *revision > last)
            })
            .is_some_and(|revision| {
                pending.checkpoint_needed() && {
                    self.last_request_revision = Some(revision);
                    true
                }
            });
        if newer_capacity_request {
            return Some(CheckpointSelectionReason::CapacityRequest);
        }
        if pending.total < CHECKPOINT_REARM_THRESHOLD {
            self.early_attempted = false;
        }
        let periodic_due = now >= self.next_deadline;
        if periodic_due {
            // A periodic attempt does not reset the pressure epoch. High work
            // alone cannot trigger another early attempt without a real drain.
            if pending.total >= CHECKPOINT_REARM_THRESHOLD {
                self.early_attempted = true;
            }
            return Some(CheckpointSelectionReason::Deadline);
        }
        let early = !self.early_attempted && pending.checkpoint_needed();
        if early {
            self.early_attempted = true;
        }
        early.then_some(CheckpointSelectionReason::Threshold)
    }

    pub(in crate::persistence) fn completed(&mut self, now: Instant, pending: Snapshot) {
        self.immediate_successor = None;
        if pending.total >= CHECKPOINT_REARM_THRESHOLD {
            self.early_attempted = true;
        }
        self.next_deadline = now + self.period;
    }

    fn take_successor(
        &mut self,
        owner: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
    ) -> bool {
        let Some(armed_revision) = self.immediate_successor else {
            return false;
        };
        let Some(owner) = owner.filter(|owner| owner.work_revision >= armed_revision) else {
            self.immediate_successor = None;
            return false;
        };
        if owner.active < crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER {
            // Reservations are not checkpointable. Keep the successful
            // publication's one successor until this owner's active work
            // crosses the existing trigger, rather than consuming it on a
            // below-trigger poll and waiting for the next periodic deadline.
            return false;
        }
        self.immediate_successor = None;
        true
    }

    pub(in crate::persistence) fn completed_success(
        &mut self,
        now: Instant,
        after: Snapshot,
        owner_before: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
        owner_after: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
    ) {
        self.next_deadline = now + self.period;
        self.early_attempted = after.total >= CHECKPOINT_REARM_THRESHOLD;
        self.immediate_successor = match (owner_before, owner_after) {
            (Some(_), Some(after))
                if after.active < crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER =>
            {
                Some(after.work_revision)
            }
            (Some(before), Some(after))
                if after.active > 0
                    && after.work_revision > before.work_revision
                    && (after.active
                        >= crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER
                        || after
                            .checkpoint_request_revision
                            .is_some_and(|revision| revision > before.work_revision)) =>
            {
                Some(after.work_revision)
            }
            _ => None,
        };
        if after.total < crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER {
            self.early_attempted = false;
        }
    }
}

#[cfg(test)]
mod tests;
