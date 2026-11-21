//! Leader-last member choice for one three-voter Raft shard.
//!
//! [`plan_leader_last`] picks the next member to update. A healthy follower
//! goes first. The Raft leader goes last, and only after quorum and
//! replication are healthy. It builds on [`plan_rollout`], which owns the
//! partition and ordinal order, and adds the Raft-role rule.
//!
//! The function is pure. It reads no cluster state and performs no I/O.

use std::collections::BTreeMap;

use service_k8s::rollout::{
    plan_rollout, HoldReason, ObservedRollout, Ordinal, RolloutGate, RolloutStep,
};

#[cfg(test)]
mod tests;

/// Facts about one shard's StatefulSet and its Raft members at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardRolloutFacts {
    /// StatefulSet facts for the shard. `members` holds each pod's revision and Ready.
    pub rollout: ObservedRollout,
    /// Ordinal of the current Raft leader, `None` when no leader is known.
    pub leader: Option<Ordinal>,
    /// Replication lag per ordinal, in log entries behind the leader.
    /// A missing ordinal counts as lagging.
    pub replication_lag: BTreeMap<Ordinal, u64>,
    /// A member with `replication_lag <= max_replication_lag` is healthy.
    pub max_replication_lag: u64,
}

/// Why the leader-last planner holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaderLastHold {
    /// The core planner holds (paused, generation mismatch, member not updated,
    /// not ready, unhealthy, or several members in flight).
    Rollout(HoldReason),
    /// No leader is known.
    NoLeader,
    /// Fewer than `replicas / 2 + 1` members are Ready and within the lag bound.
    QuorumLost,
    /// The leader sits on the next member to update, but no other old-revision
    /// member is Ready and within the lag bound to take leadership.
    NoTransferTarget,
}

/// The next step for one shard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaderLastStep {
    /// Hold with a named reason.
    Hold { reason: LeaderLastHold },
    /// Move Raft leadership from `from` to `to` before `from` is released.
    TransferLeader { from: Ordinal, to: Ordinal },
    /// Same meaning as `RolloutStep::SetPartition`.
    SetPartition { partition: u32, member: Ordinal },
    /// Every member runs the update revision, is ready and is healthy.
    Complete,
}

/// A member is healthy when it is Ready and its replication lag is within the bound.
fn healthy(facts: &ShardRolloutFacts, ordinal: Ordinal) -> bool {
    let ready = facts
        .rollout
        .members
        .get(&ordinal)
        .is_some_and(|member| member.ready);
    let within_lag = facts
        .replication_lag
        .get(&ordinal)
        .is_some_and(|lag| *lag <= facts.max_replication_lag);
    ready && within_lag
}

/// Decide the next member to update for one shard. Leader last.
pub fn plan_leader_last(facts: &ShardRolloutFacts, paused: bool) -> LeaderLastStep {
    let Some(leader) = facts.leader else {
        return LeaderLastStep::Hold {
            reason: LeaderLastHold::NoLeader,
        };
    };

    let replicas = facts.rollout.replicas;
    let quorum = (replicas / 2 + 1) as usize;
    let healthy_count = (0..replicas).filter(|&o| healthy(facts, o)).count();
    if healthy_count < quorum {
        return LeaderLastStep::Hold {
            reason: LeaderLastHold::QuorumLost,
        };
    }

    let app_healthy: BTreeMap<Ordinal, bool> =
        (0..replicas).map(|o| (o, healthy(facts, o))).collect();
    let gate = RolloutGate::new(paused, app_healthy);

    match plan_rollout(&facts.rollout, &gate) {
        RolloutStep::Hold { reason } => LeaderLastStep::Hold {
            reason: LeaderLastHold::Rollout(reason),
        },
        RolloutStep::Complete => LeaderLastStep::Complete,
        RolloutStep::SetPartition { partition, member } => {
            // Members below `member` that still run the old revision.
            let update = &facts.rollout.update_revision;
            let old_below: Vec<Ordinal> = (0..member)
                .filter(
                    |o| !matches!(facts.rollout.members.get(o), Some(m) if &m.revision == update),
                )
                .collect();

            if leader == member && !old_below.is_empty() {
                // The leader must not be released while an old follower remains.
                match old_below.into_iter().find(|&o| healthy(facts, o)) {
                    Some(to) => LeaderLastStep::TransferLeader { from: member, to },
                    None => LeaderLastStep::Hold {
                        reason: LeaderLastHold::NoTransferTarget,
                    },
                }
            } else {
                LeaderLastStep::SetPartition { partition, member }
            }
        }
    }
}
