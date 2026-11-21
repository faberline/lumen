use std::collections::BTreeMap;

use service_k8s::rollout::{HoldReason, ObservedMember, ObservedRollout};

use super::{plan_leader_last, LeaderLastHold, LeaderLastStep, ShardRolloutFacts};

/// Build the facts of a three-voter shard. Each tuple is
/// (ordinal, revision, ready, replication lag).
fn facts(
    partition: u32,
    leader: Option<u32>,
    members: &[(u32, &str, bool, u64)],
) -> ShardRolloutFacts {
    let mut rollout = ObservedRollout::new(3, 2, 2, "rev-a", "rev-b", partition);
    let mut replication_lag = BTreeMap::new();
    for &(ordinal, revision, ready, lag) in members {
        rollout = rollout.with_member(ordinal, revision, ready);
        replication_lag.insert(ordinal, lag);
    }
    ShardRolloutFacts {
        rollout,
        leader,
        replication_lag,
        max_replication_lag: 10,
    }
}

/// All three members on `rev-a`, ready, lag 0.
fn all_old() -> [(u32, &'static str, bool, u64); 3] {
    [
        (0, "rev-a", true, 0),
        (1, "rev-a", true, 0),
        (2, "rev-a", true, 0),
    ]
}

#[test]
fn steady_follower_is_released_first() {
    let f = facts(3, Some(0), &all_old());
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::SetPartition {
            partition: 2,
            member: 2
        }
    );
}

#[test]
fn leader_on_next_member_transfers_to_lowest_old_ordinal() {
    let f = facts(3, Some(2), &all_old());
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::TransferLeader { from: 2, to: 0 }
    );
}

#[test]
fn leader_is_never_selected_while_an_old_follower_remains() {
    // Negative control: a planner that ignores the leader returns
    // SetPartition { partition: 1, member: 1 } here and fails this test.
    let f = facts(
        2,
        Some(1),
        &[
            (0, "rev-a", true, 0),
            (1, "rev-a", true, 0),
            (2, "rev-b", true, 0),
        ],
    );
    let step = plan_leader_last(&f, false);
    assert_eq!(step, LeaderLastStep::TransferLeader { from: 1, to: 0 });
    assert_ne!(
        step,
        LeaderLastStep::SetPartition {
            partition: 1,
            member: 1
        }
    );
}

#[test]
fn final_step_updates_the_leader() {
    let f = facts(
        1,
        Some(0),
        &[
            (0, "rev-a", true, 0),
            (1, "rev-b", true, 0),
            (2, "rev-b", true, 0),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::SetPartition {
            partition: 0,
            member: 0
        }
    );
}

#[test]
fn complete_when_all_updated_and_healthy() {
    let f = facts(
        0,
        Some(0),
        &[
            (0, "rev-b", true, 0),
            (1, "rev-b", true, 0),
            (2, "rev-b", true, 0),
        ],
    );
    assert_eq!(plan_leader_last(&f, false), LeaderLastStep::Complete);
}

#[test]
fn follower_loss_keeps_going_while_quorum_holds() {
    let f = facts(
        3,
        Some(0),
        &[
            (0, "rev-a", true, 0),
            (1, "rev-a", false, 0),
            (2, "rev-a", true, 0),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::SetPartition {
            partition: 2,
            member: 2
        }
    );
}

#[test]
fn follower_loss_below_quorum_pauses() {
    let f = facts(
        3,
        Some(0),
        &[
            (0, "rev-a", true, 0),
            (1, "rev-a", false, 0),
            (2, "rev-a", false, 0),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::QuorumLost
        }
    );
}

#[test]
fn stale_generation_waits() {
    let mut f = facts(3, Some(0), &all_old());
    f.rollout.observed_generation = 1;
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::Rollout(HoldReason::GenerationMismatch)
        }
    );
}

#[test]
fn lag_above_bound_waits() {
    let f = facts(
        2,
        Some(0),
        &[
            (0, "rev-a", true, 0),
            (1, "rev-a", true, 0),
            (2, "rev-b", true, 11),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::Rollout(HoldReason::MemberUnhealthy)
        }
    );
}

#[test]
fn ready_false_member_waits() {
    let f = facts(
        2,
        Some(0),
        &[
            (0, "rev-a", true, 0),
            (1, "rev-a", true, 0),
            (2, "rev-b", false, 0),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::Rollout(HoldReason::MemberNotReady)
        }
    );
}

#[test]
fn no_leader_waits() {
    let f = facts(3, None, &all_old());
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::NoLeader
        }
    );
}

#[test]
fn paused_waits() {
    let f = facts(3, Some(0), &all_old());
    assert_eq!(
        plan_leader_last(&f, true),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::Rollout(HoldReason::Paused)
        }
    );
}

#[test]
fn transfer_target_must_be_healthy() {
    // Ordinal 0 is lagging, so the lowest healthy old ordinal is 1.
    let f = facts(
        3,
        Some(2),
        &[
            (0, "rev-a", true, 11),
            (1, "rev-a", true, 0),
            (2, "rev-a", true, 0),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::TransferLeader { from: 2, to: 1 }
    );
}

#[test]
fn no_transfer_target_waits() {
    // Quorum holds (1 and 2 are healthy). The leader sits on 1, the next
    // member. The only old ordinal below it, 0, is lagging.
    let f = facts(
        2,
        Some(1),
        &[
            (0, "rev-a", true, 11),
            (1, "rev-a", true, 0),
            (2, "rev-b", true, 0),
        ],
    );
    assert_eq!(
        plan_leader_last(&f, false),
        LeaderLastStep::Hold {
            reason: LeaderLastHold::NoTransferTarget
        }
    );
}

/// Apply each step to the facts, as the StatefulSet and Raft would, until the
/// planner completes. Returns the released members in order.
fn run_rollout(start_leader: u32) -> Vec<u32> {
    let mut f = facts(3, Some(start_leader), &all_old());
    let mut released = Vec::new();
    for _ in 0..20 {
        match plan_leader_last(&f, false) {
            LeaderLastStep::TransferLeader { from, to } => {
                assert_eq!(f.leader, Some(from), "transfer must start from the leader");
                f.leader = Some(to);
            }
            LeaderLastStep::SetPartition { partition, member } => {
                if f.leader == Some(member) {
                    let old_below = (0..member).any(
                        |o| !matches!(f.rollout.members.get(&o), Some(m) if m.revision == "rev-b"),
                    );
                    assert!(
                        !old_below,
                        "leader {member} released while an old follower remains below it"
                    );
                }
                released.push(member);
                f.rollout.partition = partition;
                f.rollout
                    .members
                    .insert(member, ObservedMember::new("rev-b", true));
            }
            LeaderLastStep::Complete => {
                assert_eq!(
                    released,
                    vec![2, 1, 0],
                    "members must be released leader last"
                );
                return released;
            }
            LeaderLastStep::Hold { reason } => {
                panic!("unexpected hold {reason:?} from start leader {start_leader}")
            }
        }
    }
    panic!("rollout from start leader {start_leader} did not complete in 20 steps");
}

#[test]
fn nx3_rollout_updates_the_leader_last_and_never_two_at_once_from_leader_0() {
    assert_eq!(run_rollout(0), vec![2, 1, 0]);
}

#[test]
fn nx3_rollout_updates_the_leader_last_and_never_two_at_once_from_leader_1() {
    assert_eq!(run_rollout(1), vec![2, 1, 0]);
}

#[test]
fn nx3_rollout_updates_the_leader_last_and_never_two_at_once_from_leader_2() {
    assert_eq!(run_rollout(2), vec![2, 1, 0]);
}
