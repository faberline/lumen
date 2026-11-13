//! Quorum-safe runtime member rollout decision engine.
//!
//! Evaluates per-member observations (role leader/follower, Pod revision vs desired,
//! Ready, replication lag, quorum) and decides whether to update a healthy follower,
//! pause/wait with a named reason, update the leader (final step only), or finish.

use async_trait::async_trait;

/// Role of a Raft group member within a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum MemberRole {
    Leader,
    Follower,
}

/// Observed state of a single member / pod in the shard's StatefulSet.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemberObservation {
    /// Pod ordinal within the StatefulSet (or shard).
    pub ordinal: u32,
    /// Raft consensus role: Leader or Follower.
    pub role: MemberRole,
    /// Current Pod / container revision.
    pub revision: String,
    /// Searchable Ready status.
    pub ready: bool,
    /// Replication lag in terms of log index or milliseconds.
    pub replication_lag: u64,
}

impl MemberObservation {
    pub fn new(
        ordinal: u32,
        role: MemberRole,
        revision: impl Into<String>,
        ready: bool,
        replication_lag: u64,
    ) -> Self {
        Self {
            ordinal,
            role,
            revision: revision.into(),
            ready,
            replication_lag,
        }
    }
}

/// Port for triggering leader transfer before updating a leader member.
/// Kept behind a trait/port with a fake in tests when #15 is not yet merged.
#[async_trait]
pub trait LeaderTransferPort: Send + Sync {
    async fn transfer_leadership(
        &self,
        shard_index: u32,
        target_follower_ordinal: u32,
    ) -> Result<(), String>;
}

/// Outcome of evaluating the next runtime rollout step.
/// Returns exactly one of: update member X, wait with a named reason, or done.
/// It never selects two members at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutDecision {
    /// Update member with the given ordinal.
    UpdateMember { ordinal: u32 },
    /// Rollout must pause / wait with a named reason.
    Wait { reason: String },
    /// Rollout is complete (all members match desired revision, are Ready, and healthy).
    Done,
}

impl RolloutDecision {
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }

    pub fn is_wait(&self) -> bool {
        matches!(self, Self::Wait { .. })
    }

    pub fn update_member(&self) -> Option<u32> {
        match self {
            Self::UpdateMember { ordinal } => Some(*ordinal),
            _ => None,
        }
    }

    pub fn wait_reason(&self) -> Option<&str> {
        match self {
            Self::Wait { reason } => Some(reason.as_str()),
            _ => None,
        }
    }
}

/// Evaluates the member rollout decision with default quorum calculation: (members.len() / 2) + 1.
pub fn evaluate_runtime_rollout(
    desired_generation: i64,
    observed_generation: i64,
    desired_revision: &str,
    members: &[MemberObservation],
    max_replication_lag: u64,
) -> RolloutDecision {
    let quorum = if members.is_empty() {
        1
    } else {
        (members.len() / 2) + 1
    };
    evaluate_runtime_rollout_with_quorum(
        desired_generation,
        observed_generation,
        desired_revision,
        members,
        max_replication_lag,
        quorum,
    )
}

/// Evaluates the member rollout decision for a shard.
///
/// Rules:
/// 1. If members is empty: wait.
/// 2. Stale generation (observed != desired): wait.
/// 3. Quorum check: if ready count < quorum: wait.
/// 4. Any member Ready=false: wait.
/// 5. Replication lag above bound: wait.
/// 6. If all members run desired revision: Done.
/// 7. A healthy follower first: if any follower does not have desired revision,
///    select exactly one follower to update. Never select the leader in this state.
/// 8. Leader is selected only after every follower runs the desired revision and
///    quorum plus replication are healthy.
pub fn evaluate_runtime_rollout_with_quorum(
    desired_generation: i64,
    observed_generation: i64,
    desired_revision: &str,
    members: &[MemberObservation],
    max_replication_lag: u64,
    quorum: usize,
) -> RolloutDecision {
    if members.is_empty() {
        return RolloutDecision::Wait {
            reason: "no members in shard".into(),
        };
    }

    // 1. Stale generation check
    if observed_generation != desired_generation {
        return RolloutDecision::Wait {
            reason: format!(
                "stale generation: observed {} != desired {}",
                observed_generation, desired_generation
            ),
        };
    }

    let ready_count = members.iter().filter(|m| m.ready).count();

    // 2. Quorum check: follower loss causing ready count < quorum pauses rollout
    if ready_count < quorum {
        return RolloutDecision::Wait {
            reason: format!(
                "quorum lost: ready count {} is less than quorum {}",
                ready_count, quorum
            ),
        };
    }

    // 3. Ready gate: any Ready=false member pauses rollout
    if let Some(unready) = members.iter().find(|m| !m.ready) {
        return RolloutDecision::Wait {
            reason: format!("member {} is not ready", unready.ordinal),
        };
    }

    // 4. Replication lag gate: any member lagging above bound pauses rollout
    if let Some(lagging) = members
        .iter()
        .find(|m| m.replication_lag > max_replication_lag)
    {
        return RolloutDecision::Wait {
            reason: format!(
                "member {} replication lag {} exceeds bound {}",
                lagging.ordinal, lagging.replication_lag, max_replication_lag
            ),
        };
    }

    // 5. Completion check: all members already on desired revision
    let all_desired = members.iter().all(|m| m.revision == desired_revision);
    if all_desired {
        return RolloutDecision::Done;
    }

    // 6. Leadership check
    let leaders: Vec<&MemberObservation> = members
        .iter()
        .filter(|m| m.role == MemberRole::Leader)
        .collect();

    if leaders.len() > 1 {
        return RolloutDecision::Wait {
            reason: format!(
                "multiple leaders detected in shard: count {}",
                leaders.len()
            ),
        };
    }

    if leaders.is_empty() {
        return RolloutDecision::Wait {
            reason: "no leader elected in shard".into(),
        };
    }

    // 7. Healthy follower first:
    // Filter followers that need update (revision != desired_revision)
    let followers_needing_update: Vec<&MemberObservation> = members
        .iter()
        .filter(|m| m.role == MemberRole::Follower && m.revision != desired_revision)
        .collect();

    if let Some(follower) = followers_needing_update.first() {
        // Return exactly one follower to update. Never return leader.
        return RolloutDecision::UpdateMember {
            ordinal: follower.ordinal,
        };
    }

    // 8. Every follower runs desired revision, quorum is healthy, replication lag is healthy.
    // The final step updates the leader.
    let leader = leaders[0];
    if leader.revision != desired_revision {
        RolloutDecision::UpdateMember {
            ordinal: leader.ordinal,
        }
    } else {
        RolloutDecision::Done
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    pub struct FakeLeaderTransferPort {
        pub transfers: Mutex<Vec<(u32, u32)>>,
    }

    impl FakeLeaderTransferPort {
        pub fn new() -> Self {
            Self {
                transfers: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl LeaderTransferPort for FakeLeaderTransferPort {
        async fn transfer_leadership(
            &self,
            shard_index: u32,
            target_follower_ordinal: u32,
        ) -> Result<(), String> {
            self.transfers
                .lock()
                .unwrap()
                .push((shard_index, target_follower_ordinal));
            Ok(())
        }
    }

    #[tokio::test]
    async fn fake_leader_transfer_records_transfer() {
        let fake = FakeLeaderTransferPort::new();
        fake.transfer_leadership(0, 1).await.unwrap();
        let records = fake.transfers.lock().unwrap().clone();
        assert_eq!(records, vec![(0, 1)]);
    }

    #[test]
    fn stale_generation_observed_not_equal_desired_waits() {
        let members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", true, 0),
            MemberObservation::new(2, MemberRole::Follower, "rev-1", true, 0),
        ];

        let decision = evaluate_runtime_rollout(2, 1, "rev-2", &members, 0);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "stale generation: observed 1 != desired 2".into()
            }
        );
    }

    #[test]
    fn follower_loss_below_quorum_pauses_rollout() {
        // 3-member cluster: quorum is 2. Both followers lost (only leader ready).
        let members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", false, 0),
            MemberObservation::new(2, MemberRole::Follower, "rev-1", false, 0),
        ];

        let decision = evaluate_runtime_rollout(1, 1, "rev-2", &members, 0);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "quorum lost: ready count 1 is less than quorum 2".into()
            }
        );
    }

    #[test]
    fn ready_false_member_waits() {
        // Ready count is 2 (>= quorum 2), but follower 1 is Ready=false (e.g. updating or restarting)
        let members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", false, 0),
            MemberObservation::new(2, MemberRole::Follower, "rev-1", true, 0),
        ];

        let decision = evaluate_runtime_rollout(1, 1, "rev-2", &members, 0);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "member 1 is not ready".into()
            }
        );
    }

    #[test]
    fn replication_lag_above_bound_waits() {
        let members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", true, 150),
            MemberObservation::new(2, MemberRole::Follower, "rev-1", true, 0),
        ];

        let max_lag = 50;
        let decision = evaluate_runtime_rollout(1, 1, "rev-2", &members, max_lag);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "member 1 replication lag 150 exceeds bound 50".into()
            }
        );
    }

    #[test]
    fn full_orderly_rollout_updates_followers_first_then_leader_finally_done() {
        let max_lag = 10;
        let generation = 1;

        // Initial state: all members rev-1, healthy
        let mut members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", true, 0),
            MemberObservation::new(2, MemberRole::Follower, "rev-1", true, 0),
        ];

        // Step 1: Follower 1 is selected first (never leader)
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(decision, RolloutDecision::UpdateMember { ordinal: 1 });

        // While follower 1 is being updated, it becomes Ready=false
        members[1].ready = false;
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "member 1 is not ready".into()
            }
        );

        // Follower 1 finishes update and recovers Ready with rev-2
        members[1].ready = true;
        members[1].revision = "rev-2".into();

        // Step 2: Follower 2 is selected next (never leader)
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(decision, RolloutDecision::UpdateMember { ordinal: 2 });

        // While follower 2 is being updated, it becomes Ready=false
        members[2].ready = false;
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "member 2 is not ready".into()
            }
        );

        // Follower 2 finishes update and recovers Ready with rev-2
        members[2].ready = true;
        members[2].revision = "rev-2".into();

        // Step 3: Now all followers (1 and 2) run desired revision "rev-2", quorum and replication are healthy.
        // The final step updates the leader (ordinal 0)!
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(decision, RolloutDecision::UpdateMember { ordinal: 0 });

        // While leader is being updated, it becomes Ready=false
        members[0].ready = false;
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(
            decision,
            RolloutDecision::Wait {
                reason: "member 0 is not ready".into()
            }
        );

        // Leader finishes update and recovers Ready with rev-2
        members[0].ready = true;
        members[0].revision = "rev-2".into();

        // Step 4: All members have desired revision -> Done!
        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, max_lag);
        assert_eq!(decision, RolloutDecision::Done);
    }

    /// Negative control test: with the leader healthy and a follower not yet updated,
    /// a decision that picks the leader fails the test (assert the function never returns
    /// the leader in that state).
    #[test]
    fn negative_control_leader_healthy_with_unupdated_follower_never_selects_leader() {
        let generation = 1;
        // Leader is healthy and ready, follower 2 is updated to rev-2, but follower 1 is still on rev-1.
        let members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", true, 0),
            MemberObservation::new(2, MemberRole::Follower, "rev-2", true, 0),
        ];

        let decision = evaluate_runtime_rollout(generation, generation, "rev-2", &members, 0);

        // Assert the decision selects the unupdated follower:
        assert_eq!(decision, RolloutDecision::UpdateMember { ordinal: 1 });

        // Negative control assertion: assert the function NEVER returns the leader (ordinal 0)
        assert_ne!(
            decision,
            RolloutDecision::UpdateMember { ordinal: 0 },
            "negative control violated: decision picked the leader while follower 1 was not yet updated"
        );
    }

    #[test]
    fn never_selects_two_members_at_once() {
        let members = vec![
            MemberObservation::new(0, MemberRole::Leader, "rev-1", true, 0),
            MemberObservation::new(1, MemberRole::Follower, "rev-1", true, 0),
            MemberObservation::new(2, MemberRole::Follower, "rev-1", true, 0),
        ];

        let decision = evaluate_runtime_rollout(1, 1, "rev-2", &members, 0);
        // Match strictly on single member variant
        match decision {
            RolloutDecision::UpdateMember { ordinal } => {
                assert!(ordinal == 1 || ordinal == 2);
            }
            _ => panic!("expected UpdateMember variant"),
        }
    }
}
