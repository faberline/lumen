//! Member rollout evaluation for StatefulSet-backed serving pods.
//!
//! Evaluates whether the StatefulSet's `updateStrategy.rollingUpdate.partition`
//! can advance down by one ordinal (updating one member at a time), or must pause
//! due to generation mismatch, member unreadiness, quorum loss, or revision mismatch.

/// The observed status of a single member / pod in the StatefulSet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberStatus {
    pub ordinal: u32,
    pub ready: bool,
    pub revision: String,
}

impl MemberStatus {
    pub fn new(ordinal: u32, ready: bool, revision: impl Into<String>) -> Self {
        Self {
            ordinal,
            ready,
            revision: revision.into(),
        }
    }
}

/// The outcome of evaluating a rollout step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutDecision {
    /// Advance to (or maintain) the next partition.
    Step { next_partition: u32 },
    /// Rollout is paused with a stable reason string. The partition remains unchanged.
    Paused { reason: String },
}

impl RolloutDecision {
    pub fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }

    pub fn next_partition(&self) -> Option<u32> {
        match self {
            Self::Step { next_partition } => Some(*next_partition),
            Self::Paused { .. } => None,
        }
    }
}

/// Pure function to evaluate the next partition step in a member rollout.
///
/// Inputs:
/// - `desired_generation`: desired StatefulSet generation (from CR / spec).
/// - `observed_generation`: observed StatefulSet generation (from StatefulSet status).
/// - `members`: ready status and revision for each ordinal.
/// - `current_partition`: current rollingUpdate.partition value.
///
/// Output:
/// - Next partition value or `Paused` with a stable reason string.
pub fn next_partition(
    desired_generation: i64,
    observed_generation: i64,
    members: &[MemberStatus],
    current_partition: u32,
) -> RolloutDecision {
    evaluate_member_rollout(
        desired_generation,
        observed_generation,
        members,
        current_partition,
        None,
    )
}

/// Variant of `next_partition` that also verifies updated members against an explicit `target_revision`.
pub fn next_partition_with_target(
    desired_generation: i64,
    observed_generation: i64,
    members: &[MemberStatus],
    current_partition: u32,
    target_revision: &str,
) -> RolloutDecision {
    evaluate_member_rollout(
        desired_generation,
        observed_generation,
        members,
        current_partition,
        Some(target_revision),
    )
}

/// Evaluates the member rollout step with optional target revision enforcement.
pub fn evaluate_member_rollout(
    desired_generation: i64,
    observed_generation: i64,
    members: &[MemberStatus],
    current_partition: u32,
    target_revision: Option<&str>,
) -> RolloutDecision {
    if members.is_empty() {
        return RolloutDecision::Paused {
            reason: "no members in statefulset".into(),
        };
    }

    // 1. Generation consistency check
    if observed_generation != desired_generation {
        return RolloutDecision::Paused {
            reason: format!(
                "generation mismatch: observed {} != desired {}",
                observed_generation, desired_generation
            ),
        };
    }

    let total_members = members.len();
    let quorum = (total_members / 2) + 1;
    let ready_count = members.iter().filter(|m| m.ready).count();

    // 2. Quorum check: if member loss causes ready count < quorum, pause.
    if ready_count < quorum {
        return RolloutDecision::Paused {
            reason: format!(
                "quorum lost: ready count {} is less than quorum {}",
                ready_count, quorum
            ),
        };
    }

    // 3. Any member not ready check
    if let Some(not_ready) = members.iter().find(|m| !m.ready) {
        return RolloutDecision::Paused {
            reason: format!("member {} is not ready", not_ready.ordinal),
        };
    }

    // 4. Check updated members (ordinal >= current_partition)
    let effective_partition = current_partition.min(total_members as u32);
    let updated_members: Vec<&MemberStatus> = members
        .iter()
        .filter(|m| m.ordinal >= effective_partition)
        .collect();

    if !updated_members.is_empty() {
        if let Some(target) = target_revision {
            for m in &updated_members {
                if m.revision != target {
                    return RolloutDecision::Paused {
                        reason: format!(
                            "member {} revision {} does not match target revision {}",
                            m.ordinal, m.revision, target
                        ),
                    };
                }
            }
        } else {
            // Without explicit target, all updated members must share the same revision
            let first_rev = &updated_members[0].revision;
            for m in &updated_members[1..] {
                if &m.revision != first_rev {
                    return RolloutDecision::Paused {
                        reason: format!(
                            "updated member {} revision {} does not match revision {}",
                            m.ordinal, m.revision, first_rev
                        ),
                    };
                }
            }

            // If some members are not yet updated, verify updated members have moved away from un-updated revision
            if (effective_partition as usize) < total_members && effective_partition > 0 {
                let unupdated: Vec<&MemberStatus> = members
                    .iter()
                    .filter(|m| m.ordinal < effective_partition)
                    .collect();
                if let Some(first_unupdated) = unupdated.first() {
                    if &first_unupdated.revision == first_rev {
                        return RolloutDecision::Paused {
                            reason: format!(
                                "member {} has not updated to new revision",
                                effective_partition
                            ),
                        };
                    }
                }
            }
        }
    }

    // 5. Partition advances by exactly one member at a time down to 0
    if effective_partition > 0 {
        RolloutDecision::Step {
            next_partition: effective_partition - 1,
        }
    } else {
        RolloutDecision::Step { next_partition: 0 }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn one_at_a_time_rollout_progresses_from_n_to_zero() {
        // Cluster of 3 members: ordinals 0, 1, 2. Target revision is "rev-2".
        let generation = 10;
        let members_start = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-1"),
            MemberStatus::new(2, true, "rev-1"),
        ];

        // Start with partition = 3 (no member updated yet)
        let step1 = next_partition_with_target(generation, generation, &members_start, 3, "rev-2");
        assert_eq!(step1, RolloutDecision::Step { next_partition: 2 });

        // Member 2 updates: during update, member 2 is not ready
        let members_updating_2 = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-1"),
            MemberStatus::new(2, false, "rev-1"),
        ];
        let step2 =
            next_partition_with_target(generation, generation, &members_updating_2, 2, "rev-2");
        assert_eq!(
            step2,
            RolloutDecision::Paused {
                reason: "member 2 is not ready".into()
            }
        );

        // Member 2 becomes ready on "rev-2"
        let members_updated_2 = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-1"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let step3 =
            next_partition_with_target(generation, generation, &members_updated_2, 2, "rev-2");
        assert_eq!(step3, RolloutDecision::Step { next_partition: 1 });

        // Member 1 updates: member 1 is not ready
        let members_updating_1 = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, false, "rev-1"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let step4 =
            next_partition_with_target(generation, generation, &members_updating_1, 1, "rev-2");
        assert_eq!(
            step4,
            RolloutDecision::Paused {
                reason: "member 1 is not ready".into()
            }
        );

        // Member 1 becomes ready on "rev-2"
        let members_updated_1 = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-2"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let step5 =
            next_partition_with_target(generation, generation, &members_updated_1, 1, "rev-2");
        assert_eq!(step5, RolloutDecision::Step { next_partition: 0 });

        // Member 0 updates and becomes ready on "rev-2"
        let members_all_updated = vec![
            MemberStatus::new(0, true, "rev-2"),
            MemberStatus::new(1, true, "rev-2"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let step6 =
            next_partition_with_target(generation, generation, &members_all_updated, 0, "rev-2");
        assert_eq!(step6, RolloutDecision::Step { next_partition: 0 });
    }

    #[test]
    fn unready_member_pauses_rollout() {
        let generation = 5;
        // Member 1 is unready
        let members = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, false, "rev-1"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let result = next_partition_with_target(generation, generation, &members, 2, "rev-2");
        assert_eq!(
            result,
            RolloutDecision::Paused {
                reason: "member 1 is not ready".into()
            }
        );
    }

    #[test]
    fn generation_mismatch_pauses_rollout() {
        let members = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-1"),
            MemberStatus::new(2, true, "rev-1"),
        ];
        let result = next_partition_with_target(5, 4, &members, 3, "rev-2");
        assert_eq!(
            result,
            RolloutDecision::Paused {
                reason: "generation mismatch: observed 4 != desired 5".into()
            }
        );
    }

    #[test]
    fn quorum_loss_pauses_and_resumes_without_restarting() {
        let generation = 3;
        // Partition is at 1. Member 2 was already updated to rev-2.
        // Two members are down: ordinal 0 and 1 are not ready.
        // Only member 2 is ready (1 ready < quorum 2).
        let members_lost = vec![
            MemberStatus::new(0, false, "rev-1"),
            MemberStatus::new(1, false, "rev-1"),
            MemberStatus::new(2, true, "rev-2"),
        ];

        let paused = next_partition_with_target(generation, generation, &members_lost, 1, "rev-2");
        assert_eq!(
            paused,
            RolloutDecision::Paused {
                reason: "quorum lost: ready count 1 is less than quorum 2".into()
            }
        );

        // Member 0 recovers (now 2 ready >= quorum 2), but member 1 is still updating (not ready)
        let members_recovering = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, false, "rev-1"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let paused_member1 =
            next_partition_with_target(generation, generation, &members_recovering, 1, "rev-2");
        assert_eq!(
            paused_member1,
            RolloutDecision::Paused {
                reason: "member 1 is not ready".into()
            }
        );

        // Member 1 recovers and has updated to rev-2!
        let members_healthy = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-2"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        // Resumes directly from partition 1 to 0 (restart-safe: does NOT restart from 3 or 2)
        let resumed =
            next_partition_with_target(generation, generation, &members_healthy, 1, "rev-2");
        assert_eq!(resumed, RolloutDecision::Step { next_partition: 0 });
    }

    #[test]
    fn repeated_calls_are_idempotent() {
        let generation = 7;
        let members = vec![
            MemberStatus::new(0, true, "rev-2"),
            MemberStatus::new(1, true, "rev-2"),
            MemberStatus::new(2, true, "rev-2"),
        ];

        let call1 = next_partition_with_target(generation, generation, &members, 0, "rev-2");
        let call2 = next_partition_with_target(generation, generation, &members, 0, "rev-2");
        assert_eq!(call1, RolloutDecision::Step { next_partition: 0 });
        assert_eq!(call1, call2);

        let members_mid = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-1"),
            MemberStatus::new(2, true, "rev-2"),
        ];
        let mid1 = next_partition_with_target(generation, generation, &members_mid, 2, "rev-2");
        let mid2 = next_partition_with_target(generation, generation, &members_mid, 2, "rev-2");
        assert_eq!(mid1, RolloutDecision::Step { next_partition: 1 });
        assert_eq!(mid1, mid2);
    }

    /// Negative control: if the Ready gate were omitted, advancing partition while a member
    /// is updating would cause a second member to begin updating simultaneously, resulting
    /// in two concurrently unavailable members and quorum loss.
    #[test]
    fn negative_control_without_ready_gate_violates_availability() {
        let generation = 1;
        // In a 3-member cluster, member 2 is currently updating (not ready).
        let members = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, true, "rev-1"),
            MemberStatus::new(2, false, "rev-2"),
        ];

        // 1. Production code WITH the Ready gate pauses:
        let decision = next_partition_with_target(generation, generation, &members, 2, "rev-2");
        assert!(
            decision.is_paused(),
            "production rollout MUST pause when a member is not ready"
        );
        assert_eq!(
            decision,
            RolloutDecision::Paused {
                reason: "member 2 is not ready".into()
            }
        );

        // 2. Simulate what happens if the Ready gate is removed:
        // A hypothetical unsafe function that steps partition without checking member.ready:
        let unsafe_advance_without_ready_gate =
            |current_partition: u32| -> u32 { current_partition.saturating_sub(1) };

        let unsafe_next_partition = unsafe_advance_without_ready_gate(2);
        assert_eq!(unsafe_next_partition, 1);

        // Applying partition = 1 prematurely triggers member 1 to restart while member 2 is still not ready:
        let members_with_two_unavailable = vec![
            MemberStatus::new(0, true, "rev-1"),
            MemberStatus::new(1, false, "rev-1"), // restarted because partition dropped to 1!
            MemberStatus::new(2, false, "rev-2"), // still restarting!
        ];

        let ready_count = members_with_two_unavailable
            .iter()
            .filter(|m| m.ready)
            .count();
        let quorum = (members_with_two_unavailable.len() / 2) + 1;

        // Two members are concurrently unavailable, violating quorum (1 < 2):
        assert!(
            ready_count < quorum,
            "negative control demonstrated: premature advance caused two concurrent unavailabilities, breaking quorum"
        );
    }
}
