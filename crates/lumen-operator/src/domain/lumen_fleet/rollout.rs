//! The rollout decision of one convergence pass: which planned instances may
//! be created or applied now, in declared order, and which are held. Pure: no
//! kube call happens here.

use crate::domain::lumen_fleet::{PlanOutcome, PlannedInstance, RolloutPolicy};

/// Whether one planned instance may be written this pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolloutDecision {
    Proceed,
    Held,
}

/// Message for a held instance while the fleet is paused.
pub const HELD_PAUSED: &str = "rollout is paused; this instance is not written";

/// Message for a held instance waiting for a rollout slot.
pub const HELD_WAITING: &str =
    "waiting for a rollout slot (maxConcurrent per pass); not written this pass";

/// The `Held` message for this policy: it names the pause when paused.
pub fn held_message(policy: &RolloutPolicy) -> &'static str {
    if policy.paused {
        HELD_PAUSED
    } else {
        HELD_WAITING
    }
}

/// One decision per planned instance, same order and length as `planned`.
///
/// When paused, every instance is held. Otherwise the instances are walked in
/// declared order: a rejected one is held and never uses a slot, and a ready
/// one proceeds while fewer than `maxConcurrent` have proceeded. `max(1)`
/// keeps a zero policy safe (one instance per pass, no division, no loop on 0).
pub fn decide(policy: &RolloutPolicy, planned: &[PlannedInstance]) -> Vec<RolloutDecision> {
    if policy.paused {
        return vec![RolloutDecision::Held; planned.len()];
    }
    let slots = policy.max_concurrent.max(1) as usize;
    let mut proceeded = 0usize;
    planned
        .iter()
        .map(|instance| match instance.outcome {
            PlanOutcome::Ready(_) if proceeded < slots => {
                proceeded += 1;
                RolloutDecision::Proceed
            }
            _ => RolloutDecision::Held,
        })
        .collect()
}
