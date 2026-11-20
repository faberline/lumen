//! The rollout policy: how many instances one convergence pass may create or
//! apply, and the manual freeze. Pure functions; no cluster call.

use serde_json::Value;

use crate::domain::lumen_fleet::plan::{remove_path, DRIVER_OWNED_PATHS};
use crate::domain::lumen_fleet::RolloutPolicy;
use crate::domain::lumen_spec::LumenSpec;

/// What one planned instance needs in this pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    /// The entry already has its own state (`Rejected`, `NamespaceMissing`,
    /// `NotAdopted`, `ObserveFailed`); the rollout does not decide for it.
    Skip,
    /// The live `Lumen` already holds the planned spec.
    Settled,
    /// The `Lumen` is absent or differs from the planned spec.
    Write,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RolloutDecision {
    /// Skip entries get this.
    Skip,
    /// Create or apply the instance now.
    Proceed,
    /// Make no call for this instance in this pass.
    Held,
}

/// One decision per item of `needs`, same order and length.
pub fn decide(policy: &RolloutPolicy, needs: &[Need]) -> Vec<RolloutDecision> {
    // `.max(1)` makes a zero limit safe: at least one write may proceed.
    let limit = policy.max_concurrent.max(1) as usize;
    let mut proceeded = 0usize;
    needs
        .iter()
        .map(|need| match need {
            Need::Skip => RolloutDecision::Skip,
            _ if policy.paused => RolloutDecision::Held,
            Need::Settled => RolloutDecision::Proceed,
            Need::Write => {
                if proceeded < limit {
                    proceeded += 1;
                    RolloutDecision::Proceed
                } else {
                    RolloutDecision::Held
                }
            }
        })
        .collect()
}

/// `"rollout is paused"` when `policy.paused`, else `"waiting for a rollout slot"`.
pub fn held_message(policy: &RolloutPolicy) -> &'static str {
    if policy.paused {
        "rollout is paused"
    } else {
        "waiting for a rollout slot"
    }
}

/// True when the planned spec and the live spec are equal after both are canonicalized.
pub fn is_settled(planned_spec: &Value, live_spec: &Value) -> bool {
    match (canonical(planned_spec), canonical(live_spec)) {
        (Some(planned), Some(live)) => planned == live,
        _ => false,
    }
}

/// The spec after the same serde round trip as `LumenSpec`, with the
/// driver-owned paths removed. `None` when the spec is not a valid `LumenSpec`.
fn canonical(spec: &Value) -> Option<Value> {
    let parsed = serde_json::from_value::<LumenSpec>(spec.clone()).ok()?;
    let mut value = serde_json::to_value(&parsed).ok()?;
    for path in DRIVER_OWNED_PATHS {
        remove_path(&mut value, path);
    }
    Some(value)
}
