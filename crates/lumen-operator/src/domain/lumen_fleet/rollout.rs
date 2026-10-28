//! Pure derivation of per-entry rollout status, desired hash, phase, and stall reasons.

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{FleetEntryLastStall, FleetEntryPhase, FleetEntryStatus, FleetStallReason};

/// Calculate a stable truncated sha256 hex hash of the planned spec.
/// The same spec produces the same hash; any change produces a different one.
pub fn desired_hash(spec: &Value) -> String {
    let bytes = serde_json::to_vec(spec).unwrap_or_default();
    let hash = Sha256::digest(&bytes);
    let hex = format!("{hash:x}");
    hex[..16].to_string()
}

/// The derived rollout fields for one fleet entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RolloutStatus {
    pub phase: FleetEntryPhase,
    pub phase_since: String,
    pub stall_reason: Option<String>,
    pub last_stall: Option<FleetEntryLastStall>,
}

/// Pure derivation of (phase, phase_since, stall_reason, last_stall) from:
/// - previous entry status
/// - this pass outcome (`"Created"`, `"Applied"`, `"Rejected"`, `"NamespaceMissing"`, `"NotAdopted"`, `"ApplyFailed"`, etc.)
/// - child's metadata.generation
/// - child's status.observedGeneration
/// - current RFC 3339 timestamp
pub fn derive_rollout_status(
    prev: Option<&FleetEntryStatus>,
    pass_outcome: &str,
    child_generation: Option<i64>,
    child_observed_generation: Option<i64>,
    now: &str,
) -> RolloutStatus {
    // 1. Determine stall reason based on pass_outcome and generation observation.
    let stall_reason = match pass_outcome {
        "Rejected" => Some(FleetStallReason::Rejected.to_string()),
        "NamespaceMissing" => Some(FleetStallReason::NamespaceMissing.to_string()),
        "NotAdopted" => Some(FleetStallReason::NotAdopted.to_string()),
        "ApplyFailed" => Some(FleetStallReason::ApplyFailed.to_string()),
        _ => {
            let observed = match (child_generation, child_observed_generation) {
                (Some(gen), Some(obs)) => obs >= gen,
                (Some(_), None) => false,
                (None, _) => true,
            };
            if !observed {
                Some(FleetStallReason::UnobservedGeneration.to_string())
            } else {
                None
            }
        }
    };

    // 2. Determine phase: Stalled if there is a stall reason, otherwise Healthy.
    let phase = if stall_reason.is_some() {
        FleetEntryPhase::Stalled
    } else {
        FleetEntryPhase::Healthy
    };

    // 3. phaseSince changes only when the phase changes.
    let phase_since = match prev {
        Some(p) if p.phase == Some(phase) => {
            p.phase_since.clone().unwrap_or_else(|| now.to_string())
        }
        _ => now.to_string(),
    };

    // 4. Recovery: when a stalled entry becomes healthy, stallReason is cleared
    // and the previous stall (reason and its start time) is kept in a lastStall field
    // until the next stall replaces it. A stalled entry that stays stalled keeps its original phaseSince.
    let last_stall = match (prev, phase) {
        (Some(p), FleetEntryPhase::Healthy) if p.phase == Some(FleetEntryPhase::Stalled) => {
            let reason = p.stall_reason.clone().unwrap_or_default();
            let start_time = p.phase_since.clone().unwrap_or_else(|| now.to_string());
            Some(FleetEntryLastStall {
                reason,
                time: start_time,
            })
        }
        (Some(p), _) => p.last_stall.clone(),
        (None, _) => None,
    };

    RolloutStatus {
        phase,
        phase_since,
        stall_reason,
        last_stall,
    }
}
