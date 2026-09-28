//! The `Lumen` status subresource: capacity and reshard progress.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(doc)]
use super::LumenSpec;

/// Status subresource, written back by the reconcile loop.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LumenStatus {
    /// `Pending | Reconciling | Ready | Degraded`.
    #[serde(default)]
    pub phase: String,
    /// The `.metadata.generation` this status reflects (drift detection).
    #[serde(default)]
    pub observed_generation: i64,
    /// Ready serving replicas (from the StatefulSet status).
    #[serde(default)]
    pub serving_ready_replicas: i32,
    /// Desired serving replicas (apply-time count, or the live count).
    #[serde(default)]
    pub desired_replicas: i32,
    /// Effective shard count.
    #[serde(default)]
    pub shard_count: u32,
    /// Reshard workflow status. Present even before a split starts so agents
    /// can distinguish "complete" from "unknown policy".
    #[serde(default)]
    pub reshard: LumenReshardStatus,
    /// Data-plane capacity status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<LumenCapacityStatus>,
    /// Last human-readable reconcile message.
    #[serde(default)]
    pub message: String,
    /// Kubernetes-convention convergence conditions (#2601): `Ready`,
    /// `Progressing`, `ReshardInProgress`. This is the surface
    /// `kubectl wait --for=condition=Ready`, Argo CD health assessment, and Flux
    /// readiness gates read; `phase` and `reshard.blockingConditions` are
    /// unchanged and still populated, so nothing already consuming them breaks.
    ///
    /// `lastTransitionTime` is stamped by the reconcile loop, not here — see
    /// [`super::reconcile`]'s no-I/O `status_patch` contract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<service_k8s::Condition>,
}

/// Data-plane capacity status.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LumenCapacityStatus {
    /// Current active GCE machine type.
    #[serde(default)]
    pub current_machine_type: String,
    /// Target GCE machine type.
    #[serde(default)]
    pub target_machine_type: String,
    /// Transition generation count.
    #[serde(default)]
    pub transition_generation: u64,
    /// Capacity lifecycle phase (`Stable` | `CapacityBlocked`).
    #[serde(default)]
    pub phase: String,
    /// Whether the old healthy member remains authoritative during transition/block.
    #[serde(default)]
    pub old_member_authoritative: bool,
    /// Human-readable capacity status message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LumenReshardStatus {
    #[serde(default)]
    pub phase: String,
    // Schema default corrected (#1319 R3) to match the actual runtime value
    // (`ReshardPolicy::default().max_shard_bytes.is_none() == true`), not
    // `bool::default()` (`false`) — the CRD's declared default used to
    // disagree with what the operator always reports at the CRD's own
    // `reshardPolicy` defaults.
    #[serde(default)]
    #[schemars(default = "default_reshard_recommendation_only")]
    pub recommendation_only: bool,
    #[serde(default)]
    pub progress_percent: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_shard_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_bytes_per_sec: Option<u64>,
    /// Highest observed percent of `maxShardBytes` across shards, from the
    /// live per-shard usage measurement (#1319 R1;
    /// [`super::reconcile`]'s pod-`/metrics` measurement loop is the only
    /// caller of [`LumenSpec::reshard_status_with_usage`], which sets this).
    /// `None` when `maxShardBytes` is unset or usage has not been measured
    /// yet — the plain [`LumenSpec::reshard_status`] never sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_observed_percent: Option<u8>,
    /// The `spec.shardMap.version` live when [`Self::max_observed_percent`]
    /// was captured (#1386 R1/R2) — the usage measurement's freshness
    /// generation. [`LumenSpec::reshard_status_with_usage`] only reports a
    /// crossed threshold when this equals the CR's *current*
    /// `spec.shardMap.version`; a mismatch means the measurement predates
    /// the most recent split's cutover (immediately after `Complete`, the
    /// shard-usage cache almost always still holds exactly this — the live
    /// #1384 proof bug this field closes) and the status instead reports
    /// `"usageStalePostCutover"`, holding until a fresh post-cutover
    /// scrape lands. `None` alongside `max_observed_percent == None` (no
    /// measurement yet).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_measured_at_map_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocking_conditions: Vec<String>,
    #[serde(default)]
    pub message: String,
    /// Mirrors `spec.reshardPolicy.workflow.convergenceRemediationRestartCount`
    /// (#1485 R1) — count of bounded remediation rolling-restart re-triggers
    /// the reshard driver has fired for the current convergence-stall
    /// episode, so operators can see the self-heal fired without reading
    /// `spec`. `status_patch` copies this straight from the spec field.
    #[serde(default)]
    pub convergence_remediation_restart_count: u32,
    /// Mirrors `spec.reshardPolicy.workflow.convergenceRemediationRestartedAt`
    /// (#1485 R1) — epoch-seconds timestamp of the last remediation restart
    /// re-trigger, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_remediation_restarted_at: Option<u64>,
}

// #1319 R3: the declared CRD schema default must match the actual runtime
// default (`ReshardPolicy::default().max_shard_bytes.is_none() == true`),
// not `bool::default()` (`false`).
fn default_reshard_recommendation_only() -> bool {
    true
}
