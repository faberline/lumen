//! The virtual-bucket shard map, the reshard policy, and the reshard workflow
//! the driver persists its phase in.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Versioned virtual-bucket map control-plane metadata.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ShardMapSpec {
    #[serde(default)]
    pub version: u64,
    #[serde(default = "default_virtual_bucket_count")]
    pub virtual_bucket_count: u32,
    /// Optional explicit `bucket -> physical shard` assignments. Empty means
    /// derive the deterministic balanced assignment `bucket % shardCount`;
    /// reshard workflows set this to move selected buckets without changing
    /// every key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assignments: Vec<u32>,
}

impl Default for ShardMapSpec {
    fn default() -> Self {
        Self {
            version: 0,
            virtual_bucket_count: default_virtual_bucket_count(),
            assignments: Vec::new(),
        }
    }
}

/// Storage-pressure policy for rare, operator-owned shard split workflows.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReshardPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_shard_bytes: Option<u64>,
    #[serde(default = "default_reshard_prepare_percent")]
    pub prepare_at_percent: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at_percent: Option<u8>,
    #[serde(default = "default_reshard_urgent_percent")]
    pub urgent_at_percent: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_shards: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub workflow: ReshardWorkflowSpec,
}

impl Default for ReshardPolicy {
    fn default() -> Self {
        Self {
            max_shard_bytes: None,
            prepare_at_percent: default_reshard_prepare_percent(),
            start_at_percent: None,
            urgent_at_percent: default_reshard_urgent_percent(),
            max_shards: None,
            migration_bytes_per_sec: None,
            workflow: ReshardWorkflowSpec::default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReshardWorkflowSpec {
    #[serde(default)]
    pub phase: ReshardPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_shard_count: Option<u32>,
    /// `shardMap.version` the reshard driver has confirmed every serving
    /// pod is `Ready` on (#1458 R1) — the persisted checkpoint
    /// `reshard_driver::advance_convergence` compares `spec.shardMap.
    /// version` against to decide whether the post-cutover write-pause
    /// fence must stay armed. `None` (or a value behind the current
    /// `shardMap.version`) means convergence for the current map is still
    /// pending or was never confirmed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub converged_shard_map_version: Option<u64>,
    /// `shardMap.version` the reshard driver's own cutover last patched
    /// into `spec.shardMap.version` (#1467 R7), stamped in the exact same
    /// `advance_catching_up_fenced` patch call that sets `shardMap.
    /// version`/`phase: Complete`. The ONLY writer of this field — a
    /// hand-authored or backup-restored `spec.shardMap` never sets it, so
    /// it stays behind (usually `None`) forever for such a CR.
    /// `advance_convergence` requires this to equal the current
    /// `shardMap.version` before engaging the post-cutover write-pause
    /// fence loop at all, closing the gap where convergence would
    /// otherwise fence indefinitely over a topology the driver never
    /// actually changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_cutover_shard_map_version: Option<u64>,
    /// Epoch-seconds wall-clock timestamp `reshard_driver::advance_convergence`
    /// first observed the *current* `shardMap.version`'s post-cutover
    /// convergence wait as pending (#1485 R2) — stamped once, on the first
    /// `AwaitingTopologyConvergence` tick, in the same `Patch::Merge` style
    /// `lastCutoverShardMapVersion` already uses (spec-is-checkpoint, not
    /// driver memory). Cleared (patched to `null`) the moment convergence is
    /// confirmed, so it is always either `None` or the start of the wait
    /// still in progress. `reshard_driver::convergence_stall_condition`
    /// computes the `topologyConvergenceStalled` budget directly from this
    /// field, so both the budget and the raised condition survive an
    /// operator restart — replacing the prior process-local-cache-only
    /// computation, which reset to zero on every restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_wait_started_at: Option<u64>,
    /// Count of bounded remediation rolling-restart re-triggers
    /// `reshard_driver::advance_convergence` has fired for the current
    /// convergence-stall episode — the same wait `convergenceWaitStartedAt`
    /// tracks (#1485 R1). Bounded to at most `1`: once the stall budget is
    /// exceeded with the ConfigMap-race signature (StatefulSet rollout
    /// complete but some pod still reporting the old shard-map version), the
    /// driver calls `ClusterControl::trigger_rolling_restart` exactly once
    /// per episode and bumps this to `1`; a later stall in the same episode
    /// never re-triggers. Reset to `0` alongside `convergenceWaitStartedAt`
    /// once the episode resolves.
    #[serde(default)]
    pub convergence_remediation_restart_count: u32,
    /// Epoch-seconds timestamp of the last remediation rolling-restart
    /// re-trigger this episode, if any (#1485 R1) — surfaced alongside
    /// `convergenceRemediationRestartCount` in `status.reshard` so operators
    /// can see when the self-heal fired without reading driver logs. `None`
    /// until `convergenceRemediationRestartCount` first becomes non-zero;
    /// cleared together with it once the episode resolves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub convergence_remediation_restarted_at: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum ReshardPhase {
    #[default]
    Complete,
    PrepareSplit,
    Splitting,
    CatchingUp,
}

impl ReshardPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "Complete",
            Self::PrepareSplit => "PrepareSplit",
            Self::Splitting => "Splitting",
            Self::CatchingUp => "CatchingUp",
        }
    }

    pub fn progress_percent(self) -> u8 {
        match self {
            Self::Complete => 100,
            Self::PrepareSplit => 10,
            Self::Splitting => 60,
            Self::CatchingUp => 90,
        }
    }
}

fn default_virtual_bucket_count() -> u32 {
    crate::routing::DEFAULT_VIRTUAL_BUCKET_COUNT
}

fn default_reshard_prepare_percent() -> u8 {
    50
}

fn default_reshard_urgent_percent() -> u8 {
    85
}
