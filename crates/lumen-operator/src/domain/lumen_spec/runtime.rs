//! The typed per-instance runtime settings of a `Lumen`, and the inventory that
//! classifies every `lumen serve` option as typed, guarded escape hatch or
//! reserved. This slice adds the types and the merge only; nothing renders
//! these fields into pods yet.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Runtime settings a Fleet instance can set for itself. Each field is
/// optional. A field that is not set keeps the value of the fleet defaults.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSettings {
    /// Seconds between snapshots (`LUMEN_SNAPSHOT_SECS`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_secs: Option<u32>,
    /// Grace period in seconds (`LUMEN_GRACE_SECS`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_secs: Option<u32>,
    /// OTLP endpoint for exported telemetry (`LUMEN_OTLP_ENDPOINT`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    /// Cap on bootstrap transfer in bytes per second (`LUMEN_BOOTSTRAP_MAX_BYTES_PER_SEC`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_max_bytes_per_sec: Option<u64>,
}

impl RuntimeSettings {
    /// A copy of `self` where every field that is set in `over` replaces the
    /// value of `self`. A field that is unset in `over` keeps the value of `self`.
    pub fn overlay(&self, over: &RuntimeSettings) -> RuntimeSettings {
        RuntimeSettings {
            snapshot_secs: over.snapshot_secs.or(self.snapshot_secs),
            grace_secs: over.grace_secs.or(self.grace_secs),
            otlp_endpoint: over
                .otlp_endpoint
                .clone()
                .or_else(|| self.otlp_endpoint.clone()),
            bootstrap_max_bytes_per_sec: over
                .bootstrap_max_bytes_per_sec
                .or(self.bootstrap_max_bytes_per_sec),
        }
    }
}

/// How a `lumen serve` option is exposed on a `Lumen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionClass {
    /// Has a typed field in the `Lumen` spec.
    Typed,
    /// Reachable through a guarded escape hatch.
    GuardedEscapeHatch,
    /// Owned by the operator. A `Lumen` cannot set it.
    Reserved,
}

/// Every `env = "..."` name of `ServeArgs` in `crates/lumen/src/bin/lumen/cli/serve.rs`,
/// with its class. A test fails when the source has an option that is not listed here.
pub const SERVE_OPTION_INVENTORY: &[(&str, OptionClass)] = &[
    ("LUMEN_LOG_LEVEL", OptionClass::Typed),
    ("LUMEN_LOG_FORMAT", OptionClass::Typed),
    ("LUMEN_SNAPSHOT_SECS", OptionClass::Typed),
    ("LUMEN_GRACE_SECS", OptionClass::Typed),
    ("LUMEN_OTLP_ENDPOINT", OptionClass::Typed),
    ("LUMEN_BOOTSTRAP_MAX_BYTES_PER_SEC", OptionClass::Typed),
    ("LUMEN_HOST", OptionClass::Reserved),
    ("LUMEN_PORT", OptionClass::Reserved),
    ("LUMEN_RAFT_PORT", OptionClass::Reserved),
    ("LUMEN_RAFT_DATA_DIR", OptionClass::Reserved),
    ("SHARD_COUNT", OptionClass::Reserved),
    ("LUMEN_DATA_DIR", OptionClass::Reserved),
    ("LUMEN_SEARCH_SHARD_SEGMENT_DIRS", OptionClass::Reserved),
    ("LUMEN_WAL", OptionClass::Reserved),
    ("LUMEN_PERSISTENCE", OptionClass::Reserved),
    ("LUMEN_BOOTSTRAP_SEED_URI", OptionClass::Reserved),
];
