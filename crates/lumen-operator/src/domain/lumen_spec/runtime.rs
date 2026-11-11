//! Typed per-instance serving settings (`runtime`), and the classification of
//! every `lumen serve` option that decides which of them are typed.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The `lumen serve` settings a Fleet instance may carry as typed fields.
///
/// Each field overrides the same field of `defaults.runtime` when set; an unset
/// field keeps the default. Field names are camelCase on the wire.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeSettings {
    /// Seconds between snapshots. Maps to the `LUMEN_SNAPSHOT_SECS` env var.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_secs: Option<u32>,
    /// Seconds allowed for a graceful shutdown. Maps to `LUMEN_GRACE_SECS`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_secs: Option<u32>,
    /// OTLP collector endpoint for traces. Maps to `LUMEN_OTLP_ENDPOINT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
    /// Bootstrap transfer cap in bytes per second. Maps to
    /// `LUMEN_BOOTSTRAP_MAX_BYTES_PER_SEC`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap_max_bytes_per_sec: Option<u64>,
}

impl RuntimeSettings {
    /// A copy of `self` where every field that is set in `over` replaces the
    /// value here. Fields unset in `over` keep the value from `self`.
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

/// How one `lumen serve` option is exposed by the operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OptionClass {
    /// Has a typed field in the Lumen spec or in [`RuntimeSettings`].
    Typed,
    /// Reachable only through a guarded free-form escape hatch.
    GuardedEscapeHatch,
    /// The operator owns it; a spec must not set it.
    Reserved,
}

/// Every `env = "..."` option of `lumen serve` (`crates/lumen/src/bin/lumen/cli/serve.rs`)
/// with its classification. The inventory test fails when the source gains an
/// option that is not listed here.
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
