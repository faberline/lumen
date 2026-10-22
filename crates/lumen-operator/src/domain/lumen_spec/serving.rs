//! The serving pods: log format, auth mode, resources, bootstrap and scheduled
//! backup.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Log output format.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Structured one-line-per-event JSON (prod/staging).
    Json,
    /// Human-readable multi-line (dev).
    #[default]
    Pretty,
}

impl LogFormat {
    /// The `LUMEN_LOG_FORMAT` value the serving binary expects.
    pub fn as_env(self) -> &'static str {
        match self {
            LogFormat::Json => "json",
            LogFormat::Pretty => "pretty",
        }
    }
}

/// Whether the client API requires a bearer token.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    /// Open API (dev / trusted network) — an explicit opt-out, never the
    /// default (#2678, R4). Serialized as `disabled` — NOT `off`, which YAML
    /// 1.1 (kubectl / go-yaml) would parse as the boolean `false` and corrupt
    /// the CRD enum/default.
    #[serde(rename = "disabled")]
    Off,
    /// Authenticated callers only, resolved by the cluster: every request
    /// carries a short-lived audience-bound ServiceAccount token, which the
    /// serving pod checks with TokenReview and authorizes with
    /// SubjectAccessReview. The default, so a `Lumen` that omits `spec.auth`
    /// requires an identity instead of serving an open API silently.
    #[default]
    Required,
}

impl AuthMode {
    /// The `LUMEN_AUTH` value the serving binary expects.
    pub fn as_env(self) -> &'static str {
        match self {
            AuthMode::Off => "off",
            AuthMode::Required => "required",
        }
    }
}

/// Stateless serving-fleet shape: per-pod resources.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServingSpec {
    /// Per-pod CPU, applied as request==limit (Guaranteed QoS). e.g. `"2"`.
    #[serde(default = "default_serving_cpu")]
    pub cpu: String,
    /// Per-pod memory, applied as request==limit. e.g. `"4Gi"`.
    #[serde(default = "default_serving_memory")]
    pub memory: String,
    /// Graceful drain window on SIGTERM (seconds); tracks
    /// `terminationGracePeriodSeconds`.
    #[serde(default = "default_grace_secs")]
    pub grace_secs: u64,
    /// Per-pod WAL/raft hard-state PVC size. Always applied — the serving
    /// StatefulSet's `raft` volumeClaimTemplate exists at every
    /// `replicasPerShard` value, not only when raft consensus (`> 1`) is
    /// active.
    #[serde(default = "default_raft_storage")]
    pub raft_storage: String,
    /// PVC StorageClass for the WAL/raft hard-state volume. Unset means
    /// cluster default — which, on most managed Kubernetes offerings, is
    /// **not** SSD-backed (e.g. GKE's default `standard-rwo` is
    /// pd-balanced, not pd-ssd). Raft/WAL write latency is sensitive to
    /// disk performance, so a deployer who cares about that latency should
    /// set this field explicitly to an SSD-backed StorageClass name rather
    /// than relying on the cluster default (see `lumen llm --topic storage` for
    /// example StorageClass names per common provider — informational
    /// reference only, not a value validated or defaulted by this field).
    #[serde(default)]
    pub raft_storage_class: Option<String>,
    /// Optional scheduled backup (#808). When set, the operator renders a
    /// `<name>-backup` CronJob (see [`super::render::backup_cron_job`]) that
    /// invokes `lumen backup` on this schedule against the running serving
    /// fleet's own already-existing `/admin/backup` endpoint — no new
    /// snapshot mechanism, only scheduling + transport. Absent means no
    /// CronJob; the admin API (`GET /admin/backup`, `POST /admin/backup/local`,
    /// `POST /admin/restore`) is still reachable for manual/scripted use
    /// either way (see `lumen llm --topic storage`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<ServingBackupSpec>,
    /// Optional empty-PVC bootstrap seed. When set, serving pods restore this
    /// snapshot before WAL/raft catch-up. Supported seed URIs are exact
    /// `file://` SnapshotV1 JSON paths and, in backup-enabled builds, exact
    /// `s3://bucket/key` SnapshotV1 objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<ServingBootstrapSpec>,
}

impl Default for ServingSpec {
    fn default() -> Self {
        Self {
            cpu: default_serving_cpu(),
            memory: default_serving_memory(),
            grace_secs: default_grace_secs(),
            raft_storage: default_raft_storage(),
            raft_storage_class: None,
            backup: None,
            bootstrap: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServingBootstrapSpec {
    /// SnapshotV1 JSON seed URI. Use an exact `file://` path or
    /// `s3://bucket/key` object, not a backup prefix. Note (#2514): the
    /// serving pod reads the seed object through its own (Workload-Identity)
    /// ServiceAccount, which needs storage read (e.g. roles/storage.objectViewer
    /// on GCS) on the seed bucket; see the README "Deployer note
    /// (seed-bucket IAM)".
    pub seed_uri: String,
    /// Optional read throttle advertised to operators/status. The current
    /// source primitive reads one object per bootstrap; transfer shaping can be
    /// enforced by the object-store client/proxy or a future streaming reader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_sec: Option<u64>,
}

/// Declarative backup policy for the serving fleet (#808).
///
/// Common CRD-safe fields come from
/// [`service_backup::ScheduledBackupPolicy`]. Lumen owns only the optional
/// admin-token Secret reference used by its runner CronJob.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServingBackupSpec {
    /// Shared flat `schedule`, `destination`, and `retentionSecs` contract.
    #[serde(flatten)]
    pub policy: service_backup::ScheduledBackupPolicy,
    /// Name of a Secret whose `token` key holds a bearer token with
    /// `Role::Admin` on `*`. Deprecated; the backup runner authenticates with
    /// its own projected ServiceAccount token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_token_secret: Option<String>,
}

impl std::ops::Deref for ServingBackupSpec {
    type Target = service_backup::ScheduledBackupPolicy;

    fn deref(&self) -> &Self::Target {
        &self.policy
    }
}

fn default_serving_cpu() -> String {
    "1".into()
}

fn default_serving_memory() -> String {
    "4Gi".into()
}

fn default_grace_secs() -> u64 {
    30
}

fn default_raft_storage() -> String {
    "10Gi".into()
}
