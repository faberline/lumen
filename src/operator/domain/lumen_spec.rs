//! The `Lumen` custom resource (`lumen.dev/v1alpha1`).
//!
//! One `Lumen` object declares a full deployment. Single-replica instances
//! write to a local WAL with no raft consensus; multi-replica instances add
//! Lumen-owned raft replication on top. Both regimes render the serving fleet
//! as a StatefulSet with a durable per-pod `raft` PVC backing the WAL —
//! `replicasPerShard` only gates raft consensus, never persistence. The
//! reconcile loop in [`super::reconcile`] turns this spec into StatefulSet,
//! Service, ConfigMap, PDB, and ServiceAccount objects, garbage-collected
//! via owner references.
//!
//! [`super::reconcile`]: crate::operator::application::reconcile

pub(crate) mod reshard_status;
pub(crate) mod serving;
pub(crate) mod status;
pub(crate) mod topology;
pub(crate) mod validation;

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::operator::domain::lumen_spec::serving::{AuthMode, LogFormat, ServingSpec};
use crate::operator::domain::lumen_spec::status::LumenStatus;
use crate::operator::domain::lumen_spec::topology::{ReshardPolicy, ShardMapSpec};

/// `lumen.dev/v1alpha1` `Lumen`. Namespaced: every child object the operator
/// renders lands in this object's namespace, so multiple independent lumen
/// deployments can coexist by name.
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "lumen.dev",
    version = "v1alpha1",
    kind = "Lumen",
    plural = "lumens",
    shortname = "lum",
    namespaced,
    status = "LumenStatus",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.servingReadyReplicas"}"#,
    printcolumn = r#"{"name":"Shards","type":"integer","jsonPath":".status.shardCount"}"#,
    // #2601: the `Ready` condition's status. Named `Converged` because the
    // `Ready` column above is already the ready *pod count*; renaming that
    // would change what every existing operator's `kubectl get lumen` prints.
    printcolumn = r#"{"name":"Converged","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].status"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct LumenSpec {
    /// Serving container image, e.g. `lumen:latest`. Required.
    pub image: String,

    /// Image pull policy. Defaults to `IfNotPresent`.
    #[serde(default)]
    pub image_pull_policy: Option<String>,

    /// Physical storage shard count. Data ownership is resolved through the
    /// versioned virtual-bucket map, not permanent `hash % shardCount`
    /// routing.
    #[serde(default = "default_shard_count")]
    pub shard_count: u32,

    /// Versioned virtual-bucket map metadata. The default one-shard map keeps
    /// existing installs compatible; future reshard workflows bump `version`
    /// and move selected virtual buckets to new physical shards.
    #[serde(default)]
    pub shard_map: ShardMapSpec,

    /// Raft replicas per shard. `1` (default) = a single-member serving
    /// StatefulSet with no raft consensus (still durable — the same
    /// PVC-backed `raft` volume). `> 1` adds raft-HA: a fixed peer set whose
    /// pods inject the downward-API env `raft_runtime::cluster` reads (raft
    /// needs a known membership).
    #[serde(default = "default_replicas_per_shard")]
    pub replicas_per_shard: u32,

    /// Voting members per shard (the rest are learners). Only meaningful when
    /// `replicasPerShard > 1`.
    #[serde(default = "default_replicas_per_shard")]
    pub voter_count: u32,

    /// Secret holding `tls.crt`, `tls.key`, and `ca.crt` — the instance-scoped
    /// X.509 identity every Raft member presents and verifies on the dedicated
    /// peer listener (#2890). Same field and Secret contract Relay and Defer
    /// already project, so one shared mechanism (`libs/peer-tls`) covers all
    /// three.
    ///
    /// Required whenever `replicasPerShard > 1`: replicated Raft traffic
    /// carries committed index mutations between pods, and Kubernetes
    /// ServiceAccount tokens authenticate *callers*, not peers — nothing else
    /// on that port says who is dialing. A replicated instance without it does
    /// not fall back to plaintext; the operator reports
    /// `PeerIdentityReady=False` naming this Secret, and `lumen serve` refuses
    /// to start.
    ///
    /// Omit only for a single-replica instance, which runs no consensus link
    /// at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_tls_secret: Option<String>,

    /// Secret holding `tls.crt`, `tls.key`, and `ca.crt` — the leaf every
    /// serving pod presents on the client port, issued for the Kubernetes
    /// Service DNS names callers actually dial (#3113 R1/R2).
    ///
    /// A different identity from [`Self::peer_tls_secret`], and deliberately a
    /// different field: a serving certificate says "I am the Service you asked
    /// for" to a client that authenticates separately with a KSA token, while
    /// a peer certificate says "I am a member of this instance's Raft group".
    /// Sharing one Secret between them would let either listener's material
    /// authenticate on the other's port.
    ///
    /// When set, the client port terminates TLS with ALPN `h2` and
    /// `http/1.1`, and refuses connections outright while no valid leaf is
    /// active — there is no plaintext fallback to notice too late. Omit it
    /// only for local/kind development, where the port stays h2c.
    ///
    /// Callers verify this leaf against the public CA distributed separately by
    /// the deployment administrator or external certificate platform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serving_tls_secret: Option<String>,

    /// Log output format: `json` (prod/staging) or `pretty` (dev).
    #[serde(default)]
    pub log_format: LogFormat,

    /// Log level (`trace|debug|info|warn|error`). Defaults to `info`.
    #[serde(default)]
    pub log_level: Option<String>,

    /// Auth mode: `required` (the default — callers are resolved through the
    /// cluster's own TokenReview/SubjectAccessReview) or `disabled`.
    ///
    /// `required` is the default because the other way round, forgetting this
    /// field ships an open cluster and nothing says so; forgetting it now
    /// fails startup with a message naming the field to set. `disabled`
    /// remains a one-word opt-out for local development (#2678, R4).
    ///
    /// Spelled `disabled`, not `off`: YAML 1.1 reads a bare `off` as the
    /// boolean `false`. (`off` is what the serving process's own `LUMEN_AUTH`
    /// env var takes — the two spellings are not interchangeable.)
    #[serde(default)]
    pub auth: AuthMode,

    /// Name of a pre-existing, externally-managed ServiceAccount for the
    /// workload pods. When set, the operator uses this SA and never creates,
    /// owns, updates, or deletes a ServiceAccount for the instance (the
    /// deployer owns its lifecycle and any Workload Identity annotations).
    /// When unset, the operator creates and owns `<instance>` as before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,

    /// Annotations applied verbatim to both rendered ServiceAccounts (the
    /// workload SA when created, and the backup SA). Default empty.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub service_account_annotations: BTreeMap<String, String>,

    /// Stateless serving-fleet shape.
    #[serde(default)]
    pub serving: ServingSpec,

    /// Which nodes the serving pods may run on.
    #[serde(default)]
    pub placement: PlacementSpec,

    /// Operator-owned storage reshard policy. This policy only
    /// prepares/recommends explicit shard topology changes.
    #[serde(default)]
    pub reshard_policy: ReshardPolicy,

    /// Emit a ServiceMonitor + PrometheusRule. Requires the prometheus-operator
    /// CRDs (`monitoring.coreos.com/v1`) to be installed in the cluster.
    #[serde(default)]
    pub observability: bool,

    /// Emit a NetworkPolicy isolating this instance (#2603): the client API
    /// (`7373`) stays reachable from any namespace, while the Raft port
    /// (`7374`) is reachable only from this instance's own pods, and egress is
    /// narrowed to DNS, TLS, and sibling Raft.
    ///
    /// Opt-in rather than default-on for one reason: a NetworkPolicy is inert
    /// unless the cluster runs a CNI that enforces it. On GKE that means
    /// Dataplane V2 or the Calico add-on; on a plain kind cluster (default
    /// kindnet) the object applies cleanly and enforces nothing, which would
    /// otherwise read as "isolation is on" when it is not. Defaulting it on
    /// would also break any cluster whose scrapers or clients live outside the
    /// pod network, with no signal beyond dropped packets.
    #[serde(default)]
    pub network_policy: bool,

    /// Optional in-process request admission (bounded token-bucket rate
    /// limiting per endpoint class), mirroring the `LUMEN_ADMISSION_*` env
    /// grammar `libs/service-http::AdmissionConfig` already parses (see
    /// `bin/lumen.rs`'s `serve` wiring). Absent means admission stays
    /// disabled — the pre-existing default; no new semantics, only a
    /// declarative surface for the existing env-driven behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<AdmissionSpec>,

    /// Data-plane request body size limit (bytes). Requests with
    /// `Content-Length` exceeding this are rejected with 413; streamed bodies
    /// are bounded mid-read. Defaults to 8 MiB. Unset means the server
    /// default (#2584). Accepted range: 1 MiB (1048576) to 64 MiB (67108864).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_limit_bytes: Option<u64>,
}

/// Minimum accepted request body limit: 1 MiB (1048576 bytes).
pub const MIN_BODY_LIMIT_BYTES: u64 = 1024 * 1024;

/// Maximum accepted request body limit: 64 MiB (67108864 bytes).
pub const MAX_BODY_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

/// Declarative form of the `LUMEN_ADMISSION_*` env grammar. Every field is
/// optional and independently maps to one env var; a field left unset never
/// enables admission for that class (mirrors `AdmissionConfig::from_env`'s
/// "capacity absent = class unbounded" semantics exactly).
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionSpec {
    /// Token-bucket capacity for read-class requests
    /// (`LUMEN_ADMISSION_READ_CAPACITY`). Unset leaves reads unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_capacity: Option<u32>,
    /// Token-bucket capacity for write-class requests
    /// (`LUMEN_ADMISSION_WRITE_CAPACITY`). Unset leaves writes unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_capacity: Option<u32>,
    /// Token-bucket capacity for admin-class requests
    /// (`LUMEN_ADMISSION_ADMIN_CAPACITY`). Unset leaves admin unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_capacity: Option<u32>,
    /// Refill window in seconds, shared by every configured class
    /// (`LUMEN_ADMISSION_REFILL_SECS`). Unset falls back to
    /// `AdmissionConfig::DEFAULT_REFILL_SECS` (60s).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refill_secs: Option<u32>,
    /// Maximum distinct admission keys retained per class
    /// (`LUMEN_ADMISSION_MAX_KEYS`). Unset falls back to
    /// `AdmissionConfig::DEFAULT_MAX_KEYS` (1024).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<u32>,
}

/// Where the serving pods are allowed to run.
///
/// Deliberately narrower than Kubernetes' `affinity`: `nodeSelector` and
/// `tolerations` together express "which node pool" completely, while the
/// operator keeps sole ownership of `podAntiAffinity` — the constraint that
/// keeps two replicas of one shard off the same host. Exposing the whole
/// `affinity` block would let a deployer replace that constraint while asking
/// only for a node pool, silently degrading a raft-HA instance into two copies
/// on one machine; the rendered StatefulSet would still look correct, and the
/// first node failure would take both replicas of the shard.
///
/// A dedicated node pool for a stateful search workload is not an exotic
/// request — local SSD and high-memory pools are the normal shape on GKE — and
/// until this existed there was no way to ask for one: the StatefulSet is
/// operator-rendered, so a manual `kubectl patch` is reverted on the next
/// reconcile.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PlacementSpec {
    /// Initial GCE machine type for the data plane (e.g. `e2-standard-2`).
    /// Defaults to `e2-standard-2`. Immutable create-time configuration.
    #[serde(default = "default_initial_machine_type")]
    pub initial_machine_type: String,

    /// `spec.template.spec.nodeSelector` for the serving pods, e.g.
    /// `{ "cloud.google.com/gke-nodepool": "lumen-ssd" }`. Empty means the
    /// scheduler picks from every node, as before.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,

    /// Taint tolerations for the serving pods, so a dedicated node pool can
    /// carry a taint that keeps every other workload off it. Note this covers
    /// the serving StatefulSet only: the optional backup CronJob is a
    /// short-lived pod that reads over the network and is left schedulable on
    /// the cluster's general pool.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,
}

impl Default for PlacementSpec {
    fn default() -> Self {
        Self {
            initial_machine_type: default_initial_machine_type(),
            node_selector: BTreeMap::new(),
            tolerations: Vec::new(),
        }
    }
}

/// One entry of [`PlacementSpec::tolerations`], mirroring the Kubernetes
/// `v1.Toleration` fields.
///
/// Declared here rather than reused from `k8s-openapi` because the CRD schema
/// is derived with `schemars`, which `k8s-openapi`'s types do not implement.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// The taint key this tolerates. Empty with `operator: Exists` tolerates
    /// every taint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// `Exists` or `Equal`. Unset means `Equal` (the Kubernetes default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,

    /// The taint value to match. Only meaningful with `operator: Equal`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// `NoSchedule`, `PreferNoSchedule`, or `NoExecute`. Unset tolerates every
    /// effect of the matching taint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,

    /// How long the pod stays bound after the node gains a matching taint.
    /// Only meaningful with `effect: NoExecute`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

fn default_shard_count() -> u32 {
    1
}

fn default_replicas_per_shard() -> u32 {
    1
}

fn default_initial_machine_type() -> String {
    "e2-standard-2".into()
}

#[cfg(test)]
mod tests;
