//! `ClusterControl`: everything the driver needs from a live cluster.

use anyhow::Result;
use async_trait::async_trait;
use service_auth::k8s::ProjectedToken;

use crate::operator::application::reshard_driver::WRITE_FENCE_TTL_SECS;
use crate::operator::domain::lumen_spec::Lumen;

/// Everything [`drive_tick`] needs from a live cluster, abstracted so the
/// state machine is testable without a real k8s API server. [`KubeClusterControl`]
/// is the production implementation; tests supply an in-memory fake.
///
/// [`drive_tick`]: crate::operator::application::reshard_driver::drive_tick
/// [`KubeClusterControl`]: crate::operator::infrastructure::kube_cluster_control::KubeClusterControl
#[async_trait]
pub trait ClusterControl: Send + Sync {
    /// JSON-merge-patch this `Lumen`'s `.spec` (see `Patch::Merge` semantics:
    /// nested objects merge recursively, a `null` leaf deletes that key,
    /// sibling fields not mentioned are untouched).
    async fn patch_spec(&self, namespace: &str, name: &str, patch: serde_json::Value)
        -> Result<()>;

    /// The serving StatefulSet's `.status.readyReplicas` (0 if absent/not
    /// found yet).
    async fn statefulset_ready_replicas(&self, namespace: &str, name: &str) -> Result<i64>;

    /// Bump a `kubectl rollout restart`-style pod-template annotation so a
    /// shard-map-only ConfigMap change gets picked up by a fresh generation
    /// of pods (see the module-level "known gap" note: a no-op today until
    /// serving actually reads that ConfigMap data, but still the correct
    /// operator action to take at cutover).
    async fn trigger_rolling_restart(&self, namespace: &str, name: &str) -> Result<()>;

    /// Trigger the bounded post-cutover convergence remediation restart. The
    /// production side effect is the same StatefulSet restart as cutover, but
    /// it is a distinct state-machine action: keeping the seam separate lets
    /// tests prove a normal cutover restart never consumes or masquerades as
    /// #1485's one-shot remediation attempt.
    async fn trigger_convergence_remediation_restart(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<()> {
        self.trigger_rolling_restart(namespace, name).await
    }

    /// The credential this driver presents on its admin calls, if
    /// `lumen.spec.auth` requires one. `Ok(None)` when auth is off — an
    /// instance that requires no identity rejects a *presented* bearer
    /// (#2871), so "no auth" has to mean sending nothing, not sending
    /// something harmless.
    ///
    /// Returning [`ProjectedToken`] rather than `String` is the point of the
    /// signature: the material cannot reach a log through a derived `Debug`
    /// on any struct that happens to hold one, because the only rendering it
    /// has is `<redacted>` and the only way to the bytes is an explicit
    /// [`ProjectedToken::expose`] at the call that builds the header.
    ///
    /// Implementations read per call. The kubelet rewrites the projected file
    /// in place partway through its lifetime with no notification, so a value
    /// cached across ticks would authenticate for a few minutes and then fail
    /// forever (#2877 R3).
    async fn admin_token(&self, namespace: &str, lumen: &Lumen) -> Result<Option<ProjectedToken>>;

    /// The client-facing admin API base URL for one shard's serving pod.
    /// [`KubeClusterControl`] resolves the real per-shard headless-Service
    /// DNS name (matching [`super::reconcile::pod_metrics_urls`]'s
    /// convention); an integration-test fake resolves to whatever real local
    /// address that shard's `TestServer` is actually bound to — the seam
    /// that lets [`run_migration_pass`] / [`evict_old_shards`] run against
    /// real HTTP servers + real `Engine`s without a live cluster.
    ///
    /// [`KubeClusterControl`]: crate::operator::infrastructure::kube_cluster_control::KubeClusterControl
    /// [`evict_old_shards`]: crate::operator::application::reshard_driver::migration::evict_old_shards
    /// [`run_migration_pass`]: crate::operator::application::reshard_driver::migration::run_migration_pass
    fn shard_base_url(&self, namespace: &str, name: &str, shard: u32) -> String;

    /// TTL (seconds) [`advance_catching_up`]/[`advance_catching_up_fenced`]
    /// arm/re-arm the write-pause fence with (#1443 R1). Defaults to
    /// [`WRITE_FENCE_TTL_SECS`] — production behavior is unchanged; this
    /// exists purely as a test seam so a short-TTL/slow-checkpoint scenario
    /// can be exercised deterministically without waiting 120 real seconds.
    ///
    /// [`advance_catching_up_fenced`]: crate::operator::application::reshard_driver::catch_up_fenced::advance_catching_up_fenced
    /// [`advance_catching_up`]: crate::operator::application::reshard_driver::phases::advance_catching_up
    fn write_fence_ttl_secs(&self) -> u64 {
        WRITE_FENCE_TTL_SECS
    }

    /// Whether every serving pod is confirmed `Ready` on the serving
    /// StatefulSet's current rollout (#1458 R1) — the same k8s "rollout
    /// status" pattern `kubectl rollout status` checks:
    /// `.status.updateRevision == .status.currentRevision` (no rollout
    /// in-flight) and `.status.readyReplicas == desired_replicas`. Reuses
    /// [`advance_prepare_split`]'s existing readiness-polling seam rather
    /// than adding a new one. Defaults to `Ok(true)` — production behavior
    /// only changes once [`KubeClusterControl`]'s override actually observes
    /// an in-progress rollout; every test double that does not override this
    /// keeps its prior "instantly converged" behavior.
    ///
    /// [`advance_prepare_split`]: crate::operator::application::reshard_driver::phases::advance_prepare_split
    /// [`KubeClusterControl`]: crate::operator::infrastructure::kube_cluster_control::KubeClusterControl
    async fn serving_topology_converged(
        &self,
        _namespace: &str,
        _name: &str,
        _desired_replicas: i64,
    ) -> Result<bool> {
        Ok(true)
    }

    /// #1467 R5: whether every serving pod (`0..shard_count`, one pod per
    /// shard — the reshard driver's admin plane already assumes
    /// `replicas_per_shard <= 1` in the routed topology it operates over,
    /// same as [`Self::shard_base_url`]) reports `lumen_shard_map_version
    /// == map_version` on its `/metrics` endpoint. [`Self::
    /// serving_topology_converged`] alone only proves the StatefulSet
    /// rollout *finished* (every pod `Ready` on the latest pod template) —
    /// not that each pod's process actually holds `map_version`, since the
    /// shard map itself is read from a ConfigMap the pod loads at startup,
    /// and a ConfigMap write racing a rollout's pod-recreate order is not
    /// something StatefulSet status observes at all. Defaults to `Ok(true)`
    /// for the same test-seam reason as `serving_topology_converged` —
    /// every test double that does not override this keeps its prior
    /// "instantly converged" behavior.
    async fn serving_pods_report_map_version(
        &self,
        _http: &reqwest::Client,
        _namespace: &str,
        _name: &str,
        _shard_count: u32,
        _map_version: u64,
    ) -> Result<bool> {
        Ok(true)
    }
}
