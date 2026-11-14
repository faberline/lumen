//! `FleetControl`: everything one fleet convergence pass needs from a live
//! cluster. The production implementation is
//! [`KubeFleetControl`](crate::infrastructure::kube_fleet_control::KubeFleetControl);
//! tests supply an in-memory fake.

use async_trait::async_trait;

/// Cluster calls made by the fleet pass, one method per call. Every method
/// returns its own error so the pass can keep each failure on its entry.
#[async_trait]
pub trait FleetControl: Send + Sync {
    /// Does this namespace exist?
    async fn namespace_exists(&self, namespace: &str) -> anyhow::Result<bool>;

    /// The live `Lumen`'s `lumen.dev/fleet` label: `Ok(None)` = object absent,
    /// `Ok(Some(None))` = present without the label, `Ok(Some(Some(v)))` =
    /// labelled `v`.
    async fn lumen_fleet_label(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<Option<String>>>;

    /// Create with field manager `FLEET_SEED_MANAGER`. `body` is `seed_object(..)`.
    async fn create_lumen(&self, body: serde_json::Value) -> anyhow::Result<()>;

    /// Server-side apply with `PatchParams::apply(FLEET_MANAGER).force()`.
    /// `body` is `apply_object(..)`.
    async fn apply_lumen(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<()>;

    /// `(namespace, name)` of every `Lumen` labelled `lumen.dev/fleet=<fleet>`,
    /// in all namespaces.
    async fn list_fleet_lumens(&self, fleet: &str) -> anyhow::Result<Vec<(String, String)>>;

    /// Delete one `Lumen`.
    async fn delete_lumen(&self, namespace: &str, name: &str) -> anyhow::Result<()>;

    /// Merge-patch the fleet's `status` subresource.
    async fn patch_fleet_status(
        &self,
        fleet: &str,
        status: serde_json::Value,
    ) -> anyhow::Result<()>;
}
