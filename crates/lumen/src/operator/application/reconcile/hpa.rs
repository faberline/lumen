//! The HPA handoff (#1385): delete an HPA once the instance's rendered shape no
//! longer wants one.

use std::collections::BTreeMap;

use kube::ResourceExt;

use crate::operator::application::render;
use crate::operator::domain::lumen_spec::Lumen;

/// Seam for [`prune_stale_hpa`]'s only two k8s side effects (#1385) —
/// abstracted the same way `reshard_driver::ClusterControl` abstracts its
/// cluster calls, so the handoff decision is testable without a live k8s API
/// server. [`KubeHpaControl`] is the production implementation; tests supply
/// an in-memory fake.
///
/// [`KubeHpaControl`]: crate::operator::infrastructure::kube_hpa_control::KubeHpaControl
#[async_trait::async_trait]
pub(in crate::operator) trait HpaControl: Send + Sync {
    /// The live HPA's labels at `(namespace, name)`, or `None` if it does not
    /// exist. A missing object is the idempotent no-op case (R2), never an
    /// error.
    async fn hpa_labels(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>>;

    /// Delete the HPA at `(namespace, name)`. Only called after
    /// [`Self::hpa_labels`] has confirmed lumen rendered it. Idempotent: a
    /// concurrent deletion between the two calls (404 on delete) is treated
    /// as success, not an error.
    async fn delete_hpa(&self, namespace: &str, name: &str) -> anyhow::Result<()>;
}

/// One CR's HPA-handoff check (#1385): if `lumen`'s currently-rendered shape
/// no longer wants an HPA ([`render::wants_hpa`], R1), delete the
/// previously-rendered one if it is still there and lumen actually rendered
/// it (R2 — live name *and* labels match [`render::hpa_labels`]; a missing
/// HPA, or a live one that doesn't look lumen-rendered, is left alone). Logs
/// the handoff (why the HPA vanished) so an operator reading logs
/// understands it (R3/AC3). Never panics; a failed cluster call is logged
/// and retried next tick, same as the other background loops in this file.
pub(super) async fn prune_stale_hpa(control: &dyn HpaControl, lumen: &Lumen) {
    if render::wants_hpa(lumen) {
        // Current shape still wants one (or keeps wanting one) — nothing to
        // hand off.
        return;
    }
    let namespace = lumen.namespace().unwrap_or_else(|| "default".to_string());
    let name = lumen.name_any();
    let live_labels = match control.hpa_labels(&namespace, &name).await {
        Ok(labels) => labels,
        Err(err) => {
            tracing::warn!(
                %namespace, %name, error = %err,
                "HPA handoff: failed to read live HPA, will retry next tick"
            );
            return;
        }
    };
    let Some(live_labels) = live_labels else {
        // R2: no HPA to hand off is a no-op, not an error.
        return;
    };
    let expected_labels = render::hpa_labels(lumen);
    if live_labels != expected_labels {
        // R2 scope guard: an object happens to share this CR's name (and
        // namespace) but its labels don't match what lumen would have
        // stamped — never touch it, it wasn't rendered by us.
        tracing::warn!(
            %namespace, %name,
            "HPA handoff: found an HPA at this CR's name whose labels don't \
             match lumen's render — leaving it alone (not operator-rendered)"
        );
        return;
    }
    match control.delete_hpa(&namespace, &name).await {
        Ok(()) => {
            tracing::info!(
                %namespace, %name,
                "HPA handoff: deleted a legacy operator-rendered data-plane \
                 HPA — direct StatefulSet HPA cannot preserve whole per-shard \
                 layers or perform Raft membership transitions"
            );
        }
        Err(err) => {
            tracing::warn!(
                %namespace, %name, error = %err,
                "HPA handoff: failed to delete stale HPA, will retry next tick"
            );
        }
    }
}
