//! The per-instance auth-delegator binding (#2876): apply it on every
//! reconcile, and sweep the ones whose instance is gone.

use std::collections::BTreeMap;

use crate::operator::application::render;
use crate::operator::domain::lumen_spec::Lumen;

/// Seam for the auth-delegator binding's three cluster side effects (#2876),
/// abstracted the same way [`HpaControl`] abstracts the HPA handoff's, so both
/// the apply decision and the sweep decision are testable without an API
/// server. [`KubeAuthDelegatorControl`] is the production implementation.
///
/// [`HpaControl`]: crate::operator::application::reconcile::hpa::HpaControl
/// [`KubeAuthDelegatorControl`]: crate::operator::infrastructure::kube_auth_delegator_control::KubeAuthDelegatorControl
#[async_trait::async_trait]
pub(in crate::operator) trait AuthDelegatorControl: Send + Sync {
    /// Server-side-apply the binding. Cluster-scoped: `Api::all_with`, not
    /// `Api::namespaced_with`, which is why this cannot ride the shared
    /// controller's child-apply loop.
    async fn apply_binding(&self, binding: &serde_json::Value) -> anyhow::Result<()>;

    /// Every ClusterRoleBinding lumen's operator manages, as
    /// `(name, labels)`. Selected server-side on the managed-by/component
    /// labels so the sweep never even sees a binding belonging to something
    /// else.
    async fn managed_bindings(&self) -> anyhow::Result<Vec<(String, BTreeMap<String, String>)>>;

    /// Delete one binding by name. A 404 is success: the sweep runs every tick
    /// and races itself across operator replicas.
    async fn delete_binding(&self, name: &str) -> anyhow::Result<()>;
}

/// Apply this instance's auth-delegator binding, returning the failure message
/// to publish if the write was refused (#2876 R5, AC4).
///
/// The error is returned rather than propagated because of where it has to
/// land. Failing the reconcile outright aborts before the status patch, so the
/// CR would go on reporting whatever it last said while its serving pods could
/// not authenticate a single request — a silent failure with a healthy-looking
/// status. Carrying the message forward instead makes the CR say the true
/// thing: not Ready, and *why*, naming the ClusterRoleBinding operation that
/// was refused. The 30-second requeue retries it either way, so nothing is
/// lost by not erroring.
pub(super) async fn apply_auth_delegator_binding(
    control: &dyn AuthDelegatorControl,
    lumen: &Lumen,
) -> Option<String> {
    let binding = render::identity::auth_delegator_binding(lumen);
    let name = render::identity::auth_delegator_binding_name(lumen);
    match control.apply_binding(&binding).await {
        Ok(()) => None,
        Err(err) => {
            tracing::warn!(
                binding = %name, error = %err,
                "auth delegation: apply of the serving ServiceAccount's \
                 system:auth-delegator ClusterRoleBinding failed"
            );
            Some(format!(
                "apply ClusterRoleBinding {name} (system:auth-delegator): {err}"
            ))
        }
    }
}

/// Delete auth-delegator bindings whose owning `Lumen` is gone (#2876 R3/R5,
/// AC3).
///
/// A cluster-scoped object cannot be owned by a namespaced CR, so there is no
/// cascading delete to rely on and — since a deleted CR is never reconciled
/// again — no reconcile that could clean up after itself either. This sweep is
/// the replacement: it runs cluster-wide against the live `Lumen` list, so the
/// object that authorizes an instance disappears with the instance rather than
/// when something remembers to look.
///
/// `live` is every `Lumen` in the cluster. A binding survives only if some
/// live instance would render *exactly* it — same name and same full label
/// set. Matching on the full label set, rather than on the name alone, is the
/// same guard [`prune_stale_hpa`] uses and for the same reason: a name is not
/// proof of authorship, and this one deletes an RBAC object.
///
/// [`prune_stale_hpa`]: crate::operator::application::reconcile::hpa::prune_stale_hpa
pub(super) async fn sweep_stale_auth_delegator_bindings(
    control: &dyn AuthDelegatorControl,
    live: &[Lumen],
) {
    let bindings = match control.managed_bindings().await {
        Ok(bindings) => bindings,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "auth delegation sweep: listing managed ClusterRoleBindings failed, will retry next tick"
            );
            return;
        }
    };
    let wanted: BTreeMap<String, BTreeMap<String, String>> = live
        .iter()
        .map(|lumen| {
            (
                render::identity::auth_delegator_binding_name(lumen),
                render::identity::auth_delegator_labels(lumen),
            )
        })
        .collect();
    for (name, labels) in bindings {
        match wanted.get(&name) {
            // Still wanted, and it looks like ours — the reconcile path keeps
            // its subject current, so there is nothing to do here.
            Some(expected) if *expected == labels => continue,
            Some(_) => {
                // A live instance claims this name but the labels are not the
                // ones lumen stamps. Deleting would be acting on an object we
                // cannot show we created; the apply path will correct the
                // fields it owns.
                tracing::warn!(
                    binding = %name,
                    "auth delegation sweep: a binding at a live instance's name carries labels \
                     lumen does not render — leaving it alone (not operator-rendered)"
                );
                continue;
            }
            None => {}
        }
        match control.delete_binding(&name).await {
            Ok(()) => tracing::info!(
                binding = %name,
                "auth delegation sweep: deleted a system:auth-delegator ClusterRoleBinding whose \
                 Lumen instance no longer exists"
            ),
            Err(err) => tracing::warn!(
                binding = %name, error = %err,
                "auth delegation sweep: delete failed, will retry next tick"
            ),
        }
    }
}
