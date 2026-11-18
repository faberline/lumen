//! The namespaced access Role and RoleBinding (`spec.access`, #365): apply them
//! on every reconcile, refuse to adopt a foreign object at the same name, and
//! sweep the ones whose `Lumen` is gone or no longer grants access.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::application::render;
use crate::domain::lumen_spec::Lumen;

/// The two namespaced kinds the access policy renders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AccessKind {
    Role,
    RoleBinding,
}

impl AccessKind {
    /// The Kubernetes kind name, as it appears in messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Role => "Role",
            Self::RoleBinding => "RoleBinding",
        }
    }
}

/// Seam for the access policy's cluster side effects, abstracted the same way
/// [`AuthDelegatorControl`] abstracts the auth-delegator binding, so the apply
/// and sweep decisions are testable without an API server.
///
/// [`AuthDelegatorControl`]: crate::application::reconcile::auth_delegator::AuthDelegatorControl
#[async_trait::async_trait]
pub trait AccessPolicyControl: Send + Sync {
    /// Server-side-apply one rendered Role or RoleBinding.
    async fn apply(&self, object: &Value) -> anyhow::Result<()>;

    /// The labels of the object at `(kind, namespace, name)`, or `None` when it
    /// does not exist.
    async fn get_labels(
        &self,
        kind: AccessKind,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>>;

    /// Every Role and RoleBinding lumen's operator manages, in all namespaces.
    /// Selected server-side on the managed-by and component labels.
    async fn managed(&self) -> anyhow::Result<Vec<ManagedAccessObject>>;

    /// Delete one object. A 404 is success.
    async fn delete(&self, kind: AccessKind, namespace: &str, name: &str) -> anyhow::Result<()>;
}

/// One managed Role or RoleBinding, as the cluster reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedAccessObject {
    pub kind: AccessKind,
    pub namespace: String,
    pub name: String,
    pub labels: BTreeMap<String, String>,
}

/// Label keys that prove an existing object is this instance's own.
const OWNERSHIP_KEYS: [&str; 3] = [
    "app.kubernetes.io/managed-by",
    "app.kubernetes.io/component",
    "app.kubernetes.io/instance",
];

/// Whether an existing object's labels carry this instance's ownership keys.
fn owned_by(labels: &BTreeMap<String, String>, expected: &BTreeMap<String, String>) -> bool {
    OWNERSHIP_KEYS
        .iter()
        .all(|key| labels.get(*key).is_some() && labels.get(*key) == expected.get(*key))
}

/// Apply this instance's access Role and RoleBinding, returning the failure
/// message to publish if a write was refused or an existing object is not ours.
///
/// Like the auth-delegator apply, the error is returned rather than
/// propagated, so the CR reports the true state (not converged, and why)
/// instead of aborting before its status patch. A foreign object at the same
/// name is never overwritten: that would silently take over someone else's
/// RBAC object.
pub async fn apply_access_policy(
    control: &dyn AccessPolicyControl,
    lumen: &Lumen,
) -> Option<String> {
    lumen.spec.access.as_ref()?;
    let expected = render::access::access_policy_labels(lumen);
    let objects = [
        (AccessKind::Role, render::access::access_role(lumen)),
        (
            AccessKind::RoleBinding,
            render::access::access_role_binding(lumen),
        ),
    ];
    let mut messages = Vec::new();
    for (kind, object) in objects {
        let namespace = object["metadata"]["namespace"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let name = object["metadata"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let label = format!("{} {namespace}/{name}", kind.as_str());
        match control.get_labels(kind, &namespace, &name).await {
            Err(err) => {
                messages.push(format!("apply {label}: {err}"));
                continue;
            }
            Ok(Some(labels)) if !owned_by(&labels, &expected) => {
                messages.push(format!(
                    "{label} exists and is not managed by lumen-operator; refusing to adopt it"
                ));
                continue;
            }
            Ok(_) => {}
        }
        if let Err(err) = control.apply(&object).await {
            messages.push(format!("apply {label}: {err}"));
        }
    }
    if messages.is_empty() {
        return None;
    }
    for message in &messages {
        tracing::warn!(message = %message, "access policy: apply refused or not converged");
    }
    Some(messages.join("; "))
}

/// Delete managed access objects whose `Lumen` is gone or no longer sets
/// `spec.access`.
///
/// `live` is every `Lumen` in the cluster. An object survives only if a live
/// instance with `spec.access` set would render exactly it: same kind, same
/// name and the same full label set. A name alone is not proof of authorship,
/// and these objects grant RBAC, so a differently labelled object at a live
/// name is left alone.
pub async fn sweep_stale_access_policy(control: &dyn AccessPolicyControl, live: &[Lumen]) {
    let managed = match control.managed().await {
        Ok(managed) => managed,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "access policy sweep: listing managed Roles and RoleBindings failed, will retry next tick"
            );
            return;
        }
    };
    let mut wanted: BTreeMap<(AccessKind, String, String), BTreeMap<String, String>> =
        BTreeMap::new();
    for lumen in live.iter().filter(|lumen| lumen.spec.access.is_some()) {
        let labels = render::access::access_policy_labels(lumen);
        for (kind, object) in [
            (AccessKind::Role, render::access::access_role(lumen)),
            (
                AccessKind::RoleBinding,
                render::access::access_role_binding(lumen),
            ),
        ] {
            let namespace = object["metadata"]["namespace"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let name = object["metadata"]["name"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            wanted.insert((kind, namespace, name), labels.clone());
        }
    }
    for object in managed {
        let key = (object.kind, object.namespace.clone(), object.name.clone());
        match wanted.get(&key) {
            Some(expected) if *expected == object.labels => continue,
            Some(_) => {
                tracing::warn!(
                    object = %format!("{} {}/{}", object.kind.as_str(), object.namespace, object.name),
                    "access policy sweep: an object at a live instance's name carries labels \
                     lumen does not render — leaving it alone (not operator-rendered)"
                );
                continue;
            }
            None => {}
        }
        match control
            .delete(object.kind, &object.namespace, &object.name)
            .await
        {
            Ok(()) => tracing::info!(
                object = %format!("{} {}/{}", object.kind.as_str(), object.namespace, object.name),
                "access policy sweep: deleted an operator-managed object whose Lumen no longer \
                 grants access"
            ),
            Err(err) => tracing::warn!(
                object = %format!("{} {}/{}", object.kind.as_str(), object.namespace, object.name),
                error = %err,
                "access policy sweep: delete failed, will retry next tick"
            ),
        }
    }
}
