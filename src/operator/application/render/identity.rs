//! The identities the rendered workloads run under: the auth-delegator
//! ClusterRoleBinding (#2876), ServiceAccount annotations, and the
//! control-plane token (#2877).

use serde_json::{json, Value};
use service_k8s::render::projected_token::ProjectedServiceAccountToken;
use service_k8s::render::rbac;

use crate::operator::application::render::{
    instance, namespace, serving_service_account_name, APP, AUTH_DELEGATION_COMPONENT,
    AUTH_DELEGATOR_ROLE, MANAGER, OWNER_NAMESPACE_LABEL,
};
use crate::operator::domain::lumen_spec::Lumen;

/// The cluster-scoped ClusterRoleBinding's name for this instance (#2876).
///
/// Dots, not dashes, join the two components. A cluster-scoped name has no
/// namespace to disambiguate it, so `lumen-<ns>-<name>-…` would map
/// `(a-b, c)` and `(a, b-c)` to one object — two Lumens in different
/// namespaces silently sharing a binding, each granting the other's
/// ServiceAccount delegated review. A namespace is a DNS-1123 *label* and
/// cannot contain a dot, so splitting at the first dot recovers the namespace
/// exactly and the mapping is injective.
pub fn auth_delegator_binding_name(lumen: &Lumen) -> String {
    format!(
        "lumen.{}.{}.auth-delegator",
        namespace(lumen),
        instance(lumen)
    )
}

/// The exact labels [`auth_delegator_binding`] stamps.
///
/// These are load-bearing, not decoration. A cluster-scoped object cannot be
/// owned by a namespaced CR (see [`service_k8s::render::rbac`]), so labels are
/// the only link back to the instance — and the only thing the cleanup sweep
/// in [`super::reconcile`] can use to prove lumen rendered a binding before
/// deleting it. `lumen.dev/owner-namespace` exists because the recommended
/// label set has no way to say which namespace an object belongs *to* when the
/// object itself has none.
///
/// [`super::reconcile`]: crate::operator::application::reconcile
pub fn auth_delegator_labels(lumen: &Lumen) -> std::collections::BTreeMap<String, String> {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), APP.to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), instance(lumen));
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        AUTH_DELEGATION_COMPONENT.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        MANAGER.to_string(),
    );
    labels.insert("app.kubernetes.io/part-of".to_string(), APP.to_string());
    labels.insert(OWNER_NAMESPACE_LABEL.to_string(), namespace(lumen));
    labels
}

/// The ClusterRoleBinding that lets the serving ServiceAccount ask the API
/// server to validate a caller's token and authorize the request (#2876).
///
/// Lumen delegates both halves of request auth: `TokenReview` decides who the
/// caller is, `SubjectAccessReview` decides what they may do. Both are
/// cluster-scoped review APIs, so no namespaced RoleBinding can grant them —
/// this has to be a ClusterRoleBinding or the serving process cannot
/// authenticate anyone.
///
/// It binds the built-in `system:auth-delegator`. Rendering a replacement
/// ClusterRole would mean maintaining a private copy of a grant Kubernetes
/// already maintains, and every future upstream change to it would be a
/// divergence nobody is watching for.
///
/// Exactly one subject, always: the resolved serving ServiceAccount. Not
/// `system:authenticated`, not the namespace's ServiceAccount group, not the
/// operator's own identity, not the backup runner's, not a client's. Each of
/// those would hand delegated authentication review to a population rather
/// than to a process.
///
/// This is deliberately *not* part of [`render`]. Everything that function
/// returns is applied into the CR's namespace and owned by the CR; this object
/// is neither, and mixing it in would either be applied to a namespaced
/// endpoint that rejects it or stamped with an owner reference that gets it
/// garbage collected. [`super::reconcile`] applies it on its own path and
/// sweeps it on its own path.
///
/// [`super::reconcile`]: crate::operator::application::reconcile
pub fn auth_delegator_binding(lumen: &Lumen) -> Value {
    let sa = serving_service_account_name(lumen);
    let ns = namespace(lumen);
    let subjects = [rbac::ServiceAccountSubject {
        namespace: &ns,
        name: &sa,
    }];
    rbac::cluster_role_binding(rbac::ClusterRoleBinding {
        name: &auth_delegator_binding_name(lumen),
        labels: serde_json::to_value(auth_delegator_labels(lumen)).unwrap_or_else(|_| json!({})),
        cluster_role: AUTH_DELEGATOR_ROLE,
        subjects: &subjects,
    })
}

pub(super) fn attach_service_account_annotations(
    sa: &mut Value,
    annotations: &std::collections::BTreeMap<String, String>,
) {
    if annotations.is_empty() {
        return;
    }
    if let Some(meta) = sa.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        if let Some(existing) = meta.get_mut("annotations").and_then(|a| a.as_object_mut()) {
            for (k, v) in annotations {
                existing.insert(k.clone(), Value::String(v.clone()));
            }
        } else {
            meta.insert(
                "annotations".to_string(),
                serde_json::to_value(annotations).unwrap(),
            );
        }
    }
}

/// The credential a Lumen control-plane workload presents to a serving
/// instance (#2877): the operator's reshard driver and the backup runner.
///
/// One definition, two consumers, and the mount path is the same constant
/// [`control_plane_token_file`] reads — a renderer that invented its own path
/// would produce a pod with a token mounted somewhere the client never looks,
/// and the symptom would be an authentication failure rather than a missing
/// file.
///
/// [`control_plane_token_file`]: crate::access::application::control_plane_token::control_plane_token_file
pub(crate) fn control_plane_token() -> ProjectedServiceAccountToken<'static> {
    use crate::access::application::control_plane_token::{
        CONTROL_PLANE_TOKEN_MOUNT, CONTROL_PLANE_TOKEN_VOLUME,
    };
    ProjectedServiceAccountToken::new(
        CONTROL_PLANE_TOKEN_VOLUME,
        CONTROL_PLANE_TOKEN_MOUNT,
        crate::access::domain::identity::AUDIENCE,
    )
}
