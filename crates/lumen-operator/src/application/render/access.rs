//! The namespaced Role and RoleBinding that grant a runtime's declared
//! ServiceAccounts whole-runtime access (`spec.access`, #365).

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::application::render::{
    instance, namespace, ACCESS_POLICY_COMPONENT, APP, FABERLINE_OWNER_NAMESPACE_LABEL, MANAGER,
    OWNER_NAMESPACE_LABEL,
};
use crate::domain::lumen_spec::Lumen;

/// Name of the access Role and of its RoleBinding: one name for both, in the
/// `Lumen`'s own namespace.
pub fn access_policy_name(lumen: &Lumen) -> String {
    format!("{}-access", instance(lumen))
}

/// The label set of the access Role and RoleBinding. Same keys as the
/// auth-delegator binding, with the component set to `access-policy`.
pub fn access_policy_labels(lumen: &Lumen) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), APP.to_string());
    labels.insert("app.kubernetes.io/instance".to_string(), instance(lumen));
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        ACCESS_POLICY_COMPONENT.to_string(),
    );
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        MANAGER.to_string(),
    );
    labels.insert("app.kubernetes.io/part-of".to_string(), APP.to_string());
    labels.insert(OWNER_NAMESPACE_LABEL.to_string(), namespace(lumen));
    labels.insert(
        FABERLINE_OWNER_NAMESPACE_LABEL.to_string(),
        namespace(lumen),
    );
    labels
}

fn metadata(lumen: &Lumen) -> Value {
    json!({
        "name": access_policy_name(lumen),
        "namespace": namespace(lumen),
        "labels": access_policy_labels(lumen),
    })
}

/// The Role: exactly one rule, `use` on this runtime's `lumenruntimes` object.
pub fn access_role(lumen: &Lumen) -> Value {
    json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "Role",
        "metadata": metadata(lumen),
        "rules": [{
            "apiGroups": ["lumen.axiom.dev"],
            "resources": ["lumenruntimes"],
            "resourceNames": [instance(lumen)],
            "verbs": ["use"],
        }],
    })
}

/// The RoleBinding: exactly the declared ServiceAccounts, in list order. An
/// absent or empty `access` renders `subjects: []`, which denies every client.
pub fn access_role_binding(lumen: &Lumen) -> Value {
    let subjects: Vec<Value> = lumen
        .spec
        .access
        .as_ref()
        .map(|access| {
            access
                .allowed_service_accounts
                .iter()
                .map(|subject| {
                    json!({
                        "kind": "ServiceAccount",
                        "name": subject.name,
                        "namespace": subject.namespace,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": metadata(lumen),
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "Role",
            "name": access_policy_name(lumen),
        },
        "subjects": subjects,
    })
}
