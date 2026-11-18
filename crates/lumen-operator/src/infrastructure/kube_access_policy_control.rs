//! `AccessPolicyControl` over the Kubernetes API (#365).

use std::collections::BTreeMap;

use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams};
use kube::Client;
use service_k8s::ManagedService;

use crate::application::reconcile::access_policy::{
    AccessKind, AccessPolicyControl, ManagedAccessObject,
};
use crate::domain::lumen_spec::Lumen;

/// The `ApiResource` for a namespaced Role or RoleBinding.
// Wired into the reconcile plan by #366; nothing calls it yet.
#[allow(dead_code)]
fn api_resource(kind: AccessKind) -> ApiResource {
    let (kind_name, plural) = match kind {
        AccessKind::Role => ("Role", "roles"),
        AccessKind::RoleBinding => ("RoleBinding", "rolebindings"),
    };
    ApiResource {
        group: "rbac.authorization.k8s.io".to_string(),
        version: "v1".to_string(),
        api_version: "rbac.authorization.k8s.io/v1".to_string(),
        kind: kind_name.to_string(),
        plural: plural.to_string(),
    }
}

/// Production [`AccessPolicyControl`]: real `kube::Client` calls.
// Wired into the reconcile plan by #366; nothing constructs it yet.
#[allow(dead_code)]
pub(crate) struct KubeAccessPolicyControl {
    pub(crate) client: Client,
}

#[async_trait::async_trait]
impl AccessPolicyControl for KubeAccessPolicyControl {
    async fn apply(&self, object: &serde_json::Value) -> anyhow::Result<()> {
        let kind = match object["kind"].as_str() {
            Some("Role") => AccessKind::Role,
            Some("RoleBinding") => AccessKind::RoleBinding,
            other => anyhow::bail!("rendered access object has an unknown kind: {other:?}"),
        };
        let namespace = object["metadata"]["namespace"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("rendered access object has no metadata.namespace"))?
            .to_string();
        let name = object["metadata"]["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("rendered access object has no metadata.name"))?
            .to_string();
        let api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), &namespace, &api_resource(kind));
        let obj: DynamicObject = serde_json::from_value(object.clone())?;
        // Server-side apply replaces the whole `subjects` list, so a removed
        // ServiceAccount disappears from the binding on the next apply.
        api.patch(
            &name,
            &PatchParams::apply(<Lumen as ManagedService>::MANAGER).force(),
            &Patch::Apply(&obj),
        )
        .await?;
        Ok(())
    }

    async fn get_labels(
        &self,
        kind: AccessKind,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        let api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), namespace, &api_resource(kind));
        Ok(api
            .get_opt(name)
            .await?
            .map(|obj| obj.metadata.labels.unwrap_or_default()))
    }

    async fn managed(&self) -> anyhow::Result<Vec<ManagedAccessObject>> {
        let params = ListParams::default().labels(&format!(
            "app.kubernetes.io/managed-by={},app.kubernetes.io/component=access-policy",
            <Lumen as ManagedService>::MANAGER
        ));
        let mut out = Vec::new();
        for kind in [AccessKind::Role, AccessKind::RoleBinding] {
            let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &api_resource(kind));
            for obj in api.list(&params).await?.items {
                let (Some(name), Some(namespace)) =
                    (obj.metadata.name.clone(), obj.metadata.namespace.clone())
                else {
                    continue;
                };
                out.push(ManagedAccessObject {
                    kind,
                    namespace,
                    name,
                    labels: obj.metadata.labels.unwrap_or_default(),
                });
            }
        }
        Ok(out)
    }

    async fn delete(&self, kind: AccessKind, namespace: &str, name: &str) -> anyhow::Result<()> {
        let api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), namespace, &api_resource(kind));
        match api.delete(name, &DeleteParams::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
