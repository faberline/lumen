//! `AuthDelegatorControl` over the Kubernetes API.

use std::collections::BTreeMap;

use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, ListParams, Patch, PatchParams};
use kube::Client;
use service_k8s::ManagedService;

use crate::operator::application::reconcile::auth_delegator::AuthDelegatorControl;
use crate::operator::domain::lumen_spec::Lumen;

/// The `ApiResource` for a cluster-scoped ClusterRoleBinding (#2876).
fn cluster_role_binding_api_resource() -> ApiResource {
    ApiResource {
        group: "rbac.authorization.k8s.io".to_string(),
        version: "v1".to_string(),
        api_version: "rbac.authorization.k8s.io/v1".to_string(),
        kind: "ClusterRoleBinding".to_string(),
        plural: "clusterrolebindings".to_string(),
    }
}

/// Production [`AuthDelegatorControl`]: real `kube::Client` calls.
pub(in crate::operator) struct KubeAuthDelegatorControl {
    pub(in crate::operator) client: Client,
}

#[async_trait::async_trait]
impl AuthDelegatorControl for KubeAuthDelegatorControl {
    async fn apply_binding(&self, binding: &serde_json::Value) -> anyhow::Result<()> {
        let name = binding["metadata"]["name"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("rendered binding has no metadata.name"))?
            .to_string();
        let api: Api<DynamicObject> =
            Api::all_with(self.client.clone(), &cluster_role_binding_api_resource());
        let obj: DynamicObject = serde_json::from_value(binding.clone())?;
        api.patch(
            &name,
            &PatchParams::apply(<Lumen as ManagedService>::MANAGER).force(),
            &Patch::Apply(&obj),
        )
        .await?;
        Ok(())
    }

    async fn managed_bindings(&self) -> anyhow::Result<Vec<(String, BTreeMap<String, String>)>> {
        let api: Api<DynamicObject> =
            Api::all_with(self.client.clone(), &cluster_role_binding_api_resource());
        let params = ListParams::default().labels(&format!(
            "app.kubernetes.io/managed-by={},app.kubernetes.io/component=auth-delegation",
            <Lumen as ManagedService>::MANAGER
        ));
        Ok(api
            .list(&params)
            .await?
            .items
            .into_iter()
            .filter_map(|obj| {
                let name = obj.metadata.name.clone()?;
                Some((name, obj.metadata.labels.unwrap_or_default()))
            })
            .collect())
    }

    async fn delete_binding(&self, name: &str) -> anyhow::Result<()> {
        let api: Api<DynamicObject> =
            Api::all_with(self.client.clone(), &cluster_role_binding_api_resource());
        match api.delete(name, &DeleteParams::default()).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
