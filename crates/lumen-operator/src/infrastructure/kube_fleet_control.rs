//! `FleetControl` over the Kubernetes API: the requests the fleet pass has
//! always sent, one per method.

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, ListParams, Patch, PatchParams, PostParams};
use kube::{Client, ResourceExt};
use serde_json::Value;

use crate::application::fleet_reconcile::control::FleetControl;
use crate::domain::lumen_fleet::{LumenFleet, FLEET_LABEL, FLEET_MANAGER, FLEET_SEED_MANAGER};
use crate::domain::lumen_spec::Lumen;

/// Production [`FleetControl`]: real `kube::Client` calls.
pub struct KubeFleetControl {
    client: Client,
}

impl KubeFleetControl {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Parse a seed body into a `Lumen`. A body that is not a `Lumen` fails here,
/// and the pass records that error on its own entry.
pub(crate) fn seed_lumen(body: Value) -> anyhow::Result<Lumen> {
    Ok(serde_json::from_value(body)?)
}

#[async_trait]
impl FleetControl for KubeFleetControl {
    async fn namespace_exists(&self, namespace: &str) -> anyhow::Result<bool> {
        let api: Api<Namespace> = Api::all(self.client.clone());
        Ok(api.get_opt(namespace).await?.is_some())
    }

    async fn lumen_fleet_label(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<Option<String>>> {
        let api: Api<Lumen> = Api::namespaced(self.client.clone(), namespace);
        Ok(api
            .get_opt(name)
            .await?
            .map(|live| live.labels().get(FLEET_LABEL).cloned()))
    }

    async fn create_lumen(&self, body: Value) -> anyhow::Result<()> {
        let lumen = seed_lumen(body)?;
        let namespace = lumen.namespace().unwrap_or_default();
        let api: Api<Lumen> = Api::namespaced(self.client.clone(), &namespace);
        let params = PostParams {
            field_manager: Some(FLEET_SEED_MANAGER.to_string()),
            ..Default::default()
        };
        api.create(&params, &lumen).await?;
        Ok(())
    }

    async fn apply_lumen(&self, namespace: &str, name: &str, body: Value) -> anyhow::Result<()> {
        let api: Api<Lumen> = Api::namespaced(self.client.clone(), namespace);
        api.patch(
            name,
            &PatchParams::apply(FLEET_MANAGER).force(),
            &Patch::Apply(&body),
        )
        .await?;
        Ok(())
    }

    async fn list_fleet_lumens(&self, fleet: &str) -> anyhow::Result<Vec<(String, String)>> {
        let api: Api<Lumen> = Api::all(self.client.clone());
        let params = ListParams::default().labels(&format!("{FLEET_LABEL}={fleet}"));
        Ok(api
            .list(&params)
            .await?
            .items
            .iter()
            .map(|live| (live.namespace().unwrap_or_default(), live.name_any()))
            .collect())
    }

    async fn delete_lumen(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        let api: Api<Lumen> = Api::namespaced(self.client.clone(), namespace);
        api.delete(name, &Default::default()).await?;
        Ok(())
    }

    async fn patch_fleet_status(&self, fleet: &str, status: Value) -> anyhow::Result<()> {
        let api: Api<LumenFleet> = Api::all(self.client.clone());
        api.patch_status(fleet, &PatchParams::default(), &Patch::Merge(&status))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::seed_lumen;

    #[test]
    fn a_seed_body_that_is_not_a_lumen_is_an_error() {
        let body = json!({
            "apiVersion": "lumen.dev/v1alpha1",
            "kind": "Lumen",
            "metadata": { "name": "x", "namespace": "n" },
            "spec": "not an object",
        });
        assert!(seed_lumen(body).is_err());
    }

    #[test]
    fn a_real_seed_body_parses() {
        let body = json!({
            "apiVersion": "lumen.dev/v1alpha1",
            "kind": "Lumen",
            "metadata": { "name": "x", "namespace": "n" },
            "spec": {
                "image": "ghcr.io/acme/lumen:0.4.27",
                "serving": { "cpu": "2", "memory": "8Gi", "raftStorage": "50Gi" },
            },
        });
        assert!(seed_lumen(body).is_ok());
    }
}
