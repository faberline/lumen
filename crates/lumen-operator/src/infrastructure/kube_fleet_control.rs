//! `FleetControl` over the Kubernetes API. Each method sends the request that
//! the fleet pass sent before this port existed.

use async_trait::async_trait;
use kube::api::{ListParams, Patch, PatchParams, PostParams};
use kube::{Api, Client, ResourceExt};

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

/// Turn a seed body into a `Lumen`. A body that does not parse is an error
/// here, so the fleet pass records it on the entry as `ApplyFailed`.
pub(crate) fn seed_lumen(body: serde_json::Value) -> anyhow::Result<Lumen> {
    Ok(serde_json::from_value(body)?)
}

#[async_trait]
impl FleetControl for KubeFleetControl {
    async fn namespace_exists(&self, namespace: &str) -> anyhow::Result<bool> {
        let api: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(self.client.clone());
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

    async fn create_lumen(&self, body: serde_json::Value) -> anyhow::Result<()> {
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

    async fn apply_lumen(
        &self,
        namespace: &str,
        name: &str,
        body: serde_json::Value,
    ) -> anyhow::Result<()> {
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

    async fn patch_fleet_status(
        &self,
        fleet: &str,
        patch: serde_json::Value,
    ) -> anyhow::Result<()> {
        let api: Api<LumenFleet> = Api::all(self.client.clone());
        api.patch_status(fleet, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::lumen_fleet::plan::{plan, seed_object};
    use crate::domain::lumen_fleet::{FleetInstance, LumenFleetSpec, PlanOutcome, PrunePolicy};
    use crate::domain::lumen_spec::LumenSpec;
    use serde_json::json;

    fn defaults() -> LumenSpec {
        serde_json::from_value(json!({
            "image": "ghcr.io/acme/lumen:0.4.27",
            "serving": { "cpu": "2", "memory": "8Gi", "raftStorage": "50Gi" },
        }))
        .expect("defaults parse")
    }

    /// A seed body built from the domain, not written by hand.
    fn seed_body() -> serde_json::Value {
        let mut fleet = LumenFleet::new(
            "search",
            LumenFleetSpec {
                defaults: defaults(),
                instances: vec![FleetInstance {
                    namespace: "search".to_string(),
                    ..Default::default()
                }],
                prune_policy: PrunePolicy::default(),
            },
        );
        fleet.metadata.generation = Some(1);
        let planned = plan(&fleet);
        let PlanOutcome::Ready(spec) = &planned[0].outcome else {
            panic!("expected a ready instance");
        };
        seed_object("search", &planned[0], spec)
    }

    #[test]
    fn a_real_seed_body_parses() {
        assert!(seed_lumen(seed_body()).is_ok());
    }

    #[test]
    fn a_seed_body_that_is_not_a_lumen_is_an_error() {
        let mut body = seed_body();
        body["spec"] = json!("not an object");
        assert!(seed_lumen(body).is_err());
    }
}
