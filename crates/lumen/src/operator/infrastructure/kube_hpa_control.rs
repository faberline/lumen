//! `HpaControl` over the Kubernetes API.

use std::collections::BTreeMap;

use kube::api::{Api, ApiResource, DeleteParams, DynamicObject};
use kube::Client;

use crate::operator::application::reconcile::hpa::HpaControl;

/// The `ApiResource` for a live HorizontalPodAutoscaler, matching what
/// `libs/service-k8s::render::horizontal_pod_autoscaler` renders
/// (`autoscaling/v2`) and what `libs/service-k8s::controller`'s generic apply
/// loop server-side-applies it as.
fn hpa_api_resource() -> ApiResource {
    ApiResource {
        group: "autoscaling".to_string(),
        version: "v2".to_string(),
        api_version: "autoscaling/v2".to_string(),
        kind: "HorizontalPodAutoscaler".to_string(),
        plural: "horizontalpodautoscalers".to_string(),
    }
}

/// Production [`HpaControl`]: real `kube::Client` calls.
pub(in crate::operator) struct KubeHpaControl {
    pub(in crate::operator) client: Client,
}

#[async_trait::async_trait]
impl HpaControl for KubeHpaControl {
    async fn hpa_labels(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<BTreeMap<String, String>>> {
        let api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), namespace, &hpa_api_resource());
        let obj = api.get_opt(name).await?;
        Ok(obj.and_then(|o| o.metadata.labels))
    }

    async fn delete_hpa(&self, namespace: &str, name: &str) -> anyhow::Result<()> {
        let api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), namespace, &hpa_api_resource());
        match api.delete(name, &DeleteParams::default()).await {
            Ok(_) => Ok(()),
            // Already gone (raced with another deletion, or a watch-triggered
            // reconcile fired again before the cache caught up) — idempotent
            // no-op, matching R2.
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
