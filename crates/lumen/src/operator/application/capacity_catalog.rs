//! Reads the capacity catalog ConfigMap the platform Terraform module
//! publishes.

use crate::operator::domain::capacity::{CapacityCatalog, Rejection, RejectionReason};

/// Fetch the capacity catalog from the in-cluster ConfigMap published by Terraform.
pub async fn fetch_capacity_catalog(
    client: &kube::Client,
    namespace: &str,
    name: &str,
) -> Result<CapacityCatalog, Rejection> {
    use k8s_openapi::api::core::v1::ConfigMap;
    let cm_api: kube::Api<ConfigMap> = kube::Api::namespaced(client.clone(), namespace);
    let cm = cm_api.get(name).await.map_err(|err| Rejection {
        reason: RejectionReason::CatalogMissing,
        field_path: "catalog".to_string(),
        message: format!("failed to read capacity catalog ConfigMap `{namespace}/{name}`: {err}"),
    })?;
    let data = cm.data.ok_or_else(|| Rejection {
        reason: RejectionReason::CatalogMissing,
        field_path: "catalog".to_string(),
        message: format!("ConfigMap `{namespace}/{name}` has no data"),
    })?;
    let raw_json = data.get("catalog.json").ok_or_else(|| Rejection {
        reason: RejectionReason::CatalogIncompatible,
        field_path: "catalog".to_string(),
        message: format!("ConfigMap `{namespace}/{name}` missing `catalog.json` key"),
    })?;
    CapacityCatalog::from_json(raw_json).map_err(|err| Rejection {
        reason: RejectionReason::CatalogIncompatible,
        field_path: "catalog".to_string(),
        message: format!("failed to parse `catalog.json` in `{namespace}/{name}`: {err}"),
    })
}
