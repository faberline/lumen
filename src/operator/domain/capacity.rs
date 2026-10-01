//! Platform capacity catalog consumption and placement derivation for Lumen.
//!
//! Lumen instances consume precreated, shared, autoscaled GCE node pools
//! provisioned via Terraform without exposing internal node pool names or
//! invented service tiers.
//!
//! The platform capacity catalog is published as a Kubernetes ConfigMap
//! (`lumen-capacity-catalog` in `lumen-system` namespace) by Terraform
//! (`terraform/modules/lumen-capacity/catalog.tf`), mapping direct GCE
//! machine types to `lumen.axiom.dev/capacity-profile` labels and tolerations.

pub(crate) mod placement;
pub(crate) mod preflight;

use serde::{Deserialize, Serialize};

/// Default initial GCE machine type.
pub const DEFAULT_INITIAL_MACHINE_TYPE: &str = "e2-standard-2";

/// Default data volume storage request.
pub const DEFAULT_DATA_STORAGE: &str = "10Gi";

/// Default storage class for persistent data volumes.
pub const DEFAULT_STORAGE_CLASS: &str = "standard-rwo";

/// Default backing disk type.
pub const DEFAULT_DISK_TYPE: &str = "pd-balanced";

/// Default namespace for the in-cluster capacity catalog ConfigMap.
pub const DEFAULT_CATALOG_NAMESPACE: &str = "lumen-system";

/// Default name of the in-cluster capacity catalog ConfigMap.
pub const DEFAULT_CATALOG_CONFIG_MAP_NAME: &str = "lumen-capacity-catalog";

/// In-cluster capacity catalog published by the platform Terraform module.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacityCatalog {
    #[serde(default = "default_catalog_version")]
    pub version: String,
    #[serde(default)]
    pub entries: Vec<CatalogEntry>,
}

fn default_catalog_version() -> String {
    "1.0.0".to_string()
}

impl CapacityCatalog {
    pub fn new(entries: Vec<CatalogEntry>) -> Self {
        Self {
            version: default_catalog_version(),
            entries,
        }
    }

    pub fn from_json(json_str: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_str)
    }
}

/// A single catalog entry representing an available direct GCE machine type.
/// Deserializes the exact 7 fields published by `catalog.tf`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CatalogEntry {
    pub machine_type: String,
    pub selector: String,
    pub stable_selector: StableSelector,
    pub max_nodes: u32,
    pub min_nodes: u32,
    pub lifecycle_state: String,
    #[serde(default)]
    pub pool_group: Option<String>,
}

impl CatalogEntry {
    pub fn new(
        machine_type: &str,
        selector_key: &str,
        lifecycle_state: &str,
        max_nodes: u32,
    ) -> Self {
        Self {
            machine_type: machine_type.to_string(),
            selector: format!("{selector_key}={machine_type}"),
            stable_selector: StableSelector {
                key: selector_key.to_string(),
                value: machine_type.to_string(),
            },
            max_nodes,
            min_nodes: 0,
            lifecycle_state: lifecycle_state.to_string(),
            pool_group: Some("lumen-data".to_string()),
        }
    }
}

/// Key-value selector pair for a catalog entry.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct StableSelector {
    pub key: String,
    pub value: String,
}

/// Reason code for capacity admission or preflight rejection.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    UnsupportedMachineType,
    CatalogMissing,
    CatalogAmbiguous,
    CatalogDraining,
    CapacityFull,
    CatalogIncompatible,
    InsufficientAllocatable,
    DataMemberNodeConflict,
    TransitionNotAllowed,
    MonetaryPolicyNotAllowed,
}

impl RejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UnsupportedMachineType => "unsupported_machine_type",
            Self::CatalogMissing => "catalog_missing",
            Self::CatalogAmbiguous => "catalog_ambiguous",
            Self::CatalogDraining => "catalog_draining",
            Self::CapacityFull => "capacity_full",
            Self::CatalogIncompatible => "catalog_incompatible",
            Self::InsufficientAllocatable => "insufficient_allocatable",
            Self::DataMemberNodeConflict => "data_member_node_conflict",
            Self::TransitionNotAllowed => "transition_not_allowed",
            Self::MonetaryPolicyNotAllowed => "monetary_policy_not_allowed",
        }
    }
}

/// Structured rejection verdict from capacity validation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Rejection {
    pub reason: RejectionReason,
    pub field_path: String,
    pub message: String,
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} ({})",
            self.reason.as_str(),
            self.message,
            self.field_path
        )
    }
}

impl std::error::Error for Rejection {}

/// Create-time machine type specification for Lumen data plane.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacitySpec {
    pub initial_machine_type: String,
}

impl CapacitySpec {
    pub fn default() -> Self {
        Self {
            initial_machine_type: DEFAULT_INITIAL_MACHINE_TYPE.to_string(),
        }
    }
}

impl Default for CapacitySpec {
    fn default() -> Self {
        Self::default()
    }
}

/// Validate that a given machine type string is a valid direct GCE machine type.
pub fn is_valid_direct_gce_machine_type(mt: &str) -> bool {
    let parts: Vec<&str> = mt.split('-').collect();
    if parts.len() < 3 {
        return false;
    }
    let family = parts[0];
    let class = parts[1];
    let vcpu = parts[2];
    if family.is_empty() || class.is_empty() || vcpu.is_empty() {
        return false;
    }
    vcpu.chars().all(|c| c.is_ascii_digit())
        && family.chars().all(|c| c.is_ascii_alphanumeric())
        && class.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Validate public `CapacitySpec` admission: reject tier names, accept direct GCE types.
pub fn decide_capacity_spec(spec: &CapacitySpec) -> Result<(), Rejection> {
    if !is_valid_direct_gce_machine_type(&spec.initial_machine_type) {
        return Err(Rejection {
            reason: RejectionReason::UnsupportedMachineType,
            field_path: "initial_machine_type".to_string(),
            message: format!(
                "machine type `{}` is not an allowed direct GCE machine type; service-tier names are forbidden",
                spec.initial_machine_type
            ),
        });
    }
    Ok(())
}

/// Storage volume specification and defaults.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacityStorage {
    pub size: String,
    pub storage_class: String,
    pub disk_type: String,
}

impl CapacityStorage {
    pub fn default() -> Self {
        Self {
            size: DEFAULT_DATA_STORAGE.to_string(),
            storage_class: DEFAULT_STORAGE_CLASS.to_string(),
            disk_type: DEFAULT_DISK_TYPE.to_string(),
        }
    }
}

impl Default for CapacityStorage {
    fn default() -> Self {
        Self::default()
    }
}

/// Validate storage specification for admission and online growth.
pub fn decide_storage(storage: &CapacityStorage) -> Result<(), Rejection> {
    if storage.size.is_empty() {
        return Err(Rejection {
            reason: RejectionReason::UnsupportedMachineType,
            field_path: "size".to_string(),
            message: "storage size must not be empty".to_string(),
        });
    }
    Ok(())
}
