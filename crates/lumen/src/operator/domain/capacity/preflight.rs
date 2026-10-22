//! Resolving a direct GCE machine type against the catalog, and the preflight
//! that fails closed before any existing member is disrupted.

use serde::{Deserialize, Serialize};

use crate::operator::domain::capacity::{
    CapacityCatalog, CapacitySpec, CatalogEntry, Rejection, RejectionReason,
};

/// Resolved profile details for placement.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ResolvedProfile {
    pub machine_type: String,
    pub selector: String,
    pub selector_key: String,
    pub selector_value: String,
    pub max_nodes: u32,
    pub min_nodes: u32,
    pub lifecycle_state: String,
}

/// Resolve a direct GCE machine type against a capacity catalog.
pub fn resolve_machine_type(
    machine_type: &str,
    catalog: &CapacityCatalog,
) -> Result<ResolvedProfile, Rejection> {
    let matching: Vec<&CatalogEntry> = catalog
        .entries
        .iter()
        .filter(|e| e.machine_type == machine_type)
        .collect();

    if matching.is_empty() {
        return Err(Rejection {
            reason: RejectionReason::UnsupportedMachineType,
            field_path: "machine_type".to_string(),
            message: format!("machine type `{machine_type}` is not present in capacity catalog"),
        });
    }

    if matching.len() > 1 {
        return Err(Rejection {
            reason: RejectionReason::CatalogAmbiguous,
            field_path: "catalog".to_string(),
            message: format!("multiple entries for machine type `{machine_type}` found in catalog"),
        });
    }

    let entry = matching[0];
    Ok(ResolvedProfile {
        machine_type: entry.machine_type.clone(),
        selector: entry.selector.clone(),
        selector_key: entry.stable_selector.key.clone(),
        selector_value: entry.stable_selector.value.clone(),
        max_nodes: entry.max_nodes,
        min_nodes: entry.min_nodes,
        lifecycle_state: entry.lifecycle_state.clone(),
    })
}

/// Request envelope for preflight validation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacityRequest {
    pub spec: CapacitySpec,
    pub old_member_disrupted: bool,
}

/// Preflight capacity validation: fails closed on missing, ambiguous, draining,
/// full, or incompatible catalog profiles before disrupting existing members.
pub fn preflight_capacity(
    request: &CapacityRequest,
    catalog: Option<&CapacityCatalog>,
) -> Result<ResolvedProfile, Rejection> {
    preflight_capacity_with_nodes(request, catalog, 0)
}

/// Preflight capacity validation with observable current node count for fullness checking.
pub fn preflight_capacity_with_nodes(
    request: &CapacityRequest,
    catalog: Option<&CapacityCatalog>,
    current_nodes: u32,
) -> Result<ResolvedProfile, Rejection> {
    let catalog = catalog.ok_or_else(|| Rejection {
        reason: RejectionReason::CatalogMissing,
        field_path: "catalog".to_string(),
        message: "capacity catalog is missing".to_string(),
    })?;

    let matching: Vec<&CatalogEntry> = catalog
        .entries
        .iter()
        .filter(|e| e.machine_type == request.spec.initial_machine_type)
        .collect();

    if matching.is_empty() {
        return Err(Rejection {
            reason: RejectionReason::UnsupportedMachineType,
            field_path: "machine_type".to_string(),
            message: format!(
                "machine type `{}` not found in catalog",
                request.spec.initial_machine_type
            ),
        });
    }

    if matching.len() > 1 {
        return Err(Rejection {
            reason: RejectionReason::CatalogAmbiguous,
            field_path: "catalog".to_string(),
            message: format!(
                "multiple catalog entries match machine type `{}`",
                request.spec.initial_machine_type
            ),
        });
    }

    let entry = matching[0];

    if entry.stable_selector.key.is_empty()
        || entry.stable_selector.value.is_empty()
        || (entry.lifecycle_state != "ready" && entry.lifecycle_state != "draining")
    {
        return Err(Rejection {
            reason: RejectionReason::CatalogIncompatible,
            field_path: "catalog".to_string(),
            message: format!(
                "capacity profile for `{}` is marked incompatible or invalid",
                entry.machine_type
            ),
        });
    }

    if entry.lifecycle_state == "draining" {
        return Err(Rejection {
            reason: RejectionReason::CatalogDraining,
            field_path: "catalog".to_string(),
            message: format!(
                "capacity profile for `{}` is in draining lifecycle",
                entry.machine_type
            ),
        });
    }

    if entry.max_nodes == 0 || (current_nodes > 0 && current_nodes >= entry.max_nodes) {
        return Err(Rejection {
            reason: RejectionReason::CapacityFull,
            field_path: "catalog".to_string(),
            message: format!(
                "capacity pool for `{}` is at maximum capacity (max: {}, current: {})",
                entry.machine_type, entry.max_nodes, current_nodes
            ),
        });
    }

    Ok(ResolvedProfile {
        machine_type: entry.machine_type.clone(),
        selector: entry.selector.clone(),
        selector_key: entry.stable_selector.key.clone(),
        selector_value: entry.stable_selector.value.clone(),
        max_nodes: entry.max_nodes,
        min_nodes: entry.min_nodes,
        lifecycle_state: entry.lifecycle_state.clone(),
    })
}
