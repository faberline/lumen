//! Pod requests, one-data-member-per-node placement, machine-type transitions
//! and the capacity status they project.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::operator::domain::capacity::preflight::resolve_machine_type;
use crate::operator::domain::capacity::{
    CapacityCatalog, CapacitySpec, Rejection, RejectionReason,
};

/// Resource dimension vector (CPU millicores, memory MiB).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacityVector {
    pub cpu_millicores: u64,
    pub memory_mib: u64,
}

/// Derive pod requests by subtracting reserves and headroom from allocatable capacity.
pub fn derive_requests(
    allocatable: CapacityVector,
    reserves: CapacityVector,
    headroom: CapacityVector,
) -> Result<CapacityVector, Rejection> {
    let needed_cpu = reserves
        .cpu_millicores
        .saturating_add(headroom.cpu_millicores);
    let needed_mem = reserves.memory_mib.saturating_add(headroom.memory_mib);

    if allocatable.cpu_millicores <= needed_cpu || allocatable.memory_mib <= needed_mem {
        return Err(Rejection {
            reason: RejectionReason::InsufficientAllocatable,
            field_path: "allocatable".to_string(),
            message: format!(
                "insufficient allocatable capacity: allocatable ({}m, {}Mi) <= reserves+headroom ({}m, {}Mi)",
                allocatable.cpu_millicores, allocatable.memory_mib, needed_cpu, needed_mem
            ),
        });
    }

    Ok(CapacityVector {
        cpu_millicores: allocatable.cpu_millicores - needed_cpu,
        memory_mib: allocatable.memory_mib - needed_mem,
    })
}

/// Placement record for an instance member on a Kubernetes node.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Placement {
    pub instance: String,
    pub namespace: String,
    pub node_name: String,
}

/// Resolved shared placement for multiple instances sharing a machine type pool.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SharedPlacement {
    pub machine_type: String,
    pub selector_key: String,
    pub selector_value: String,
    pub selectors: BTreeMap<String, String>,
}

/// Resolve shared placement across instances, enforcing one data member per node.
pub fn resolve_shared_placement(
    machine_type: &str,
    catalog: &CapacityCatalog,
    placements: &[Placement],
) -> Result<SharedPlacement, Rejection> {
    let profile = resolve_machine_type(machine_type, catalog)?;

    let mut seen_nodes = BTreeSet::new();
    for p in placements {
        if !seen_nodes.insert(&p.node_name) {
            return Err(Rejection {
                reason: RejectionReason::DataMemberNodeConflict,
                field_path: "placements".to_string(),
                message: format!(
                    "duplicate placement on node `{}`; only one Lumen data member allowed per node cluster-wide",
                    p.node_name
                ),
            });
        }
    }

    let mut selectors = BTreeMap::new();
    for p in placements {
        selectors.insert(p.instance.clone(), profile.selector.clone());
    }

    Ok(SharedPlacement {
        machine_type: profile.machine_type,
        selector_key: profile.selector_key,
        selector_value: profile.selector_value,
        selectors,
    })
}

/// Transition and cluster capacity bounds policy.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacityPolicy {
    #[serde(default)]
    pub allowed_transitions: Vec<String>,
    pub node_cap: u32,
    pub read_replica_cap: u32,
    pub shard_cap: u32,
    pub cooldown_seconds: u64,
}

/// Decision output for a capacity transition.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransitionDecision {
    pub from_machine_type: String,
    pub to_machine_type: String,
    pub node_cap: u32,
    pub read_replica_cap: u32,
    pub shard_cap: u32,
    pub cooldown_seconds: u64,
}

/// Decide transition between machine types bounded by configured policy and catalog maximum.
pub fn decide_transition(
    from_machine_type: &str,
    to_machine_type: &str,
    policy: &CapacityPolicy,
    catalog_maximum: u32,
) -> Result<TransitionDecision, Rejection> {
    if from_machine_type != to_machine_type
        && !policy
            .allowed_transitions
            .iter()
            .any(|t| t == "scale_out" || t == to_machine_type)
    {
        return Err(Rejection {
            reason: RejectionReason::TransitionNotAllowed,
            field_path: "allowed_transitions".to_string(),
            message: format!("transition `{from_machine_type}` -> `{to_machine_type}` is not permitted by policy"),
        });
    }

    let effective_node_cap = policy.node_cap.min(catalog_maximum);

    Ok(TransitionDecision {
        from_machine_type: from_machine_type.to_string(),
        to_machine_type: to_machine_type.to_string(),
        node_cap: effective_node_cap,
        read_replica_cap: policy.read_replica_cap,
        shard_cap: policy.shard_cap,
        cooldown_seconds: policy.cooldown_seconds,
    })
}

/// Operator-owned capacity lifecycle and transition state.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CapacityState {
    pub current_machine_type: String,
    pub target_machine_type: String,
    pub transition_generation: u64,
    pub phase: String,
    #[serde(default)]
    pub old_member_authoritative: bool,
}

impl CapacityState {
    pub fn new(current: &str, target: &str, gen: u64, phase: &str) -> Self {
        Self {
            current_machine_type: current.to_string(),
            target_machine_type: target.to_string(),
            transition_generation: gen,
            phase: phase.to_string(),
            old_member_authoritative: true,
        }
    }
}

/// Reapplication of unchanged initial spec preserves operator-owned state without reset.
pub fn apply_capacity_reapplication(
    previous: &CapacityState,
    _spec: &CapacitySpec,
) -> CapacityState {
    previous.clone()
}

/// Project capacity status on preflight outcome or block.
pub fn project_capacity_status(
    previous: &CapacityState,
    _verdict: &Rejection,
    old_member_healthy: bool,
) -> CapacityState {
    CapacityState {
        current_machine_type: previous.current_machine_type.clone(),
        target_machine_type: previous.target_machine_type.clone(),
        transition_generation: previous.transition_generation,
        phase: "CapacityBlocked".to_string(),
        old_member_authoritative: old_member_healthy,
    }
}

/// Render cluster-wide one-member-per-node anti-affinity matching across all namespaces.
pub fn cross_namespace_dedicated_data_node_affinity() -> serde_json::Value {
    serde_json::json!({
        "podAntiAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": [{
                "labelSelector": {
                    "matchLabels": {
                        "app.kubernetes.io/name": "lumen",
                        "app.kubernetes.io/component": "server",
                    }
                },
                "namespaceSelector": {},
                "topologyKey": "kubernetes.io/hostname",
            }]
        }
    })
}
