//! K8s Operator for lumen. The code lives in the `lumen-operator` crate; this module keeps the `lumen::operator` paths and the pre-split aliases below.

#[cfg(feature = "operator")]
pub use lumen_operator::*;

#[cfg(feature = "operator")]
pub mod capacity {
    pub use crate::operator::application::capacity_catalog::fetch_capacity_catalog;
    pub use crate::operator::domain::capacity::placement::{
        apply_capacity_reapplication, cross_namespace_dedicated_data_node_affinity,
        decide_transition, derive_requests, project_capacity_status, resolve_shared_placement,
        CapacityPolicy, CapacityState, CapacityVector, Placement, SharedPlacement,
        TransitionDecision,
    };
    pub use crate::operator::domain::capacity::preflight::{
        preflight_capacity, preflight_capacity_with_nodes, resolve_machine_type, CapacityRequest,
        ResolvedProfile,
    };
    pub use crate::operator::domain::capacity::{
        decide_capacity_spec, decide_storage, is_valid_direct_gce_machine_type, CapacityCatalog,
        CapacitySpec, CapacityStorage, CatalogEntry, Rejection, RejectionReason, StableSelector,
        DEFAULT_CATALOG_CONFIG_MAP_NAME, DEFAULT_CATALOG_NAMESPACE, DEFAULT_DATA_STORAGE,
        DEFAULT_DISK_TYPE, DEFAULT_INITIAL_MACHINE_TYPE, DEFAULT_STORAGE_CLASS,
    };
}
#[cfg(feature = "operator")]
pub mod crd {
    pub use crate::operator::domain::lumen_spec::serving::{
        AuthMode, LogFormat, ServingBackupSpec, ServingBootstrapSpec, ServingSpec,
    };
    pub use crate::operator::domain::lumen_spec::status::{
        LumenCapacityStatus, LumenReshardStatus, LumenStatus,
    };
    pub use crate::operator::domain::lumen_spec::topology::{
        ReshardPhase, ReshardPolicy, ReshardWorkflowSpec, ShardMapSpec,
    };
    pub use crate::operator::domain::lumen_spec::{
        AdmissionSpec, LocalObjectReference, Lumen, LumenSpec, PlacementSpec, Toleration,
        MAX_BODY_LIMIT_BYTES, MIN_BODY_LIMIT_BYTES,
    };
}
#[cfg(feature = "operator")]
pub mod fleet {
    pub use crate::operator::application::fleet_reconcile::spawn_fleet_loop;
    pub use crate::operator::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
    pub use crate::operator::domain::lumen_fleet::{
        FleetEntryStatus, FleetInstance, LumenFleet, LumenFleetSpec, LumenFleetStatus, PlanOutcome,
        PlannedInstance, PrunePolicy, RolloutPolicy, FLEET_LABEL, FLEET_MANAGER,
        FLEET_SEED_MANAGER,
    };
    pub use crate::operator::infrastructure::crd_manifest::fleet_crd_yaml;
}
#[cfg(feature = "operator")]
pub mod reconcile {
    pub use crate::operator::application::reconcile::plan_verdicts::PEER_IDENTITY_CONTEXT_KEY;
    pub use crate::operator::application::reconcile::run;
}
#[cfg(feature = "operator")]
pub mod render {
    pub use crate::operator::application::render::identity::{
        auth_delegator_binding, auth_delegator_binding_name, auth_delegator_labels,
    };
    pub use crate::operator::application::render::{
        prunes, render, render_with_profile, serving_dns_names, PEER_TLS_KEYS, SERVING_TLS_KEYS,
    };
}
#[cfg(feature = "operator")]
pub mod reshard_driver {
    pub use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
    pub use crate::operator::application::reshard_driver::convergence_stall::{
        convergence_stall_budget_secs, convergence_stall_condition,
    };
    pub use crate::operator::application::reshard_driver::driver_loop::spawn_reshard_driver_loop;
    pub use crate::operator::application::reshard_driver::migration::run_migration_pass;
    pub use crate::operator::application::reshard_driver::oversize::{
        oversize_block_condition, OversizedDocumentBlock,
    };
    pub use crate::operator::application::reshard_driver::trigger::{
        compute_target_map, current_shard_map, should_start_split,
    };
    pub use crate::operator::application::reshard_driver::{
        default_write_fence_ttl_secs, drive_tick, DriveOutcome,
    };
    pub use crate::operator::infrastructure::kube_cluster_control::KubeClusterControl;
}
