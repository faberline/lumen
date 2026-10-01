//! `crate::operator::capacity` before the DDD split. Re-exports its public
//! items from their new homes so callers outside the crate keep compiling.

pub use crate::operator::application::capacity_catalog::fetch_capacity_catalog;
pub use crate::operator::domain::capacity::placement::{
    apply_capacity_reapplication, cross_namespace_dedicated_data_node_affinity, decide_transition,
    derive_requests, project_capacity_status, resolve_shared_placement, CapacityPolicy,
    CapacityState, CapacityVector, Placement, SharedPlacement, TransitionDecision,
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
