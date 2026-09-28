//! `crate::operator::fleet` before the DDD split. Re-exports its public items
//! from their new homes so callers outside the crate keep compiling.

pub use crate::operator::application::fleet_reconcile::spawn_fleet_loop;
pub use crate::operator::domain::lumen_fleet::plan::{apply_object, plan, seed_object};
pub use crate::operator::domain::lumen_fleet::{
    FleetEntryStatus, FleetInstance, LumenFleet, LumenFleetSpec, LumenFleetStatus, PlanOutcome,
    PlannedInstance, PrunePolicy, FLEET_LABEL, FLEET_MANAGER, FLEET_SEED_MANAGER,
};
pub use crate::operator::infrastructure::crd_manifest::fleet_crd_yaml;
