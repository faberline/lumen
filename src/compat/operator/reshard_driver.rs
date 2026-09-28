//! `crate::operator::reshard_driver` before the DDD split. Re-exports its
//! public items from their new homes so callers outside the crate keep
//! compiling.

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
