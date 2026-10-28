//! What the operator does with a `Lumen`: render its child objects, reconcile
//! them onto the cluster, drive reshards, materialize fleets, and issue its
//! certificates.

pub mod capacity_catalog;
pub mod certificate;
pub mod fleet_reconcile;
pub mod member_rollout;
pub mod reconcile;
pub mod render;
pub mod reshard_driver;
pub mod resize;
