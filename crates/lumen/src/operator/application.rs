//! What the operator does with a `Lumen`: render its child objects, reconcile
//! them onto the cluster, drive reshards, materialize fleets, and issue its
//! certificates.

pub(crate) mod capacity_catalog;
pub mod certificate;
pub(crate) mod fleet_reconcile;
pub(crate) mod reconcile;
pub(crate) mod render;
pub(crate) mod reshard_driver;
pub mod resize;
