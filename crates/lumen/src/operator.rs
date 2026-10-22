//! K8s Operator for lumen. The code lives in the `lumen-operator` crate; this
//! module keeps the `lumen::operator` paths and adds the pre-split aliases
//! from [`crate::compat::operator`].

#[cfg(feature = "operator")]
pub use lumen_operator::*;

#[cfg(feature = "operator")]
pub use crate::compat::operator::{capacity, crd, fleet, reconcile, render, reshard_driver};
