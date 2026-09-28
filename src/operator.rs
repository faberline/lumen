//! K8s Operator for lumen: a `Lumen` custom resource, plus a reconcile loop
//! that renders and applies the serving data-plane.
//!
//! The operator consumes externally provisioned TLS Secrets; it does not own
//! certificate issuer configuration or lifecycle.
//!
//! ```text
//! Lumen (lumen.dev/v1alpha1)  --reconcile-->  ServiceAccount, ConfigMap,
//!                                             Deployment/StatefulSet, Service,
//!                                             PDB,
//!                                             [ServiceMonitor, PrometheusRule]
//! ```

#[cfg(feature = "operator")]
pub(crate) mod application;
#[cfg(feature = "operator")]
pub(crate) mod domain;
#[cfg(feature = "operator")]
pub(crate) mod infrastructure;

#[cfg(feature = "operator")]
pub use crate::compat::operator::{capacity, crd, fleet, reconcile, render, reshard_driver};
#[cfg(feature = "operator")]
pub use crate::operator::application::{certificate, resize};
#[cfg(feature = "operator")]
pub use crate::operator::infrastructure::lease;

#[cfg(feature = "operator")]
pub use crate::operator::application::reconcile::run;
#[cfg(feature = "operator")]
pub use crate::operator::domain::lumen_fleet::{LumenFleet, LumenFleetSpec, LumenFleetStatus};
#[cfg(feature = "operator")]
pub use crate::operator::domain::lumen_spec::{status::LumenStatus, Lumen, LumenSpec};
#[cfg(feature = "operator")]
pub use crate::operator::infrastructure::crd_manifest::{crd_yaml, lumen_crd_yaml};
