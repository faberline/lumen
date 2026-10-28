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

use lumen_access as access;
use lumen_index::{index, sharding};

pub mod application;
pub mod domain;
pub mod infrastructure;

pub use crate::application::{certificate, resize, runtime_rollout};
pub use crate::infrastructure::lease;

pub use crate::application::reconcile::run;
pub use crate::domain::lumen_fleet::{LumenFleet, LumenFleetSpec, LumenFleetStatus};
pub use crate::domain::lumen_spec::{status::LumenStatus, Lumen, LumenSpec};
pub use crate::infrastructure::crd_manifest::{crd_yaml, lumen_crd_yaml};
