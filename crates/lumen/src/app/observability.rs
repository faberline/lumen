//! What the composition root exposes about a running lumen: the Prometheus
//! metrics `/metrics` serves.

pub(crate) mod metrics;
pub(crate) mod write_phase;
