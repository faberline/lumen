//! The serving container's user-declared env entries (#41).
//!
//! Every rendering of a user entry lives in this file, so the Secret reference
//! text stays in the one place the R4 guard exempts. A Secret or ConfigMap
//! reaches the pod only as a `valueFrom` reference: the operator never reads,
//! copies or logs its value.

use serde_json::{json, Value};

use crate::domain::lumen_spec::Lumen;

/// One container env entry per `spec.extraEnv` item, in declaration order.
/// Validation guarantees exactly one source per entry.
pub fn extra_env_entries(lumen: &Lumen) -> Vec<Value> {
    let mut out = Vec::new();
    for entry in &lumen.spec.extra_env {
        if let Some(value) = &entry.value {
            out.push(json!({ "name": entry.name, "value": value }));
        } else if let Some(r) = &entry.secret_key_ref {
            out.push(json!({
                "name": entry.name,
                "valueFrom": { "secretKeyRef": { "name": r.name, "key": r.key } },
            }));
        } else if let Some(r) = &entry.config_map_key_ref {
            out.push(json!({
                "name": entry.name,
                "valueFrom": { "configMapKeyRef": { "name": r.name, "key": r.key } },
            }));
        }
    }
    out
}
