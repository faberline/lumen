//! Pure status projection of one fleet pass. This module uses no `kube` types.

use crate::domain::lumen_fleet::{FleetEntryStatus, LumenFleetStatus, PlannedInstance};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Canonical JSON: object keys sorted at every depth, no whitespace.
///
/// The workspace enables `serde_json`'s `preserve_order`, so `Value::to_string`
/// alone keeps insertion order and is not canonical.
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("a string key serializes"));
                out.push(':');
                write_canonical(&map[key.as_str()], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&serde_json::to_string(other).expect("a JSON value serializes")),
    }
}

/// Lowercase hex SHA-256 of `canonical_json(spec)`.
pub fn spec_hash(spec: &Value) -> String {
    format!("{:x}", Sha256::digest(canonical_json(spec).as_bytes()))
}

/// Builds the whole status of one pass from the entry list.
///
/// The counts and the message come from `entries`, so they cannot drift from it.
/// A state that is not one of the four counted states, such as `ApplyFailed`,
/// counts in none of them.
pub fn project_status(
    planned: &[PlannedInstance],
    entries: Vec<FleetEntryStatus>,
    generation: i64,
) -> LumenFleetStatus {
    let count = |state: &str| entries.iter().filter(|entry| entry.state == state).count() as i32;
    let materialized = entries
        .iter()
        .filter(|entry| entry.state == "Created" || entry.state == "Applied")
        .count() as i32;
    let rejected = count("Rejected");
    let orphaned = count("Orphaned");
    let pruned = count("Pruned");
    let desired = planned.len() as i32;

    let mut message = format!("{materialized}/{desired} instances converged");
    for (n, label) in [
        (rejected, "rejected"),
        (orphaned, "orphaned"),
        (pruned, "pruned"),
    ] {
        if n > 0 {
            message.push_str(&format!(", {n} {label}"));
        }
    }

    LumenFleetStatus {
        observed_generation: generation,
        desired_instances: desired,
        applied_instances: materialized,
        materialized_instances: materialized,
        rejected_instances: rejected,
        orphaned_instances: orphaned,
        pruned_instances: pruned,
        entries,
        message,
    }
}

/// The merge patch for one pass.
///
/// `entries` is always sent, even when empty. A JSON merge patch keeps every key it does
/// not name, so an omitted `entries` would keep the rows of an earlier pass.
pub fn status_patch(status: &LumenFleetStatus) -> Value {
    let mut status_value = serde_json::to_value(status).expect("the status serializes");
    status_value["entries"] = serde_json::to_value(&status.entries).expect("the entries serialize");
    json!({ "status": status_value })
}
