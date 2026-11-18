//! Status projection of one fleet pass. The counts and the message come from
//! the entry list, so they cannot drift from it. No `kube` types live here.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::domain::lumen_fleet::{FleetEntryStatus, LumenFleetStatus, PlannedInstance};

/// Canonical JSON: object keys sorted at every depth, no whitespace.
///
/// `serde_json` runs with `preserve_order` in this workspace, so `to_string`
/// alone keeps insertion order and is not canonical. The keys are sorted here.
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut members: Vec<(&String, &Value)> = map.iter().collect();
            members.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (key, member)) in members.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap_or_default());
                out.push(':');
                write_canonical(member, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        other => out.push_str(&serde_json::to_string(other).unwrap_or_default()),
    }
}

/// Lowercase hex SHA-256 of `canonical_json(spec)`.
pub fn spec_hash(spec: &Value) -> String {
    format!("{:x}", Sha256::digest(canonical_json(spec).as_bytes()))
}

/// Build the whole status of one pass from the entry list.
///
/// `materialized` counts `Created` and `Applied`. Any other state (for
/// example `NamespaceMissing`, `NotAdopted`, `ApplyFailed`) counts in none of
/// the four new fields.
pub fn project_status(
    planned: &[PlannedInstance],
    entries: Vec<FleetEntryStatus>,
    generation: i64,
) -> LumenFleetStatus {
    let count = |state: &str| entries.iter().filter(|e| e.state == state).count() as i32;
    let materialized = entries
        .iter()
        .filter(|e| e.state == "Created" || e.state == "Applied")
        .count() as i32;
    let rejected = count("Rejected");
    let orphaned = count("Orphaned");
    let pruned = count("Pruned");
    let desired = planned.len() as i32;

    let mut message = format!("{materialized}/{desired} instances converged");
    if rejected > 0 {
        message.push_str(&format!(", {rejected} rejected"));
    }
    if orphaned > 0 {
        message.push_str(&format!(", {orphaned} orphaned"));
    }
    if pruned > 0 {
        message.push_str(&format!(", {pruned} pruned"));
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

/// The merge patch for one pass's status.
///
/// A JSON merge patch keeps every key it does not name. `entries` is skipped
/// by serde when empty, so this always writes the key, and an empty pass sends
/// `entries: []`. Without it the rows of an earlier pass would stay.
pub fn status_patch(status: &LumenFleetStatus) -> Value {
    let mut patch = json!({ "status": status });
    patch["status"]["entries"] = json!(status.entries);
    patch
}
