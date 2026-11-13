//! The pure status projection of one fleet pass: the counts, the summary
//! message and the per-entry spec hash all come from the entry list, so they
//! cannot drift from it. No kube types live here.

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::domain::lumen_fleet::{FleetEntryStatus, LumenFleetStatus, PlannedInstance};

/// Canonical JSON: object keys sorted at every depth, no whitespace.
///
/// `serde_json` keeps insertion order here (the `preserve_order` feature), so
/// keys are sorted by hand.
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).expect("string key serializes"),
                        canonical_json(&map[key])
                    )
                })
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", parts.join(","))
        }
        other => serde_json::to_string(other).expect("scalar JSON serializes"),
    }
}

/// Lowercase hex SHA-256 of `canonical_json(spec)`.
pub fn spec_hash(spec: &Value) -> String {
    format!("{:x}", Sha256::digest(canonical_json(spec).as_bytes()))
}

/// Build the whole status of one pass from the entry list.
pub fn project_status(
    planned: &[PlannedInstance],
    entries: Vec<FleetEntryStatus>,
    generation: i64,
) -> LumenFleetStatus {
    let count = |state: &str| entries.iter().filter(|e| e.state == state).count() as i32;
    let materialized = count("Created") + count("Applied");
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
