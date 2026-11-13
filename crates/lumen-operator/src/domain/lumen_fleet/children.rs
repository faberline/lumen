//! Pure decisions for a fleet's children: which `Lumen` a fleet owns, whether
//! it may adopt one, a stable hash of its desired spec, and whether a child
//! that left the spec is retained or pruned. No cluster and no I/O types, and no
//! Lumen topology policy.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::domain::lumen_fleet::{PrunePolicy, FLEET_LABEL};

/// `(namespace, name)` of a child `Lumen`.
pub type ChildKey = (String, String);

/// Message reported for a child that is no longer declared and is retained.
pub const ORPHAN_RETAINED_MESSAGE: &str =
    "no longer declared by this fleet; retained (set prunePolicy: Delete to remove)";

/// Who owns a live child, judged from its fleet label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ownership {
    Absent,
    Owned,
    Foreign { found: Option<String> },
}

/// What the fleet may do with a child that already has (or lacks) a live object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Adoption {
    Create,
    Apply,
    NotAdopted { message: String },
}

/// What the fleet does with a child it did not declare in this pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneDecision {
    Keep,
    Orphaned,
    Pruned,
}

pub fn child_key(namespace: &str, name: &str) -> ChildKey {
    (namespace.to_string(), name.to_string())
}

/// Judge a live child by its fleet label. `None` means the label is absent.
pub fn ownership(live_label: Option<&str>, fleet_name: &str) -> Ownership {
    match live_label {
        Some(label) if label == fleet_name => Ownership::Owned,
        Some(label) => Ownership::Foreign {
            found: Some(label.to_string()),
        },
        None => Ownership::Foreign { found: None },
    }
}

/// `live` is `None` when no object exists, otherwise the object's fleet label.
pub fn adoption(live: Option<Option<&str>>, fleet_name: &str) -> Adoption {
    match live {
        None => Adoption::Create,
        Some(label) => match ownership(label, fleet_name) {
            Ownership::Owned | Ownership::Absent => Adoption::Apply,
            Ownership::Foreign { found } => Adoption::NotAdopted {
                message: format!(
                    "a Lumen already exists here and is not labelled {FLEET_LABEL}={fleet_name} \
                     (found {}); left untouched",
                    found.as_deref().unwrap_or("no label")
                ),
            },
        },
    }
}

/// FNV-1a 64 over the canonical JSON of `spec`: object keys sorted at every
/// level, array order kept. Same spec, same hash; any value change, another.
pub fn desired_hash(spec: &Value) -> String {
    let canonical = serde_json::to_string(&canonical(spec)).unwrap_or_default();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in canonical.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<&String, Value> =
                map.iter().map(|(key, v)| (key, canonical(v))).collect();
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, v)| (key.clone(), v))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        other => other.clone(),
    }
}

/// A child the fleet declares is kept. One it does not declare is orphaned
/// under `Retain` and pruned under `Delete`.
pub fn prune_decision(
    key: &ChildKey,
    declared: &BTreeSet<ChildKey>,
    policy: PrunePolicy,
) -> PruneDecision {
    if declared.contains(key) {
        return PruneDecision::Keep;
    }
    match policy {
        PrunePolicy::Retain => PruneDecision::Orphaned,
        PrunePolicy::Delete => PruneDecision::Pruned,
    }
}
