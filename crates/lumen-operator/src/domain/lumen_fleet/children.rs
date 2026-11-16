//! Pure decisions about the child `Lumen` objects of a fleet: their keys,
//! who owns them, whether a live object may be adopted, the hash of the desired
//! spec, and whether an undeclared child is retained or deleted.
//!
//! Nothing here touches a cluster or awaits anything. The I/O loop in
//! `application::fleet_reconcile` asks these functions what to do. The module
//! holds no Lumen topology policy: no shard, replica or reshard knowledge.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::domain::lumen_fleet::{PrunePolicy, FLEET_LABEL};

/// A child's identity: its namespace and its name.
pub type ChildKey = (String, String);

/// The child key of an instance.
pub fn child_key(namespace: &str, name: &str) -> ChildKey {
    (namespace.to_string(), name.to_string())
}

/// Who owns a live child, judged from its [`FLEET_LABEL`] value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ownership {
    /// No live object. Not produced by [`ownership`]; [`adoption`] returns
    /// [`Adoption::Create`] for a missing object instead.
    Absent,
    /// The live object carries this fleet's name in [`FLEET_LABEL`].
    Owned,
    /// The live object belongs to someone else, or has no label. `found` is the
    /// label value that was there.
    Foreign { found: Option<String> },
}

/// Judges a live object from its [`FLEET_LABEL`] value.
pub fn ownership(live_label: Option<&str>, fleet_name: &str) -> Ownership {
    match live_label {
        Some(label) if label == fleet_name => Ownership::Owned,
        Some(label) => Ownership::Foreign {
            found: Some(label.to_string()),
        },
        None => Ownership::Foreign { found: None },
    }
}

/// What the fleet does with a child it declares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Adoption {
    /// No live object: create it.
    Create,
    /// A live object this fleet owns: apply the desired spec to it.
    Apply,
    /// A live object this fleet does not own: leave it untouched.
    NotAdopted { message: String },
}

/// Decides how to reconcile a declared child.
///
/// `live` is `None` when the object does not exist. Otherwise it is the value of
/// [`FLEET_LABEL`] on the live object, or `None` when that label is absent.
pub fn adoption(live: Option<Option<&str>>, fleet_name: &str) -> Adoption {
    let Some(live_label) = live else {
        return Adoption::Create;
    };
    match ownership(live_label, fleet_name) {
        Ownership::Owned => Adoption::Apply,
        Ownership::Foreign { found } => Adoption::NotAdopted {
            message: format!(
                "a Lumen already exists here and is not labelled {FLEET_LABEL}={fleet_name} \
                 (found {}); left untouched",
                found.as_deref().unwrap_or("no label")
            ),
        },
        Ownership::Absent => Adoption::Create,
    }
}

/// A hash of the desired spec. Equal specs hash equal; object key order does
/// not matter; array order does.
///
/// The canonical form sorts object keys recursively, because the workspace
/// enables `serde_json/preserve_order`. The hash is FNV-1a 64-bit over the
/// canonical JSON text, printed as 16 lowercase hex digits.
pub fn desired_hash(spec: &Value) -> String {
    let canonical = serde_json::to_string(&sorted_keys(spec))
        .expect("a JSON value always serializes to a string");
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in canonical.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn sorted_keys(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<&String, Value> = map
                .iter()
                .map(|(key, item)| (key, sorted_keys(item)))
                .collect();
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, item)| (key.clone(), item))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.iter().map(sorted_keys).collect()),
        other => other.clone(),
    }
}

/// What happens to a live child that the fleet did not declare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PruneDecision {
    /// Declared: the child stays and is reconciled.
    Keep,
    /// Not declared, and the policy retains it: report it.
    Orphaned,
    /// Not declared, and the policy deletes it.
    Pruned,
}

/// Decides what to do with a live child under `policy`.
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

/// The status message of an orphan that the policy retains.
pub const ORPHAN_RETAINED_MESSAGE: &str =
    "no longer declared by this fleet; retained (set prunePolicy: Delete to remove)";
