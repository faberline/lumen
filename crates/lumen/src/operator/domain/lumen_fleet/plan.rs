//! Turning a fleet into the `Lumen` specs it declares, and the create and apply
//! bodies for each.

use std::collections::BTreeSet;

use serde_json::{json, Value};

use crate::operator::domain::lumen_fleet::{
    LumenFleet, PlanOutcome, PlannedInstance, FLEET_LABEL, FLEET_MANAGER,
};
use crate::operator::domain::lumen_spec::LumenSpec;

/// Spec paths the reshard driver owns at runtime; see the module docs.
const DRIVER_OWNED_PATHS: &[&[&str]] = &[
    &["shardCount"],
    &["shardMap"],
    &["reshardPolicy", "workflow"],
];

/// Turn one fleet into the list of `Lumen` specs it declares, without touching
/// a cluster. Every rejection is per-entry: one malformed override must not
/// stop the other tenants from converging.
pub fn plan(fleet: &LumenFleet) -> Vec<PlannedInstance> {
    let fleet_name = fleet.metadata.name.clone().unwrap_or_default();
    let defaults = serde_json::to_value(&fleet.spec.defaults).unwrap_or(Value::Null);
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut planned = Vec::new();

    for entry in &fleet.spec.instances {
        let name = entry.name.clone().unwrap_or_else(|| fleet_name.clone());
        let namespace = entry.namespace.clone();

        // Two entries writing the same object would each revert the other on
        // every pass, and the losing tenant's settings would depend on list
        // order. Reject the duplicate rather than let the fleet oscillate.
        if !seen.insert((namespace.clone(), name.clone())) {
            planned.push(PlannedInstance {
                namespace,
                name,
                outcome: PlanOutcome::Rejected(
                    "a previous entry already targets this namespace/name; \
                     two entries writing one object would revert each other every pass"
                        .to_string(),
                ),
            });
            continue;
        }

        let mut merged = defaults.clone();
        if let Some(patch) = &entry.spec {
            merge_patch(&mut merged, patch);
        }
        planned.push(PlannedInstance {
            namespace,
            name,
            outcome: validate_merged(merged),
        });
    }
    planned
}

/// Deserialize the merged document into a `LumenSpec` and prove nothing in it
/// was silently ignored.
fn validate_merged(merged: Value) -> PlanOutcome {
    // Through `serde_path_to_error` so the rejection names the field. Plain
    // serde would report `invalid type: integer, expected a string` for a
    // wrong `serving.cpu`, leaving whoever reads the fleet's status to guess
    // which of ~40 spec fields is meant.
    let spec: LumenSpec = match serde_path_to_error::deserialize(&merged) {
        Ok(spec) => spec,
        Err(err) => {
            let path = err.path().to_string();
            return PlanOutcome::Rejected(format!(
                "merged spec is not a valid Lumen at `{path}`: {}",
                err.into_inner()
            ));
        }
    };
    let round_trip = match serde_json::to_value(&spec) {
        Ok(value) => value,
        Err(err) => {
            return PlanOutcome::Rejected(format!("merged spec does not re-serialize: {err}"))
        }
    };
    let mut unknown = Vec::new();
    unknown_keys(&merged, &round_trip, "", &mut unknown);
    if !unknown.is_empty() {
        return PlanOutcome::Rejected(format!(
            "the merged spec names fields a Lumen does not have: {}; \
             a misspelled override would otherwise leave this instance silently on the defaults",
            unknown.join(", ")
        ));
    }
    if let Err(err) = spec.validate() {
        return PlanOutcome::Rejected(err);
    }
    PlanOutcome::Ready(merged)
}

/// RFC 7386 JSON Merge Patch: objects merge key-by-key, `null` deletes, every
/// other value replaces wholesale.
fn merge_patch(base: &mut Value, patch: &Value) {
    let Value::Object(patch) = patch else {
        *base = patch.clone();
        return;
    };
    if !base.is_object() {
        *base = Value::Object(Default::default());
    }
    let map = base.as_object_mut().expect("just made it an object");
    for (key, value) in patch {
        if value.is_null() {
            map.remove(key);
        } else {
            merge_patch(map.entry(key.clone()).or_insert(Value::Null), value);
        }
    }
}

/// Keys present in `input` that a `LumenSpec` round-trip dropped — i.e. fields
/// the spec does not have.
///
/// Empty collections and explicit nulls are exempt because `LumenSpec` skips
/// serializing them, so their absence from the round-trip is the serializer's
/// doing rather than evidence of a typo.
fn unknown_keys(input: &Value, round_trip: &Value, prefix: &str, out: &mut Vec<String>) {
    let (Value::Object(input), Value::Object(round_trip)) = (input, round_trip) else {
        return;
    };
    for (key, value) in input {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match round_trip.get(key) {
            Some(known) => unknown_keys(value, known, &path, out),
            None => {
                let vacuous = value.is_null()
                    || value.as_array().is_some_and(|items| items.is_empty())
                    || value.as_object().is_some_and(|map| map.is_empty());
                if !vacuous {
                    out.push(path);
                }
            }
        }
    }
}

/// The body of the one-time create: the whole merged spec, initial topology
/// included.
pub fn seed_object(fleet: &str, planned: &PlannedInstance, spec: &Value) -> Value {
    json!({
        "apiVersion": "lumen.dev/v1alpha1",
        "kind": "Lumen",
        "metadata": {
            "name": planned.name,
            "namespace": planned.namespace,
            "labels": fleet_labels(fleet),
        },
        "spec": spec,
    })
}

/// The body of every apply after the first: the merged spec **minus** the
/// paths the reshard driver owns. Omitting them from the apply-set is what
/// keeps the fleet from ever reverting a completed split — see the module
/// docs.
pub fn apply_object(fleet: &str, planned: &PlannedInstance, spec: &Value) -> Value {
    let mut spec = spec.clone();
    for path in DRIVER_OWNED_PATHS {
        remove_path(&mut spec, path);
    }
    seed_object(fleet, planned, &spec)
}

fn remove_path(value: &mut Value, path: &[&str]) {
    let Some((head, rest)) = path.split_first() else {
        return;
    };
    let Some(map) = value.as_object_mut() else {
        return;
    };
    if rest.is_empty() {
        map.remove(*head);
    } else if let Some(child) = map.get_mut(*head) {
        remove_path(child, rest);
    }
}

fn fleet_labels(fleet: &str) -> Value {
    json!({
        FLEET_LABEL: fleet,
        "app.kubernetes.io/managed-by": FLEET_MANAGER,
    })
}
