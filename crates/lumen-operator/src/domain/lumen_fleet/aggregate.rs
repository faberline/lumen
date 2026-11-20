//! Pure aggregation of one fleet pass into the fleet's conditions and counts.
//! No `kube` types and no async code live here.

use serde_json::Value;

use crate::domain::lumen_fleet::FleetCondition;

/// What the aggregation needs to know about one live child `Lumen`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveChild {
    pub ready: bool,
    pub phase: String,
}

/// One declared entry of the pass (never an orphan).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredEntry {
    pub namespace: String,
    pub name: String,
    pub state: String,
    pub child: Option<LiveChild>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetAggregate {
    pub conditions: Vec<FleetCondition>,
    pub ready_instances: i32,
    pub degraded_instances: i32,
    pub not_ready_instances: i32,
}

/// `lumen` is the whole live `Lumen` as JSON. `phase` is `status.phase` as a
/// string (empty when missing). `ready` is true only when `status.conditions`
/// has an object whose `type` is `Ready` and `status` is `True`. A missing
/// `status` or `conditions` gives `ready: false`; never panics.
pub fn live_child_from_json(lumen: &Value) -> LiveChild {
    let status = lumen.get("status");
    let phase = status
        .and_then(|s| s.get("phase"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let ready = status
        .and_then(|s| s.get("conditions"))
        .and_then(Value::as_array)
        .is_some_and(|conditions| {
            conditions.iter().any(|c| {
                c.get("type").and_then(Value::as_str) == Some("Ready")
                    && c.get("status").and_then(Value::as_str) == Some("True")
            })
        });
    LiveChild { ready, phase }
}

/// The class of one declared entry. First matching rule wins.
enum Class {
    Ready,
    Degraded,
    NotReady(String),
}

fn classify(entry: &DeclaredEntry) -> Class {
    let materialized = entry.state == "Created" || entry.state == "Applied";
    match &entry.child {
        // Rule 1: a new child that has no status yet.
        None if materialized => Class::NotReady("no status yet".to_string()),
        // Rule 2: the entry has its own state (Rejected, NamespaceMissing, ...).
        _ if !materialized => Class::NotReady(format!("state {}", entry.state)),
        // Rules 3 to 5 need the child, which is `Some` here.
        None => Class::NotReady("no status yet".to_string()),
        Some(child) if child.phase == "Degraded" => Class::Degraded,
        Some(child) if child.ready => Class::Ready,
        Some(child) if child.phase.is_empty() => Class::NotReady("phase unknown".to_string()),
        Some(child) => Class::NotReady(format!("phase {}", child.phase)),
    }
}

/// Fold the declared entries of one pass into the three counts and the three
/// conditions `Ready`, `Degraded` and `Orphaned`, in that order.
///
/// `orphaned` is the number of `Orphaned` entries of the pass. An orphan never
/// changes `Ready`.
pub fn aggregate(declared: &[DeclaredEntry], orphaned: i32) -> FleetAggregate {
    let mut ready_instances = 0;
    let mut degraded_instances = 0;
    let mut not_ready_instances = 0;
    // `namespace/name: detail` of the first entry that is not ready.
    let mut first_blocking: Option<String> = None;
    // `namespace/name` of the first degraded entry.
    let mut first_degraded: Option<String> = None;

    for entry in declared {
        let key = format!("{}/{}", entry.namespace, entry.name);
        match classify(entry) {
            Class::Ready => ready_instances += 1,
            Class::Degraded => {
                degraded_instances += 1;
                first_degraded.get_or_insert_with(|| key.clone());
                first_blocking.get_or_insert_with(|| format!("{key}: Degraded"));
            }
            Class::NotReady(detail) => {
                not_ready_instances += 1;
                first_blocking.get_or_insert_with(|| format!("{key}: {detail}"));
            }
        }
    }

    let ready = match first_blocking {
        None => FleetCondition {
            type_: "Ready".to_string(),
            status: "True".to_string(),
            reason: "AllInstancesReady".to_string(),
            message: String::new(),
        },
        Some(message) => FleetCondition {
            type_: "Ready".to_string(),
            status: "False".to_string(),
            reason: "InstanceNotReady".to_string(),
            message,
        },
    };

    let degraded = match first_degraded {
        Some(first) => FleetCondition {
            type_: "Degraded".to_string(),
            status: "True".to_string(),
            reason: "InstancesDegraded".to_string(),
            message: format!("{degraded_instances} instance(s) degraded; first: {first}"),
        },
        None => FleetCondition {
            type_: "Degraded".to_string(),
            status: "False".to_string(),
            reason: "NoneDegraded".to_string(),
            message: String::new(),
        },
    };

    let orphaned_condition = if orphaned > 0 {
        FleetCondition {
            type_: "Orphaned".to_string(),
            status: "True".to_string(),
            reason: "InstancesRetained".to_string(),
            message: format!("{orphaned} instance(s) no longer declared; retained"),
        }
    } else {
        FleetCondition {
            type_: "Orphaned".to_string(),
            status: "False".to_string(),
            reason: "NoneOrphaned".to_string(),
            message: String::new(),
        }
    };

    FleetAggregate {
        conditions: vec![ready, degraded, orphaned_condition],
        ready_instances,
        degraded_instances,
        not_ready_instances,
    }
}

#[cfg(test)]
#[path = "aggregate_tests.rs"]
mod tests;
