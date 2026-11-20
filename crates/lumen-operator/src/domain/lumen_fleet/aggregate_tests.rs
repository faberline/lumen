use super::*;
use crate::domain::lumen_fleet::FleetCondition;
use serde_json::json;

fn declared(ns: &str, name: &str, state: &str, child: Option<(bool, &str)>) -> DeclaredEntry {
    DeclaredEntry {
        namespace: ns.to_string(),
        name: name.to_string(),
        state: state.to_string(),
        child: child.map(|(ready, phase)| LiveChild {
            ready,
            phase: phase.to_string(),
        }),
    }
}

fn cond(agg: &FleetAggregate, kind: &str) -> FleetCondition {
    agg.conditions
        .iter()
        .find(|c| c.type_ == kind)
        .cloned()
        .expect("condition present")
}

#[test]
fn all_ready() {
    let agg = aggregate(
        &[
            declared("team-a", "search", "Applied", Some((true, "Ready"))),
            declared("team-b", "search", "Applied", Some((true, "Ready"))),
        ],
        0,
    );
    assert_eq!(agg.ready_instances, 2);
    assert_eq!(agg.degraded_instances, 0);
    assert_eq!(agg.not_ready_instances, 0);
    let kinds: Vec<&str> = agg.conditions.iter().map(|c| c.type_.as_str()).collect();
    assert_eq!(kinds, ["Ready", "Degraded", "Orphaned"]);
    let ready = cond(&agg, "Ready");
    assert_eq!(ready.status, "True");
    assert_eq!(ready.reason, "AllInstancesReady");
    assert_eq!(ready.message, "");
    assert_eq!(cond(&agg, "Degraded").status, "False");
    assert_eq!(cond(&agg, "Orphaned").status, "False");
}

#[test]
fn one_degraded_is_visible_and_blocks_ready() {
    let agg = aggregate(
        &[
            declared("team-a", "search", "Applied", Some((true, "Ready"))),
            declared("team-b", "search", "Applied", Some((false, "Degraded"))),
        ],
        0,
    );
    assert_eq!(agg.degraded_instances, 1);
    let ready = cond(&agg, "Ready");
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "InstanceNotReady");
    assert_eq!(ready.message, "team-b/search: Degraded");
    let degraded = cond(&agg, "Degraded");
    assert_eq!(degraded.status, "True");
    assert_eq!(degraded.reason, "InstancesDegraded");
    assert_eq!(
        degraded.message,
        "1 instance(s) degraded; first: team-b/search"
    );
}

#[test]
fn orphan_does_not_fail_ready() {
    let agg = aggregate(
        &[declared(
            "team-a",
            "search",
            "Applied",
            Some((true, "Ready")),
        )],
        1,
    );
    assert_eq!(cond(&agg, "Ready").status, "True");
    let orphaned = cond(&agg, "Orphaned");
    assert_eq!(orphaned.status, "True");
    assert_eq!(orphaned.reason, "InstancesRetained");
    assert_eq!(
        orphaned.message,
        "1 instance(s) no longer declared; retained"
    );
    assert_eq!(agg.ready_instances, 1);
}

#[test]
fn rejected_and_namespace_missing_are_not_ready() {
    let agg = aggregate(
        &[
            declared("team-a", "search", "Rejected", None),
            declared("team-b", "search", "NamespaceMissing", None),
        ],
        0,
    );
    assert_eq!(agg.not_ready_instances, 2);
    assert_eq!(cond(&agg, "Ready").message, "team-a/search: state Rejected");
}

#[test]
fn held_and_waiting_entries_are_not_ready() {
    let agg = aggregate(&[declared("team-a", "search", "Held", None)], 0);
    assert_eq!(agg.not_ready_instances, 1);
    assert_eq!(cond(&agg, "Ready").message, "team-a/search: state Held");
}

#[test]
fn a_recovering_child_reports_the_exact_reason_and_then_recovers() {
    let reconciling = aggregate(
        &[declared(
            "team-a",
            "search",
            "Applied",
            Some((false, "Reconciling")),
        )],
        0,
    );
    let ready = cond(&reconciling, "Ready");
    assert_eq!(ready.status, "False");
    assert_eq!(ready.reason, "InstanceNotReady");
    assert_eq!(ready.message, "team-a/search: phase Reconciling");

    let recovered = aggregate(
        &[declared(
            "team-a",
            "search",
            "Applied",
            Some((true, "Ready")),
        )],
        0,
    );
    let ready = cond(&recovered, "Ready");
    assert_eq!(ready.status, "True");
    assert_eq!(ready.reason, "AllInstancesReady");
    assert_eq!(ready.message, "");

    let unknown = aggregate(
        &[declared("team-a", "search", "Applied", Some((false, "")))],
        0,
    );
    assert_eq!(
        cond(&unknown, "Ready").message,
        "team-a/search: phase unknown"
    );
}

#[test]
fn a_new_child_with_no_status_is_not_ready() {
    let agg = aggregate(&[declared("team-a", "search", "Created", None)], 0);
    assert_eq!(agg.not_ready_instances, 1);
    assert_eq!(cond(&agg, "Ready").message, "team-a/search: no status yet");
}

#[test]
fn an_empty_fleet_is_ready() {
    let agg = aggregate(&[], 0);
    assert_eq!(cond(&agg, "Ready").status, "True");
    assert_eq!(agg.ready_instances, 0);
    assert_eq!(agg.degraded_instances, 0);
    assert_eq!(agg.not_ready_instances, 0);
}

#[test]
fn counts_add_up_to_the_declared_entries() {
    let entries = [
        declared("a", "one", "Applied", Some((true, "Ready"))),
        declared("a", "two", "Applied", Some((false, "Degraded"))),
        declared("a", "three", "Rejected", None),
        declared("a", "four", "Applied", Some((false, "Pending"))),
        declared("a", "five", "Created", None),
    ];
    let agg = aggregate(&entries, 0);
    assert_eq!(
        agg.ready_instances + agg.degraded_instances + agg.not_ready_instances,
        5
    );
    assert_eq!(agg.ready_instances, 1);
    assert_eq!(agg.degraded_instances, 1);
    assert_eq!(agg.not_ready_instances, 3);
}

#[test]
fn live_child_from_json_reads_phase_and_the_ready_condition() {
    let ready = live_child_from_json(&json!({
        "status": {"phase": "Ready", "conditions": [{"type": "Ready", "status": "True"}]}
    }));
    assert!(ready.ready);
    assert_eq!(ready.phase, "Ready");

    let empty = live_child_from_json(&json!({}));
    assert!(!empty.ready);
    assert_eq!(empty.phase, "");

    let not_ready = live_child_from_json(&json!({
        "status": {"conditions": [{"type": "Ready", "status": "False"}]}
    }));
    assert!(!not_ready.ready);
}

#[test]
fn orphans_do_not_fail_ready_and_degraded_is_not_ignored() {
    let orphans = aggregate(&[declared("a", "one", "Applied", Some((true, "Ready")))], 3);
    assert_eq!(cond(&orphans, "Ready").status, "True");

    let degraded = aggregate(
        &[declared("a", "one", "Applied", Some((false, "Degraded")))],
        0,
    );
    assert_eq!(cond(&degraded, "Ready").status, "False");
}
