use std::collections::HashMap;

use service_k8s::{ManagedService, ReadyFacts};

use crate::application::reconcile::tests::hpa::hpa_test_lumen;

/// `ReadyFacts` reporting the given `readyReplicas` per StatefulSet name.
fn facts(counts: &[(&str, i64)]) -> ReadyFacts {
    let map: HashMap<String, i64> = counts
        .iter()
        .map(|(name, count)| (name.to_string(), *count))
        .collect();
    ReadyFacts::new(map)
}

#[test]
fn single_shard_has_one_readiness_target() {
    let lumen = hpa_test_lumen("search", "ns", 1, 1);
    let targets = lumen.readiness_targets();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].kind(), "StatefulSet");
    assert_eq!(targets[0].name(), "search");
}

#[test]
fn two_shards_have_one_readiness_target_each() {
    let lumen = hpa_test_lumen("search", "ns", 2, 1);
    let targets = lumen.readiness_targets();
    let names: Vec<&str> = targets.iter().map(|target| target.name()).collect();
    assert_eq!(names, vec!["search-s0", "search-s1"]);
    assert!(targets.iter().all(|target| target.kind() == "StatefulSet"));
}

#[test]
fn status_sums_ready_pods_over_shard_sets() {
    let lumen = hpa_test_lumen("search", "ns", 2, 1);
    let patch = lumen.status_patch(&facts(&[("search-s0", 1), ("search-s1", 1)]));
    assert_eq!(patch["status"]["servingReadyReplicas"], 2);
}

#[test]
fn one_unready_shard_keeps_status_below_desired() {
    // Negative control: with one shard not ready, the status must not
    // report the same phase as with both shards ready.
    let lumen = hpa_test_lumen("search", "ns", 2, 1);
    let both = lumen.status_patch(&facts(&[("search-s0", 1), ("search-s1", 1)]));
    let one = lumen.status_patch(&facts(&[("search-s0", 1), ("search-s1", 0)]));
    assert_eq!(one["status"]["servingReadyReplicas"], 1);
    assert_eq!(one["status"]["desiredReplicas"], 2);
    assert_ne!(one["status"]["phase"], both["status"]["phase"]);
}

#[test]
fn single_shard_status_reads_the_unsuffixed_set() {
    let lumen = hpa_test_lumen("search", "ns", 1, 1);
    let patch = lumen.status_patch(&facts(&[("search", 1)]));
    assert_eq!(patch["status"]["servingReadyReplicas"], 1);
}

#[test]
fn conditions_read_each_shard_set() {
    use crate::application::reconcile::tests::{condition, sharded_ready_facts};
    use service_k8s::ConditionStatus;

    let lumen = hpa_test_lumen("search", "ns", 2, 1);

    let partial = lumen.conditions(
        &sharded_ready_facts("search", &[1, 0]),
        &serde_json::Value::Null,
    );
    let ready = condition(&partial, "Ready");
    assert_eq!(ready.status, ConditionStatus::False, "got: {partial:?}");
    assert!(ready.message.contains("1/2"), "got: {ready:?}");

    let full = lumen.conditions(
        &sharded_ready_facts("search", &[1, 1]),
        &serde_json::Value::Null,
    );
    assert_eq!(
        condition(&full, "Ready").status,
        ConditionStatus::True,
        "got: {full:?}"
    );
}
