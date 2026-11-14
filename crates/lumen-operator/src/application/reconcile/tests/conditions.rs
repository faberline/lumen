use serde_json::json;
use service_k8s::{ConditionStatus, ManagedService};

use crate::application::reconcile::plan_verdicts::{
    CAPACITY_CONTEXT_KEY, PEER_IDENTITY_CONTEXT_KEY,
};
use crate::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::application::reconcile::tests::status_patch::{
    cutover_pending_convergence_lumen, test_now_epoch_secs,
};
use crate::application::reconcile::tests::{condition, ready_facts};

#[test]
fn a_fully_ready_default_cr_reports_ready_true() {
    // The load-bearing case: at plain defaults `reshard_status` always
    // reports `maxShardBytesUnset` in `blockingConditions`. Gating `Ready`
    // on that list would leave every install that never opted into
    // auto-splitting permanently not-ready.
    let lumen = hpa_test_lumen("search", "acme-cond-ready", 2, 1);
    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);

    let ready = condition(&facts, "Ready");
    assert_eq!(ready.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(ready.reason, "AllReplicasReady");
    assert_eq!(
        condition(&facts, "Progressing").status,
        ConditionStatus::False,
        "a settled CR is not progressing, got: {facts:?}"
    );
    assert_eq!(
        condition(&facts, "ReshardInProgress").status,
        ConditionStatus::False,
        "no reshard workflow is in flight, got: {facts:?}"
    );
}

#[test]
fn short_replicas_report_ready_false_and_progressing_true() {
    let lumen = hpa_test_lumen("search", "acme-cond-short", 2, 1);
    let facts = lumen.conditions(&ready_facts("search", 1), &serde_json::Value::Null);

    let ready = condition(&facts, "Ready");
    assert_eq!(ready.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(ready.reason, "ReplicasNotReady");
    assert!(
        ready.message.contains("1/2"),
        "the message must name the counts, got: {ready:?}"
    );

    let progressing = condition(&facts, "Progressing");
    assert_eq!(progressing.status, ConditionStatus::True);
    assert_eq!(progressing.reason, "ReplicasConverging");
}

#[test]
fn a_reshard_wedge_degrades_writable_but_not_ready() {
    // Every pod Ready and searchable service is up, so `Ready` says so. The
    // unappliable write is a write-plane fault: it shows on `Writable` and
    // `Degraded`, and never flips `Ready`.
    let namespace = "acme-cond-wedged";
    let name = "search";
    let lumen = hpa_test_lumen(name, namespace, 2, 1);
    crate::application::reshard_driver::oversize::record_oversize_block(
        namespace,
        name,
        "",
        crate::application::reshard_driver::oversize::OversizedDocumentBlock {
            collection: "widgets".to_string(),
            external_id: "doc-42".to_string(),
            bytes: 9_000_000,
        },
    );

    let facts = lumen.conditions(&ready_facts(name, 2), &serde_json::Value::Null);
    crate::application::reshard_driver::oversize::clear_oversize_block(namespace, name);

    let ready = condition(&facts, "Ready");
    assert_eq!(ready.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(ready.reason, "AllReplicasReady");
    let writable = condition(&facts, "Writable");
    assert_eq!(writable.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(writable.reason, "ReshardWedged");
    assert!(
        writable.message.contains("reshardOversizedDocument")
            && writable.message.contains("doc-42"),
        "the message must name the wedge and its remediation detail, got: {writable:?}"
    );
    let degraded = condition(&facts, "Degraded");
    assert_eq!(degraded.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(degraded.reason, "WritableNotReady");
}

#[test]
fn the_post_cutover_fence_reports_reshard_in_progress_at_phase_complete() {
    // `awaitingTopologyConvergence` happens *at* phase `Complete`, which is
    // why `reshard_active` cannot be a phase comparison alone.
    let lumen = cutover_pending_convergence_lumen("search", "acme-cond-fence", 1);
    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);

    let reshard = condition(&facts, "ReshardInProgress");
    assert_eq!(reshard.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(reshard.reason, "AwaitingTopologyConvergence");
    assert_eq!(
        condition(&facts, "Progressing").reason,
        "ReshardInFlight",
        "got: {facts:?}"
    );
    assert_eq!(
        condition(&facts, "Ready").status,
        ConditionStatus::True,
        "an armed fence is expected mid-reshard, not a wedge — only a stall is, \
             got: {facts:?}"
    );
}

#[test]
fn a_stalled_fence_degrades_writable_but_not_ready() {
    let mut lumen = cutover_pending_convergence_lumen("search", "acme-cond-stalled", 1);
    let stall_budget =
        crate::application::reshard_driver::convergence_stall::convergence_stall_budget_secs();
    lumen
        .spec
        .reshard_policy
        .workflow
        .convergence_wait_started_at = Some(test_now_epoch_secs().saturating_sub(stall_budget + 1));

    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);
    assert_eq!(
        condition(&facts, "Ready").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
    let writable = condition(&facts, "Writable");
    assert_eq!(writable.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(writable.reason, "ReshardWedged");
    assert_eq!(
        condition(&facts, "Degraded").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
}

#[test]
fn a_healthy_default_cr_has_all_planes_healthy() {
    // Negative control: a plane wrongly False on a healthy CR would show up
    // here as a degraded CR, so this pins every plane to its healthy value.
    let lumen = hpa_test_lumen("search", "acme-cond-planes-ok", 2, 1);
    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);
    for type_ in [
        "Writable",
        "ReplicationReady",
        "ConfigReady",
        "CapacityReady",
    ] {
        assert_eq!(
            condition(&facts, type_).status,
            ConditionStatus::True,
            "{type_} must be healthy, got: {facts:?}"
        );
    }
    let degraded = condition(&facts, "Degraded");
    assert_eq!(degraded.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(degraded.reason, "AllPlanesHealthy");
}

#[test]
fn an_armed_fence_pauses_writable_without_touching_ready() {
    let lumen = cutover_pending_convergence_lumen("search", "acme-cond-fence-w", 1);
    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);
    let writable = condition(&facts, "Writable");
    assert_eq!(writable.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(writable.reason, "WritePauseFenceArmed");
    assert_eq!(
        condition(&facts, "Ready").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
    assert_eq!(
        condition(&facts, "Degraded").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
}

#[test]
fn a_missing_peer_identity_fails_replication_ready() {
    let lumen = hpa_test_lumen("search", "acme-cond-repl", 1, 3);
    let context = json!({ PEER_IDENTITY_CONTEXT_KEY: "no peer secret" });
    let facts = lumen.conditions(&ready_facts("search", 3), &context);

    let replication = condition(&facts, "ReplicationReady");
    assert_eq!(replication.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(replication.reason, "PeerIdentityNotConfigured");
    for type_ in ["Writable", "ConfigReady", "CapacityReady"] {
        assert_eq!(
            condition(&facts, type_).status,
            ConditionStatus::True,
            "{type_} must stay healthy, got: {facts:?}"
        );
    }
    let degraded = condition(&facts, "Degraded");
    assert_eq!(degraded.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(degraded.reason, "ReplicationReadyNotReady");
    // `Ready` keeps its existing refusal: a replicated group with no peer
    // identity cannot replicate, and that is unchanged by this projection.
    assert_eq!(
        condition(&facts, "Ready").reason,
        "PeerIdentityNotConfigured"
    );
}

#[test]
fn an_invalid_spec_fails_config_ready_only() {
    let mut lumen = hpa_test_lumen("search", "acme-cond-cfg", 2, 1);
    lumen.spec.body_limit_bytes = Some(1);
    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);

    let config = condition(&facts, "ConfigReady");
    assert_eq!(config.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(config.reason, "SpecInvalid");
    assert!(config.message.contains("bodyLimitBytes"), "got: {config:?}");
    assert_eq!(
        condition(&facts, "Ready").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
    let degraded = condition(&facts, "Degraded");
    assert_eq!(degraded.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(degraded.reason, "ConfigReadyNotReady");
}

#[test]
fn a_capacity_refusal_fails_capacity_ready_only() {
    let lumen = hpa_test_lumen("search", "acme-cond-capacity", 2, 1);
    let context = json!({ CAPACITY_CONTEXT_KEY: "catalog ConfigMap missing" });
    let facts = lumen.conditions(&ready_facts("search", 2), &context);

    let capacity = condition(&facts, "CapacityReady");
    assert_eq!(capacity.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(capacity.reason, "CapacityRefused");
    assert!(
        capacity.message.contains("catalog ConfigMap missing"),
        "got: {capacity:?}"
    );
    assert_eq!(
        condition(&facts, "Ready").status,
        ConditionStatus::True,
        "got: {facts:?}"
    );
    let degraded = condition(&facts, "Degraded");
    assert_eq!(degraded.status, ConditionStatus::True, "got: {facts:?}");
    assert_eq!(degraded.reason, "CapacityReadyNotReady");
}

#[test]
fn degraded_names_the_first_failing_plane() {
    // Replication is checked before config, so it is the plane `Degraded` names
    // when both are False.
    let mut lumen = hpa_test_lumen("search", "acme-cond-first", 1, 3);
    lumen.spec.body_limit_bytes = Some(1);
    let context = json!({ PEER_IDENTITY_CONTEXT_KEY: "no peer secret" });
    let facts = lumen.conditions(&ready_facts("search", 3), &context);
    assert_eq!(
        condition(&facts, "Degraded").reason,
        "ReplicationReadyNotReady",
        "got: {facts:?}"
    );
}

#[test]
fn conditions_are_a_pure_function_of_spec_and_observed_facts() {
    // The determinism the clock-free split exists to preserve: no wall
    // clock is read here, so repeated projection is byte-identical.
    let lumen = hpa_test_lumen("search", "acme-cond-deterministic", 2, 1);
    let ready = ready_facts("search", 1);
    assert_eq!(
        lumen.conditions(&ready, &serde_json::Value::Null),
        lumen.conditions(&ready, &serde_json::Value::Null)
    );
}

#[test]
fn the_flat_status_and_the_conditions_agree_on_readiness() {
    // Both surfaces project from one `Observation`; this pins that they
    // cannot drift as either side grows.
    let lumen = hpa_test_lumen("search", "acme-cond-agree", 2, 1);
    for count in [0i64, 1, 2] {
        let ready = ready_facts("search", count);
        let phase = lumen.status_patch(&ready)["status"]["phase"]
            .as_str()
            .expect("phase")
            .to_string();
        let ready_condition =
            condition(&lumen.conditions(&ready, &serde_json::Value::Null), "Ready").status;
        assert_eq!(
            phase == "Ready",
            ready_condition == ConditionStatus::True,
            "phase {phase:?} disagrees with the Ready condition at {count} ready pods"
        );
        // A healthy CR's planes do not degrade, whatever `Ready` reports.
        assert_eq!(
            condition(
                &lumen.conditions(&ready, &serde_json::Value::Null),
                "Degraded"
            )
            .status,
            ConditionStatus::False,
            "a healthy CR must not be degraded at {count} ready pods"
        );
    }
}

#[test]
fn observed_conditions_round_trip_through_the_projection() {
    // The `Patch::Merge` array-replacement trap: unless prior conditions
    // are read back off the watched object, every reconcile would restamp
    // `lastTransitionTime` and every watcher would see the 30s requeue as
    // a state change.
    let mut lumen = hpa_test_lumen("search", "acme-cond-transition", 2, 1);
    let ready = ready_facts("search", 2);
    let first = service_k8s::service::project(
        &lumen.observed_conditions(),
        lumen.conditions(&ready, &serde_json::Value::Null),
        1,
        "2026-07-25T00:00:00Z",
    );
    assert!(lumen.observed_conditions().is_empty());

    lumen.status = Some(crate::domain::lumen_spec::status::LumenStatus {
        conditions: first.clone(),
        ..Default::default()
    });
    assert_eq!(lumen.observed_conditions(), first);

    let second = service_k8s::service::project(
        &lumen.observed_conditions(),
        lumen.conditions(&ready, &serde_json::Value::Null),
        2,
        "2026-07-25T01:00:00Z",
    );
    assert_eq!(
        second[0].last_transition_time, "2026-07-25T00:00:00Z",
        "an unchanged status must keep its original transition time"
    );
    assert_eq!(
        second[0].observed_generation,
        Some(2),
        "observedGeneration tracks every reconcile, unlike lastTransitionTime"
    );
}
