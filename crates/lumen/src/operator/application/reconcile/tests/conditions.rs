use service_k8s::{ConditionStatus, ManagedService};

use crate::operator::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::operator::application::reconcile::tests::status_patch::{
    cutover_pending_convergence_lumen, test_now_epoch_secs,
};
use crate::operator::application::reconcile::tests::{condition, ready_facts};

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
fn a_reshard_wedge_outranks_a_healthy_replica_count() {
    // Every pod Ready, but writes are unappliable: `Ready=True` here would
    // tell `kubectl wait` the CR converged while it is in fact stuck.
    let namespace = "acme-cond-wedged";
    let name = "search";
    let lumen = hpa_test_lumen(name, namespace, 2, 1);
    crate::operator::application::reshard_driver::oversize::record_oversize_block(
        namespace,
        name,
        "",
        crate::operator::application::reshard_driver::oversize::OversizedDocumentBlock {
            collection: "widgets".to_string(),
            external_id: "doc-42".to_string(),
            bytes: 9_000_000,
        },
    );

    let facts = lumen.conditions(&ready_facts(name, 2), &serde_json::Value::Null);
    crate::operator::application::reshard_driver::oversize::clear_oversize_block(namespace, name);

    let ready = condition(&facts, "Ready");
    assert_eq!(ready.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(ready.reason, "ReshardWedged");
    assert!(
        ready.message.contains("reshardOversizedDocument") && ready.message.contains("doc-42"),
        "the message must name the wedge and its remediation detail, got: {ready:?}"
    );
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
fn a_stalled_fence_is_a_wedge() {
    let mut lumen = cutover_pending_convergence_lumen("search", "acme-cond-stalled", 1);
    let stall_budget = crate::operator::application::reshard_driver::convergence_stall::convergence_stall_budget_secs();
    lumen
        .spec
        .reshard_policy
        .workflow
        .convergence_wait_started_at = Some(test_now_epoch_secs().saturating_sub(stall_budget + 1));

    let facts = lumen.conditions(&ready_facts("search", 2), &serde_json::Value::Null);
    let ready = condition(&facts, "Ready");
    assert_eq!(ready.status, ConditionStatus::False, "got: {facts:?}");
    assert_eq!(ready.reason, "ReshardWedged");
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

    lumen.status = Some(crate::operator::domain::lumen_spec::status::LumenStatus {
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
