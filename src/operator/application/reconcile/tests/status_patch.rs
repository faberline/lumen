use service_k8s::{ManagedService, ReadyFacts};

use crate::operator::application::reconcile::tests::hpa::hpa_test_lumen;
use crate::operator::domain::lumen_spec::Lumen;

// ---- #1444 R2: oversized-doc blocking condition in status.reshard -----

#[test]
fn status_patch_surfaces_oversize_block_as_distinct_reshard_condition() {
    let lumen = hpa_test_lumen("search", "acme-status-oversize", 2, 1);
    let namespace = "acme-status-oversize";
    let name = "search";
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

    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"]
        .as_array()
        .expect("blockingConditions must be present");
    assert!(
        blocking
            .iter()
            .any(|c| c.as_str() == Some("reshardOversizedDocument")),
        "status.reshard.blockingConditions must include reshardOversizedDocument, got: {reshard}"
    );
    let message = reshard["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("widgets") && message.contains("doc-42"),
        "status.reshard.message must name the collection and external_id, got: {message}"
    );

    crate::operator::application::reshard_driver::oversize::clear_oversize_block(namespace, name);
}

#[test]
fn status_patch_has_no_oversize_condition_when_none_recorded() {
    let lumen = hpa_test_lumen("search", "acme-status-clean", 2, 1);
    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"].as_array();
    let has_condition = blocking
        .map(|arr| {
            arr.iter()
                .any(|c| c.as_str() == Some("reshardOversizedDocument"))
        })
        .unwrap_or(false);
    assert!(
        !has_condition,
        "no oversize wedge was recorded for this namespace/name; \
             status.reshard.blockingConditions must not report one, got: {reshard}"
    );
}

/// #1458 R4/AC4: a CR recorded an oversize wedge under `uid` "old-uid";
/// a deleted-and-recreated CR under the *same* namespace/name gets a
/// fresh `uid` from the API server, so its status must be clean
/// immediately — not wait for `prune_oversize_cache`'s next poll to
/// catch up with the driver-loop's live-CR listing.
#[test]
fn status_patch_is_clean_for_a_recreated_cr_with_a_stale_cached_uid() {
    let namespace = "acme-status-recreated";
    let name = "search";
    crate::operator::application::reshard_driver::oversize::record_oversize_block(
        namespace,
        name,
        "old-uid",
        crate::operator::application::reshard_driver::oversize::OversizedDocumentBlock {
            collection: "widgets".to_string(),
            external_id: "doc-42".to_string(),
            bytes: 9_000_000,
        },
    );

    let mut lumen = hpa_test_lumen(name, namespace, 2, 1);
    lumen.metadata.uid = Some("new-uid".to_string());

    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"].as_array();
    let has_condition = blocking
        .map(|arr| {
            arr.iter()
                .any(|c| c.as_str() == Some("reshardOversizedDocument"))
        })
        .unwrap_or(false);
    assert!(
        !has_condition,
        "a recreated CR (new uid) must not inherit the deleted CR's stale oversize wedge \
             cached under the old uid, got: {reshard}"
    );

    crate::operator::application::reshard_driver::oversize::clear_oversize_block(namespace, name);
}

// ---- #1467 R7: bounded topology-convergence stall escalation ----------

/// A `shardMap.version` the workflow actually cut over to
/// (`lastCutoverShardMapVersion == shardMap.version`), still unconverged
/// (`convergedShardMapVersion` absent): the shape `status_patch`'s
/// `awaitingTopologyConvergence` gate requires.
pub(super) fn cutover_pending_convergence_lumen(name: &str, ns: &str, map_version: u64) -> Lumen {
    let mut lumen = hpa_test_lumen(name, ns, 2, 1);
    lumen.spec.shard_map.version = map_version;
    lumen
        .spec
        .reshard_policy
        .workflow
        .last_cutover_shard_map_version = Some(map_version);
    lumen
}

#[test]
fn status_patch_reports_awaiting_convergence_without_a_stall_condition_before_the_budget() {
    let lumen = cutover_pending_convergence_lumen("search", "acme-convergence-fresh", 1);
    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"]
        .as_array()
        .expect("blockingConditions must be present");
    assert!(
        blocking
            .iter()
            .any(|c| c.as_str() == Some("awaitingTopologyConvergence")),
        "got: {reshard}"
    );
    assert!(
        !blocking
            .iter()
            .any(|c| c.as_str() == Some("topologyConvergenceStalled")),
        "a freshly-awaiting convergence (no recorded stall ticks yet) must not report the \
             stalled condition, got: {reshard}"
    );
}

/// Wall-clock "epoch seconds" helper duplicated from `reshard_driver`'s
/// own private one (not exposed beyond `pub fn
/// convergence_stall_budget_secs`) — this test only needs `now`, not the
/// budget constant itself, to back-date `convergenceWaitStartedAt`.
pub(super) fn test_now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[test]
fn status_patch_surfaces_topology_convergence_stall_as_distinct_condition() {
    let namespace = "acme-convergence-stalled";
    let name = "search";
    let map_version = 1u64;
    let mut lumen = cutover_pending_convergence_lumen(name, namespace, map_version);

    // #1485 R2: the stall budget is now computed purely from the
    // persisted `workflow.convergenceWaitStartedAt` checkpoint — no
    // driver-side cache to drive here — so simulate an extended wait by
    // back-dating it past `convergence_stall_budget_secs()` directly,
    // exactly what a real long-running wait (or a wait that started
    // before the operator restarted) would leave behind in the CR.
    let stall_budget = crate::operator::application::reshard_driver::convergence_stall::convergence_stall_budget_secs();
    lumen
        .spec
        .reshard_policy
        .workflow
        .convergence_wait_started_at = Some(test_now_epoch_secs().saturating_sub(stall_budget + 1));

    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"]
        .as_array()
        .expect("blockingConditions must be present");
    assert!(
        blocking
            .iter()
            .any(|c| c.as_str() == Some("awaitingTopologyConvergence")),
        "topologyConvergenceStalled must be layered on top of, not instead of, \
             awaitingTopologyConvergence, got: {reshard}"
    );
    assert!(
        blocking
            .iter()
            .any(|c| c.as_str() == Some("topologyConvergenceStalled")),
        "expected a distinct topologyConvergenceStalled condition once the stall budget \
             is exceeded, got: {reshard}"
    );
}

/// #1485 R2/AC2: the raised `topologyConvergenceStalled` condition is a
/// pure function of the persisted CR (`workflow.convergenceWaitStartedAt`
/// alone) — it is computed identically whether or not the operator
/// process serving this reconcile has ever seen this CR before, i.e. it
/// survives an operator restart by construction, unlike the pre-#1485
/// process-local-cache-only computation which required 30+ consecutive
/// in-process ticks to re-accumulate before re-raising.
#[test]
fn status_patch_stalled_condition_survives_a_fresh_process_seeing_the_cr_for_the_first_time() {
    let map_version = 1u64;
    let mut lumen = cutover_pending_convergence_lumen(
        "search",
        "acme-convergence-restart-durable",
        map_version,
    );
    let stall_budget = crate::operator::application::reshard_driver::convergence_stall::convergence_stall_budget_secs();
    lumen
        .spec
        .reshard_policy
        .workflow
        .convergence_wait_started_at = Some(test_now_epoch_secs().saturating_sub(stall_budget + 1));
    // #1485 R1: also prove a completed remediation restart's own
    // bookkeeping round-trips through status untouched by process
    // identity — status_patch never resets it.
    lumen
        .spec
        .reshard_policy
        .workflow
        .convergence_remediation_restart_count = 1;
    lumen
        .spec
        .reshard_policy
        .workflow
        .convergence_remediation_restarted_at = Some(test_now_epoch_secs());

    // No driver-side cache is ever populated in this test process for
    // this namespace/name — `status_patch` (called by whichever operator
    // replica happens to reconcile this CR next) must still report the
    // stall purely from `lumen.spec` above.
    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"]
        .as_array()
        .expect("blockingConditions must be present");
    assert!(
        blocking
            .iter()
            .any(|c| c.as_str() == Some("topologyConvergenceStalled")),
        "stalled condition must be derived purely from persisted spec state, got: {reshard}"
    );
    assert_eq!(
        reshard["convergenceRemediationRestartCount"].as_u64(),
        Some(1),
        "convergenceRemediationRestartCount must be surfaced in status.reshard, got: {reshard}"
    );
    assert!(
        reshard["convergenceRemediationRestartedAt"].is_number(),
        "convergenceRemediationRestartedAt must be surfaced in status.reshard, got: {reshard}"
    );
}

#[test]
fn status_patch_never_reports_awaiting_convergence_for_a_manually_authored_map_version() {
    // #1467 R7: a shardMap.version the driver never itself cut over to
    // (lastCutoverShardMapVersion absent/stale) must not report
    // awaitingTopologyConvergence at all — status_patch uses the same
    // gate advance_convergence does, so a manually-edited map version
    // never wedges status forever waiting on a fence the driver never
    // armed.
    let mut lumen = hpa_test_lumen("search", "acme-convergence-manual", 2, 1);
    lumen.spec.shard_map.version = 5;
    // last_cutover_shard_map_version left at its default (None).
    let ready = ReadyFacts {
        ready: std::collections::HashMap::new(),
    };
    let patch = lumen.status_patch(&ready);
    let reshard = &patch["status"]["reshard"];
    let blocking = reshard["blockingConditions"].as_array();
    let has_condition = blocking
        .map(|arr| {
            arr.iter()
                .any(|c| c.as_str() == Some("awaitingTopologyConvergence"))
        })
        .unwrap_or(false);
    assert!(
        !has_condition,
        "a shardMap.version with no matching lastCutoverShardMapVersion must not report \
             awaitingTopologyConvergence, got: {reshard}"
    );
}
