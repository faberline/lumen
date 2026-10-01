//! After the cutover: wait for the serving topology to converge on the new map.

use std::collections::BTreeSet;

use kube::ResourceExt;
use serde_json::json;

use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::convergence_stall::{
    clear_convergence_stall, convergence_stall_condition, now_epoch_secs, record_convergence_await,
};
use crate::operator::application::reshard_driver::fence::{
    buckets_on_newest_shard, set_write_fence,
};
use crate::operator::application::reshard_driver::trigger::current_shard_map;
use crate::operator::application::reshard_driver::DriveOutcome;
use crate::operator::domain::lumen_spec::Lumen;

/// #1458 R1: `Complete`-phase convergence step, checked ahead of
/// [`should_start_split`] so a CR is never allowed to start a *new* split
/// while a prior one's serving pods have not all confirmed `Ready` on the
/// map that prior split cutover to. Keeps the write-pause fence armed over
/// [`buckets_on_newest_shard`] — the buckets that moved into the current
/// `spec.shardMap` — re-arming it every tick this returns `Some(
/// AwaitingTopologyConvergence)`, until [`ClusterControl::
/// serving_topology_converged`] confirms every serving pod is `Ready` on
/// the new topology, at which point the fence is cleared and
/// `workflow.convergedShardMapVersion` is patched to the converged version.
///
/// "Converging" is derived purely from persisted state (`spec.shardMap.
/// version` compared against `workflow.convergedShardMapVersion`), not
/// driver memory, so this resumes correctly across a driver restart:
/// [`buckets_on_newest_shard`] recomputes the exact same bucket set from
/// `spec.shardMap` alone (see that function's doc for the invariant this
/// relies on), and [`ClusterControl::serving_topology_converged`] reuses
/// the same StatefulSet readiness plumbing [`advance_prepare_split`]
/// already polls rather than adding a new seam.
///
/// This replaces #1442 R2's "leave the fence armed once for a fixed
/// [`WRITE_FENCE_TTL_SECS`]" behavior: a slow rolling restart across many
/// pods could outlive that single fixed TTL, silently reopening the
/// mixed-map write-loss window the fence exists to close.
///
/// Returns `None` when convergence is not pending — either `shard_map.
/// version == 0` (a CR that has never resharded; no cutover has ever run
/// to converge from), the current version is already recorded converged, or
/// (#1467 R7) `workflow.lastCutoverShardMapVersion` does not equal the
/// current `shard_map.version` — letting the caller fall through to
/// [`should_start_split`].
///
/// #1467 R7: the `lastCutoverShardMapVersion` check is what keeps this
/// function from ever engaging the write-pause fence for a CR whose
/// `spec.shardMap` was hand-authored (or restored from a backup/migration)
/// rather than reached via a cutover this driver actually ran —
/// `advance_catching_up_fenced`'s cutover patch is the ONLY writer of
/// `lastCutoverShardMapVersion`, and it always sets it to the exact
/// `target.version()` it patches into `shardMap.version` in the same call,
/// so the two fields are equal immediately after every real cutover. A
/// manually-set `shardMap.version` therefore leaves
/// `lastCutoverShardMapVersion` unequal (usually `None`) forever, and this
/// function never engages for it — closing the gap where convergence would
/// otherwise fence indefinitely over a topology the driver never actually
/// changed.
///
/// [`should_start_split`]: crate::operator::application::reshard_driver::trigger::should_start_split
/// [`advance_prepare_split`]: crate::operator::application::reshard_driver::phases::advance_prepare_split
/// [`WRITE_FENCE_TTL_SECS`]: crate::operator::application::reshard_driver::WRITE_FENCE_TTL_SECS
pub(super) async fn advance_convergence(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
) -> Option<DriveOutcome> {
    let map_version = lumen.spec.shard_map.version;
    let workflow = &lumen.spec.reshard_policy.workflow;
    if map_version == 0
        || workflow.converged_shard_map_version == Some(map_version)
        || workflow.last_cutover_shard_map_version != Some(map_version)
    {
        clear_convergence_stall(namespace, name);
        return None;
    }

    let current = match current_shard_map(lumen) {
        Ok(m) => m,
        Err(err) => return Some(DriveOutcome::Blocked(err.to_string())),
    };
    let moving_buckets = buckets_on_newest_shard(&current);
    let desired_replicas = lumen.spec.storage_pod_count() as i64;

    let rollout_converged = match control
        .serving_topology_converged(namespace, name, desired_replicas)
        .await
    {
        Ok(converged) => converged,
        Err(err) => return Some(DriveOutcome::Blocked(err.to_string())),
    };

    // #1467 R5: StatefulSet rollout completion alone doesn't prove every
    // serving pod actually holds the new shard map — it only proves the
    // pod template/generation converged. Require every pod to also report
    // the new map version on its own `/metrics` before treating topology
    // as converged. Gated behind `rollout_converged` so we don't scrape
    // every shard on every tick while a rollout is still in flight.
    let converged = if rollout_converged {
        match control
            .serving_pods_report_map_version(
                http,
                namespace,
                name,
                current.physical_shard_count(),
                map_version,
            )
            .await
        {
            Ok(reported) => reported,
            Err(err) => return Some(DriveOutcome::Blocked(err.to_string())),
        }
    } else {
        false
    };

    if converged {
        // #1467 R7: convergence resolved — clear the stall tracker so a
        // future, unrelated wait (a later split's own convergence) starts
        // from a fresh budget instead of inheriting this one's tick count.
        //
        // #1467 R5: once every serving pod has been observed reporting
        // `map_version` on `/metrics`, the fence is cleared below. A
        // *subsequent* rollout that only changes the pod template (image,
        // resources, env — not the shard map) is safe by construction:
        // every pod already holds `map_version` before that rollout
        // starts, so no re-arm or re-verification is needed for it. Only a
        // *new* cutover (which bumps `shardMap.version` again and re-stamps
        // `lastCutoverShardMapVersion`) re-engages this convergence gate.
        clear_convergence_stall(namespace, name);
        if !moving_buckets.is_empty() {
            if let Err(err) = set_write_fence(
                control,
                http,
                namespace,
                name,
                lumen,
                &current,
                &BTreeSet::new(),
                0,
            )
            .await
            {
                tracing::warn!(
                    error = %err,
                    "reshard driver: failed to clear write fence after topology convergence; \
                     bounded by WRITE_FENCE_TTL_SECS"
                );
            }
        }
        let patch = json!({
            "spec": {
                "reshardPolicy": {
                    "workflow": {
                        "convergedShardMapVersion": map_version,
                        // #1485 R1/R2: episode resolved — clear the durable
                        // wait-start/remediation bookkeeping in the SAME
                        // patch so a future, unrelated wait (a later
                        // split's own convergence) starts from a fresh
                        // budget and a fresh one-shot remediation slot,
                        // instead of inheriting this episode's state.
                        "convergenceWaitStartedAt": null,
                        "convergenceRemediationRestartCount": 0,
                        "convergenceRemediationRestartedAt": null,
                    }
                }
            }
        });
        if let Err(err) = control.patch_spec(namespace, name, patch).await {
            return Some(DriveOutcome::Blocked(err.to_string()));
        }
        return Some(DriveOutcome::TopologyConverged { map_version });
    }

    // #1467 R7: bounded escalation — bump this map_version's
    // consecutive-awaiting-ticks counter. This in-process cache stays as a
    // fast-path/logging-only signal (#1485 R2); it is no longer what decides
    // whether the budget is exceeded (see below).
    record_convergence_await(
        namespace,
        name,
        &lumen.uid().unwrap_or_default(),
        map_version,
    );

    // #1485 R2: the durable wait-start checkpoint. Stamped once, on the
    // first tick this map_version is observed unconverged — every later
    // tick (including after an operator restart, when the in-process cache
    // above is empty again) reads the SAME persisted value back off `lumen`,
    // so the elapsed-time budget below is computed identically regardless of
    // driver process lifetime.
    let now = now_epoch_secs();
    let wait_started_at = workflow.convergence_wait_started_at;
    if wait_started_at.is_none() {
        let patch = json!({
            "spec": {
                "reshardPolicy": {
                    "workflow": {
                        "convergenceWaitStartedAt": now,
                    }
                }
            }
        });
        if let Err(err) = control.patch_spec(namespace, name, patch).await {
            return Some(DriveOutcome::Blocked(format!(
                "persist convergence-wait start: {err}"
            )));
        }
    }
    // `wait_started_at.or(Some(now))`: on this very first tick the patch
    // above just persisted `now`, but `lumen` itself (this tick's snapshot)
    // still predates it — treat this tick as freshly started (elapsed 0),
    // exactly like the pre-#1485 tick-count budget did.
    let stalled = convergence_stall_condition(wait_started_at.or(Some(now)));
    if stalled {
        tracing::warn!(
            namespace,
            name,
            map_version,
            "reshard driver: topology convergence has not been confirmed after \
             CONVERGENCE_STALL_SECS; fence stays armed, raising topologyConvergenceStalled"
        );
    }

    // #1485 R1: bounded remediation restart. The ConfigMap-race signature is
    // exactly what this branch already establishes above: the StatefulSet
    // rollout itself is done (`rollout_converged`) but at least one pod is
    // still reporting the old shard-map version (`!converged`, this
    // function's outer `if converged` already returned). Bounded to exactly
    // one re-trigger per episode via `convergenceRemediationRestartCount`
    // (persisted, so a driver restart never re-triggers a second time for
    // the same episode) — the fence stays armed and `stalled` stays raised
    // either way; this only attempts a self-heal, it never changes whether
    // the wait keeps being reported.
    if stalled && rollout_converged && workflow.convergence_remediation_restart_count == 0 {
        tracing::warn!(
            namespace,
            name,
            map_version,
            "reshard driver: convergence stalled on a version mismatch (rollout complete, pod(s) \
             still on the old shard-map version); durably claiming one bounded remediation \
             rolling restart"
        );
        // Persist the one-shot claim BEFORE the external StatefulSet patch.
        // These two systems have no shared transaction: triggering first and
        // then failing this CR patch would leave the next tick seeing a zero
        // count and issuing a duplicate restart. A failed pre-trigger patch
        // instead leaves no side effect and is safely retried on the next
        // tick; after the claim commits, even a failing restart API call is
        // deliberately a single bounded attempt for this episode.
        let patch = json!({
            "spec": {
                "reshardPolicy": {
                    "workflow": {
                        "convergenceRemediationRestartCount": 1,
                        "convergenceRemediationRestartedAt": now,
                    }
                }
            }
        });
        if let Err(err) = control.patch_spec(namespace, name, patch).await {
            return Some(DriveOutcome::Blocked(format!(
                "persist convergence remediation restart claim: {err}"
            )));
        }
        if let Err(err) = control
            .trigger_convergence_remediation_restart(namespace, name)
            .await
        {
            // Non-fatal, matching the cutover-tick trigger's own handling —
            // the durable claim above makes this a single bounded attempt
            // even if Kubernetes rejects it. The fence and stalled condition
            // remain in place for an operator to remediate a repeated failure.
            tracing::warn!(error = %err, "reshard driver: convergence remediation rolling-restart trigger failed");
        }
    }

    if !moving_buckets.is_empty() {
        if let Err(err) = set_write_fence(
            control,
            http,
            namespace,
            name,
            lumen,
            &current,
            &moving_buckets,
            control.write_fence_ttl_secs(),
        )
        .await
        {
            return Some(DriveOutcome::Blocked(format!(
                "re-arm write fence while awaiting topology convergence: {err}"
            )));
        }
    }
    Some(DriveOutcome::AwaitingTopologyConvergence { map_version })
}
