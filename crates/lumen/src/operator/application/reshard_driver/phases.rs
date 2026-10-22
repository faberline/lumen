//! The workflow phases up to catch-up: start a split, prepare it, split, and
//! catch up.

use std::collections::BTreeSet;

use kube::ResourceExt;
use serde_json::json;

use crate::operator::application::reshard_driver::catch_up_fenced::advance_catching_up_fenced;
use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::fence::set_write_fence;
use crate::operator::application::reshard_driver::migration::run_migration_pass;
use crate::operator::application::reshard_driver::oversize::should_skip_for_oversize;
use crate::operator::application::reshard_driver::trigger::{
    compute_target_map, current_shard_map,
};
use crate::operator::application::reshard_driver::{DriveOutcome, WRITE_FENCE_TTL_SECS};
use crate::operator::domain::lumen_spec::Lumen;
use crate::sharding::domain::bucket_move::bucket_moves;

pub(super) async fn start_split(
    control: &dyn ClusterControl,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
) -> DriveOutcome {
    let current = match current_shard_map(lumen) {
        Ok(m) => m,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    let target = match compute_target_map(&current) {
        Ok(m) => m,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    let target_shard_count = target.physical_shard_count();
    let patch = json!({
        "spec": {
            "shardCount": target_shard_count,
            "reshardPolicy": {
                "workflow": {
                    "phase": "PrepareSplit",
                    "targetShardCount": target_shard_count,
                }
            }
        }
    });
    match control.patch_spec(namespace, name, patch).await {
        Ok(()) => DriveOutcome::StartedSplit { target_shard_count },
        Err(err) => DriveOutcome::Blocked(err.to_string()),
    }
}

pub(super) async fn advance_prepare_split(
    control: &dyn ClusterControl,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
) -> DriveOutcome {
    let Some(target_shard_count) = lumen.spec.reshard_policy.workflow.target_shard_count else {
        return DriveOutcome::Blocked("PrepareSplit with no targetShardCount set".to_string());
    };
    let ready = match control.statefulset_ready_replicas(namespace, name).await {
        Ok(r) => r,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    if ready < i64::from(target_shard_count) {
        return DriveOutcome::WaitingForNewShard { target_shard_count };
    }
    let patch = json!({
        "spec": { "reshardPolicy": { "workflow": { "phase": "Splitting" } } }
    });
    match control.patch_spec(namespace, name, patch).await {
        Ok(()) => DriveOutcome::AdvancedToSplitting,
        Err(err) => DriveOutcome::Blocked(err.to_string()),
    }
}

pub(super) async fn advance_splitting(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
) -> DriveOutcome {
    let batches = match run_migration_pass(control, http, namespace, name, lumen).await {
        Ok(n) => n,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    let patch = json!({
        "spec": { "reshardPolicy": { "workflow": { "phase": "CatchingUp" } } }
    });
    match control.patch_spec(namespace, name, patch).await {
        Ok(()) => {
            if batches == 0 {
                // Nothing moved on this pass (already caught up from a prior
                // attempt); still safe to advance.
                DriveOutcome::AdvancedToCatchingUp
            } else {
                DriveOutcome::MigratedBatches { batches }
            }
        }
        Err(err) => DriveOutcome::Blocked(err.to_string()),
    }
}

/// #1396 R1/R2: durably ordered cutover. Old order was migrate -> evict ->
/// checkpoint-everything -> cutover, which could make a source shard's
/// eviction durable (or even attempt it) before the target shard's copy of
/// the same data was durably checkpointed — a crash between eviction and
/// that too-late checkpoint could lose data that, at that instant, existed
/// on no durable shard at all (#1387's exact failure shape, reintroduced by
/// evicting ahead of a per-shard-ordered checkpoint). New order: migrate ->
/// checkpoint target only -> evict sources -> checkpoint sources -> cutover.
/// Crash-safety at every boundary (moved data is durable on at least one
/// shard at every point once migration completes):
/// - Crash after migrate, before the target checkpoint: sources still hold
///   their data (not yet evicted); retry re-migrates (idempotent, #1380)
///   and the target checkpoint eventually succeeds.
/// - Crash after the target checkpoint, before eviction: the target already
///   durably holds the moved data; retry replays migrate (no-op) and the
///   target checkpoint (no-op re-confirm), then proceeds to evict.
/// - Crash after eviction (RAM-only until its own checkpoint), before the
///   source checkpoint: even if the pod restart the driver is about to
///   trigger loses the in-RAM eviction, a retry is still safe — the target
///   already durably has the moved data from the earlier target checkpoint,
///   so a retried migrate is a no-op, a retried evict is idempotent, and the
///   source checkpoint retries until it succeeds. Eviction is never durable
///   nor attempted before the target's copy is durable, so this crash can
///   never lose data.
/// - Crash after the source checkpoint, before the cutover patch: both
///   sides are durable; retry replays every step as a no-op until the
///   cutover patch finally lands.
///
/// R2: the whole sequence below runs under a write-pause fence (`POST
/// /admin/reshard:fence`) armed over every still-moving bucket on its
/// current (source) owners, so this tick's migration pass is guaranteed a
/// converged snapshot of those buckets — closing the gap where a write
/// lands on a source shard after the last migration-copy read but before
/// that bucket's eviction and is silently dropped. See
/// [`crate::app::http::write_fence::WriteFence`] for why a crashed driver can never leave it
/// armed permanently.
///
/// The fence is cleared immediately on every exit path *except*
/// [`DriveOutcome::CompletedSplit`] (#1442 R2): a completed split just
/// called [`ClusterControl::trigger_rolling_restart`], and pods only read
/// `SHARD_MAP_*`/`SHARD_COUNT` env at boot, so old-map pods keep serving
/// (and, without this, keep accepting local writes for) the just-evicted
/// source buckets until the rolling restart actually reaches them —
/// clearing the fence right after triggering the restart would open exactly
/// that mixed-map window back up. Leaving it armed here lets
/// [`WRITE_FENCE_TTL_SECS`] bound the window instead (the design's simpler,
/// non-blocking alternative to synchronously polling every serving pod
/// Ready on the new topology from inside one CR's tick, which would stall
/// `drive_tick`'s other CRs); the next split for this CR can only start once
/// `drive_tick` sees the phase back at `Complete`, well after the TTL, so
/// there is no risk of a subsequent tick trying to arm a fence that is
/// already armed from a prior split.
pub(super) async fn advance_catching_up(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
) -> DriveOutcome {
    let current = match current_shard_map(lumen) {
        Ok(m) => m,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    let target = match compute_target_map(&current) {
        Ok(m) => m,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    let moves = match bucket_moves(&current, &target) {
        Ok(m) => m,
        Err(err) => return DriveOutcome::Blocked(err.to_string()),
    };
    let moving_buckets: BTreeSet<u32> = moves.iter().map(|m| m.bucket).collect();

    // #1444 R2: a tick already known-wedged on an oversized single-document
    // batch is a permanent no-progress condition until the document shrinks
    // (see [`OversizedDocumentBlock`]) — arming the write fence anyway would
    // pause writes to these buckets for a pass that cannot possibly finish,
    // recurring every tick's `WRITE_FENCE_TTL_SECS` window for no benefit.
    // `should_skip_for_oversize` still periodically lets a real attempt
    // through (`OVERSIZE_RECHECK_TICKS`) so a fixed document self-heals.
    if let Some(block) = should_skip_for_oversize(namespace, name, &lumen.uid().unwrap_or_default())
    {
        return DriveOutcome::Blocked(block.to_string());
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
            return DriveOutcome::Blocked(err.to_string());
        }
    }

    let outcome = advance_catching_up_fenced(
        control,
        http,
        namespace,
        name,
        lumen,
        &current,
        &target,
        &moving_buckets,
    )
    .await;

    let completed_split = matches!(outcome, DriveOutcome::CompletedSplit { .. });
    if !moving_buckets.is_empty() && !completed_split {
        // Clear on every exit path except a completed split (#1442 R2, see
        // this fn's doc comment): the fence must not outlive this tick
        // *unless* the cutover it guarded just triggered a rolling restart,
        // in which case leaving it armed and TTL-bounded closes the
        // mixed-map window instead of reopening it. If this clear itself
        // fails (or the process dies before reaching it), WRITE_FENCE_TTL_SECS
        // still bounds how long writes to these buckets stay paused — the
        // serving pod enforces that deadline on its own, independent of the
        // driver ever coming back.
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
                "reshard driver: failed to clear write fence after CatchingUp tick; \
                 bounded by WRITE_FENCE_TTL_SECS"
            );
        }
    } else if !moving_buckets.is_empty() && completed_split {
        tracing::info!(
            "reshard driver: split completed and rolling restart triggered; leaving write fence \
             armed for WRITE_FENCE_TTL_SECS={WRITE_FENCE_TTL_SECS}s to close the old-map pods' \
             mixed-map window instead of clearing it immediately (#1442 R2)"
        );
    }

    outcome
}
