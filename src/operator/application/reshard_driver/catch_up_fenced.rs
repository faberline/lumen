//! The fenced final catch-up pass that ends in the shard-map cutover.

use std::collections::BTreeSet;
use std::time::Instant;

use serde_json::json;

use crate::operator::application::reshard_driver::checkpoint::checkpoint_shards;
use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::fence::{map_assignments, set_write_fence};
use crate::operator::application::reshard_driver::migration::{
    evict_old_shards, run_migration_pass_impl,
};
use crate::operator::application::reshard_driver::DriveOutcome;
use crate::operator::domain::lumen_spec::Lumen;
use crate::routing::VirtualBucketShardMap;

/// The migrate/checkpoint/evict/checkpoint/cutover sequence proper, run
/// under [`advance_catching_up`]'s write fence. Split out so the fence's
/// arm/clear bracket is unconditional (always runs, regardless of which
/// step below fails) without duplicating the sequence itself.
///
/// #1443 R1: this sequence's migration pass is `final_pass` (R2, reworked
/// #1457 R1 into a separate `POST /admin/reshard:prune` step) and runs
/// under time-based in-loop re-arming (#1458 R3); the fence is additionally
/// re-armed with a fresh TTL at every phase boundary below (after the
/// migration pass and before the target checkpoint, again before eviction,
/// and again before the sources' checkpoint round) so a real pass —
/// hundreds of sequential batch/checkpoint HTTP round trips — can never
/// silently outlive [`ClusterControl::write_fence_ttl_secs`] mid-sequence.
/// [`checkpoint_shards`] itself also re-arms on the same TTL/4 clock between
/// individual shard checkpoints (#1458 R3), so a slow multi-shard
/// checkpoint round is covered too, not just the boundary before it. Any
/// re-arm failure aborts to [`DriveOutcome::Blocked`] immediately, always
/// strictly before [`evict_old_shards`] runs.
///
/// [`advance_catching_up`]: crate::operator::application::reshard_driver::phases::advance_catching_up
pub(super) async fn advance_catching_up_fenced(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
    current: &VirtualBucketShardMap,
    target: &VirtualBucketShardMap,
    moving_buckets: &BTreeSet<u32>,
) -> DriveOutcome {
    if let Err(err) = run_migration_pass_impl(
        control,
        http,
        namespace,
        name,
        lumen,
        true,
        Some(moving_buckets),
    )
    .await
    {
        return DriveOutcome::Blocked(err.to_string());
    }

    // #1458 R3: re-armed (unconditionally) at every phase boundary below;
    // reset alongside each one so `checkpoint_shards`' own in-loop
    // time-based re-arm measures elapsed time from the boundary that
    // actually just armed the fence, not from this sequence's start.
    let mut last_armed_at = Instant::now();
    let fence_buckets = (!moving_buckets.is_empty()).then_some(moving_buckets);

    if !moving_buckets.is_empty() {
        if let Err(err) = set_write_fence(
            control,
            http,
            namespace,
            name,
            lumen,
            current,
            moving_buckets,
            control.write_fence_ttl_secs(),
        )
        .await
        {
            return DriveOutcome::Blocked(format!(
                "re-arm write fence before target checkpoint: {err}"
            ));
        }
        last_armed_at = Instant::now();
    }

    // R1: the target/new shard's copy of the just-migrated data must be
    // durable BEFORE any source eviction is even attempted.
    let new_shard = target.physical_shard_count().saturating_sub(1);
    if let Err(err) = checkpoint_shards(
        control,
        http,
        namespace,
        name,
        lumen,
        std::iter::once(new_shard),
        current,
        fence_buckets,
        &mut last_armed_at,
    )
    .await
    {
        return DriveOutcome::Blocked(err.to_string());
    }

    if !moving_buckets.is_empty() {
        if let Err(err) = set_write_fence(
            control,
            http,
            namespace,
            name,
            lumen,
            current,
            moving_buckets,
            control.write_fence_ttl_secs(),
        )
        .await
        {
            return DriveOutcome::Blocked(format!("re-arm write fence before eviction: {err}"));
        }
        last_armed_at = Instant::now();
    }

    if let Err(err) = evict_old_shards(
        control,
        http,
        namespace,
        name,
        lumen,
        current,
        target,
        fence_buckets,
        &mut last_armed_at,
    )
    .await
    {
        return DriveOutcome::Blocked(err.to_string());
    }

    if !moving_buckets.is_empty() {
        if let Err(err) = set_write_fence(
            control,
            http,
            namespace,
            name,
            lumen,
            current,
            moving_buckets,
            control.write_fence_ttl_secs(),
        )
        .await
        {
            return DriveOutcome::Blocked(format!(
                "re-arm write fence before sources' checkpoint round: {err}"
            ));
        }
        last_armed_at = Instant::now();
    }

    // R1: sources' eviction must itself be durable before cutover, same
    // rationale #1389 already established — a crash-then-restart must never
    // resurrect data this shard no longer owns.
    if let Err(err) = checkpoint_shards(
        control,
        http,
        namespace,
        name,
        lumen,
        0..current.physical_shard_count(),
        current,
        fence_buckets,
        &mut last_armed_at,
    )
    .await
    {
        return DriveOutcome::Blocked(err.to_string());
    }

    let patch = json!({
        "spec": {
            "shardMap": {
                "version": target.version(),
                "virtualBucketCount": target.virtual_bucket_count(),
                "assignments": map_assignments(target),
            },
            "reshardPolicy": {
                "workflow": {
                    "phase": "Complete",
                    "targetShardCount": null,
                    // #1467 R7: stamped in the SAME patch as `shardMap.
                    // version` — proof this cutover, and not a hand-authored
                    // or restored `shardMap`, is what produced this map
                    // version, gating `advance_convergence`'s engagement.
                    "lastCutoverShardMapVersion": target.version(),
                }
            }
        }
    });
    if let Err(err) = control.patch_spec(namespace, name, patch).await {
        return DriveOutcome::Blocked(err.to_string());
    }
    if let Err(err) = control.trigger_rolling_restart(namespace, name).await {
        // Non-fatal: the map has already flipped; a failed restart trigger
        // only delays picking up the new ConfigMap once consumption exists
        // (see the module doc's "known gap"), it does not corrupt data.
        tracing::warn!(error = %err, "reshard driver: cutover rolling-restart trigger failed");
    }
    DriveOutcome::CompletedSplit {
        new_map_version: target.version(),
    }
}
