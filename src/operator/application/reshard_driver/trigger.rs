//! Whether to start a split this tick, and the shard map it grows into.

use anyhow::Result;

use crate::operator::domain::lumen_spec::topology::ReshardPhase;
use crate::operator::domain::lumen_spec::Lumen;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

/// Pure trigger gate (R3 safety rail; AC4): whether `lumen` should start a
/// **new** split this tick. `false` whenever `maxShardBytes` is unset —
/// recommendation-only mode never auto-splits, regardless of any other
/// field, including a stale/manually-forced `status.reshard`.
pub fn should_start_split(lumen: &Lumen) -> bool {
    if lumen.spec.reshard_policy.max_shard_bytes.is_none() {
        return false;
    }
    if lumen.spec.replicas_per_shard > 1 {
        // Raft-HA: growing shardCount reshuffles ordinal->shard for every
        // existing pod, not just an added one. See the module doc's "Scope
        // rail" note.
        return false;
    }
    if !matches!(
        lumen.spec.reshard_policy.workflow.phase,
        ReshardPhase::Complete
    ) {
        // Already mid-workflow: PrepareSplit/Splitting/CatchingUp resume via
        // drive_tick's other branches, never restart from should_start_split.
        return false;
    }
    if let Some(max) = lumen.spec.reshard_policy.max_shards {
        if lumen.spec.shard_count >= max {
            return false;
        }
    }
    let Some(status) = lumen.status.as_ref() else {
        return false;
    };
    // #1396 R5: re-derive freshness here rather than trusting the status
    // subresource's own `blockingConditions` alone. `reshard_status_with_usage`
    // (crd.rs) already refuses to report a *threshold* condition against a
    // stale usage measurement (it reports `usageStalePostCutover` instead —
    // see the #1386 tests below), but that only protects the write path: a
    // status write from an in-flight scrape can still be the one currently
    // stored when a *later* `spec.shardMap` cutover lands (a second,
    // independent split racing this one, or an operator restart reordering
    // writes), leaving a `prepareThresholdCrossed`/`urgentThresholdCrossed`
    // condition on disk that was computed against a map version the CR has
    // already moved past. Requiring the status's `usageMeasuredAtMapVersion`
    // to equal the CR's *current* `spec.shardMap.version` at the moment this
    // trigger decision is made closes that race without needing the status
    // writer and this reader to be perfectly ordered.
    if status.reshard.usage_measured_at_map_version != Some(lumen.spec.shard_map.version) {
        return false;
    }
    status
        .reshard
        .blocking_conditions
        .iter()
        .any(|c| c == "prepareThresholdCrossed" || c == "urgentThresholdCrossed")
}

/// The virtual-bucket map `lumen.spec.shardMap` currently describes — the
/// map still live for routing/data placement right now, as opposed to
/// `spec.shardCount`'s StatefulSet-sizing intent.
///
/// While a split is in flight (`workflow.targetShardCount` is set),
/// `start_split` has already bumped `spec.shardCount` to the target so the
/// new StatefulSet replica can come up, but the actual live topology —
/// what `bucket_moves`/`snapshot_reshard_batches` must diff against, and
/// what eviction must iterate — is still the pre-split shard count until
/// the `Complete`-phase cutover commits `shardMap`. This driver only ever
/// grows a map by exactly one shard per split (R1), so the pre-split count
/// is always `targetShardCount - 1`.
pub fn current_shard_map(lumen: &Lumen) -> Result<VirtualBucketShardMap> {
    let sm = &lumen.spec.shard_map;
    let physical = match lumen.spec.reshard_policy.workflow.target_shard_count {
        Some(target) => target.saturating_sub(1).max(1),
        None => lumen.spec.shard_count.max(1),
    };
    if sm.assignments.is_empty() {
        VirtualBucketShardMap::balanced(sm.version, sm.virtual_bucket_count, physical)
    } else {
        VirtualBucketShardMap::new(sm.version, sm.assignments.clone(), physical)
    }
}

/// The target map for growing `current` by exactly one shard (R1).
pub fn compute_target_map(current: &VirtualBucketShardMap) -> Result<VirtualBucketShardMap> {
    current.split_one_shard(current.version() + 1)
}
