//! One migration pass over every moved bucket, and the post-cutover eviction on
//! old shards.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use anyhow::{bail, Context, Result};
use kube::ResourceExt;
use service_auth::k8s::ProjectedToken;

use crate::operator::application::reshard_driver::checkpoint::evict_shard;
use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::fence::{map_assignments, maybe_rearm_fence};
use crate::operator::application::reshard_driver::oversize::{
    clear_oversize_block, record_oversize_block, OversizedDocumentBlock,
};
use crate::operator::application::reshard_driver::transfer::{
    apply_reshard_batch, apply_reshard_prune_chunk, fetch_all_collection_ids, fetch_scoped_backup,
};
use crate::operator::application::reshard_driver::trigger::{
    compute_target_map, current_shard_map,
};
use crate::operator::application::reshard_driver::MAX_EXTERNAL_IDS_PER_BATCH;
use crate::operator::domain::lumen_spec::Lumen;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::sharding::domain::{
    bucket_move::bucket_moves, prune_chunk::snapshot_reshard_prune_chunks,
    reshard_batch::snapshot_reshard_batches,
};

/// One migration pass: every bucket [`bucket_moves`] says moved between
/// `current_shard_map` and [`compute_target_map`], grouped by its old
/// (`from_shard`) owner, fetched via `POST /admin/backup:scoped` and applied
/// to its new owner via `POST /admin/reshard:apply`
/// ([`snapshot_reshard_batches`] builds the bounded batches). Real,
/// non-test caller of both — AC3. Idempotent: re-running against unchanged
/// data re-applies the same batches, which `POST /admin/reshard:apply`
/// already treats as a no-op (#1380).
pub async fn run_migration_pass(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
) -> Result<usize> {
    run_migration_pass_impl(control, http, namespace, name, lumen, false, None).await
}

/// Shared migration-pass implementation. `final_pass` is `true` only for the
/// final, fenced `CatchingUp` pass: every `snapshot_reshard_batches` apply
/// below stays purely additive regardless (#1457 R1), but a `final_pass`
/// additionally sends the authoritative-replace scope for every moved bucket
/// via `POST /admin/reshard:prune` — see [`snapshot_reshard_prune_chunks`].
/// `moving_buckets` (#1443 R1), when `Some`, marks this pass as running
/// under a write fence and re-arms it with a fresh
/// [`ClusterControl::write_fence_ttl_secs`] deadline via [`maybe_rearm_fence`]
/// (#1458 R3: time-based, checked around every fetch/apply/prune step, not
/// a fixed applied-batch count) — a re-arm failure aborts the whole pass
/// immediately (propagated as `Err`, which every caller already surfaces as
/// `DriveOutcome::Blocked` before eviction ever runs).
pub(super) async fn run_migration_pass_impl(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
    final_pass: bool,
    moving_buckets: Option<&BTreeSet<u32>>,
) -> Result<usize> {
    let current = current_shard_map(lumen)?;
    let target = compute_target_map(&current)?;
    let moves = bucket_moves(&current, &target)?;
    if moves.is_empty() {
        return Ok(0);
    }

    let mut buckets_by_from_shard: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
    let mut to_shard_by_bucket: BTreeMap<u32, u32> = BTreeMap::new();
    for mv in &moves {
        buckets_by_from_shard
            .entry(mv.from_shard)
            .or_default()
            .insert(mv.bucket);
        to_shard_by_bucket.insert(mv.bucket, mv.to_shard);
    }

    let token = control.admin_token(namespace, lumen).await?;
    let mut total_batches = 0usize;
    // #1458 R3: the fence was armed by the caller immediately before this
    // call, so `Instant::now()` here is that arm's timestamp — the clock
    // [`maybe_rearm_fence`] measures elapsed time against at every
    // fetch/apply/prune step below, independent of which of the two loops
    // is currently running or how many calls each has made.
    let mut last_armed_at = Instant::now();
    for (from_shard, buckets) in buckets_by_from_shard {
        maybe_rearm_fence(
            control,
            http,
            namespace,
            name,
            lumen,
            &current,
            moving_buckets,
            &mut last_armed_at,
        )
        .await?;
        let source_url = control.shard_base_url(namespace, name, from_shard);
        let snapshot = fetch_scoped_backup(
            http,
            &source_url,
            token.as_ref().map(ProjectedToken::expose),
            current.virtual_bucket_count(),
            &buckets,
        )
        .await?;
        let max_batch_bytes = lumen
            .spec
            .body_limit_bytes
            .map(|b| b as usize)
            .unwrap_or(crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES)
            / 2;
        let batches = snapshot_reshard_batches(
            &snapshot,
            &current,
            &target,
            &buckets,
            MAX_EXTERNAL_IDS_PER_BATCH,
            max_batch_bytes,
        )?;
        for batch in &batches {
            let dest_url = control.shard_base_url(namespace, name, batch.to_shard);
            if let Err(err) = apply_reshard_batch(
                http,
                &dest_url,
                token.as_ref().map(ProjectedToken::expose),
                batch,
            )
            .await
            {
                // #1444 R2: record the wedge distinctly before propagating, so
                // callers that turn this `Err` into `DriveOutcome::Blocked`
                // still leave a structured trace behind for `status.reshard`
                // and for `advance_catching_up`'s fence-skip check, even
                // though the `Err` itself stays a generic message.
                if let Some(oversized) = err.downcast_ref::<OversizedDocumentBlock>() {
                    record_oversize_block(
                        namespace,
                        name,
                        &lumen.uid().unwrap_or_default(),
                        oversized.clone(),
                    );
                }
                return Err(err);
            }
            total_batches += 1;
            maybe_rearm_fence(
                control,
                http,
                namespace,
                name,
                lumen,
                &current,
                moving_buckets,
                &mut last_armed_at,
            )
            .await?;
        }

        // #1457 R1/R2: the final pass's authoritative-replace scope, sent as
        // its own independently byte-capped `POST /admin/reshard:prune`
        // chunks rather than stamped onto every `ReshardBatch` above — see
        // `reshard.rs`'s `ReshardBatch`/`ReshardPruneChunk` docs for why. The
        // full collection list is fetched from the source shard directly
        // (#1457 R2) rather than derived from `snapshot`'s own keys, so a
        // collection a batch of deletes emptied out of these buckets still
        // gets an (empty) keep scope instead of being silently skipped.
        if final_pass {
            let collection_ids = fetch_all_collection_ids(
                http,
                &source_url,
                token.as_ref().map(ProjectedToken::expose),
            )
            .await
            .context("fetch source shard's full collection list for the final reshard pass")?;
            maybe_rearm_fence(
                control,
                http,
                namespace,
                name,
                lumen,
                &current,
                moving_buckets,
                &mut last_armed_at,
            )
            .await?;
            let prune_chunks = snapshot_reshard_prune_chunks(
                &snapshot,
                &target,
                &buckets,
                &collection_ids,
                max_batch_bytes,
            )?;
            for chunk in &prune_chunks {
                let Some(&to_shard) = to_shard_by_bucket.get(&chunk.bucket) else {
                    bail!(
                        "prune chunk for bucket {} has no known destination shard",
                        chunk.bucket
                    );
                };
                let dest_url = control.shard_base_url(namespace, name, to_shard);
                apply_reshard_prune_chunk(
                    http,
                    &dest_url,
                    token.as_ref().map(ProjectedToken::expose),
                    chunk,
                )
                .await?;
                maybe_rearm_fence(
                    control,
                    http,
                    namespace,
                    name,
                    lumen,
                    &current,
                    moving_buckets,
                    &mut last_armed_at,
                )
                .await?;
            }
        }
    }
    // A full pass completed without hitting the oversize wedge (whether or
    // not one was ever recorded) — clear any stale block so a fixed document
    // doesn't leave `status.reshard` reporting a condition that no longer
    // applies.
    clear_oversize_block(namespace, name);
    Ok(total_batches)
}

/// Post-cutover eviction (idempotent, #1380) on every **old** shard, using
/// only the already-committed target map — the driver never needs to retain
/// the old map across a restart.
///
/// `moving_buckets`/`last_armed_at` (#1467 R3) thread the same
/// [`maybe_rearm_fence`] time-based re-arm [`checkpoint_shards`] already
/// has into this loop: eviction round-trips one HTTP call per **old**
/// physical shard, and a slow round (many old shards, a slow network) could
/// otherwise outlive the fence TTL mid-eviction with no re-arm to catch it
/// — the caller's unconditional phase-boundary re-arm immediately before
/// this call only covers the moment the loop starts.
///
/// [`checkpoint_shards`]: crate::operator::application::reshard_driver::checkpoint::checkpoint_shards
pub(super) async fn evict_old_shards(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
    current: &VirtualBucketShardMap,
    target: &VirtualBucketShardMap,
    moving_buckets: Option<&BTreeSet<u32>>,
    last_armed_at: &mut Instant,
) -> Result<()> {
    let token = control.admin_token(namespace, lumen).await?;
    let assignments = map_assignments(target);
    for shard in 0..current.physical_shard_count() {
        maybe_rearm_fence(
            control,
            http,
            namespace,
            name,
            lumen,
            current,
            moving_buckets,
            last_armed_at,
        )
        .await?;
        let url = control.shard_base_url(namespace, name, shard);
        evict_shard(
            http,
            &url,
            token.as_ref().map(ProjectedToken::expose),
            shard,
            target.version(),
            &assignments,
            target.physical_shard_count(),
        )
        .await?;
    }
    Ok(())
}
