//! The write-pause fence over still-moving buckets (#1396 R2): arm, clear and
//! re-arm it on the shards that own them.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde_json::json;
use service_auth::k8s::ProjectedToken;

use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::domain::lumen_spec::Lumen;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

/// `POST /admin/reshard:fence` (#1396 R2) against one shard: `buckets`
/// non-empty arms a bounded write pause over those virtual buckets;
/// `buckets` empty clears any currently-armed pause. See
/// [`crate::api::WriteFence`].
async fn reshard_fence_call(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
    virtual_bucket_count: u32,
    buckets: &BTreeSet<u32>,
    ttl_secs: u64,
) -> Result<()> {
    let mut req = http
        .post(format!("{base_url}/admin/reshard:fence"))
        .json(&json!({
            "virtual_bucket_count": virtual_bucket_count,
            "buckets": buckets,
            "ttl_secs": ttl_secs,
        }));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("POST {base_url}/admin/reshard:fence"))?;
    if !resp.status().is_success() {
        bail!("{base_url}/admin/reshard:fence returned {}", resp.status());
    }
    Ok(())
}

/// Arm (non-empty `buckets`) or clear (empty `buckets`) the write-pause
/// fence on every shard `current` owns — the live map's current owners,
/// where writes to a still-moving bucket land until this tick's own cutover
/// patch flips `spec.shardMap`.
///
/// #1443 R4: arming loops over shards sequentially and can fail partway
/// through (one shard unreachable). A failure used to return immediately via
/// `?`, leaving every shard armed *before* the failing one fenced with no
/// caller ever reaching the clear bracket — an indefinite intermittent write
/// outage on those shards (re-armed every tick) even though the migration
/// made zero progress. Now tracks which shards actually armed and, on
/// failure, best-effort clears exactly those before surfacing the original
/// error, so a partial arm never outlives this call.
pub(super) async fn set_write_fence(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
    current: &VirtualBucketShardMap,
    buckets: &BTreeSet<u32>,
    ttl_secs: u64,
) -> Result<()> {
    let token = control.admin_token(namespace, lumen).await?;
    let mut armed_urls: Vec<String> = Vec::new();
    for shard in 0..current.physical_shard_count() {
        let url = control.shard_base_url(namespace, name, shard);
        if let Err(err) = reshard_fence_call(
            http,
            &url,
            token.as_ref().map(ProjectedToken::expose),
            current.virtual_bucket_count(),
            buckets,
            ttl_secs,
        )
        .await
        {
            if !buckets.is_empty() {
                for armed_url in &armed_urls {
                    if let Err(clear_err) = reshard_fence_call(
                        http,
                        armed_url,
                        token.as_ref().map(ProjectedToken::expose),
                        current.virtual_bucket_count(),
                        &BTreeSet::new(),
                        0,
                    )
                    .await
                    {
                        tracing::warn!(
                            shard_url = %armed_url,
                            error = %clear_err,
                            "reshard driver: best-effort fence clear after a partial arm \
                             failure also failed; this shard stays fenced until its own TTL \
                             expires"
                        );
                    }
                }
            }
            return Err(err);
        }
        if !buckets.is_empty() {
            armed_urls.push(url);
        }
    }
    Ok(())
}

pub(super) fn map_assignments(map: &VirtualBucketShardMap) -> Vec<u32> {
    (0..map.virtual_bucket_count())
        .map(|bucket| map.assignment_for_bucket(bucket).unwrap_or(0))
        .collect()
}

/// Every virtual bucket currently assigned to `map`'s highest-index
/// (newest) physical shard (#1458 R1). [`VirtualBucketShardMap::
/// split_one_shard`] only ever moves a bucket directly into the new shard
/// it appends — never between two pre-existing shards — so immediately
/// after a cutover to `map`, this is exactly the set of buckets that just
/// moved, recoverable purely from the already-persisted `spec.shardMap`
/// with no separate bookkeeping. [`advance_convergence`] re-fences this same
/// set every tick until every serving pod is confirmed Ready on `map`.
///
/// [`advance_convergence`]: crate::operator::application::reshard_driver::convergence::advance_convergence
pub(super) fn buckets_on_newest_shard(map: &VirtualBucketShardMap) -> BTreeSet<u32> {
    let newest = map.physical_shard_count().saturating_sub(1);
    (0..map.virtual_bucket_count())
        .filter(|&bucket| map.assignment_for_bucket(bucket) == Some(newest))
        .collect()
}

/// #1458 R3: re-arm the write fence once more than
/// `write_fence_ttl_secs() / FENCE_REARM_FRACTION` has elapsed since the
/// last arm. Replaces the earlier fixed-count re-arm (#1443 R1, every
/// `FENCE_REARM_BATCH_INTERVAL = 20` applied batches/chunks): a count-based
/// clock can still be outrun by a sequence whose batches are individually
/// slow (a large scoped-backup fetch, a slow network, or a handful of huge
/// byte-capped batches) even though few *batches* have been applied — a
/// time-based clock, checked between every batch/chunk apply and around
/// each fetch/prune step, cannot.
const FENCE_REARM_FRACTION: u32 = 4;

/// Re-arm the write-pause fence if more than `write_fence_ttl_secs() /
/// [`FENCE_REARM_FRACTION`]` has elapsed since `*last_armed_at` (#1458 R3).
/// No-op — and leaves `*last_armed_at` untouched — when `moving_buckets` is
/// `None`/empty (this pass/step is not running under a fence) or the
/// fraction has not yet elapsed. A re-arm failure propagates as `Err`,
/// which every caller already surfaces as [`DriveOutcome::Blocked`] before
/// eviction ever runs (R3's "a failed re-arm still aborts to `Blocked`
/// before eviction").
///
/// [`DriveOutcome::Blocked`]: crate::operator::application::reshard_driver::DriveOutcome::Blocked
pub(super) async fn maybe_rearm_fence(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
    current: &VirtualBucketShardMap,
    moving_buckets: Option<&BTreeSet<u32>>,
    last_armed_at: &mut Instant,
) -> Result<()> {
    let Some(buckets) = moving_buckets.filter(|b| !b.is_empty()) else {
        return Ok(());
    };
    let ttl_secs = control.write_fence_ttl_secs();
    let rearm_after = Duration::from_secs(ttl_secs) / FENCE_REARM_FRACTION;
    if last_armed_at.elapsed() < rearm_after {
        return Ok(());
    }
    set_write_fence(
        control, http, namespace, name, lumen, current, buckets, ttl_secs,
    )
    .await
    .context("re-arm write fence mid migration pass")?;
    *last_armed_at = Instant::now();
    Ok(())
}
