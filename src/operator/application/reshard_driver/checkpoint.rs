//! Durability after migration (#1389, #1396 R3): checkpoint the shards a pass
//! wrote to, and evict moved buckets from an old owner.

use std::collections::BTreeSet;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use serde_json::json;
use service_auth::k8s::ProjectedToken;

use crate::operator::application::reshard_driver::cluster_control::ClusterControl;
use crate::operator::application::reshard_driver::fence::maybe_rearm_fence;
use crate::operator::domain::lumen_spec::Lumen;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

pub(super) async fn evict_shard(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
    shard: u32,
    map_version: u64,
    assignments: &[u32],
    physical_shard_count: u32,
) -> Result<()> {
    let mut req = http
        .post(format!("{base_url}/admin/reshard:evict"))
        .json(&json!({
            "shard": shard,
            "map_version": map_version,
            "assignments": assignments,
            "physical_shard_count": physical_shard_count,
        }));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("POST {base_url}/admin/reshard:evict"))?;
    if !resp.status().is_success() {
        bail!("{base_url}/admin/reshard:evict returned {}", resp.status());
    }
    Ok(())
}

/// `POST /admin/checkpoint` (#1389 R1/R2; durability gate hardened by #1396
/// R3) against one shard: force its migration mutations (`:apply`/`:evict`,
/// which bypass `WriteCoordinator`/the AOF) into the same durability domain
/// ordinary writes reach, and wait for the response before this shard is
/// considered safe to restart.
///
/// A 200 response alone is not proof of durability: [`checkpoint`]'s
/// `admin_checkpoint` handler returns `200 {"persisted": false}` — not an
/// error status — when the shard has no durable store configured (the
/// vacuous, RAM-only [`NoopCheckpoint`] sink; see that type's
/// docs), which is exactly the "checkpoint looked like it worked but nothing
/// was actually made durable" gap #1396's review confirmed (a bare
/// `is_success()` check treated that response as a satisfied gate). This
/// function now parses the body and requires `persisted == true`; anything
/// else — `false`, or a body this shard's response doesn't even carry the
/// key for — is treated as a failed checkpoint, surfacing as
/// [`DriveOutcome::Blocked`] naming the shard rather than a cutover that
/// proceeds over undurable data.
///
/// [`DriveOutcome::Blocked`]: crate::operator::application::reshard_driver::DriveOutcome::Blocked
///
/// [`checkpoint`]: crate::persistence::interfaces::http::checkpoint
/// [`NoopCheckpoint`]: crate::persistence::application::ports::checkpoint_sink::NoopCheckpoint
pub(super) async fn checkpoint_shard(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
) -> Result<()> {
    let mut req = http.post(format!("{base_url}/admin/checkpoint"));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("POST {base_url}/admin/checkpoint"))?;
    if !resp.status().is_success() {
        bail!("{base_url}/admin/checkpoint returned {}", resp.status());
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .with_context(|| format!("decode {base_url}/admin/checkpoint response"))?;
    let persisted = body
        .get("persisted")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !persisted {
        bail!(
            "{base_url}/admin/checkpoint did not report persisted=true (shard has no durable \
             checkpoint sink configured, or the checkpoint failed) — cutover cannot proceed \
             over undurable migration mutations on this shard"
        );
    }
    Ok(())
}

/// #1389 R3, generalized by #1396 R1 into an explicit shard set: checkpoint
/// exactly `shards`. [`advance_catching_up`] now calls this twice per tick —
/// once for just the target/new shard immediately after migration and
/// *before* any source eviction is attempted, and again for every source
/// shard after eviction — rather than once for `0..target.physical_shard_
/// count()` after both migration and eviction had already run (the ordering
/// #1396's review found: an eviction becoming durable, or even being
/// attempted, before the target's copy of the same data was durably
/// checkpointed, could lose data on a crash between the two). A failure here
/// leaves the workflow in `CatchingUp` — resumable, never mid-cutover with
/// undurable data — and the next tick retries the same idempotent
/// migration/checkpoint/eviction/checkpoint sequence.
///
/// `moving_buckets`/`last_armed_at` (#1458 R3) thread the same
/// [`maybe_rearm_fence`] time-based re-arm into this loop: a real cutover
/// can checkpoint many source shards sequentially, and the caller's
/// unconditional phase-boundary re-arm (immediately before this call) only
/// covers the moment this loop starts, not however long the loop itself
/// takes.
///
/// [`advance_catching_up`]:
///   crate::operator::application::reshard_driver::phases::advance_catching_up
pub(super) async fn checkpoint_shards(
    control: &dyn ClusterControl,
    http: &reqwest::Client,
    namespace: &str,
    name: &str,
    lumen: &Lumen,
    shards: impl Iterator<Item = u32>,
    current: &VirtualBucketShardMap,
    moving_buckets: Option<&BTreeSet<u32>>,
    last_armed_at: &mut Instant,
) -> Result<()> {
    let token = control.admin_token(namespace, lumen).await?;
    for shard in shards {
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
        checkpoint_shard(http, &url, token.as_ref().map(ProjectedToken::expose)).await?;
    }
    Ok(())
}
