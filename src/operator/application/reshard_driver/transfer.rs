//! Moving documents between shards: fetch a scoped backup from the old owner,
//! then apply it to, or prune it from, the new one.

use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use serde_json::json;

use crate::operator::application::reshard_driver::oversize::OversizedDocumentBlock;
use crate::sharding::domain::{prune_chunk::ReshardPruneChunk, reshard_batch::ReshardBatch};
use crate::storage::SnapshotV1;

pub(super) async fn fetch_scoped_backup(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
    virtual_bucket_count: u32,
    buckets: &BTreeSet<u32>,
) -> Result<SnapshotV1> {
    let mut req = http
        .post(format!("{base_url}/admin/backup:scoped"))
        .json(&json!({
            "virtual_bucket_count": virtual_bucket_count,
            "buckets": buckets,
        }));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("POST {base_url}/admin/backup:scoped"))?;
    if !resp.status().is_success() {
        bail!("{base_url}/admin/backup:scoped returned {}", resp.status());
    }
    resp.json::<SnapshotV1>()
        .await
        .context("decode backup:scoped response")
}

/// `GET /collections` (#1457 R2): the full list of collections that exist on
/// this shard right now, independent of any bucket scope. The reshard
/// driver's admin token carries wildcard `Role::Admin` on `"*"`, which
/// already satisfies this data-plane route's per-collection `Role::Read`
/// filter for every collection id, so no new admin-only endpoint is needed
/// here. This is deliberately **not** derived from a bucket-scoped
/// snapshot's own `collections` keys: [`crate::sharding::domain::snapshot_subset::snapshot_bucket_subset`]
/// (backing `POST /admin/backup:scoped`) omits a collection entirely from
/// its output when it has zero matching docs in the requested buckets — a
/// collection a batch of deletes emptied out of a moved bucket would then be
/// silently skipped by [`snapshot_reshard_prune_chunks`], leaving its stale
/// copies on the target unpruned (the exact edge #1443 disclosed and #1457
/// R2 closes).
///
/// [`snapshot_reshard_prune_chunks`]: crate::sharding::domain::prune_chunk::snapshot_reshard_prune_chunks
pub(super) async fn fetch_all_collection_ids(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
) -> Result<BTreeSet<String>> {
    let mut req = http.get(format!("{base_url}/collections"));
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("GET {base_url}/collections"))?;
    if !resp.status().is_success() {
        bail!("{base_url}/collections returned {}", resp.status());
    }
    let ids: Vec<String> = resp
        .json()
        .await
        .with_context(|| format!("decode {base_url}/collections response"))?;
    Ok(ids.into_iter().collect())
}

/// If `batch`'s actual wire payload is over
/// [`crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES`], name the collection and
/// external_id to blame (#1444 R2). `snapshot_reshard_batches`'
/// `byte_cap_chunk` only ever emits an over-the-limit batch when it floored
/// at a single external_id (a bucket group's byte cap already keeps every
/// multi-id batch under half the route limit), so the first id found in
/// `external_ids` is that one document.
pub(super) fn detect_oversized_batch(batch: &ReshardBatch) -> Option<OversizedDocumentBlock> {
    let bytes = serde_json::to_vec(batch)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if bytes <= crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES {
        return None;
    }
    let (collection, external_id) = batch.external_ids.iter().find_map(|(collection, ids)| {
        ids.iter()
            .next()
            .map(|external_id| (collection.clone(), external_id.clone()))
    })?;
    Some(OversizedDocumentBlock {
        collection,
        external_id,
        bytes,
    })
}

pub(super) async fn apply_reshard_batch(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
    batch: &ReshardBatch,
) -> Result<()> {
    // Pre-flight (#1444 R2): a batch this crate can already tell is over the
    // route's body limit is skipped rather than sent — no wasted round trip,
    // and the classification never depends on how a given HTTP stack renders
    // its own 413.
    if let Some(oversized) = detect_oversized_batch(batch) {
        return Err(oversized.into());
    }
    let mut req = http
        .post(format!("{base_url}/admin/reshard:apply"))
        .json(batch);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("POST {base_url}/admin/reshard:apply"))?;
    if !resp.status().is_success() {
        // Defense in depth: even if the pre-flight estimate above missed it
        // (e.g. framing/compression skew), classify a live 413 on this exact
        // batch shape the same way rather than a generic Blocked message.
        if resp.status() == reqwest::StatusCode::PAYLOAD_TOO_LARGE {
            if let Some(oversized) = detect_oversized_batch(batch) {
                return Err(oversized.into());
            }
        }
        bail!("{base_url}/admin/reshard:apply returned {}", resp.status());
    }
    Ok(())
}

/// `POST /admin/reshard:prune` (#1457 R1): send one [`ReshardPruneChunk`] of
/// the final migration pass's authoritative keep set. Unlike
/// [`apply_reshard_batch`], a chunk carries only external_id strings (no
/// document content), so it never needs the same pre-flight/live-413
/// oversize classification — `snapshot_reshard_prune_chunks`'s recursive
/// byte-cap halving already keeps every chunk under `max_chunk_bytes` short
/// of a single id long enough alone to exceed it, an unrealistic edge this
/// function does not special-case. A failure here
/// propagates as a generic error, surfaced by every caller as
/// [`DriveOutcome::Blocked`] the same as any other step; the next tick's
/// retry recomputes and re-sends the same deterministic chunk set (the final
/// pass runs under the write fence, so bucket population cannot change
/// between ticks), converging via [`crate::index::application::engine::Engine::
/// apply_reshard_prune_chunk`]'s idempotent accumulator.
///
/// [`DriveOutcome::Blocked`]: crate::operator::application::reshard_driver::DriveOutcome::Blocked
pub(super) async fn apply_reshard_prune_chunk(
    http: &reqwest::Client,
    base_url: &str,
    token: Option<&str>,
    chunk: &ReshardPruneChunk,
) -> Result<()> {
    let mut req = http
        .post(format!("{base_url}/admin/reshard:prune"))
        .json(chunk);
    if let Some(token) = token {
        req = req.bearer_auth(token);
    }
    let resp = req
        .send()
        .await
        .with_context(|| format!("POST {base_url}/admin/reshard:prune"))?;
    if !resp.status().is_success() {
        bail!("{base_url}/admin/reshard:prune returned {}", resp.status());
    }
    Ok(())
}
