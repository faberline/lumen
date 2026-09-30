//! The final migration pass's authoritative keep set, chunked by bytes, and the
//! replace scope a receiving shard assembles from it.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

/// #1443 R2 / #1457 R1: the authoritative-subset-replace scope one applying
/// shard must enforce for a `bucket`+collection, derived from one or more
/// [`ReshardPruneChunk`]s sharing the same `(to_map_version, bucket,
/// collection_id, total_chunks)` key once every chunk has been received
/// (see [`crate::index::application::engine::Engine::apply_reshard_prune_chunk`]'s receiver-side
/// accumulator). Applying this scope after a batch's additive merge closes
/// the delete-resurrection gap #1443 found: a document deleted on the source
/// during the split is absent from the final pass's authoritative id set and
/// is pruned from the target rather than surviving only because an earlier,
/// now-stale copy landed on the target from a prior additive pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReshardBatchReplaceScope {
    pub bucket: u32,
    pub virtual_bucket_count: u32,
    pub replace_ids: BTreeMap<String, BTreeSet<String>>,
}

/// #1457 R1: one byte-capped chunk of the authoritative "keep" id set for a
/// single `(bucket, collection_id)` pair under the final migration pass's
/// `to` map. Unlike [`ReshardBatch`] (purely additive, chunked by
/// `max_external_ids_per_batch`/[`MAX_BATCH_BYTES`] with no cross-chunk
/// coupling), every chunk of one `(bucket, collection_id)`'s keep set shares
/// the same `to_map_version`/`bucket`/`collection_id`/`total_chunks` and
/// carries only its own slice of `keep_ids` — the receiver
/// ([`crate::index::application::engine::Engine::apply_reshard_prune_chunk`]) accumulates
/// chunks by that key and prunes only once every `chunk_index` in
/// `0..total_chunks` has arrived, so re-sending any subset (413 retry) or
/// all chunks (whole-pass retry after a driver restart) converges to the
/// same pruned result rather than pruning against a partial, still-assembling
/// keep set. `keep_ids` may be empty — every moved bucket emits a chunk for
/// every collection that exists on the source shard, even one a batch of
/// deletes emptied entirely (#1457 R2 / #1443's disclosed edge), so the
/// bucket's copies of that collection are still pruned on cutover.
///
/// #1467 R2 ordering contract: the sender
/// (`run_migration_pass_impl`/`snapshot_reshard_prune_chunks` in
/// `src/operator/reshard_driver.rs`) always emits every chunk for one
/// `(bucket, collection_id, total_chunks)` key strictly in `chunk_index`
/// order (`0..total_chunks`, awaited sequentially, one HTTP round trip per
/// chunk) within a single migration pass, and never starts a second pass
/// for the same key before the first either completes or the driver gives
/// up on it entirely. The receiver relies on this: `chunk_index == 0`
/// unambiguously marks the start of a fresh pass and resets any stale
/// partial left by an earlier, never-completed pass for the same key
/// instead of unioning into it (see
/// [`crate::index::application::engine::Engine::apply_reshard_prune_chunk`]'s R2 doc comment).
/// If a future sender ever needs to send chunks for one key out of order or
/// interleaved across concurrent passes, this ordering contract — and the
/// receiver's chunk-0 reset — must change together (e.g. to a per-pass
/// nonce field on this struct).
///
/// [`ReshardBatch`]: crate::sharding::domain::reshard_batch::ReshardBatch
/// [`MAX_BATCH_BYTES`]: crate::sharding::domain::reshard_batch::MAX_BATCH_BYTES
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReshardPruneChunk {
    pub to_map_version: u64,
    pub bucket: u32,
    pub virtual_bucket_count: u32,
    pub collection_id: String,
    pub chunk_index: u32,
    pub total_chunks: u32,
    pub keep_ids: BTreeSet<String>,
}

/// #1457 R1 / R2: build the final migration pass's authoritative "keep"
/// chunks for every `(bucket, collection_id)` pair, `bucket` restricted to
/// `buckets` (the caller's current `from_shard` group — never the whole
/// map's moved buckets, so one from-shard's prune scope can never claim
/// authority over a bucket another from-shard actually owns) and
/// `collection_id` ranging over `collection_ids` (the *full* list of
/// collections that exist on the source shard, fetched independently of
/// `snapshot` — see module docs on why the bucket-scoped snapshot's own
/// collection keys are not sufficient).
///
/// `snapshot` only needs to cover `buckets` (a bucket-scoped export is
/// enough): for each `(bucket, collection_id)` pair, `keep_ids` is exactly
/// the external_ids in `snapshot` that route to that bucket for that
/// collection, computed with `to.route_document` (identical to
/// `from.route_document`'s bucket component, since [`bucket_moves`] already
/// requires a stable `virtual_bucket_count` between the two maps). A
/// collection with **zero** matching docs in a bucket still emits exactly
/// one chunk with an empty `keep_ids` (`total_chunks == 1`) — this is what
/// makes a collection a batch of deletes emptied entirely still get pruned
/// on the target rather than silently keeping its stale copies (#1457 R2,
/// the edge #1443 disclosed).
///
/// Each `(bucket, collection_id)`'s keep set is independently byte-capped by
/// `max_chunk_bytes` via [`chunk_ids_by_bytes`] — unlike [`ReshardBatch`],
/// no *other* pair's chunk count or size is affected by how large one pair's
/// population is.
///
/// [`bucket_moves`]: crate::sharding::domain::bucket_move::bucket_moves
/// [`ReshardBatch`]: crate::sharding::domain::reshard_batch::ReshardBatch
pub fn snapshot_reshard_prune_chunks(
    snapshot: &SnapshotV1,
    to: &VirtualBucketShardMap,
    buckets: &BTreeSet<u32>,
    collection_ids: &BTreeSet<String>,
    max_chunk_bytes: usize,
) -> Result<Vec<ReshardPruneChunk>> {
    if max_chunk_bytes == 0 {
        bail!("max_chunk_bytes must be > 0");
    }

    let virtual_bucket_count = to.virtual_bucket_count();
    let mut keep: BTreeMap<(u32, String), BTreeSet<String>> = BTreeMap::new();
    for &bucket in buckets {
        for collection_id in collection_ids {
            keep.insert((bucket, collection_id.clone()), BTreeSet::new());
        }
    }
    for (collection_id, collection) in &snapshot.collections {
        if !collection_ids.contains(collection_id) {
            continue;
        }
        for external_id in collection.eid_fields.keys() {
            let bucket = to.route_document(collection_id, None, external_id).bucket;
            if let Some(ids) = keep.get_mut(&(bucket, collection_id.clone())) {
                ids.insert(external_id.clone());
            }
        }
    }

    let mut chunks = Vec::new();
    for ((bucket, collection_id), ids) in keep {
        let pieces = chunk_ids_by_bytes(&ids, max_chunk_bytes);
        let total_chunks = pieces.len() as u32;
        for (chunk_index, keep_ids) in pieces.into_iter().enumerate() {
            chunks.push(ReshardPruneChunk {
                to_map_version: to.version(),
                bucket,
                virtual_bucket_count,
                collection_id: collection_id.clone(),
                chunk_index: chunk_index as u32,
                total_chunks,
                keep_ids,
            });
        }
    }
    Ok(chunks)
}

/// Recursively halve `ids` until each emitted chunk's serialized size is at
/// or under `max_bytes`, or the chunk is down to a single id (mirrors
/// [`byte_cap_chunk`]'s same one-item floor for the same reason: a single id
/// long enough alone to exceed the cap cannot be split further). An empty
/// `ids` still returns exactly one (empty) chunk — [`snapshot_reshard_prune_chunks`]
/// relies on this to always emit at least one chunk per `(bucket,
/// collection_id)` pair, including pairs with nothing left to keep.
///
/// [`byte_cap_chunk`]: crate::sharding::domain::reshard_batch::byte_cap_chunk
fn chunk_ids_by_bytes(ids: &BTreeSet<String>, max_bytes: usize) -> Vec<BTreeSet<String>> {
    if ids.is_empty() {
        return vec![BTreeSet::new()];
    }
    let size = serde_json::to_vec(ids)
        .map(|b| b.len())
        .unwrap_or(usize::MAX);
    if size <= max_bytes || ids.len() == 1 {
        return vec![ids.clone()];
    }
    let mid = ids.len() / 2;
    let first: BTreeSet<String> = ids.iter().take(mid).cloned().collect();
    let rest: BTreeSet<String> = ids.iter().skip(mid).cloned().collect();
    let mut out = chunk_ids_by_bytes(&first, max_bytes);
    out.extend(chunk_ids_by_bytes(&rest, max_bytes));
    out
}
