//! Snapshot-level resharding primitives.
//!
//! The operator owns when a shard split is allowed; this module owns the
//! data-plane unit that makes a split gradual: compare two versioned
//! virtual-bucket maps, identify moved buckets, and emit bounded SnapshotV1
//! batches containing only the external_ids that now belong to a different
//! physical shard.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

use crate::sharding::domain::bucket_move::{bucket_moves, BucketMove};
use crate::sharding::domain::snapshot_subset::{
    collection_subset, ids_map_from_pairs, snapshot_subset,
};
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::storage::SnapshotV1;

/// The hard body-size limit `POST /admin/reshard:apply` (and every other
/// admin route) enforces at the HTTP layer — `api.rs`'s
/// `DefaultBodyLimit::max(..)` is built from this exact constant (#1444 R2),
/// so the two can never drift apart: a batch this crate computes as
/// "under the limit" is always actually under the limit the route enforces,
/// and [`crate::operator::reshard_driver`]'s oversize-wedge detection
/// compares a batch's real wire size against this same number rather than a
/// second, hand-copied literal.
pub const ADMIN_ROUTE_BODY_LIMIT_BYTES: usize = 8 * 1024 * 1024;

/// Upper bound on one batch's serialized `snapshot` payload (#1396 R4):
/// [`ADMIN_ROUTE_BODY_LIMIT_BYTES`] is the route's hard 413 cutoff, but
/// [`snapshot_reshard_batches`] used to cap batches only by external-id count
/// (`MAX_EXTERNAL_IDS_PER_BATCH`-style caller constants) — a bucket of large
/// documents (long text fields, vectors, hashes) can still serialize well
/// past the route limit even at a small id count, and a 413 from an
/// oversized batch is deterministically recomputed identically every driver
/// tick, wedging the split forever (the confirmed defect). Half the route
/// limit leaves comfortable headroom for JSON/wire overhead and per-item
/// framing above the raw snapshot bytes measured here.
pub const MAX_BATCH_BYTES: usize = ADMIN_ROUTE_BODY_LIMIT_BYTES / 2;

/// #1380: `Serialize`/`Deserialize` make a batch postable to `POST
/// /admin/reshard:apply` as-is — the wire payload for the admin apply verb
/// is this struct's exact JSON shape, no separate DTO.
///
/// #1457 R1: `ReshardBatch` is now purely additive — it used to also carry
/// an authoritative `replace_ids`/`virtual_bucket_count` pair for the final
/// migration pass (#1443 R2), but stamping the *complete* id set onto every
/// byte-capped chunk of a bucket made the final pass's wire size scale with
/// total bucket population rather than the chunk's own content, so a bucket
/// whose id set alone serialized past the byte cap produced chunks over the
/// route's hard body limit no matter how small `snapshot`/`external_ids`
/// were — and [`crate::operator::application::reshard_driver::transfer::detect_oversized_batch`]
/// wrongly blamed whichever document happened to be first in the chunk. The
/// authoritative-replace concern moved to its own dedicated, independently
/// chunked message: [`ReshardPruneChunk`] /
/// [`snapshot_reshard_prune_chunks`].
///
/// [`ReshardPruneChunk`]: crate::sharding::domain::prune_chunk::ReshardPruneChunk
/// [`snapshot_reshard_prune_chunks`]: crate::sharding::domain::prune_chunk::snapshot_reshard_prune_chunks
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReshardBatch {
    pub from_map_version: u64,
    pub to_map_version: u64,
    pub bucket: u32,
    pub from_shard: u32,
    pub to_shard: u32,
    pub external_ids: BTreeMap<String, BTreeSet<String>>,
    pub snapshot: SnapshotV1,
}

/// Build bounded, purely-additive snapshot batches for documents that move
/// under `to`. Batches are grouped by `(bucket, from_shard, to_shard)` and
/// capped by `max_external_ids_per_batch`/[`MAX_BATCH_BYTES`], so an
/// operator can checkpoint progress after every emitted batch instead of
/// blocking on one full-shard copy.
///
/// #1457 R1: every pass — including the reshard driver's final `CatchingUp`
/// pass, run under the write fence — uses this purely-additive form. The
/// final pass's authoritative prune scope is now a separate, independently
/// byte-capped message: see [`snapshot_reshard_prune_chunks`].
///
/// #2496: `buckets` (the caller's current `from_shard` group — same
/// restriction [`snapshot_reshard_prune_chunks`] already takes, and the same
/// set `snapshot` was fetched with via `snapshot_bucket_subset`) scopes a
/// second, schema-only class of batch: for every `(from_shard, to_shard)`
/// pair a bucket in `buckets` actually moves under, any collection present
/// in `snapshot.collections` (schema-complete since #2496's
/// `snapshot_bucket_subset` fix — every collection appears there, doc-driven
/// or not) that produced *no* doc-carrying batch for that pair still gets
/// one small batch carrying just its schema (empty `external_ids`, a
/// single-collection schema-only `snapshot`), anchored at that pair's
/// lowest bucket. Without this, a collection with zero documents in these
/// buckets — most commonly a brand new, still-empty collection — never
/// produces a wire message at all, so a shard gaining buckets never learns
/// the collection exists (the sibling gap `routing_remote::RoutedRouter`'s
/// `create_collection`/`drop_collection` fan-out closes for the live write
/// path). Restricting to `buckets` (rather than every move `bucket_moves`
/// reflects cluster-wide) matters: `from`/`to` are the *whole cluster's*
/// maps, reused unchanged across every `from_shard` group the driver loops
/// over, so an unscoped scan would misattribute a schema-only batch to a
/// `from_shard`/`to_shard` pair this call's `snapshot` (itself scoped to one
/// source shard) has no authority to speak for. Applying such a batch is
/// harmless idempotent no-op on a repeat pass (`Engine::apply_reshard_batch`
/// already documents this).
///
/// [`snapshot_reshard_prune_chunks`]: crate::sharding::domain::prune_chunk::snapshot_reshard_prune_chunks
pub fn snapshot_reshard_batches(
    snapshot: &SnapshotV1,
    from: &VirtualBucketShardMap,
    to: &VirtualBucketShardMap,
    buckets: &BTreeSet<u32>,
    max_external_ids_per_batch: usize,
    max_batch_bytes: usize,
) -> Result<Vec<ReshardBatch>> {
    if max_external_ids_per_batch == 0 {
        bail!("max_external_ids_per_batch must be > 0");
    }
    if max_batch_bytes == 0 {
        bail!("max_batch_bytes must be > 0");
    }

    let moves = bucket_moves(from, to)?;
    if moves.is_empty() {
        return Ok(Vec::new());
    }
    let moves_by_bucket: BTreeMap<u32, BucketMove> =
        moves.into_iter().map(|m| (m.bucket, m)).collect();
    let mut ids_by_move: BTreeMap<(u32, u32, u32), BTreeMap<String, Vec<String>>> = BTreeMap::new();

    for (collection_id, collection) in &snapshot.collections {
        for external_id in collection.eid_fields.keys() {
            let route = from.route_document(collection_id, None, external_id);
            let Some(mv) = moves_by_bucket.get(&route.bucket) else {
                continue;
            };
            let new_route = to.route_document(collection_id, None, external_id);
            if new_route.shard != mv.to_shard {
                bail!(
                    "route for bucket {} changed unexpectedly: assignment={} route={}",
                    mv.bucket,
                    mv.to_shard,
                    new_route.shard
                );
            }
            ids_by_move
                .entry((mv.bucket, mv.from_shard, mv.to_shard))
                .or_default()
                .entry(collection_id.clone())
                .or_default()
                .push(external_id.clone());
        }
    }

    // #2496: which `(from_shard, to_shard)` pairs already have at least one
    // doc-carrying batch for a given collection, so the schema-only pass
    // below only fills in collections that would otherwise never be
    // mentioned in this call's output.
    let mut collections_with_docs: BTreeMap<(u32, u32), BTreeSet<String>> = BTreeMap::new();
    for (&(_, from_shard, to_shard), by_collection) in &ids_by_move {
        collections_with_docs
            .entry((from_shard, to_shard))
            .or_default()
            .extend(by_collection.keys().cloned());
    }

    let mut batches = Vec::new();
    for ((bucket, from_shard, to_shard), by_collection) in ids_by_move {
        let mut pending: Vec<(String, String)> = by_collection
            .into_iter()
            .flat_map(|(collection_id, mut ids)| {
                ids.sort();
                ids.into_iter()
                    .map(move |external_id| (collection_id.clone(), external_id))
            })
            .collect();
        pending.sort();

        for chunk in pending.chunks(max_external_ids_per_batch) {
            let mut sub_batches = Vec::new();
            byte_cap_chunk(snapshot, chunk, max_batch_bytes, &mut sub_batches)?;
            for (external_ids, partial) in sub_batches {
                batches.push(ReshardBatch {
                    from_map_version: from.version(),
                    to_map_version: to.version(),
                    bucket,
                    from_shard,
                    to_shard,
                    external_ids,
                    snapshot: partial,
                });
            }
        }
    }

    // #2496: schema-only registration, restricted to `buckets` (this call's
    // actual scope) so a schema-registration batch is never attributed to a
    // `from_shard`/`to_shard` pair this `snapshot` has no authority over.
    let mut anchor_bucket_by_pair: BTreeMap<(u32, u32), u32> = BTreeMap::new();
    for bucket in buckets {
        if let Some(mv) = moves_by_bucket.get(bucket) {
            anchor_bucket_by_pair
                .entry((mv.from_shard, mv.to_shard))
                .and_modify(|anchor| *anchor = (*anchor).min(mv.bucket))
                .or_insert(mv.bucket);
        }
    }
    for (&(from_shard, to_shard), &bucket) in &anchor_bucket_by_pair {
        let already = collections_with_docs.get(&(from_shard, to_shard));
        for (collection_id, collection) in &snapshot.collections {
            if already.is_some_and(|set| set.contains(collection_id)) {
                continue;
            }
            let schema_only = collection_subset(collection, &BTreeSet::new());
            batches.push(ReshardBatch {
                from_map_version: from.version(),
                to_map_version: to.version(),
                bucket,
                from_shard,
                to_shard,
                external_ids: BTreeMap::new(),
                snapshot: SnapshotV1 {
                    version: snapshot.version,
                    collections: BTreeMap::from([(collection_id.clone(), schema_only)]),
                },
            });
        }
    }

    Ok(batches)
}

/// Recursively halve `chunk` (already `<= max_external_ids_per_batch` ids)
/// until each emitted `(external_ids, snapshot)` pair's serialized snapshot
/// is at or under `max_batch_bytes`, or the chunk is down to a single
/// external_id — one oversized document cannot be split further, so it is
/// emitted as its own (over-budget) batch rather than looping forever; a
/// single document that alone exceeds [`ADMIN_ROUTE_BODY_LIMIT_BYTES`] is a
/// data-modeling problem this splitter cannot solve, not a batching bug —
/// [`crate::operator::reshard_driver`] detects and surfaces exactly this
/// batch shape (#1444 R2) rather than retrying it forever as a generic 413.
pub(super) fn byte_cap_chunk(
    snapshot: &SnapshotV1,
    chunk: &[(String, String)],
    max_batch_bytes: usize,
    out: &mut Vec<(BTreeMap<String, BTreeSet<String>>, SnapshotV1)>,
) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }
    let external_ids = ids_map_from_pairs(chunk.iter().cloned());
    let partial = snapshot_subset(snapshot, &external_ids)?;
    let size = serde_json::to_vec(&partial)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if size <= max_batch_bytes || chunk.len() == 1 {
        out.push((external_ids, partial));
        return Ok(());
    }
    let mid = chunk.len() / 2;
    byte_cap_chunk(snapshot, &chunk[..mid], max_batch_bytes, out)?;
    byte_cap_chunk(snapshot, &chunk[mid..], max_batch_bytes, out)?;
    Ok(())
}
