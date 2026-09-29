//! The reshard verbs a target and a source shard run: a batch's partial
//! snapshot merged into the live engine, and the documents a newer bucket map
//! no longer assigns to this shard evicted.

use std::collections::BTreeMap;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::storage::{SnapshotV1, SNAPSHOT_VERSION};

impl Engine {
    /// `POST /admin/reshard:apply`: additively merge one `ReshardBatch`'s
    /// partial snapshot into the live engine — upsert semantics for the
    /// batch's documents, never a full replace, so a target shard's
    /// pre-existing collections/documents outside the batch are untouched.
    ///
    /// Reuses the same [`Collection::to_snapshot`]/[`Collection::from_snapshot`]
    /// machinery `snapshot`/`restore` use: for a collection the target
    /// already has, the live collection is snapshotted, merged with the
    /// delta via [`crate::sharding::domain::merge_delta::merge_snapshot_delta`] (bucket-batch
    /// deltas are computed by [`crate::sharding::domain::reshard_batch::snapshot_reshard_batches`]),
    /// and rebuilt in one write-lock scope so a concurrent read never
    /// observes a torn state. A collection the target doesn't have yet is
    /// inserted straight from the delta.
    ///
    /// Idempotent: `merge_snapshot_delta` unions postings/forward maps by
    /// key, so replaying the same batch (operator resume after a checkpoint)
    /// re-inserts identical entries and leaves query-visible state
    /// unchanged. Per-field `bytes` size counters are a saturating-add
    /// heuristic and are not strictly idempotent, but they never feed query
    /// results.
    /// `replace` (#1443 R2, reworked #1457 R1): when `Some`, applied *after*
    /// the additive merge below — for every `(collection_id, keep_ids)` in
    /// its `replace_ids`, prunes any document this shard currently holds
    /// that routes to `replace.bucket` (under `replace.virtual_bucket_count`)
    /// but is absent from `keep_ids`. This is what makes the reshard
    /// driver's final, fenced `CatchingUp` pass authoritative for the
    /// buckets it copies: a document deleted on the source during the split
    /// is absent from the final pass's authoritative keep set and is pruned
    /// here rather than surviving as a stale copy from an earlier additive
    /// pass. Non-final passes never call this with `replace` set, so their
    /// merge stays purely additive. Callers reach this branch through
    /// [`Self::apply_reshard_prune_chunk`]'s receiver-side chunk
    /// accumulator, not directly from a wire `ReshardBatch` (#1457 R1 split
    /// the authoritative-replace scope out of that purely-additive type).
    pub fn apply_reshard_batch(
        &self,
        delta: SnapshotV1,
        replace: Option<crate::sharding::domain::prune_chunk::ReshardBatchReplaceScope>,
    ) -> Result<ReshardApplyOutcome> {
        let _apply = self.capture_barrier.apply();
        if !(1..=SNAPSHOT_VERSION).contains(&delta.version) {
            bail!(
                "reshard batch snapshot version mismatch: got {}, supported 1..={}",
                delta.version,
                SNAPSHOT_VERSION
            );
        }
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let mut collections_touched = 0u32;
        let mut documents_upserted = 0u32;
        for (collection_id, delta_collection) in delta.collections {
            documents_upserted =
                documents_upserted.saturating_add(delta_collection.eid_fields.len() as u32);
            let merged_collection = match state.collections.get(&collection_id) {
                Some(existing) => {
                    let base_snapshot = existing.to_snapshot()?;
                    let base = SnapshotV1 {
                        version: delta.version,
                        collections: BTreeMap::from([(collection_id.clone(), base_snapshot)]),
                    };
                    let delta_wrap = SnapshotV1 {
                        version: delta.version,
                        collections: BTreeMap::from([(collection_id.clone(), delta_collection)]),
                    };
                    let merged = crate::sharding::domain::merge_delta::merge_snapshot_delta(
                        base, delta_wrap,
                    )?;
                    merged
                        .collections
                        .into_iter()
                        .next()
                        .map(|(_, c)| c)
                        .ok_or_else(|| anyhow!("reshard merge produced no collection"))?
                }
                None => delta_collection,
            };
            let mut merged = Collection::from_snapshot(merged_collection)?;
            if let Some(existing) = state.collections.get(&collection_id) {
                merged.collection_generation = existing.collection_generation;
                merged.data_version = existing.data_version.saturating_add(1);
            } else {
                merged.collection_generation = state.allocate_collection_generation()?;
            }
            state.collections.insert(collection_id, merged);
            collections_touched += 1;
        }

        let mut documents_pruned = 0u32;
        if let Some(scope) = replace {
            let bucket_map = VirtualBucketShardMap::balanced(0, scope.virtual_bucket_count, 1)?;
            for (collection_id, keep_ids) in &scope.replace_ids {
                let Some(coll) = state.collections.get_mut(collection_id) else {
                    continue;
                };
                if coll.deleted_at.is_some() {
                    continue;
                }
                let to_prune: Vec<(u32, String)> = coll
                    .eid_fields
                    .keys()
                    .filter_map(|&id| {
                        let external_id = coll.interner.resolve(id).to_string();
                        let route = bucket_map
                            .route_document(collection_id, None, &external_id)
                            .bucket;
                        (route == scope.bucket && !keep_ids.contains(&external_id))
                            .then_some((id, external_id))
                    })
                    .collect();
                if to_prune.is_empty() {
                    continue;
                }
                coll.clear_search_cache();
                coll.clear_number_filter_caches();
                for (id, external_id) in &to_prune {
                    let fields: Vec<String> = coll
                        .eid_fields
                        .get(id)
                        .map(|s| s.iter().cloned().collect())
                        .unwrap_or_default();
                    for f in fields {
                        if let Some(fi) = coll.fields.get_mut(&f) {
                            fi.drop_eid(*id, external_id);
                        }
                        coll.mark_field_dirty(&f, external_id)?;
                    }
                    coll.eid_fields.remove(id);
                }
                documents_pruned = documents_pruned.saturating_add(to_prune.len() as u32);
            }
            self.publish_storage_bytes(&state);
        }

        Ok(ReshardApplyOutcome {
            collections_touched,
            documents_upserted,
            documents_pruned,
        })
    }

    /// `POST /admin/reshard:evict`: source-side post-cutover eviction.
    /// Given a newer virtual-bucket map `to` and this shard's physical
    /// index `this_shard`, removes exactly the documents whose bucket now
    /// routes to a different shard under `to` — nothing else. A separate,
    /// explicitly-invoked step: never implicit in `apply_reshard_batch` or
    /// `snapshot`. Idempotent — a doc already evicted by a prior call no
    /// longer matches and is skipped on retry.
    ///
    /// Refreshes `lumen_storage_bytes` inline for every collection actually
    /// touched (#1386 R2): the gauge otherwise only refreshes on the next
    /// `GET /collections/{id}/stats` call, which may not come before the
    /// reshard driver's own usage loop scrapes `/metrics` again — a scrape
    /// timed right after this cutover's eviction would otherwise still
    /// report pre-eviction bytes even though it is chronologically
    /// post-cutover, defeating the cutover-generation freshness check in
    /// [`crate::operator::domain::lumen_spec::LumenSpec::reshard_status_with_usage`].
    pub fn evict_not_owned(
        &self,
        to: &VirtualBucketShardMap,
        this_shard: u32,
    ) -> Result<ReshardEvictOutcome> {
        let _apply = self.capture_barrier.apply();
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let mut collections_touched = 0u32;
        let mut documents_evicted = 0u32;
        for (collection_id, coll) in state.collections.iter_mut() {
            if coll.deleted_at.is_some() {
                continue;
            }
            let to_evict: Vec<(u32, String)> = coll
                .eid_fields
                .keys()
                .filter_map(|&id| {
                    let external_id = coll.interner.resolve(id).to_string();
                    let route = to.route_document(collection_id, None, &external_id);
                    (route.shard != this_shard).then_some((id, external_id))
                })
                .collect();
            if to_evict.is_empty() {
                continue;
            }
            coll.clear_search_cache();
            coll.clear_number_filter_caches();
            for (id, external_id) in &to_evict {
                let fields: Vec<String> = coll
                    .eid_fields
                    .get(id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                for f in fields {
                    if let Some(fi) = coll.fields.get_mut(&f) {
                        fi.drop_eid(*id, external_id);
                    }
                    coll.mark_field_dirty(&f, external_id)?;
                }
                coll.eid_fields.remove(id);
            }
            documents_evicted = documents_evicted.saturating_add(to_evict.len() as u32);
            collections_touched += 1;
        }
        // #1386 R2 / #1397 R2: publish the engine-wide byte footprint (summed
        // across every live collection), not just whichever collection this
        // loop happened to touch last — the reshard split trigger reads this
        // gauge, and a per-collection last-writer-wins value under-reports
        // any engine holding more than one collection. Computed once, after
        // the loop, over the same live-collection view `stats()` uses.
        self.publish_storage_bytes(&state);
        Ok(ReshardEvictOutcome {
            collections_touched,
            documents_evicted,
        })
    }
}

/// Response summary for `POST /admin/reshard:apply` (#1380 R1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardApplyOutcome {
    pub collections_touched: u32,
    pub documents_upserted: u32,
    /// #1443 R2: documents pruned by an authoritative-subset `replace`
    /// scope, `0` when the batch carried none.
    #[serde(default)]
    pub documents_pruned: u32,
}

/// Response summary for `POST /admin/reshard:evict` (#1380 R3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardEvictOutcome {
    pub collections_touched: u32,
    pub documents_evicted: u32,
}
