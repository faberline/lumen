//! The reshard prune: chunks of a final prune accumulated per bucket and
//! collection until the set is complete, bounded in entries, age and total
//! chunks, then applied as one delete.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::index::infrastructure::snapshot_v1::{SnapshotV1, SNAPSHOT_VERSION};

/// `(to_map_version, bucket, collection_id, total_chunks)` — see
/// [`Engine::apply_reshard_prune_chunk`].
pub(super) type PruneAccumKey = (u64, u32, String, u32);

/// #1467 R4: hard cap on distinct in-flight prune accumulator groups —
/// rejected with [`StorageError::PruneAccumulatorFull`] once reached, so an
/// abandoned migration or a flood of bogus keys can't grow the accumulator
/// without bound.
const PRUNE_ACCUM_MAX_ENTRIES: usize = 256;

/// #1467 R4: an accumulator entry older than this many
/// [`Engine::apply_reshard_prune_chunk`] calls (tracked via
/// `Engine::prune_accum_tick`, not wall-clock time) without completing is
/// considered abandoned and is dropped on the next call. A real migration
/// pass sends every chunk for a key back-to-back within one HTTP round
/// trip loop, so thousands of intervening calls is generous slack for
/// concurrent unrelated passes on other keys.
const PRUNE_ACCUM_MAX_AGE_TICKS: u64 = 4096;

/// #1467 R4: sanity cap on `ReshardPruneChunk::total_chunks` — rejected
/// with [`StorageError::InvalidPruneChunk`] outright rather than accepted
/// into the accumulator, since a chunk count this large could never
/// plausibly complete from the sender's own chunking (`chunk_ids_by_bytes`
/// in `src/sharding/domain/reshard_batch.rs` targets far fewer, larger chunks).
const PRUNE_ACCUM_MAX_TOTAL_CHUNKS: u32 = 4096;

#[derive(Debug, Default)]
pub(super) struct PruneAccumState {
    received_chunks: BTreeSet<u32>,
    keep_ids: BTreeSet<String>,
    /// #1467 R4: the `Engine::prune_accum_tick` value when this entry was
    /// first created (i.e. when its first chunk — of either index, since a
    /// mid-sequence chunk can legitimately arrive first — landed).
    created_tick: u64,
}

/// #1467 R4: drops every accumulator entry older than
/// [`PRUNE_ACCUM_MAX_AGE_TICKS`]. Called with the accumulator already
/// locked, once per [`Engine::apply_reshard_prune_chunk`] call, before that
/// call's own chunk is considered — so a steady trickle of prune traffic
/// keeps the accumulator bounded even if some passes are never completed
/// (driver crash, superseded plan, ...).
fn gc_prune_accumulator(accumulator: &mut BTreeMap<PruneAccumKey, PruneAccumState>, now: u64) {
    accumulator
        .retain(|_, state| now.saturating_sub(state.created_tick) <= PRUNE_ACCUM_MAX_AGE_TICKS);
}

impl Engine {
    /// `POST /admin/reshard:prune` (#1457 R1, hardened #1467 R1/R2/R4):
    /// accumulate one [`crate::sharding::domain::prune_chunk::ReshardPruneChunk`] of a final
    /// migration pass's authoritative "keep" set for one `(bucket,
    /// collection_id)` pair, and prune once every chunk in `0..total_chunks`
    /// has arrived.
    ///
    /// Chunks are keyed by `(to_map_version, bucket, collection_id,
    /// total_chunks)`; `keep_ids` are unioned and `chunk_index`es collected
    /// into a set as each chunk lands. Once the set's length equals
    /// `total_chunks`, the accumulated key is removed and the union is
    /// applied via [`Self::apply_reshard_batch`] with an empty additive
    /// delta and a single-collection [`crate::sharding::domain::prune_chunk::ReshardBatchReplaceScope`]
    /// — reusing that method's already-tested prune logic rather than
    /// duplicating it.
    ///
    /// Idempotent by construction: re-sending any subset of chunks (a 413
    /// retry, or a network retry) only re-inserts identical set members;
    /// re-sending *every* chunk of an already-completed group re-runs the
    /// same accumulate-then-apply sequence from scratch (the completed
    /// entry was removed) and calls `apply_reshard_batch` with the same
    /// scope again, which is itself idempotent (pruning against
    /// already-pruned state is a no-op).
    ///
    /// #1467 R1 (TOCTOU fix): readiness-check-and-removal is now a SINGLE
    /// critical section — the lock is held across inserting this chunk,
    /// checking whether the group is now complete, and (if so) removing the
    /// entry, so two concurrent completions for the same key can never both
    /// observe "ready". The loser of such a race (its chunk lands after the
    /// winner already removed the entry) starts a fresh, necessarily
    /// incomplete accumulation instead of ever pruning against an empty
    /// keep set.
    ///
    /// #1467 R2 (stale-partial reset): the sender (`run_migration_pass_impl`
    /// in `src/operator/application/reshard_driver/`) always emits chunks
    /// `0..total_chunks` for one key strictly in order, back-to-back within
    /// a single migration pass, and never starts a second pass for the same
    /// `(bucket, collection_id)` before the first either completes or the
    /// driver gives up on it entirely — see
    /// [`crate::sharding::domain::prune_chunk::ReshardPruneChunk`]'s doc comment for the sender-side
    /// half of this contract. So `chunk_index == 0` unambiguously marks the
    /// start of a new pass: it resets any stale partial left by an earlier
    /// pass for the same key that never reached completion (driver
    /// crash/restart, superseded plan) instead of unioning into it — a
    /// delete landing between the abandoned pass and its retry could
    /// otherwise resurrect via a keep_id carried over from the stale entry.
    ///
    /// #1467 R4 (bounded accumulator): every call first age-GCs the
    /// accumulator ([`gc_prune_accumulator`]) and validates `total_chunks`
    /// against [`PRUNE_ACCUM_MAX_TOTAL_CHUNKS`]; a brand-new key is rejected
    /// with [`StorageError::PruneAccumulatorFull`] once
    /// [`PRUNE_ACCUM_MAX_ENTRIES`] distinct in-flight groups are already
    /// held, so neither an abandoned migration nor a flood of bogus keys can
    /// grow the accumulator without bound.
    pub fn apply_reshard_prune_chunk(
        &self,
        chunk: crate::sharding::domain::prune_chunk::ReshardPruneChunk,
    ) -> Result<ReshardPruneOutcome> {
        let _apply = self.capture_barrier.apply();
        if chunk.total_chunks == 0 || chunk.total_chunks > PRUNE_ACCUM_MAX_TOTAL_CHUNKS {
            return Err(StorageError::InvalidPruneChunk {
                total_chunks: chunk.total_chunks,
                max: PRUNE_ACCUM_MAX_TOTAL_CHUNKS,
            }
            .into());
        }
        let key: PruneAccumKey = (
            chunk.to_map_version,
            chunk.bucket,
            chunk.collection_id.clone(),
            chunk.total_chunks,
        );
        let keep_ids = {
            let mut accumulator = self
                .prune_accumulator
                .lock()
                .map_err(|_| anyhow!("prune accumulator poisoned"))?;
            let now = self.prune_accum_tick.fetch_add(1, Ordering::Relaxed) + 1;
            gc_prune_accumulator(&mut accumulator, now);

            // R2: chunk 0 always starts a fresh pass — drop any stale
            // partial left by an earlier, never-completed pass for this key.
            if chunk.chunk_index == 0 {
                accumulator.remove(&key);
            }

            // R4: bound distinct in-flight groups (only a brand-new key
            // consumes a new slot; a chunk for an already-tracked key is
            // always accepted so an in-progress pass can still complete).
            if !accumulator.contains_key(&key) && accumulator.len() >= PRUNE_ACCUM_MAX_ENTRIES {
                return Err(StorageError::PruneAccumulatorFull {
                    count: accumulator.len(),
                    max: PRUNE_ACCUM_MAX_ENTRIES,
                }
                .into());
            }

            let entry = accumulator
                .entry(key.clone())
                .or_insert_with(|| PruneAccumState {
                    created_tick: now,
                    ..Default::default()
                });
            entry.received_chunks.insert(chunk.chunk_index);
            entry.keep_ids.extend(chunk.keep_ids);
            let ready = entry.received_chunks.len() as u32 >= chunk.total_chunks;
            if !ready {
                return Ok(ReshardPruneOutcome {
                    complete: false,
                    documents_pruned: 0,
                });
            }
            // R1: remove-and-capture inside the SAME guard as the readiness
            // check above — no other caller can observe "ready" for this
            // key again until a fresh chunk 0 re-creates it.
            accumulator
                .remove(&key)
                .map(|state| state.keep_ids)
                .unwrap_or_default()
        };
        let empty_delta = SnapshotV1 {
            version: SNAPSHOT_VERSION,
            collections: BTreeMap::new(),
        };
        let scope = crate::sharding::domain::prune_chunk::ReshardBatchReplaceScope {
            bucket: chunk.bucket,
            virtual_bucket_count: chunk.virtual_bucket_count,
            replace_ids: BTreeMap::from([(chunk.collection_id, keep_ids)]),
        };
        let outcome = self.apply_reshard_batch(empty_delta, Some(scope))?;
        Ok(ReshardPruneOutcome {
            complete: true,
            documents_pruned: outcome.documents_pruned,
        })
    }
}

/// Response summary for `POST /admin/reshard:prune` (#1457 R1). `complete`
/// is `false` while the receiver is still accumulating chunks for this
/// `(to_map_version, bucket, collection_id, total_chunks)` group (the
/// common case for every chunk but the last); `documents_pruned` is only
/// meaningful once `complete` is `true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardPruneOutcome {
    pub complete: bool,
    pub documents_pruned: u32,
}

#[cfg(test)]
mod tests;
