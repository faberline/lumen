//! CheckpointSink, the durable on-demand checkpoint the reshard driver's
//! cutover waits on, with the planned-restart HNSW cache seal, its receipt and
//! the errors that refuse it; and NoopCheckpoint, the default when no durable
//! store is configured.

use anyhow::Result;
use async_trait::async_trait;

/// Forces a synchronous, awaited durability checkpoint of the live engine
/// state (#1389). The reshard driver's `advance_catching_up` cutover
/// (`operator::application::reshard_driver::phases`) calls
/// `POST /admin/checkpoint` — which routes here —
/// on every shard it just migrated data into or evicted data from, and waits
/// for the response before flipping `spec.shardMap` and triggering the
/// cutover rolling restart. `Engine::apply_reshard_batch`/`evict_not_owned`
/// in `index::application::engine::reshard_apply` (#1380) mutate engine
/// state directly rather than through
/// `WriteCoordinator`/the AOF, so — unlike ordinary writes — their durability
/// is not implied by `applied_seq()`; this seam is what makes it durable
/// on-demand instead of only on the next periodic `LUMEN_SNAPSHOT_SECS` tick.
///
/// [`NoopCheckpoint`] is the default (no `--data-dir`/non-segment-persistence
/// deployments, including every existing test `AppState`): `checkpoint_now`
/// trivially returns `Ok(false)` (nothing configured to persist, so nothing
/// to lose across an in-process test's non-restart). The server binary wires
/// a real segment-checkpoint-backed implementation whenever
/// `--persistence=segment` + `--data-dir` are configured — exactly the
/// combination the operator now renders unconditionally at
/// `replicasPerShard <= 1` (#1387), which is the same topology the reshard
/// driver is scoped to (see `reshard_driver`'s "Scope rail" doc).
#[async_trait]
pub trait CheckpointSink: Send + Sync {
    /// Persist current engine state durably and return only once the write
    /// is committed. `Ok(true)` when a checkpoint was actually written;
    /// `Ok(false)` when no durable store is configured (a checkpoint request
    /// against such a deployment is vacuously satisfied — there is nothing
    /// on disk to fall behind). `Err` on a real write failure, which callers
    /// (the reshard driver) must treat as "not yet durable" and retry.
    async fn checkpoint_now(&self) -> Result<bool>;

    /// Publish an optional HNSW recovery cache under a durable mutation
    /// boundary. Only segment persistence implements this: a memory-only or
    /// non-segment process must refuse the planned-restart optimization rather
    /// than claim a cache it cannot make durable.
    async fn seal_hnsw_graph_cache(&self) -> Result<HnswCacheSealReceipt> {
        Err(anyhow::Error::new(HnswCacheSealUnavailable(
            "HNSW restart cache sealing requires configured segment persistence".to_string(),
        )))
    }
}

/// Durable boundary named in a successful planned-restart HNSW cache receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HnswCacheDurability {
    AofSynced,
    CheckpointCommitted,
}

impl HnswCacheDurability {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AofSynced => "aof_synced",
            Self::CheckpointCommitted => "checkpoint_committed",
        }
    }
}

/// Process-local receipt for an optional HNSW recovery cache publication.
/// `persistence::interfaces::http::checkpoint` emits its fixed JSON shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HnswCacheSealReceipt {
    pub cache_fields: usize,
    pub durability: HnswCacheDurability,
    pub mutation_epoch: u64,
    pub mutation_apply_revision: u64,
}

/// The server has no configured segment cache root or no live HNSW graph to
/// seal. A caller can retry after its planned restart input becomes available.
#[derive(Debug)]
pub(crate) struct HnswCacheSealUnavailable(pub(crate) String);

impl std::fmt::Display for HnswCacheSealUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for HnswCacheSealUnavailable {}

/// A concurrent checkpoint or replacement changed the live capture stamp
/// while an optional cache was being written. The cache cannot be reused.
#[derive(Debug)]
pub(crate) struct HnswCacheSealInvalidated(pub(crate) String);

impl std::fmt::Display for HnswCacheSealInvalidated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for HnswCacheSealInvalidated {}

/// Default [`CheckpointSink`] for deployments/tests with no configured
/// durable store — see the trait doc.
pub(crate) struct NoopCheckpoint;

#[async_trait]
impl CheckpointSink for NoopCheckpoint {
    async fn checkpoint_now(&self) -> Result<bool> {
        Ok(false)
    }
}
