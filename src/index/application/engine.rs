//! The Engine: the aggregate root of one shard's collections. Every use case
//! takes its state lock, applies to the collections inside, and publishes the
//! shard's indexed bytes; the child modules split its methods by use case.

mod checkpoint_delta;
mod checkpoint_flush;
mod checkpoint_freeze;
mod checkpoint_origins;
mod checkpoint_publish;
mod checkpoint_vectors;
pub(crate) mod collections;
pub(super) mod cost;
mod delete;
mod duplicates;
pub(crate) mod index;
mod lookup;
pub(crate) mod raft_dispatch;
mod reopen;
pub(super) mod replace;
pub(crate) mod reshard_apply;
pub(crate) mod reshard_prune;
mod restore;
mod search;
pub(crate) mod stats;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::index::application::engine::reshard_prune::{PruneAccumKey, PruneAccumState};
use crate::index::domain::engine_state::EngineState;
use crate::metrics::Metrics;

#[derive(Default)]
pub struct Engine {
    pub(crate) capture_barrier: crate::shared_kernel::capture_barrier::CaptureBarrier,
    pub(crate) state: RwLock<EngineState>,
    pub(crate) metrics: Metrics,
    draining: AtomicBool,
    /// #1457 R1: receiver-side accumulator for `POST /admin/reshard:prune`
    /// chunks, keyed by `(to_map_version, bucket, collection_id,
    /// total_chunks)`. See [`Engine::apply_reshard_prune_chunk`].
    prune_accumulator: Mutex<BTreeMap<PruneAccumKey, PruneAccumState>>,
    /// #1467 R4: monotonic call counter for the prune accumulator's
    /// age-based GC — incremented once per [`Engine::apply_reshard_prune_chunk`]
    /// call and stamped onto each new [`PruneAccumState`] as `created_tick`.
    /// A tick counter rather than wall-clock time keeps GC behavior
    /// deterministic in tests (no sleeping required to exercise it) and
    /// immune to system clock adjustments.
    prune_accum_tick: AtomicU64,
    // Release metadata charges only after the live state has dropped.
    pub(crate) changes: crate::index::application::admission::record_reservation::EngineChanges,
    pub(crate) layer_maintenance: Arc<crate::persistence::application::capacity::Registry>,
    // Last: files remain available until live readers and pending payloads drop.
    checkpoint_root_guards:
        Mutex<Vec<crate::persistence::infrastructure::segment_rdb_store::CheckpointRootGuard>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("draining", &self.draining)
            .finish()
    }
}

impl Engine {
    pub fn new() -> Self {
        // #2475: `metrics` needs `Metrics::new()`'s sentinel seeding (the
        // `raft_shard` "never touched by the raft poller" sentinel), not
        // the derived `Metrics::default()` every other `Engine` field is
        // fine relying on.
        Self {
            metrics: Metrics::new(),
            ..Default::default()
        }
    }

    pub(crate) fn retain_checkpoint_root(
        &self,
        root_guard: crate::persistence::infrastructure::segment_rdb_store::CheckpointRootGuard,
    ) {
        let mut guards = self
            .checkpoint_root_guards
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !guards
            .iter()
            .any(|existing| Arc::ptr_eq(existing, &root_guard))
        {
            guards.push(root_guard);
        }
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Switch the engine into "draining" — readiness flips to false so
    /// K8s stops sending new traffic. In-flight requests continue to
    /// succeed; the caller is expected to wait its `terminationGracePeriod`
    /// before terminating the process. Idempotent.
    pub fn start_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Publish the current engine-wide indexed footprint while the caller
    /// still holds the state lock that protected the mutation. The operator
    /// makes reshard decisions from this gauge, so waiting for an unrelated
    /// `/stats` request after a write or local restore can leave capacity
    /// control looking at zero (or stale) bytes.
    pub(super) fn publish_storage_bytes(&self, state: &EngineState) {
        let total_bytes: u64 = state
            .collections
            .values()
            .filter(|c| c.deleted_at.is_none())
            .flat_map(|c| c.fields.values())
            .map(|fi| fi.bytes())
            .sum();
        self.metrics.set_storage_bytes(total_bytes);
    }
}
