//! Process memory the checkpoint sink keeps and never writes to disk: the
//! counter that numbers diagnostic checkpoint attempts, and the HNSW cache seal
//! markers, keyed by the engine and store allocation each belongs to.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, OnceLock, Weak};

use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

pub(in crate::persistence) static NEXT_CHECKPOINT_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

/// A successfully sealed cache is valid only for this exact live engine and
/// store allocation. Weak references prevent a dropped test/server allocation
/// from leaving a stale marker if its address is later reused. This process
/// memory is intentionally never written to the checkpoint or graph cache.
pub(in crate::persistence) struct HnswCacheSealMarker {
    pub(in crate::persistence) engine: Weak<Engine>,
    pub(in crate::persistence) store: Weak<SegmentRdbStore>,
    pub(in crate::persistence) stamp: crate::shared_kernel::capture_barrier::MutationStamp,
}

pub(in crate::persistence) type HnswCacheSealKey = (usize, usize);

pub(in crate::persistence) static HNSW_CACHE_SEALS: OnceLock<
    Mutex<HashMap<HnswCacheSealKey, HnswCacheSealMarker>>,
> = OnceLock::new();
