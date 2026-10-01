//! The HNSW backend on the CPU: HnswCpuIndex over hnsw_rs, one graph variant
//! per metric's distance type, the restore diagnostics, and building an index
//! new, from checkpoint vectors with an optional graph cache, or from a vector
//! segment.

mod backend;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};

use crate::index::domain::vector::distance::normalize_unit_safe;
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::index::domain::vector::vector_store::VectorStore;
use crate::index::domain::vector::VectorIndex;
#[cfg(test)]
use crate::index::domain::vector::HNSW_SEARCH_POOLS;
use crate::shared_kernel::types::schema::{VectorMetric, VectorSpec};

// ---------------------------------------------------------------------------
// HNSW CPU backend
// ---------------------------------------------------------------------------

/// HNSW-on-CPU backend. Uses `hnsw_rs` 0.3 under the hood with the
/// appropriate distance type for the requested metric.
///
/// Because `hnsw_rs::Hnsw` is parameterised by a concrete distance
/// type, we keep three internal variants in `HnswInner` and dispatch
/// on the field's metric at construction time.
pub struct HnswCpuIndex {
    pub(in crate::index) inner: RwLock<HnswCpuInner>,
    pub(in crate::index) exact_scan_fallbacks: AtomicU64,
}

/// Diagnostic-only outcome for one HNSW restore attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::index) enum HnswGraphCacheResult {
    Hit,
    Absent,
    Rejected,
}

impl HnswGraphCacheResult {
    pub(in crate::index) fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Absent => "absent",
            Self::Rejected => "rejected",
        }
    }
}

/// Timings for one authoritative HNSW restoration. They are emitted by the
/// storage layer and do not alter graph-cache acceptance or fallback behavior.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::index) struct HnswRestoreTiming {
    pub(in crate::index) cache_result: Option<HnswGraphCacheResult>,
    pub(in crate::index) cache_prepare: Duration,
    pub(in crate::index) cache_fingerprint: Duration,
    pub(in crate::index) cache_manifest: Duration,
    pub(in crate::index) cache_payload_hash: Duration,
    pub(in crate::index) cache_materialize: Duration,
    pub(in crate::index) cache_deserialize: Duration,
    pub(in crate::index) cache_validate: Duration,
    pub(in crate::index) fallback_rebuild: Duration,
    pub(in crate::index) total: Duration,
}

impl HnswRestoreTiming {
    fn record_cache_timing(
        &mut self,
        cache: crate::index::infrastructure::vector::graph_cache::LoadTiming,
    ) {
        self.cache_prepare = cache.prepare;
        self.cache_fingerprint = cache.fingerprint;
        self.cache_manifest = cache.manifest;
        self.cache_payload_hash = cache.payload_hash;
        self.cache_materialize = cache.materialize;
        self.cache_deserialize = cache.deserialize;
        self.cache_validate = cache.validate;
    }
}

#[derive(Debug)]
pub(in crate::index) struct HnswRestoreFailure {
    pub(in crate::index) error: anyhow::Error,
    pub(in crate::index) timing: HnswRestoreTiming,
}

pub(in crate::index) struct HnswCpuInner {
    pub(in crate::index) store: VectorStore,
    /// external_id ↔ internal id (HNSW DataId).
    pub(in crate::index) eid_to_id: HashMap<String, usize>,
    pub(in crate::index) id_to_eid: HashMap<usize, String>,
    pub(in crate::index) next_id: usize,
    pub(in crate::index) hnsw: HnswBackend,
    /// Search-time beam width (recall/latency trade-off). Defaults to
    /// [`hnsw_search_ef`]; a tuning/bench harness can override it via
    /// [`HnswCpuIndex::set_ef_search`] without rebuilding the graph.
    pub(in crate::index) ef_search: usize,
}

/// HNSW dispatch by metric. We keep an `Option` because for f32-SQ
/// paths we sometimes want to lazily rebuild on demand; for the
/// metric-specific cases below we eagerly build at construction.
pub(in crate::index) enum HnswBackend {
    L2(hnsw_rs::hnsw::Hnsw<'static, f32, hnsw_rs::anndists::dist::DistL2>),
    // Cosine is served by DistDot over internally unit-normalized vectors:
    // dot(â,b̂) == cos, so the distance (1 − dot) is identical to DistCosine's,
    // but DistDot has a NEON/AVX kernel and skips the two per-comparison norm
    // recomputations that make DistCosine the hot-path cost.
    Cosine(hnsw_rs::hnsw::Hnsw<'static, f32, hnsw_rs::anndists::dist::DistDot>),
    Dot(hnsw_rs::hnsw::Hnsw<'static, f32, hnsw_rs::anndists::dist::DistDot>),
    Cached(Box<crate::index::infrastructure::vector::graph_cache::OwnedGraph>),
}

// SAFETY-equivalent: `hnsw_rs::Hnsw` is `Send + Sync` for the distance
// types we use; the wrapping `RwLock` enforces external borrow rules.
unsafe impl Send for HnswBackend {}

unsafe impl Sync for HnswBackend {}

// Graph-build quality drives recall (more so than search ef). M=32 +
// ef_construction=400 keeps recall@10 ≥ 0.95 out to 1M densely-clustered
// vectors, at the cost of a larger graph + slower build.
pub(in crate::index) const HNSW_MAX_NB_CONNECTION: usize = 32;

pub(in crate::index) const HNSW_EF_CONSTRUCTION: usize = 400;

pub(in crate::index) const HNSW_MAX_LAYER: usize = 16;

const HNSW_DEFAULT_MAX_ELEMENTS: usize = 10_000;

// ef controls the recall/latency trade-off at query time. The previous default
// (512) over-fetched ~4–5x more graph nodes than needed: the dense M=32 graph
// already holds recall@10 ≥ 0.95 at a far smaller beam. 128 is the recall-matched
// default (validated against brute-force ground truth on a clustered corpus);
// override with `LUMEN_HNSW_EF` for tuning sweeps.
const HNSW_SEARCH_EF_DEFAULT: usize = 128;

/// Resolve the default search-`ef` (env `LUMEN_HNSW_EF`, else the const).
/// Read once per index at construction; the bench harness can still override
/// per-index via [`HnswCpuIndex::set_ef_search`].
pub(in crate::index) fn hnsw_search_ef() -> usize {
    std::env::var("LUMEN_HNSW_EF")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&e| e > 0)
        .unwrap_or(HNSW_SEARCH_EF_DEFAULT)
}

impl HnswBackend {
    fn new(metric: VectorMetric, max_elements: usize) -> Self {
        match metric {
            VectorMetric::L2 => HnswBackend::L2(hnsw_rs::hnsw::Hnsw::new(
                HNSW_MAX_NB_CONNECTION,
                max_elements.max(HNSW_DEFAULT_MAX_ELEMENTS),
                HNSW_MAX_LAYER,
                HNSW_EF_CONSTRUCTION,
                hnsw_rs::anndists::dist::DistL2,
            )),
            VectorMetric::Cosine => HnswBackend::Cosine(hnsw_rs::hnsw::Hnsw::new(
                HNSW_MAX_NB_CONNECTION,
                max_elements.max(HNSW_DEFAULT_MAX_ELEMENTS),
                HNSW_MAX_LAYER,
                HNSW_EF_CONSTRUCTION,
                hnsw_rs::anndists::dist::DistDot,
            )),
            VectorMetric::Dot => HnswBackend::Dot(hnsw_rs::hnsw::Hnsw::new(
                HNSW_MAX_NB_CONNECTION,
                max_elements.max(HNSW_DEFAULT_MAX_ELEMENTS),
                HNSW_MAX_LAYER,
                HNSW_EF_CONSTRUCTION,
                hnsw_rs::anndists::dist::DistDot,
            )),
        }
    }

    pub(in crate::index) fn insert(&self, vec: &[f32], id: usize) {
        match self {
            HnswBackend::L2(h) => h.insert((vec, id)),
            // Cosine: feed the unit-normalized vector so DistDot == cosine.
            HnswBackend::Cosine(h) => h.insert((&normalize_unit_safe(vec), id)),
            HnswBackend::Dot(h) => h.insert((vec, id)),
            HnswBackend::Cached(h) => h.insert(vec, id),
        }
    }

    fn search(&self, vec: &[f32], k: usize, ef: usize) -> Vec<(usize, f32)> {
        #[cfg(test)]
        HNSW_SEARCH_POOLS.with(|pools| pools.borrow_mut().push(k));
        let ef = k.max(ef);
        let raw = match self {
            HnswBackend::L2(h) => h.search(vec, k, ef),
            HnswBackend::Cosine(h) => {
                let q = normalize_unit_safe(vec);
                h.search(&q, k, ef)
            }
            HnswBackend::Dot(h) => h.search(vec, k, ef),
            HnswBackend::Cached(h) => return h.search(vec, k, ef),
        };
        raw.into_iter().map(|n| (n.d_id, n.distance)).collect()
    }
}

impl HnswCpuIndex {
    pub(in crate::index) fn restore_with_graph_cache(
        spec: VectorSpec,
        vectors: Vec<(String, Vec<f32>)>,
        codebook: Option<ScalarCodebook>,
        directory: Option<&std::path::Path>,
    ) -> Result<Self> {
        Self::restore_with_graph_cache_timed(spec, vectors, codebook, directory)
            .map(|(index, _)| index)
            .map_err(|failure| failure.error)
    }

    pub(in crate::index) fn restore_with_graph_cache_timed(
        spec: VectorSpec,
        vectors: Vec<(String, Vec<f32>)>,
        codebook: Option<ScalarCodebook>,
        directory: Option<&std::path::Path>,
    ) -> std::result::Result<(Self, HnswRestoreTiming), HnswRestoreFailure> {
        let started = Instant::now();
        let mut timing = HnswRestoreTiming::default();
        if let Some(directory) = directory {
            match crate::index::infrastructure::vector::graph_cache::load::load_timed(
                spec, &vectors, codebook, directory,
            ) {
                Ok((Some(index), cache_timing)) => {
                    timing.cache_result = Some(HnswGraphCacheResult::Hit);
                    timing.record_cache_timing(cache_timing);
                    timing.total = started.elapsed();
                    tracing::info!(rows = vectors.len(), "HNSW graph cache loaded");
                    return Ok((index, timing));
                }
                Ok((None, cache_timing)) => {
                    timing.cache_result = Some(HnswGraphCacheResult::Absent);
                    timing.record_cache_timing(cache_timing);
                }
                Err(failure) => {
                    timing.cache_result = Some(HnswGraphCacheResult::Rejected);
                    timing.record_cache_timing(failure.timing);
                    tracing::warn!(error = %failure.error, "ignoring optional HNSW graph cache");
                }
            }
        } else {
            timing.cache_result = Some(HnswGraphCacheResult::Absent);
        }
        let rebuild_started = Instant::now();
        let rebuilt = Self::restore(spec, vectors, codebook);
        timing.fallback_rebuild = rebuild_started.elapsed();
        timing.total = started.elapsed();
        rebuilt
            .map(|index| (index, timing))
            .map_err(|error| HnswRestoreFailure { error, timing })
    }

    /// Construct a fresh, empty CPU HNSW index for the given spec.
    pub fn new(spec: VectorSpec) -> Self {
        Self {
            inner: RwLock::new(HnswCpuInner {
                store: VectorStore::new(spec),
                eid_to_id: HashMap::new(),
                id_to_eid: HashMap::new(),
                next_id: 0,
                hnsw: HnswBackend::new(spec.metric, HNSW_DEFAULT_MAX_ELEMENTS),
                ef_search: hnsw_search_ef(),
            }),
            exact_scan_fallbacks: AtomicU64::new(0),
        }
    }

    /// Restore an HNSW index from a snapshot — bulk-inserts the saved
    /// vectors back into a fresh graph. The codebook is carried over
    /// directly because the snapshot already encoded under it.
    pub fn restore(
        spec: VectorSpec,
        vectors: Vec<(String, Vec<f32>)>,
        codebook: Option<ScalarCodebook>,
    ) -> Result<Self> {
        let idx = Self::new(spec);
        {
            let mut inner = idx
                .inner
                .write()
                .map_err(|_| anyhow!("hnsw lock poisoned"))?;
            // Override the freshly-initialized codebook with the one
            // that was actually used to encode the persisted bytes.
            // Required so decode_sq() round-trips exactly.
            if codebook.is_some() {
                inner.store.codebook = codebook;
            }
        }
        for (eid, v) in vectors {
            idx.add(&eid, &v)?;
        }
        Ok(idx)
    }

    /// Reopen an HNSW index from a sealed vector segment plus its row→eid
    /// mapping, with NO snapshot — the segment counterpart of
    /// [`HnswCpuIndex::restore`], and the reason a field declared `hnsw-cpu`
    /// is still HNSW-backed after a restart.
    ///
    /// Row `i`'s vector is `seg.vector_at(i, dim)` (read once off the mmap) and
    /// its external id is `row_eids[i]`; each is re-inserted into a fresh
    /// graph. Unlike [`FlatCpuIndex::open_from_segment`], the vectors DO come
    /// back into RAM — a graph cannot be traversed against a demand-paged
    /// column it has no edges for — which is the cost the declared backend
    /// asks for and what [`VectorIndex::seal_releases_ram`] reports as `false`.
    /// The segment is consumed and dropped: once the rows are re-inserted the
    /// store owns them.
    ///
    /// This fallback builds the graph synchronously before serving. A matched
    /// graph cache can avoid this path during checkpoint recovery. Without
    /// cached edges, the graph must be rebuilt from the authoritative vectors.
    /// Log both edges so an operator can distinguish recovery from a hung node.
    ///
    /// [`FlatCpuIndex::open_from_segment`]: super::flat_cpu_index::FlatCpuIndex::open_from_segment
    pub fn open_from_segment(
        spec: VectorSpec,
        seg: std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        row_eids: Vec<String>,
    ) -> Result<Self> {
        let dim = spec.dim as usize;
        let rows = row_eids.len();
        tracing::info!(
            rows,
            dim,
            "rebuilding an hnsw-cpu graph from its sealed segment; the node does \
             not serve this field until it completes"
        );
        let started = std::time::Instant::now();
        let idx = Self::new(spec);
        for (row, eid) in row_eids.iter().enumerate() {
            let vector = seg.vector_at(row as u32, dim).ok_or_else(|| {
                anyhow!("vector segment is missing row {row} of {rows} (eid `{eid}`)")
            })?;
            idx.add(eid, vector)?;
        }
        debug_assert_eq!(idx.len(), rows);
        tracing::info!(
            rows,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "hnsw-cpu graph rebuilt"
        );
        Ok(idx)
    }

    /// Override the search-time `ef` (beam width). Used by tuning/bench
    /// harnesses to sweep recall/latency on an already-built graph; production
    /// uses the [`hnsw_search_ef`] default set at construction.
    pub fn set_ef_search(&self, ef: usize) {
        if let Ok(mut inner) = self.inner.write() {
            inner.ef_search = ef.max(1);
        }
    }

    /// Returns the monotonic count of queries that fell through to the exact store scan.
    pub fn exact_scan_fallbacks(&self) -> u64 {
        self.exact_scan_fallbacks.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for HnswCpuIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HnswCpuIndex")
            .field("len", &self.len())
            .finish()
    }
}
