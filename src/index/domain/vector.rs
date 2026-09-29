//! Vector index backends for `FieldType::Vector`.
//!
//! Two CPU backends ship in this version:
//!
//! - [`HnswCpuIndex`] — pure-Rust HNSW via `hnsw_rs`. Default. Sub-ms
//!   kNN at ≤ 10 M vectors per field.
//! - [`FlatCpuIndex`] — exact CPU brute-force full scan. 100% recall;
//!   no index build. GPU-native vector search is a future chapter.
//!
//! Scalar quantization (f32 → u8 linear) is applied transparently
//! when the field's `VectorSpec::quantize` slot is `Some(Sq)`. The
//! codebook is learned at insert time (running min/max) and snapshot
//! together with the raw vectors on `Engine::snapshot()`.
//!
//! Score semantics: every backend returns `score = -distance` so
//! `score` is monotone-decreasing across the top-K — higher = better,
//! matching the BM25 contract used by the text path.

pub(in crate::index) mod distance;
pub(crate) mod flat_cpu_index;
pub(crate) mod hnsw_cpu_index;
pub(crate) mod quantize;
pub(in crate::index) mod vector_store;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};

use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::quantize::ScalarCodebook;
use crate::shared_kernel::types::schema::VectorSpec;

#[cfg(test)]
thread_local! {
    pub(crate) static HNSW_CHECKPOINT_FULL_SCANS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static HNSW_SEARCH_POOLS: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
    static VECTOR_STORE_OWNED_SCANS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// ---------------------------------------------------------------------------
// VectorIndex trait
// ---------------------------------------------------------------------------

/// Common backend contract — every concrete index implementation
/// (HNSW, flat CPU brute force) goes through this trait so the storage
/// layer doesn't care which one is in use.
pub trait VectorIndex: Send + Sync {
    /// Optional shutdown-only acceleration. This is never durable authority.
    #[doc(hidden)]
    fn save_graph_cache(&self, _directory: &std::path::Path) -> Result<bool> {
        Ok(false)
    }
    /// Insert (or overwrite) the vector associated with `external_id`.
    fn add(&self, external_id: &str, vector: &[f32]) -> Result<()>;

    /// Consume the timings for the most recent HNSW write-lock acquisition on
    /// this calling path. Other backends have no HNSW lock and return `None`.
    /// The caller records this only after releasing its engine writer lock.
    fn take_hnsw_write_lock_timing(&self) -> Option<(Duration, Duration)> {
        None
    }

    /// Consume the graph-rebuild interval from the latest HNSW add on this
    /// calling path. Other backends, and normal HNSW adds, return `None`.
    fn take_hnsw_graph_rebuild_timing(&self) -> Option<Duration> {
        None
    }

    /// Remove the vector for `external_id`. No-op if it isn't present.
    /// Returns `true` if a vector was removed.
    fn remove(&self, external_id: &str) -> Result<bool>;

    /// Read one current vector for an incremental checkpoint. Backends must
    /// provide a keyed lookup; a full-corpus snapshot is not a fallback.
    fn checkpoint_vector(&self, _external_id: &str) -> Result<Option<Vec<f32>>> {
        bail!("backend does not support keyed checkpoint capture")
    }

    /// Internal preparation snapshot for a staged Vector checkpoint row. This
    /// copies only the scalar-quantization codebook; it never enumerates or
    /// decodes stored vectors. Callers must separately recheck their apply cut.
    #[doc(hidden)]
    fn checkpoint_codebook_for_preparation(&self) -> Result<Option<ScalarCodebook>> {
        bail!("vector backend does not support codebook preparation snapshots")
    }

    /// Install an already decoded checkpoint value. Cold readers use this to
    /// avoid quantizing persisted vectors again while composing their layers.
    fn restore_checkpoint_vector(&self, external_id: &str, vector: &[f32]) -> Result<()> {
        self.add(external_id, vector)
    }

    /// Like [`VectorIndex::search_knn`] but only `external_id`s for
    /// which `allow` returns `true` are eligible for the result.
    ///
    /// This is the primitive behind filter-correct kNN (`knn AND
    /// <filter>`): instead of taking the global top-`k` and intersecting
    /// the filter afterwards (post-filter — recall collapses when the
    /// filter is selective), the candidate pool is widened until `k`
    /// *allowed* neighbours are found or the index is exhausted, so the
    /// caller gets the nearest `k` neighbours **within the filtered
    /// set**. For an always-true `allow` this is identical to
    /// `search_knn`, with no extra work on the hot path.
    fn search_knn_filtered(
        &self,
        query: &[f32],
        k: usize,
        allow: &dyn Fn(&str) -> bool,
    ) -> Result<Vec<(String, f32)>>;

    /// Return the top-`k` nearest external_ids and their scores
    /// (`score = -distance` so higher = better). Vectors shorter than
    /// the index's declared `dim` are rejected.
    fn search_knn(&self, query: &[f32], k: usize) -> Result<Vec<(String, f32)>> {
        self.search_knn_filtered(query, k, &|_| true)
    }

    /// Batched top-`k` kNN: answer many query vectors in one call, returning
    /// one result list per query (same order as `queries`). This is the heavy
    /// RAG / re-rank / fan-out access pattern. The default implementation loops
    /// [`VectorIndex::search_knn`]; a backend may override it to amortize
    /// per-query setup across the batch.
    fn search_knn_batch(&self, queries: &[Vec<f32>], k: usize) -> Result<Vec<Vec<(String, f32)>>> {
        queries.iter().map(|q| self.search_knn(q, k)).collect()
    }

    /// Number of vectors currently held by the index.
    fn len(&self) -> usize;

    /// Whether the index has zero vectors.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Materialize every stored (eid, vector) pair for snapshot, plus
    /// the scalar codebook when SQ is enabled. Default impl returns
    /// an empty dump — backends that intend to be snapshotted must
    /// override this.
    fn dump_for_snapshot(&self) -> Result<(Vec<(String, Vec<f32>)>, Option<ScalarCodebook>)> {
        Ok((Vec::new(), None))
    }

    /// TEST SEAM (Stage 2 Phase 2d): seal the exact-CPU flat corpus into a
    /// columnar mmap vector segment at `path` and attach it, so the flat kNN
    /// scan reads each vector zero-copy off the page instead of the in-RAM
    /// `FlatVecs::data`. The corpus row order (the eid↔row mapping) is locked
    /// when the cached flat buffer is built and is reused verbatim, so the
    /// segment-backed scan returns byte-identical (eid, distance) pairs.
    ///
    /// Returns `Ok(Some(n))` with the sealed vector count for a [`FlatCpuIndex`];
    /// `Ok(None)` for any other backend (HNSW is out of scope for this slice).
    /// No scalar quantization — the segment stores decoded `f32`, so the scan
    /// stays a pure `&[f32]` read.
    #[cfg(test)]
    fn __seal_flat_to_segment(&self, _path: &std::path::Path) -> Result<Option<u32>> {
        Ok(None)
    }

    /// PRODUCTION seal (Stage 2 Phase 2f-1): seal this backend's corpus into a
    /// columnar mmap vector segment at `path` — decoded `f32` rows in row
    /// order — and return `Ok(Some(row_eids))`, the eid of each sealed row in
    /// segment-row order, so the caller can persist that row→eid mapping (the
    /// segment stores only the f32 rows) and rebuild the index on reopen.
    ///
    /// Every backend that can be checkpointed implements this: reopen requires
    /// the `<field>.eids.lseg` sidecar unconditionally for any `Vector` field,
    /// so a backend returning `Ok(None)` here cannot be checkpointed at all
    /// (issue #3951). What backends differ on is whether sealing also FREES the
    /// in-RAM copy — see [`VectorIndex::seal_releases_ram`], which is what the
    /// caller asks instead of inspecting the declared backend. The default impl
    /// no-ops for a backend with no segment format yet.
    fn seal_to_segment_prod(&self, _path: &std::path::Path) -> Result<Option<Vec<String>>> {
        Ok(None)
    }

    /// Seal an unpublished checkpoint with its actual WAL cut. The default
    /// preserves compatibility for third-party backends using the legacy hook.
    fn seal_to_segment_prod_at(
        &self,
        path: &std::path::Path,
        _sequence: u64,
    ) -> Result<Option<Vec<String>>> {
        self.seal_to_segment_prod(path)
    }

    /// Whether a successful [`VectorIndex::seal_to_segment_prod`] left this
    /// index holding NO in-RAM copy of the sealed vectors, so the caller should
    /// zero the field's reported byte footprint.
    ///
    /// This is a property of what the seal implementation actually did, not of
    /// the backend the schema declares, and only the index can answer it: a
    /// flat index hands the mmap its whole `data` buffer and keeps nothing,
    /// while an HNSW index keeps its graph and raw vectors resident so the live
    /// process goes on serving approximate kNN off the graph. A caller that
    /// decided this by matching on `VectorSpec::backend` would have to be
    /// edited again for every backend added, and would silently report the
    /// wrong footprint until someone remembered to.
    fn seal_releases_ram(&self) -> bool {
        false
    }

    fn attach_checkpoint_delta(
        &self,
        _reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        _external_ids: &[String],
        _acknowledged: &[bool],
    ) -> Result<()> {
        Ok(())
    }

    /// Immutable inputs used to verify a staged compaction at publication.
    fn checkpoint_delta_readers(
        &self,
    ) -> Vec<Arc<crate::persistence::infrastructure::segment::SegmentReader>> {
        Vec::new()
    }

    fn checkpoint_base_reader(
        &self,
    ) -> Option<Arc<crate::persistence::infrastructure::segment::SegmentReader>> {
        None
    }

    fn replace_checkpoint_base(
        &self,
        _base: &Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        _inputs: &[Arc<crate::persistence::infrastructure::segment::SegmentReader>],
        _reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        _external_ids: &[String],
    ) -> Result<()> {
        bail!("vector backend does not support mapped base replacement")
    }

    fn replace_checkpoint_deltas(
        &self,
        _inputs: &[Arc<crate::persistence::infrastructure::segment::SegmentReader>],
        _reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        _external_ids: &[String],
    ) -> Result<()> {
        bail!("vector backend does not support checkpoint delta replacement")
    }

    fn resident_vector_payload_rows(&self) -> usize {
        self.len()
    }
    /// The existing field-byte estimate for payloads still resident after a
    /// checkpoint. Backends that retain their complete corpus keep the
    /// caller's estimate by returning None.
    fn checkpoint_resident_bytes(&self) -> Option<u64> {
        None
    }
    fn has_checkpoint_mapping(&self) -> bool {
        false
    }

    /// Install a newly written full vector base.  `acknowledged[eid] == false`
    /// means this live index has a later update or delete which must remain
    /// above the new mmap base.
    fn install_checkpoint_base(
        &self,
        _reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        _external_ids: &[String],
        _acknowledged: &HashMap<String, bool>,
    ) -> Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Backend selection
// ---------------------------------------------------------------------------

/// Construct the backend implied by `spec.backend`. This version ships
/// CPU backends only (HNSW + flat brute-force); GPU-native vector search
/// is a future chapter.
pub fn open_backend(spec: VectorSpec) -> Box<dyn VectorIndex> {
    match spec.backend {
        crate::shared_kernel::types::schema::VectorBackend::HnswCpu => {
            Box::new(HnswCpuIndex::new(spec))
        }
        crate::shared_kernel::types::schema::VectorBackend::FlatCpu => {
            Box::new(FlatCpuIndex::new(spec))
        }
    }
}

#[cfg(test)]
mod tests;
