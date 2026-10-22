//! Immutable scalar and text views over one base and ordered sparse deltas.
//!
//! Coverage includes absent rows. It therefore hides every older value even
//! when the newest row is a deletion. Only the ID maps live in memory; payload
//! and postings stay in their original mmaps. Dictionary walks keep one head
//! per segment and never materialize the complete dictionary.

mod checkpoint_publication;
pub(crate) mod layers;
pub(crate) mod replacement;
pub(crate) mod scalar_reader;
pub(crate) mod term_cursor;
pub(crate) mod text_reader;

use crate::persistence::infrastructure::segment::{
    reader_cache::{cached_text_posting_weight, posting_cache_bytes, CachedTextPosting},
    SegmentReader,
};
use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

pub(crate) use checkpoint_publication::{PreparedScalarPublication, ScalarCheckpointCut};

#[derive(Debug)]
struct DeltaLayer {
    reader: Arc<SegmentReader>,
    ids: Vec<u32>,
    local_by_global: BTreeMap<u32, u32>,
    coverage: RoaringBitmap,
    /// Private layers are post-capture runtime state. They never describe the
    /// durable catalog and generic compaction cannot consume them.
    private: bool,
}

/// Catalog deltas and post-checkpoint private deltas share one query-time
/// composition bound. The base reader is not an incremental layer.
pub(crate) const MAX_INCREMENTAL_LAYERS: usize = 16;

/// A Text token's posting resolved for a small candidate set by
/// [`ComposedSegmentReader::text_posting_at`] (#4246).
#[derive(Debug)]
pub(crate) enum TextPostingAt {
    /// The full composed posting was already resident; the caller uses it as
    /// it would a `text_postings_arc` result.
    Cached(CachedTextPosting),
    /// The posting was cold and was streamed, not materialized: the exact df
    /// of the composed posting minus hidden ids, and the `(id, tf)` of every
    /// candidate that holds the token, ascending by id.
    Sparse { df: usize, hits: Vec<(u32, u32)> },
}

// #4246 cost oracle: every Text posting `text_tokens_all` materializes as an
// owned pair of vectors. A distinct-term count must materialize none.
#[cfg(test)]
thread_local! {
    static TEXT_POSTING_CLONES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_text_posting_clones() {
    TEXT_POSTING_CLONES.with(|clones| clones.set(0));
}

#[cfg(test)]
pub(crate) fn text_posting_clones() -> u64 {
    TEXT_POSTING_CLONES.with(std::cell::Cell::get)
}

// #4246 cost oracle: dictionary terms visited and composed postings decoded on
// behalf of one read. A distinct-term count must be independent of both.
#[cfg(test)]
thread_local! {
    static TEXT_TERM_PROBES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_text_term_probes() {
    TEXT_TERM_PROBES.with(|probes| probes.set(0));
}

#[cfg(test)]
pub(crate) fn text_term_probes() -> u64 {
    TEXT_TERM_PROBES.with(std::cell::Cell::get)
}

#[inline]
fn note_text_term_probes(_probes: u64) {
    #[cfg(test)]
    TEXT_TERM_PROBES.with(|probes| probes.set(probes.get().saturating_add(_probes)));
}

#[inline]
fn note_text_posting_clones(_clones: u64) {
    #[cfg(test)]
    TEXT_POSTING_CLONES.with(|clones| clones.set(clones.get().saturating_add(_clones)));
}

#[derive(Debug, thiserror::Error)]
#[error(
    "scalar field has reached the {MAX_INCREMENTAL_LAYERS}-layer incremental composition limit"
)]
pub(crate) struct LayerCapacityFull;

#[derive(Clone, Debug)]
pub(crate) struct ComposedSegmentReader {
    base: Arc<SegmentReader>,
    base_map: Option<Arc<DeltaLayer>>,
    layers: Vec<Arc<DeltaLayer>>,
    n_docs: u32,
    /// False only for the synthetic empty base used before the first catalog
    /// checkpoint. It must never escape as a catalog identity or compaction input.
    has_catalog_base: bool,
    /// EXACT count of dictionary terms with a non-empty composed posting, when
    /// it is known without a walk (#4246).
    ///
    /// `Some(n)` is an invariant, not a hint: it is carried only while every
    /// composition step has been ADDITIVE — a sealed base (whose projection
    /// writes only terms that had a live document) plus delta layers that
    /// covered no runtime ID which already held a Text value. Under that
    /// invariant no older posting can be shadowed empty, so the composed term
    /// set is exactly the union of the layer dictionaries and each attach adds
    /// the delta's terms that no lower dictionary holds — work bounded by one
    /// checkpoint interval of writes, paid once per publication. Any step that
    /// can hide an older row (an update, a delete, a compaction, a base
    /// replacement) sets `None`, and the reader that needs the number walks for
    /// it instead of reporting an approximation.
    distinct_terms: Option<u64>,
    /// Query-time caches derived purely from this composition (#4246); see
    /// [`QueryCache`]. Shared by clones of the same composition, fresh for
    /// every constructor that changes the base or the layers.
    query_cache: Arc<QueryCache>,
}

/// Results a `ComposedSegmentReader` derives from nothing but its immutable
/// base + layers, so they stay valid for the composition's whole life
/// (#4246). Without them a composition that is not a bare dense base paid,
/// on EVERY probe, a full re-decode + re-merge of a token's posting through
/// every layer (`text_postings_arc`) and one `BTreeMap` probe per layer per
/// scored row for its doc length (`text_doc_len`) — 20 × 500k rows and 500k
/// winner walks per cold ngram AND query against the 500k-hot-doc fixture.
pub(crate) struct QueryCache {
    /// Composed `(docids, tfs)` per token, byte-budgeted with the same
    /// weigher and `LUMEN_SEG_POSTING_CACHE_MB` budget as the base reader's
    /// own `text_posting_cache`.
    text_postings: moka::sync::Cache<String, CachedTextPosting>,
    /// Global id → text doc length for every `id < n_docs`, materialized
    /// once by the first hot BM25 walk (see `text_doc_lens`).
    text_doc_lens: OnceLock<Vec<u32>>,
    /// Coverage unions shared by every token's `merge_text_postings`,
    /// materialized once per composition (see [`CoverageUnions`]).
    coverage_unions: OnceLock<CoverageUnions>,
}

/// What a composed text posting merge needs to know about its layers, once
/// per composition rather than once per token (#4246).
///
/// A base row survives only when NO layer covers its id, and a layer row only
/// when no NEWER layer covers it. With those two facts precomputed, one token's
/// merge is a filter of the base posting by `all` plus a filter of each layer's
/// (small) posting by its `newer_than` union — O(|coverage| · log df) galloping
/// over a sorted base posting instead of the O(df × layers) `retain` sweep that
/// cost ~58 ms per distinct ngram token against a 500k-row base in Docker.
struct CoverageUnions {
    /// Union of every layer's coverage.
    all: RoaringBitmap,
    /// `newer_than[l]` is the union of the coverage of every layer newer than
    /// layer `l` (empty for the newest layer).
    newer_than: Vec<RoaringBitmap>,
    /// Whether `base_map.ids` is strictly ascending, so the mapped base posting
    /// is already sorted by global id and needs no sort.
    base_map_ascending: bool,
}

impl Default for QueryCache {
    fn default() -> Self {
        Self {
            text_postings: moka::sync::Cache::builder()
                .weigher(|k: &String, v: &CachedTextPosting| {
                    cached_text_posting_weight(v)
                        .saturating_add(k.len().min(u32::MAX as usize) as u32)
                })
                .max_capacity(posting_cache_bytes())
                .build(),
            text_doc_lens: OnceLock::new(),
            coverage_unions: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for QueryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryCache")
            .field("text_postings", &self.text_postings.entry_count())
            .field("text_doc_lens", &self.text_doc_lens.get().map(Vec::len))
            .field(
                "coverage_unions",
                &self.coverage_unions.get().map(|u| u.all.len()),
            )
            .finish()
    }
}

impl ComposedSegmentReader {
    pub(crate) fn incremental_layer_count(&self) -> usize {
        self.layers.len()
    }

    pub(crate) fn has_private_layers(&self) -> bool {
        self.layers.iter().any(|layer| layer.private)
    }

    pub(crate) fn can_append_incremental(&self) -> Result<()> {
        if self.incremental_layer_count() >= MAX_INCREMENTAL_LAYERS {
            return Err(anyhow::Error::new(LayerCapacityFull));
        }
        Ok(())
    }
}

/// Row maps and coverage are built before publication. Installing this object
/// checks immutable identities and moves only layer Arcs, never row arrays.
pub(crate) struct PreparedScalarReplacement {
    base: Arc<SegmentReader>,
    inputs: Vec<Arc<SegmentReader>>,
    replacement: Arc<DeltaLayer>,
    includes_base: bool,
    n_docs: u32,
    has_catalog_base: bool,
    catalog_len: usize,
}

impl DeltaLayer {
    fn new(reader: Arc<SegmentReader>, ids: Vec<u32>) -> Result<Self> {
        if ids.len() != reader.n_docs() as usize {
            bail!("delta local ID count does not match segment");
        }
        let mut local_by_global = BTreeMap::new();
        let mut coverage = RoaringBitmap::new();
        for (local, &global) in ids.iter().enumerate() {
            if local_by_global.insert(global, local as u32).is_some() {
                bail!("duplicate delta runtime ID");
            }
            coverage.insert(global);
        }
        Ok(Self {
            reader,
            ids,
            local_by_global,
            coverage,
            private: false,
        })
    }
}

#[cfg(test)]
mod tests;
