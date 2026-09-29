//! A number field's index: each value's doc bitmap in value order, the
//! doc-to-value forward map and the lazy order statistics that answer a range
//! estimate, over a sealed segment base plus the live tail.

pub(crate) mod range;
pub(crate) mod sorted;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::RwLock;

use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::query::range::in_sortable_bits_range;
use crate::index::domain::sortable_f64::{SortableF64, MISSING_SORTABLE_F64_BITS};
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

/// Order-statistic snapshot of the live `values` tree: distinct value bits
/// ascending plus cumulative doc counts. Built lazily the first time a range
/// ESTIMATE walks past [`RANGE_STATS_BUILD_THRESHOLD`] distinct keys; dropped
/// with the keyword range caches on any write batch, delete, or seal. Both
/// `range_df` and `range_distinct_count` answer from two binary searches —
/// O(log distinct) instead of an O(distinct-in-range) tree walk per query.
/// Estimation-only state: it never feeds result evaluation, so a rebuild race
/// can at worst pick a different (still correct) plan.
#[derive(Debug)]
pub(crate) struct NumberRangeStats {
    /// Distinct live values (sortable bits), ascending.
    keys: Vec<u64>,
    /// `cum_df[i]` = total docs across `keys[..i]`; `len = keys.len() + 1`.
    cum_df: Vec<u64>,
}

/// Distinct-key walk budget for a single range estimate before the walk is
/// abandoned and [`NumberRangeStats`] is built instead. Narrow ranges stay on
/// the exact walk and never pay the build; one wide estimate pays roughly two
/// walks (the abandoned prefix + the build) and every later estimate on the
/// unchanged tree is O(log distinct).
const RANGE_STATS_BUILD_THRESHOLD: u64 = 1024;

impl NumberRangeStats {
    pub(crate) fn build(values: &BTreeMap<SortableF64, RoaringBitmap>) -> Self {
        let mut keys = Vec::with_capacity(values.len());
        let mut cum_df = Vec::with_capacity(values.len() + 1);
        cum_df.push(0);
        let mut running = 0u64;
        for (k, set) in values {
            keys.push(k.bits());
            running += set.len();
            cum_df.push(running);
        }
        Self { keys, cum_df }
    }

    /// `(distinct, df)` over the half-open index window the bounds select.
    pub(crate) fn range(
        &self,
        low: std::ops::Bound<SortableF64>,
        high: std::ops::Bound<SortableF64>,
    ) -> (u64, u64) {
        use std::ops::Bound;
        let lo_ix = match low {
            Bound::Unbounded => 0,
            Bound::Included(x) => self.keys.partition_point(|&k| k < x.bits()),
            Bound::Excluded(x) => self.keys.partition_point(|&k| k <= x.bits()),
        };
        let hi_ix = match high {
            Bound::Unbounded => self.keys.len(),
            Bound::Included(x) => self.keys.partition_point(|&k| k <= x.bits()),
            Bound::Excluded(x) => self.keys.partition_point(|&k| k < x.bits()),
        };
        if hi_ix <= lo_ix {
            return (0, 0);
        }
        (
            (hi_ix - lo_ix) as u64,
            self.cum_df[hi_ix] - self.cum_df[lo_ix],
        )
    }
}

#[derive(Debug, Default)]
pub(crate) struct NumberIndex {
    pub(crate) values: BTreeMap<SortableF64, RoaringBitmap>,
    /// Side-index of LIVE-tail values whose posting holds >= 2 docs (see
    /// `KeywordIndex::dup_values`); drives `duplicates` without a full
    /// `values` scan. Cleared at seal.
    pub(crate) dup_values: BTreeSet<SortableF64>,
    pub(crate) forward: FastHashMap<u32, SortableF64>,
    /// Dense live-tail forward cache for hot per-doc predicates. `forward`
    /// remains the sparse ownership/snapshot map; this avoids HashMap lookup in
    /// bitmap-driven numeric filters on large in-memory indexes.
    pub(crate) dense_forward: Vec<u64>,
    /// Per `(keyword field, term)` numeric skip lists. Each cached term stores its
    /// posting docids sorted by this number field's sortable bits, so
    /// `Term(s)(keyword) ∩ Range(number)` counts by binary-searching one small list
    /// instead of random-probing the numeric forward column. This is deliberately
    /// lazy per term, not whole-field, so the segment tier does not materialize a
    /// high-cardinality keyword field into RAM just because one hot term was read.
    pub(crate) keyword_range_cache: RwLock<FastHashMap<String, std::sync::Arc<Vec<(u64, u32)>>>>,
    /// Per `(keyword field, term, numeric range)` candidate bitmaps for hot
    /// `Match(text) ∩ Term(keyword) ∩ Range(number)` shapes. This is query-only
    /// derived state, invalidated with `keyword_range_cache`.
    pub(crate) keyword_range_bitmap_cache:
        RwLock<FastHashMap<String, std::sync::Arc<RoaringBitmap>>>,
    /// Lazy [`NumberRangeStats`] over the live `values` tree for planner range
    /// ESTIMATES (`range_df` / `range_distinct_count`). Query-only derived
    /// state, invalidated with `keyword_range_cache`.
    pub(crate) range_stats: RwLock<Option<std::sync::Arc<NumberRangeStats>>>,
    pub(crate) bytes: u64,
    /// Stage 2 disk-tier (Phase 2c): a sealed columnar mmap segment covering
    /// doc ids `[0..n_docs)`. When present, per-doc Number PREDICATE point
    /// lookups (`number_at`) read the segment for sealed ids and the in-RAM
    /// `forward` tail for ids `>= n_docs`. DEFAULTS to `None`; while it is
    /// `None` (nothing sealed) every
    /// read path is byte-for-byte the in-RAM path. Purely additive: the write
    /// path, `values` range walk, and inverted index are untouched.
    ///
    /// Phase 2h-3: the segment now ALSO carries the SORTED-VALUE range index
    /// (`ROLE_NUMBER_SORTED` + `ROLE_NUMBER_POSTINGS`), so range / exact /
    /// boolean queries drive from the mmap (`value_postings` / `range_postings`)
    /// and the in-RAM `values` BTreeMap is DROPPED at seal (RAM after reopen is
    /// O(live tail), not O(distinct numeric values)).
    pub(crate) segment: Option<std::sync::Arc<ComposedSegmentReader>>,
    /// QUERY-TIME TOMBSTONE (Phase 2h-3): base docids `[0..seg.n_docs)` deleted
    /// SINCE the last seal. The inverted/range `values` index was DROPPED to disk
    /// at seal, so `drop_eid` can no longer remove a sealed base id from the
    /// immutable on-disk postings — instead it records the id here, and the
    /// segment-ON accessors (`value_postings`, `range_postings`,
    /// duplicate / unique-value enumeration) SUBTRACT this set so a delete is
    /// reflected before the next re-seal. The next seal bakes the deletions into
    /// the new segment (its `live(id)` gather excludes them) and this is reset to
    /// empty. Live-tail ids (`>= seg.n_docs`) are NOT tombstoned — they are
    /// deleted directly out of the in-RAM `values` tail. DEFAULTS empty; stays
    /// empty while no segment is attached. The exact reuse of the Keyword 2h-1 /
    /// Set 2h-2 tombstone (same shape, same four touch-points: record in
    /// `drop_eid`, subtract in the posting accessors, exclude in `live_values`,
    /// clear at re-seal).
    pub(crate) tombstones: RoaringBitmap,
}

impl NumberIndex {
    /// The doc's value for a per-doc PREDICATE point lookup. When a sealed
    /// segment is attached it serves ids in its covered range `[0..n_docs)`
    /// (the live tail keeps ids `>= n_docs`); otherwise — and always when the
    /// no segment is attached — this is exactly the live forward value, served
    /// through a dense cache before falling back to the sparse map.
    ///
    /// The segment stores raw `f64` bits, so a hit is re-wrapped through
    /// `SortableF64::new`; that is the same order-preserving transform the live
    /// `forward` entry already holds, so `in_range` / equality compare
    /// identically. A NaN can never reach the index (rejected at index time),
    /// so `SortableF64::new` cannot fail here; if it ever did we fall back to
    /// the live map rather than panic.
    #[inline]
    pub(crate) fn number_at(&self, id: u32) -> Option<SortableF64> {
        self.number_bits_at(id).map(SortableF64::from_bits)
    }

    #[inline]
    pub(crate) fn live_number_at(&self, id: u32) -> Option<SortableF64> {
        self.number_at(id)
    }

    #[inline]
    pub(crate) fn number_bits_at(&self, id: u32) -> Option<u64> {
        if let Some(bits) = self.dense_forward.get(id as usize).copied() {
            if bits != MISSING_SORTABLE_F64_BITS {
                return Some(bits);
            }
        }
        if let Some(value) = self.forward.get(&id) {
            return Some(value.bits());
        }
        if self.tombstones.contains(id) {
            return None;
        }
        self.segment.as_ref().and_then(|seg| {
            (id < seg.n_docs())
                .then(|| seg.number_at(id))
                .flatten()
                .and_then(|x| SortableF64::new(x).ok())
                .map(|s| s.bits())
        })
    }

    #[inline]
    pub(crate) fn number_in_bounds(
        &self,
        id: u32,
        lo: &std::ops::Bound<SortableF64>,
        hi: &std::ops::Bound<SortableF64>,
    ) -> bool {
        self.number_bits_at(id)
            .is_some_and(|bits| in_sortable_bits_range(bits, lo, hi))
    }

    #[inline]
    pub(crate) fn set_number(&mut self, id: u32, key: SortableF64) {
        // Callers clear keyword range caches once before a write batch; doing it
        // per numeric item dominated bulk ingest.
        let ix = id as usize;
        if self.dense_forward.len() <= ix {
            self.dense_forward.resize(ix + 1, MISSING_SORTABLE_F64_BITS);
        }
        self.dense_forward[ix] = key.bits();
    }

    #[inline]
    pub(crate) fn remove_number(&mut self, id: u32) -> Option<SortableF64> {
        let dense = self.dense_forward.get_mut(id as usize).and_then(|slot| {
            if *slot == MISSING_SORTABLE_F64_BITS {
                None
            } else {
                let bits = *slot;
                *slot = MISSING_SORTABLE_F64_BITS;
                Some(SortableF64::from_bits(bits))
            }
        });
        let sparse = self.forward.remove(&id);
        self.clear_keyword_range_cache();
        dense.or(sparse)
    }

    pub(crate) fn forward_len(&self) -> usize {
        self.dense_forward
            .iter()
            .filter(|bits| **bits != MISSING_SORTABLE_F64_BITS)
            .count()
            + self.forward.len()
    }

    /// `true` once the sealed forward payload has been dropped to disk
    /// (Phase 2f-1) — the segment is attached. See `KeywordIndex::forward_dropped`.
    #[inline]
    fn forward_dropped(&self) -> bool {
        self.segment.is_some()
    }

    /// The attached disk segment, if any — the entry the SORT planner
    /// (`try_plan`) uses to drive `sorted_walk_segment` (Phase 2m). `None` when no
    /// segment is attached (the in-RAM `values` BTreeMap is the sort driver).
    #[inline]
    pub(crate) fn segment_ref(&self) -> Option<&std::sync::Arc<ComposedSegmentReader>> {
        self.segment.as_ref()
    }
}
