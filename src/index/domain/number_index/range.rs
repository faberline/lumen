//! A number field's range reads: the postings and document frequencies of a
//! value or a range, the distinct values in a range, the planner's range
//! estimate and the cached keyword-range doc lists.

use roaring::RoaringBitmap;

use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::number_index::{
    NumberIndex, NumberRangeStats, RANGE_STATS_BUILD_THRESHOLD,
};
use crate::index::domain::query::range::sorted_bits_window;
use crate::index::domain::sortable_f64::{
    bound_to_bits, range_cache_key, range_is_empty, SortableF64,
};

impl NumberIndex {
    #[inline]
    pub(crate) fn clear_keyword_range_cache(&mut self) {
        if let Ok(cache) = self.keyword_range_cache.get_mut() {
            cache.clear();
        }
        if let Ok(cache) = self.keyword_range_bitmap_cache.get_mut() {
            cache.clear();
        }
        if let Ok(stats) = self.range_stats.get_mut() {
            *stats = None;
        }
    }

    /// `(distinct, df)` of the LIVE `values` tree in `[low, high]` for planner
    /// estimates. Walks the tree exactly for narrow ranges; a walk that passes
    /// [`RANGE_STATS_BUILD_THRESHOLD`] distinct keys abandons and answers from
    /// the (built-on-demand) [`NumberRangeStats`] snapshot instead.
    fn live_range_estimate(
        &self,
        low: std::ops::Bound<SortableF64>,
        high: std::ops::Bound<SortableF64>,
    ) -> (u64, u64) {
        if let Some(stats) = self.range_stats.read().ok().and_then(|guard| guard.clone()) {
            return stats.range(low, high);
        }
        let mut distinct = 0u64;
        let mut df = 0u64;
        for (_, set) in self.values.range((low, high)) {
            distinct += 1;
            if distinct > RANGE_STATS_BUILD_THRESHOLD {
                return self.build_range_stats().range(low, high);
            }
            df += set.len();
        }
        (distinct, df)
    }

    pub(crate) fn build_range_stats(&self) -> std::sync::Arc<NumberRangeStats> {
        let built = std::sync::Arc::new(NumberRangeStats::build(&self.values));
        if let Ok(mut guard) = self.range_stats.write() {
            // A concurrent estimate may have built it first; either snapshot is
            // valid for the same (read-locked, unmutated) tree — keep ours.
            *guard = Some(built.clone());
        }
        built
    }

    pub(crate) fn keyword_range_docs(
        &self,
        keyword_field: &str,
        term: &str,
        keyword: &KeywordIndex,
    ) -> std::sync::Arc<Vec<(u64, u32)>> {
        let cache_key = format!("{keyword_field}\0{term}");
        if let Some(cached) = self
            .keyword_range_cache
            .read()
            .expect("number keyword-range cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            return cached;
        }

        let mut docs = Vec::new();
        if let Some(posting) = keyword.term_postings(term) {
            docs.reserve(posting.len() as usize);
            for id in posting.as_ref() {
                if let Some(bits) = self.number_bits_at(id) {
                    docs.push((bits, id));
                }
            }
        }
        docs.sort_unstable_by_key(|(bits, id)| (*bits, *id));
        let built = std::sync::Arc::new(docs);

        let mut cache = self
            .keyword_range_cache
            .write()
            .expect("number keyword-range cache poisoned");
        if let Some(cached) = cache.get(&cache_key) {
            return cached.clone();
        }
        cache.insert(cache_key, built.clone());
        built
    }

    pub(crate) fn keyword_range_bitmap(
        &self,
        keyword_field: &str,
        term: &str,
        keyword: &KeywordIndex,
        lo: &std::ops::Bound<SortableF64>,
        hi: &std::ops::Bound<SortableF64>,
    ) -> std::sync::Arc<RoaringBitmap> {
        let cache_key = format!("{}\0{}\0{}", keyword_field, term, range_cache_key(lo, hi));
        if let Some(cached) = self
            .keyword_range_bitmap_cache
            .read()
            .expect("number keyword-range bitmap cache poisoned")
            .get(&cache_key)
            .cloned()
        {
            return cached;
        }

        let docs = self.keyword_range_docs(keyword_field, term, keyword);
        let window = sorted_bits_window(docs.as_slice(), lo, hi);
        let mut bitmap = RoaringBitmap::new();
        for &(_, id) in &docs[window] {
            bitmap.insert(id);
        }
        let built = std::sync::Arc::new(bitmap);

        let mut cache = self
            .keyword_range_bitmap_cache
            .write()
            .expect("number keyword-range bitmap cache poisoned");
        if let Some(cached) = cache.get(&cache_key) {
            return cached.clone();
        }
        cache.insert(cache_key, built.clone());
        built
    }

    /// UNIFIED EXACT-MATCH accessor (Phase 2h-3): the posting bitmap for the
    /// exact numeric value `key` on the ACTIVE source, composing the disk segment
    /// (sealed base ids `[0..seg.n_docs)`) with the in-RAM `values` tail (ids
    /// added after the seal, or — when no segment is attached — the WHOLE index).
    /// The Number analogue of `KeywordIndex::term_postings`.
    ///
    /// - segment OFF (no segment attached): returns `Cow::Borrowed` of
    ///   `values[key]` — ZERO clone, byte-for-byte the old `n.values.get(&key)`
    ///   path every exact-match / selectivity / planner site read.
    /// - segment ON: the segment's stored postings for `key` (the RAM `values`
    ///   index was DROPPED at seal), MINUS the query-time `tombstones`, UNION any
    ///   live-tail `values[key]`. The union is `Cow::Owned`. Both bases are
    ///   ascending-docid RoaringBitmaps, so the result is identical to what an
    ///   un-dropped in-RAM `values` held.
    ///
    /// `None` only when the value is absent from BOTH sources (callers keep their
    /// `.unwrap_or_default()` empty-posting semantics).
    #[inline]
    pub(crate) fn value_postings(
        &self,
        key: SortableF64,
    ) -> Option<std::borrow::Cow<'_, RoaringBitmap>> {
        if let Some(seg) = &self.segment {
            // Segment base, MINUS the query-time tombstone (base docids deleted
            // since the last seal — the on-disk postings can't be mutated), then
            // UNION the live-tail postings indexed back into `values`. Tombstones
            // only ever hold base ids (`< seg.n_docs`) and tail ids are
            // `>= seg.n_docs`, so the subtraction never touches the tail.
            let mut base = seg.number_value_postings(key.bits()).unwrap_or_default();
            if !self.tombstones.is_empty() {
                base -= &self.tombstones;
            }
            if let Some(t) = self.values.get(&key) {
                base |= t;
            }
            return if base.is_empty() {
                None
            } else {
                Some(std::borrow::Cow::Owned(base))
            };
        }
        self.values.get(&key).map(std::borrow::Cow::Borrowed)
    }

    /// UNIFIED RANGE accessor (Phase 2h-3): the posting union of every value in
    /// the half-open/inclusive range `(low, high)` on the ACTIVE source. The
    /// Number analogue of a range-walk over `values`.
    ///
    /// - segment OFF (no segment attached): walks `values.range((low,
    ///   high))` and ORs each posting — byte-for-byte the old `eval_range` walk.
    /// - segment ON: the segment's `number_range` (binary-search to the lo/hi
    ///   index bounds, union the in-window postings — SELECTIVE, not a forward
    ///   scan), MINUS the query-time `tombstones`, UNION the live-tail
    ///   `values.range((low, high))` postings. Byte-identical result set to the
    ///   in-RAM range walk over the same data.
    #[inline]
    pub(crate) fn range_postings(
        &self,
        low: std::ops::Bound<SortableF64>,
        high: std::ops::Bound<SortableF64>,
    ) -> RoaringBitmap {
        // An empty/inverted range yields no docs on BOTH paths — and short-circuits
        // before `values.range`, which PANICS on an inverted/degenerate-exclusive
        // pair. The on-disk `number_range_window` already collapses such a range to
        // an empty window, so this keeps the two paths identical.
        if range_is_empty(low, high) {
            return RoaringBitmap::new();
        }
        if let Some(seg) = &self.segment {
            let mut acc = seg
                .number_range(bound_to_bits(low), bound_to_bits(high))
                .unwrap_or_default();
            if !self.tombstones.is_empty() {
                acc -= &self.tombstones;
            }
            // UNION the live tail (ids added after the seal). Tail ids are
            // `>= seg.n_docs`, disjoint from the segment base, so the OR never
            // double-counts.
            for (_, set) in self.values.range((low, high)) {
                acc |= set;
            }
            return acc;
        }
        let mut acc = RoaringBitmap::new();
        for (_, set) in self.values.range((low, high)) {
            acc |= set;
        }
        acc
    }

    /// UNIFIED exact-match document frequency (Phase 2h-3): value `key`'s `df` on
    /// the ACTIVE source — the boolean planner's rarest-first selectivity input.
    /// segment OFF: the live `values[key]` length. segment ON: the segment's
    /// CHEAP count-prefix df (no posting decode) PLUS any live-tail length. Like
    /// `KeywordIndex::term_df`, this deliberately does NOT subtract the tombstone
    /// (a small over-count only affects clause ORDERING, never the result set,
    /// which comes from `value_postings`).
    #[inline]
    pub(crate) fn value_df(&self, key: SortableF64) -> u64 {
        if let Some(seg) = &self.segment {
            let base = seg.number_value_df(key.bits()).unwrap_or(0);
            let tail = self.values.get(&key).map(|p| p.len()).unwrap_or(0);
            return base + tail;
        }
        self.values.get(&key).map(|p| p.len()).unwrap_or(0)
    }

    /// UNIFIED range selectivity (Phase 2h-3): the summed df of every value in
    /// `(low, high)` on the ACTIVE source. segment OFF: sum of the live
    /// `values.range` posting lengths. segment ON: the segment's cheap
    /// count-prefix range df PLUS the live-tail range lengths. Like `value_df`,
    /// does NOT subtract the tombstone (ordering-only input).
    #[inline]
    pub(crate) fn range_df(
        &self,
        low: std::ops::Bound<SortableF64>,
        high: std::ops::Bound<SortableF64>,
    ) -> u64 {
        // Guard the empty/inverted range (would panic `values.range`); 0 on both.
        if range_is_empty(low, high) {
            return 0;
        }
        let (_, tail) = self.live_range_estimate(low, high);
        if let Some(seg) = &self.segment {
            let base = seg
                .number_range_df(bound_to_bits(low), bound_to_bits(high))
                .unwrap_or(0);
            return base + tail;
        }
        tail
    }

    #[inline]
    pub(crate) fn range_distinct_count(
        &self,
        low: std::ops::Bound<SortableF64>,
        high: std::ops::Bound<SortableF64>,
    ) -> u64 {
        if range_is_empty(low, high) {
            return 0;
        }
        let (tail, _) = self.live_range_estimate(low, high);
        if let Some(seg) = &self.segment {
            return seg
                .number_range_distinct_count(bound_to_bits(low), bound_to_bits(high))
                .unwrap_or(0)
                + tail;
        }
        tail
    }
}

#[cfg(test)]
mod tests;
