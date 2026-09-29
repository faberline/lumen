//! Number lookups: per-doc values, value and range postings, and counts.

use crate::persistence::infrastructure::segment::codecs::read_varint;
use crate::persistence::infrastructure::segment::reader_cache::CachedPosting;
use crate::persistence::infrastructure::segment::{
    SegmentReader, ROLE_NUMBER, ROLE_NUMBER_POSTINGS, ROLE_NUMBER_SORTED,
};

impl SegmentReader {
    /// The decoded `f64` for doc `id`, or `None` if `id` is out of range, the
    /// doc is absent, or any column is torn/misaligned. Never panics.
    pub fn number_at(&self, id: u32) -> Option<f64> {
        if !self.is_present(id) {
            return None;
        }
        // Number forward column: u64 raw bits.
        let number = self.column(ROLE_NUMBER)?;
        let number_bytes = self.column_bytes(number)?;
        let number_words: &[u64] = bytemuck::try_cast_slice(number_bytes).ok()?;
        let bits = number_words.get(id as usize)?;
        Some(f64::from_bits(*bits))
    }

    // -----------------------------------------------------------------------
    // Number SORTED-VALUE range index (Phase 2h-3)
    // -----------------------------------------------------------------------

    /// The ascending `u64[distinct]` sorted-value range index ([`ROLE_NUMBER_SORTED`]),
    /// borrowed zero-copy off the mmap. Each entry is a distinct `SortableF64`
    /// bit key (monotone in numeric order, so the slice is ascending). `None`
    /// when the column is absent (a pre-2h-3 segment) or torn/misaligned — every
    /// range/exact reader gates on this and falls back to `None`/empty, never a
    /// panic. The slice borrows `&self` (it lives on the page).
    fn number_sorted(&self) -> Option<&[u64]> {
        let col = self.column(ROLE_NUMBER_SORTED)?;
        let bytes = self.column_bytes(col)?;
        bytemuck::try_cast_slice::<u8, u64>(bytes).ok()
    }

    /// The number of DISTINCT numeric values in this segment — the length of the
    /// sorted-value column ([`ROLE_NUMBER_SORTED`]). 0 when absent/torn. Mirrors
    /// the in-RAM `NumberIndex.values.len()`. Phase 2h-3.
    pub fn number_distinct_count(&self) -> u64 {
        self.number_sorted().map(|s| s.len() as u64).unwrap_or(0)
    }

    /// Decode the docid-only posting blob at SORTED INDEX `i` (the value's
    /// position in [`ROLE_NUMBER_SORTED`]) into an ascending docid
    /// `RoaringBitmap`. `None` for an out-of-range index or a torn block — never
    /// panics. The Number analogue of `keyword_postings`, but located by the
    /// numeric value's sorted index (the binary-search result), not a string
    /// dict-id. Phase 2h-3.
    fn number_postings_at(&self, i: u32) -> Option<roaring::RoaringBitmap> {
        self.cached_number_postings_at(i).map(|p| (*p).clone())
    }

    /// The cache-resident Number posting at sorted-value index `i` (Phase 2m).
    /// `number_range` / sort-via-sorted-index call this directly to OR the
    /// `Arc<RoaringBitmap>` without an intermediate clone; `number_postings_at`
    /// keeps its owned-bitmap signature for the `number_values_all` enumeration.
    fn cached_number_postings_at(&self, i: u32) -> Option<CachedPosting> {
        self.cached_docid_postings(ROLE_NUMBER_POSTINGS, i)
    }

    /// Binary-search the ascending sorted-value column for `bits`, returning its
    /// index when present, else `None`. The column is the distinct `SortableF64`
    /// bit keys; `bits` is the probe value's `SortableF64.0`. Phase 2h-3.
    fn number_value_index(&self, bits: u64) -> Option<u32> {
        let sorted = self.number_sorted()?;
        match sorted.binary_search(&bits) {
            Ok(i) => u32::try_from(i).ok(),
            Err(_) => None,
        }
    }

    /// EXACT-MATCH: the docids whose number EQUALS the value with `SortableF64`
    /// bit key `bits`, as an ascending docid `RoaringBitmap`. `None` if no
    /// distinct value matches (or the column/block is torn). Byte-identical to
    /// the in-RAM `values.get(&key)` posting at seal. Phase 2h-3.
    pub fn number_value_postings(&self, bits: u64) -> Option<roaring::RoaringBitmap> {
        let i = self.number_value_index(bits)?;
        self.number_postings_at(i)
    }

    /// EXACT-MATCH document frequency: the stored posting length for the value
    /// with bit key `bits`, decoding ONLY the LEB128 `count` prefix (cheap — the
    /// boolean planner's rarest-first selectivity input). `None` if absent.
    /// Phase 2h-3.
    pub fn number_value_df(&self, bits: u64) -> Option<u64> {
        let i = self.number_value_index(bits)?;
        let (block, within) = self.dict_block_at(ROLE_NUMBER_POSTINGS, i)?;
        let blob = block.get(within)?;
        let mut pos = 0usize;
        read_varint(blob, &mut pos)
    }

    /// The half-open `[i_lo, i_hi)` index window into the ascending sorted-value
    /// column selected by the range bounds, honoring inclusivity and open ends.
    /// This is exactly the window `BTreeMap::range((lo, hi))` would walk over the
    /// same total order: the sorted-value `u64` keys are monotone in numeric
    /// order, so `partition_point` over the unsigned keys reproduces the BTreeMap
    /// range bound semantics byte-for-byte (NaN never reaches a key; ±0.0 are
    /// distinct keys handled identically here and in RAM). Phase 2h-3.
    ///
    /// - lo `Some((b, true))`  = `Included(b)` → first index with `k >= b`.
    /// - lo `Some((b, false))` = `Excluded(b)` → first index with `k >  b`.
    /// - lo `None`             = `Unbounded`   → `0`.
    /// - hi `Some((b, true))`  = `Included(b)` → first index with `k >  b`.
    /// - hi `Some((b, false))` = `Excluded(b)` → first index with `k >= b`.
    /// - hi `None`             = `Unbounded`   → `n`.
    fn number_range_window(
        sorted: &[u64],
        lo: Option<(u64, bool)>,
        hi: Option<(u64, bool)>,
    ) -> (usize, usize) {
        let n = sorted.len();
        let i_lo = match lo {
            // Included(b): keep k >= b → first index NOT (k < b).
            Some((b, true)) => sorted.partition_point(|&k| k < b),
            // Excluded(b): keep k > b → first index NOT (k <= b).
            Some((b, false)) => sorted.partition_point(|&k| k <= b),
            None => 0,
        };
        let i_hi = match hi {
            // Included(b): keep k <= b → first index NOT (k <= b).
            Some((b, true)) => sorted.partition_point(|&k| k <= b),
            // Excluded(b): keep k < b → first index NOT (k < b).
            Some((b, false)) => sorted.partition_point(|&k| k < b),
            None => n,
        };
        // A pathological / inverted range yields an empty window, matching an
        // empty BTreeMap range (`hi <= lo`).
        (i_lo, i_hi.max(i_lo))
    }

    /// RANGE: union the docid postings of every distinct value in `[lo, hi)` (per
    /// the inclusive/open-ended bound semantics of [`Self::number_range_window`])
    /// into one ascending `RoaringBitmap`. Binary-searches the sorted-value
    /// column to the lo/hi index bounds (SELECTIVE — it jumps to `i_lo` and scans
    /// to `i_hi`, NOT a full O(n_docs) forward scan), then ORs each in-window
    /// value's posting block. Result is byte-identical to the in-RAM
    /// `values.range((low, high))` posting union at seal. `None` only when the
    /// sorted-value column is absent/torn (a pre-2h-3 segment); an empty window
    /// yields `Some(empty)`. Phase 2h-3.
    pub fn number_range(
        &self,
        lo: Option<(u64, bool)>,
        hi: Option<(u64, bool)>,
    ) -> Option<roaring::RoaringBitmap> {
        let sorted = self.number_sorted()?;
        let (i_lo, i_hi) = Self::number_range_window(sorted, lo, hi);
        let mut acc = roaring::RoaringBitmap::new();
        for i in i_lo..i_hi {
            // Phase 2m: OR the CACHED per-value posting (resident `Arc` on a warm
            // hit) — the in-window values' bitmaps are exactly what the in-RAM
            // `values.range` walk held. Tombstone subtraction stays in
            // `storage.rs` AFTER the union, so the result is byte-identical.
            if let Some(p) = self.cached_number_postings_at(i as u32) {
                acc |= p.as_ref();
            }
        }
        Some(acc)
    }

    /// The number of distinct sorted values plus a per-index cached-posting
    /// accessor are the two primitives the SORT-via-sorted-index walk
    /// (`storage.rs::try_plan`) drives from: it iterates index `0..distinct`
    /// (ascending) or in reverse (descending), reading each value's `SortableF64`
    /// bits ([`Self::number_sorted_bits_at`]) and its cache-resident posting
    /// ([`Self::number_sorted_postings_at`]) IN value order — the disk analogue of
    /// walking the in-RAM `values` BTreeMap, WITHOUT the pre-2m
    /// gather-`number_at`-per-doc + sort. Streaming index-by-index keeps the cache
    /// the only resident structure (no whole-field BTreeMap materialized).

    /// The `SortableF64` bit key at sorted-value index `i` (ascending value
    /// order), or `None` when out of range / column torn. Phase 2m.
    pub fn number_sorted_bits_at(&self, i: u32) -> Option<u64> {
        self.number_sorted()?.get(i as usize).copied()
    }

    /// The cache-resident posting at sorted-value index `i` (Phase 2m) — the
    /// public-to-`storage.rs` name for [`Self::cached_number_postings_at`]. The
    /// posting is the RAW immutable stream; `storage.rs` applies the tombstone via
    /// the per-doc predicate path during the sort walk, so the order + membership
    /// are byte-identical to the in-RAM walk.
    pub fn number_sorted_postings_at(
        &self,
        i: u32,
    ) -> Option<std::sync::Arc<roaring::RoaringBitmap>> {
        self.cached_number_postings_at(i)
    }

    /// RANGE selectivity: the SUM of posting lengths (df) of every distinct value
    /// in `[lo, hi)`, decoding only each value's cheap LEB128 `count` prefix. The
    /// boolean planner's rarest-first cost input for a range conjunct. `None`
    /// when the sorted-value column is absent/torn. Phase 2h-3.
    pub fn number_range_df(&self, lo: Option<(u64, bool)>, hi: Option<(u64, bool)>) -> Option<u64> {
        let sorted = self.number_sorted()?;
        let (i_lo, i_hi) = Self::number_range_window(sorted, lo, hi);
        let mut sum = 0u64;
        for i in i_lo..i_hi {
            if let Some((block, within)) = self.dict_block_at(ROLE_NUMBER_POSTINGS, i as u32) {
                if let Some(blob) = block.get(within) {
                    let mut pos = 0usize;
                    sum += read_varint(blob, &mut pos).unwrap_or(0);
                }
            }
        }
        Some(sum)
    }

    /// Number of distinct numeric values selected by a range. This is a planner
    /// cost primitive: high distinct-window ranges are expensive to materialize
    /// by ORing per-value postings, and can be cheaper as predicates against a
    /// smaller peer bitmap.
    pub fn number_range_distinct_count(
        &self,
        lo: Option<(u64, bool)>,
        hi: Option<(u64, bool)>,
    ) -> Option<u64> {
        let sorted = self.number_sorted()?;
        let (i_lo, i_hi) = Self::number_range_window(sorted, lo, hi);
        Some((i_hi - i_lo) as u64)
    }

    /// The sorted-value index window selected by a range. This is the streaming
    /// primitive for storage's segment-backed standalone range planner: it can walk
    /// only the selected distinct values instead of materializing the whole
    /// `number_values_all()` map.
    pub fn number_range_index_window(
        &self,
        lo: Option<(u64, bool)>,
        hi: Option<(u64, bool)>,
    ) -> Option<(u32, u32)> {
        let sorted = self.number_sorted()?;
        let (i_lo, i_hi) = Self::number_range_window(sorted, lo, hi);
        Some((u32::try_from(i_lo).ok()?, u32::try_from(i_hi).ok()?))
    }

    /// Materialize EVERY distinct numeric value (as its `SortableF64` bit key)
    /// paired with its stored ascending-docid postings, in ascending value
    /// order. `None` if any posting block is torn. The Number analogue of
    /// `keyword_terms_all` / `set_elements_all`: a sealed Number field dropped its
    /// in-RAM `values` driver, so the only way to walk distinct values (for the
    /// segment-aware sorted-iteration / unique-value enumeration) is this on-disk
    /// sorted-value column. The docid stream is byte-identical to what the live
    /// `values[key]` bitmap held at seal. Phase 2h-3.
    pub fn number_values_all(&self) -> Option<Vec<(u64, roaring::RoaringBitmap)>> {
        let sorted = self.number_sorted()?;
        let mut out = Vec::with_capacity(sorted.len());
        for (i, &bits) in sorted.iter().enumerate() {
            let postings = self.number_postings_at(i as u32)?;
            out.push((bits, postings));
        }
        Some(out)
    }
}
