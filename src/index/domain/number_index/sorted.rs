//! A number field's sorted walks: the live values in order, and the sort and
//! range pages that stream a sealed segment's values without materializing
//! them.

use std::collections::BTreeMap;

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::sortable_f64::{bound_to_bits, SortableF64};
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

impl NumberIndex {
    /// Every distinct value (as a `SortableF64` key) with its LIVE posting
    /// bitmap, on the ACTIVE source, ASCENDING by value — the unified enumeration
    /// that `unique_terms` and the sort/range planner pages drive from so they
    /// stay correct after a seal drops the in-RAM `values` driver (Phase 2h-3).
    /// Values with no live doc are EXCLUDED (the same invariant the in-RAM
    /// `values` map keeps). The Number analogue of `KeywordIndex::live_terms`.
    ///
    /// - segment OFF: clones the live `values` map verbatim.
    /// - segment ON: enumerate the segment sorted-value column
    ///   (`number_values_all`), subtract the query-time `tombstones` from each
    ///   base posting, UNION the live-tail `values` for the same key, keep the
    ///   value iff >=1 live doc remains. Tail-only values (indexed after the seal)
    ///   are folded in too. Returns a `BTreeMap`, so iteration is ascending by
    ///   value — identical order to the in-RAM `values`.
    pub(crate) fn live_values(&self) -> BTreeMap<SortableF64, RoaringBitmap> {
        let Some(seg) = &self.segment else {
            return self.values.clone();
        };
        let mut out: BTreeMap<SortableF64, RoaringBitmap> = BTreeMap::new();
        if let Some(dict) = seg.number_values_all() {
            for (bits, mut postings) in dict {
                if !self.tombstones.is_empty() {
                    postings -= &self.tombstones;
                }
                let key = SortableF64::from_bits(bits);
                if let Some(tail) = self.values.get(&key) {
                    postings |= tail;
                }
                if !postings.is_empty() {
                    out.insert(key, postings);
                }
            }
        }
        // Fold any live-tail-only values the sorted column did not carry (indexed
        // after the seal). A value already present from the column is left merged.
        for (key, tail) in &self.values {
            if !out.contains_key(key) {
                out.insert(*key, tail.clone());
            }
        }
        out
    }

    /// The ascending-by-value distinct-value map for the SORT / standalone-range
    /// PLANNER walks (`try_plan`), as a `Cow` so the default path stays
    /// zero-clone (Phase 2h-3):
    ///
    /// - segment OFF (no segment attached): `Cow::Borrowed(&self.values)` —
    ///   byte-for-byte the old `n.values.iter()` / `.range()` zero-clone walk.
    /// - segment ON: `Cow::Owned(self.live_values())` — the on-disk sorted-value
    ///   column merged with the live tail (minus tombstones), since the in-RAM
    ///   `values` driver was DROPPED at seal. Iterating it is ascending by value,
    ///   identical order to the in-RAM map on the same data.
    #[inline]
    pub(crate) fn sorted_values(
        &self,
    ) -> std::borrow::Cow<'_, BTreeMap<SortableF64, RoaringBitmap>> {
        if self.segment.is_some() {
            return std::borrow::Cow::Owned(self.live_values());
        }
        std::borrow::Cow::Borrowed(&self.values)
    }

    /// SEGMENT-ON sort-via-sorted-index walk (Phase 2m). Drives the SORT planner
    /// (`try_plan`) directly off the on-disk ascending sorted-value index
    /// (`ROLE_NUMBER_SORTED`) + the BOUNDED per-value posting cache, MERGED with
    /// the live tail (`self.values`) in value order — the disk analogue of walking
    /// the in-RAM `values` BTreeMap, WITHOUT `live_values()`'s whole-field
    /// BTreeMap rebuild (the 525x sort regression) and WITHOUT the
    /// gather-`number_at`-per-doc + sort the original sealed path paid.
    ///
    /// For each distinct value in the requested order (`asc`/`desc`), the per-value
    /// posting is the segment posting MINUS the query-time `tombstones` (deleted
    /// base docids — `query_predicate` can't exclude them because `number_at` still
    /// reads the immutable forward column), UNION the live-tail `values[key]`. The
    /// `visit` callback receives `(value_as_f64, docid)` in:
    ///   * value order = requested sort order (ascending bits == ascending numeric
    ///     value, identical to the in-RAM `values` `Ord`; reversed for desc), and
    ///   * within-value docid order = ascending (base ids `< seg.n_docs` precede
    ///     tail ids, exactly the OR'd-bitmap iteration order the in-RAM walk used),
    /// so the emitted (value, docid) sequence is BYTE-IDENTICAL to the in-RAM
    /// `sorted_values().iter()[.rev()]` walk. `visit` returns `Ok(true)` to
    /// continue or `Ok(false)` to stop early (page full + `!track_total`); the walk
    /// short-circuits on `Ok(false)` so `pure_sort`/`filter_sort` only touch (and
    /// cache) the first few values' postings. Returns `Ok(())` once every value is
    /// visited or the callback stops.
    ///
    /// Both sources are ascending; a value present in BOTH the segment column and
    /// the live tail is visited ONCE with the unioned posting (the merge advances
    /// both cursors on a key tie), matching the in-RAM single-entry-per-value map.
    pub(crate) fn sorted_walk_segment<F>(
        &self,
        seg: &ComposedSegmentReader,
        descending: bool,
        after: Option<u64>,
        mut visit: F,
    ) -> Result<()>
    where
        F: FnMut(f64, u32) -> Result<bool>,
    {
        let mut cursor = seg.number_keys(
            (!descending)
                .then_some(after)
                .flatten()
                .map(|bits| (bits, true)),
            descending
                .then_some(after)
                .flatten()
                .map(|bits| (bits, true)),
            descending,
        )?;
        let mut disk = cursor.next()?;
        let tail_keys: Vec<SortableF64> = if descending {
            self.values
                .keys()
                .rev()
                .copied()
                .filter(|key| after.is_none_or(|bits| key.bits() <= bits))
                .collect()
        } else {
            self.values
                .keys()
                .copied()
                .filter(|key| after.is_none_or(|bits| key.bits() >= bits))
                .collect()
        };
        let mut tail = tail_keys.into_iter();
        let mut tail_key = tail.next();
        loop {
            let (bits, take_disk, take_tail) = match (disk, tail_key) {
                (None, None) => break,
                (Some(bits), None) => (bits, true, false),
                (None, Some(key)) => (key.bits(), false, true),
                (Some(disk_bits), Some(key)) => {
                    let tail_bits = key.bits();
                    if disk_bits == tail_bits {
                        (disk_bits, true, true)
                    } else if (disk_bits < tail_bits) != descending {
                        (disk_bits, true, false)
                    } else {
                        (tail_bits, false, true)
                    }
                }
            };
            let mut posting = if take_disk {
                seg.number_value_postings(bits).unwrap_or_default()
            } else {
                RoaringBitmap::new()
            };
            if take_disk && !self.tombstones.is_empty() {
                posting -= &self.tombstones;
            }
            if take_tail {
                posting |= self
                    .values
                    .get(&SortableF64::from_bits(bits))
                    .expect("tail key came from values");
                tail_key = tail.next();
            }
            if take_disk {
                disk = cursor.next()?;
            }
            let value = SortableF64::from_bits(bits).to_f64();
            for id in posting {
                if !visit(value, id)? {
                    return Ok(());
                }
            }
        }
        return Ok(());
    }

    /// SEGMENT-ON standalone range page. Unlike `sorted_values()`, this streams only
    /// the selected sorted-value window and never materializes the whole field into a
    /// BTreeMap. Exact total is counted from posting lengths whenever possible; docid
    /// iteration is only needed while filling the page, or when tombstones force a
    /// live count for a base posting.
    pub(crate) fn range_page_segment(
        &self,
        seg: &ComposedSegmentReader,
        low: std::ops::Bound<SortableF64>,
        high: std::ops::Bound<SortableF64>,
        want: usize,
        track_total: bool,
    ) -> Result<(Vec<(u32, f32)>, u64)> {
        let mut page = Vec::with_capacity(want.min(1024));
        let mut total = 0u64;
        let mut cursor = seg.number_keys(bound_to_bits(low), bound_to_bits(high), false)?;
        let mut disk = cursor.next()?;
        let mut tail = self.values.range((low, high)).peekable();
        loop {
            let (bits, take_disk, take_tail) = match (disk, tail.peek()) {
                (None, None) => break,
                (Some(bits), None) => (bits, true, false),
                (None, Some((key, _))) => (key.bits(), false, true),
                (Some(disk_bits), Some((key, _))) => match disk_bits.cmp(&key.bits()) {
                    std::cmp::Ordering::Less => (disk_bits, true, false),
                    std::cmp::Ordering::Equal => (disk_bits, true, true),
                    std::cmp::Ordering::Greater => (key.bits(), false, true),
                },
            };
            let mut posting = if take_disk {
                seg.number_value_postings(bits).unwrap_or_default()
            } else {
                RoaringBitmap::new()
            };
            if take_disk && !self.tombstones.is_empty() {
                posting -= &self.tombstones;
            }
            if take_tail {
                let (_, tail_posting) = tail.next().expect("peeked tail entry");
                posting |= tail_posting;
            }
            if take_disk {
                disk = cursor.next()?;
            }
            total += posting.len();
            for id in posting {
                if page.len() < want {
                    page.push((id, 1.0));
                } else if !track_total {
                    total = total.max(page.len() as u64);
                    return Ok((page, total));
                }
            }
        }
        if !track_total {
            total = total.max(page.len() as u64);
        }
        return Ok((page, total));
    }
}
