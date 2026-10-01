//! A set field's inverted index: each element's doc bitmap, the duplicates
//! side-index and the doc-to-elements forward map, over a sealed segment base
//! plus the live tail.

use std::collections::{BTreeMap, BTreeSet};

use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::shared_kernel::types::document::FieldValue;

#[derive(Debug, Default)]
pub(in crate::index) struct SetIndex {
    pub(in crate::index) elements: BTreeMap<String, RoaringBitmap>,
    /// Side-index of LIVE-tail elements whose posting holds >= 2 docs (see
    /// `KeywordIndex::dup_values`); drives `duplicates` without a full
    /// `elements` scan. Cleared at seal.
    pub(in crate::index) dup_values: BTreeSet<String>,
    pub(in crate::index) forward: FastHashMap<u32, BTreeSet<String>>,
    pub(in crate::index) bytes: u64,
    /// Stage 2 disk-tier (Phase 2e-A): a sealed columnar mmap segment covering
    /// doc ids `[0..n_docs)` — a shared sorted string DICT (var-width) + a fixed
    /// `u32[n_docs + 1]` CSR offsets column + a fixed `u32[*]` packed dict-id
    /// column (doc `i`'s members = `packed[offsets[i]..offsets[i+1]]`) PLUS a
    /// parallel per-element INVERTED posting column (Phase 2h-2). When present,
    /// per-doc Set membership PREDICATE lookups (`set_contains`) read the segment
    /// for sealed ids and the in-RAM `forward` tail for ids `>= n_docs`, AND the
    /// inverted membership / Terms / boolean driver reads the on-disk postings
    /// (the RAM `elements` index was DROPPED at seal). RAM after reopen is
    /// O(live tail), not O(distinct set elements). DEFAULTS to `None`; while it
    /// is `None` (nothing sealed) every read path is byte-for-byte the
    /// in-RAM path. Purely additive.
    pub(in crate::index) segment: Option<std::sync::Arc<ComposedSegmentReader>>,
    /// QUERY-TIME TOMBSTONE (Phase 2h-2): base docids `[0..seg.n_docs)` deleted
    /// SINCE the last seal. The inverted `elements` index was DROPPED to disk at
    /// seal, so `drop_eid` can no longer remove a sealed base id from the
    /// immutable on-disk postings — instead it records the id here, and the
    /// segment-ON accessors (`element_postings`, duplicate / unique-element
    /// enumeration) SUBTRACT this set so a delete is reflected before the next
    /// re-seal. The next seal bakes the deletions into the new segment (its
    /// live(id) gather excludes them) and this is reset to empty. Live-tail ids
    /// (`>= seg.n_docs`) are NOT tombstoned — they are deleted directly out of
    /// the in-RAM `elements` tail. DEFAULTS empty; stays empty while no
    /// segment is attached. The exact reuse of the Keyword 2h-1 tombstone (same
    /// shape, same four touch-points: record in `drop_eid`, subtract in the
    /// posting accessor, exclude in `live_elements`, clear at re-seal).
    pub(in crate::index) tombstones: RoaringBitmap,
}

impl SetIndex {
    /// Does doc `id`'s set contain `el`, for a per-doc PREDICATE point lookup?
    /// When a sealed segment is attached it serves ids in its covered range
    /// `[0..n_docs)` (the live tail keeps ids `>= n_docs`); otherwise — and
    /// always when no segment is attached — this is exactly
    /// `self.forward.get(&id).map(|s| s.contains(el))`. The segment stores the
    /// exact member strings, so membership matches the live `forward` entry.
    #[inline]
    pub(in crate::index) fn set_contains(&self, id: u32, el: &str) -> bool {
        if let Some(set) = self.forward.get(&id) {
            return set.contains(el);
        }
        if self.tombstones.contains(id) {
            return false;
        }
        if let Some(seg) = &self.segment {
            if id < seg.n_docs() {
                return seg
                    .set_at(id)
                    .map(|members| members.iter().any(|m| m == el))
                    .unwrap_or(false);
            }
        }
        false
    }

    /// Does doc `id`'s set contain ANY of `values`'s string members? The
    /// `Terms` predicate site. Reads the segment members ONCE (not once per
    /// candidate value) when sealed, falling back to the live `forward` set.
    #[inline]
    pub(super) fn set_contains_any(&self, id: u32, values: &[FieldValue]) -> bool {
        if let Some(set) = self.forward.get(&id) {
            return values
                .iter()
                .any(|val| matches!(val, FieldValue::String(el) if set.contains(el)));
        }
        if self.tombstones.contains(id) {
            return false;
        }
        if let Some(seg) = &self.segment {
            if id < seg.n_docs() {
                let Some(members) = seg.set_at(id) else {
                    return false;
                };
                return values.iter().any(
                    |val| matches!(val, FieldValue::String(el) if members.iter().any(|m| m == el)),
                );
            }
        }
        false
    }

    /// Doc `id`'s full member set, routed through the segment for sealed ids
    /// (Phase 2f-1) or the live `forward` tail. `None` for a doc with no set
    /// value. Used by `drop_eid` after the forward payload has been dropped to
    /// disk, so the inverted `elements` postings are still removed on delete.
    #[inline]
    pub(in crate::index) fn set_members(&self, id: u32) -> Option<BTreeSet<String>> {
        if let Some(set) = self.forward.get(&id) {
            return Some(set.clone());
        }
        if self.tombstones.contains(id) {
            return None;
        }
        if let Some(seg) = &self.segment {
            if id < seg.n_docs() {
                return seg.set_at(id).map(|m| m.into_iter().collect());
            }
        }
        None
    }

    /// `set_members` MINUS the query-time tombstones — doc `id`'s member set as
    /// a reader sees it, rather than as the immutable segment stored it.
    ///
    /// `set_members` deliberately does NOT subtract them: `drop_eid` calls it to
    /// read the members it is about to un-index, and at that moment the id is not
    /// yet tombstoned. Every OTHER caller wants the live answer, and a sealed-base
    /// doc deleted after the seal is `Some(members)` there — the on-disk column
    /// cannot be mutated, so the delete is recorded only in `tombstones`.
    ///
    /// This is the `live_` counterpart the other three arms already have
    /// (`live_terms`, `live_elements`, `live_number_at`), and `KeywordIndex::keyword_at`
    /// folds the same check inline. Without it, `to_snapshot` would resurrect a
    /// deleted doc's memberships into the forward column, and `from_snapshot`
    /// rebuilds the inverted index from exactly that column.
    #[inline]
    pub(super) fn live_set_members(&self, id: u32) -> Option<BTreeSet<String>> {
        self.set_members(id)
    }

    /// `true` once the sealed forward payload has been dropped to disk
    /// (Phase 2f-1) — the segment is attached. See `KeywordIndex::forward_dropped`.
    #[inline]
    fn forward_dropped(&self) -> bool {
        self.segment.is_some()
    }

    /// UNIFIED inverted-index accessor (Phase 2h-2): element `el`'s posting
    /// bitmap (docids whose set contains `el`) on the ACTIVE source, composing
    /// the disk segment (sealed base ids `[0..seg.n_docs)`) with the in-RAM
    /// `elements` tail (ids added after the seal, or — when no segment is
    /// attached — the WHOLE inverted index). The Set analogue of
    /// `KeywordIndex::term_postings`.
    ///
    /// - segment OFF (no segment attached): returns `Cow::Borrowed` of
    ///   `elements[el]` — ZERO clone, byte-for-byte the old `s.elements.get(el)`
    ///   path that every membership / Terms / selectivity / planner site read.
    /// - segment ON: decodes the segment's stored postings for `el` (the RAM
    ///   `elements` index was DROPPED at seal), SUBTRACTS the query-time
    ///   `tombstones` (sealed-base deletes), and UNIONs any live-tail postings
    ///   indexed back into `elements`. The union is `Cow::Owned`. Both bases are
    ///   ascending-docid RoaringBitmaps, so the result is identical to what an
    ///   un-dropped in-RAM `elements` held.
    ///
    /// `None` only when the element is absent from BOTH sources (so callers keep
    /// their `.unwrap_or_default()` empty-posting semantics).
    #[inline]
    pub(in crate::index) fn element_postings(
        &self,
        el: &str,
    ) -> Option<std::borrow::Cow<'_, RoaringBitmap>> {
        if let Some(seg) = &self.segment {
            // Segment base, MINUS the query-time tombstone (base docids deleted
            // since the last seal — the on-disk postings can't be mutated), then
            // UNION the live-tail postings indexed back into `elements` after the
            // seal. Tombstones only ever hold base ids (`< seg.n_docs`) and tail
            // ids are `>= seg.n_docs`, so the subtraction never touches the tail.
            let mut base = seg.set_postings(el).unwrap_or_default();
            if !self.tombstones.is_empty() {
                base -= &self.tombstones;
            }
            if let Some(t) = self.elements.get(el) {
                base |= t;
            }
            return if base.is_empty() {
                None
            } else {
                Some(std::borrow::Cow::Owned(base))
            };
        }
        self.elements.get(el).map(std::borrow::Cow::Borrowed)
    }

    /// UNIFIED document-frequency accessor (Phase 2h-2): element `el`'s `df` on
    /// the ACTIVE source — the boolean planner's rarest-first selectivity input.
    /// segment OFF: the live `elements[el]` length. segment ON: the segment's
    /// CHEAP count-prefix df (no posting decode) PLUS any live-tail length. The
    /// Set analogue of `KeywordIndex::term_df`.
    ///
    /// TOMBSTONE NOTE (Phase 2h-2): like the Keyword `term_df`, this deliberately
    /// does NOT subtract the query-time `tombstones`. `df` only drives the
    /// boolean planner's rarest-first clause ORDERING — never the result set,
    /// which comes from `element_postings` (which DOES subtract the tombstone).
    /// A small df over-count can at worst pick a marginally less-rare lead
    /// clause; the emitted documents are identical. Keeping df off the full
    /// posting decode is the whole point of the count-prefix.
    #[inline]
    pub(in crate::index) fn element_df(&self, el: &str) -> u64 {
        if let Some(seg) = &self.segment {
            let base = seg.set_df(el).unwrap_or(0);
            let tail = self.elements.get(el).map(|p| p.len()).unwrap_or(0);
            return base + tail;
        }
        self.elements.get(el).map(|p| p.len()).unwrap_or(0)
    }

    /// Every distinct element with its LIVE posting bitmap, on the ACTIVE source
    /// — the unified enumeration that `unique_terms` and `duplicates` drive from
    /// so they stay correct after a seal drops the in-RAM `elements` driver
    /// (Phase 2h-2 FIX). Elements with no live doc are EXCLUDED (the same
    /// invariant the in-RAM `elements` map keeps — `drop_eid` removes an emptied
    /// element). The Set analogue of `KeywordIndex::live_terms`.
    ///
    /// - segment OFF: clones each live `elements` entry verbatim.
    /// - segment ON: enumerate the segment dict (`set_elements_all`), subtract
    ///   the query-time `tombstones` from each base posting, UNION the live-tail
    ///   `elements` for the same value, and keep the element iff >=1 live doc
    ///   remains. Tail-only elements (indexed after the seal, absent from the
    ///   dict) are folded in too.
    pub(in crate::index) fn live_elements(&self) -> BTreeMap<String, RoaringBitmap> {
        let Some(seg) = &self.segment else {
            return self.elements.clone();
        };
        let mut out: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        if let Some(dict) = seg.set_elements_all() {
            for (el, mut postings) in dict {
                if !self.tombstones.is_empty() {
                    postings -= &self.tombstones;
                }
                if let Some(tail) = self.elements.get(&el) {
                    postings |= tail;
                }
                if !postings.is_empty() {
                    out.insert(el, postings);
                }
            }
        }
        // Fold any live-tail-only elements the dict did not carry (indexed after
        // the seal). An element already present from the dict is left merged.
        for (el, tail) in &self.elements {
            if !out.contains_key(el) {
                out.insert(el.clone(), tail.clone());
            }
        }
        out
    }
}
