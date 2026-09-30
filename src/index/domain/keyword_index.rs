//! A keyword field's inverted index: each exact term's doc bitmap, the
//! duplicates side-index and the doc-to-term forward map, over a sealed segment
//! base plus the live tail.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

#[derive(Debug, Default)]
pub(crate) struct KeywordIndex {
    /// term → docs (RoaringBitmap so AND/OR is compressed-SIMD, not random
    /// per-doc forward lookups — the difference at 1M-doc filter intersection).
    pub(crate) terms: BTreeMap<String, RoaringBitmap>,
    /// Side-index of LIVE-tail terms whose posting holds >= 2 docs, maintained
    /// O(log n) at index/delete time so `duplicates` iterates only candidate
    /// groups instead of scanning every distinct term. Tail-only state: cleared
    /// at seal (the sealed path enumerates the segment dict instead).
    pub(crate) dup_values: BTreeSet<String>,
    /// Dense live-tail forward cache for hot per-doc predicates and snapshots.
    /// `forward` remains a sparse compatibility/fallback map for restored older
    /// snapshots, while new writes avoid a per-doc HashMap insert.
    pub(in crate::index) dense_forward: Vec<Option<String>>,
    pub(in crate::index) forward: FastHashMap<u32, String>,
    pub(crate) bytes: u64,
    /// Stage 2 disk-tier (Phase 2e-A): a sealed columnar mmap segment covering
    /// doc ids `[0..n_docs)` — a sorted prefix-compressed string DICT plus a
    /// fixed `u32[n_docs]` dict-id forward column. When present, per-doc Keyword
    /// PREDICATE point lookups (`keyword_at`) read the segment for sealed ids
    /// and the in-RAM `forward` tail for ids `>= n_docs`. The inverted `terms`
    /// index is NOT stored on disk; the seal seam rebuilds it from the forward
    /// state so OR/AND posting walks are untouched. DEFAULTS to `None`; while it
    /// is `None` (nothing sealed) every read path is byte-for-byte the
    /// in-RAM path. Purely additive.
    pub(in crate::index) segment: Option<std::sync::Arc<ComposedSegmentReader>>,
    /// QUERY-TIME TOMBSTONE (Phase 2h-1 FIX): base docids `[0..seg.n_docs)`
    /// deleted SINCE the last seal. The inverted `terms` index was DROPPED to
    /// disk at seal, so `drop_eid` can no longer remove a sealed base id from
    /// the immutable on-disk postings — instead it records the id here, and the
    /// segment-ON accessors (`term_postings`, duplicate/unique-term enumeration)
    /// SUBTRACT this set so a delete is reflected before the next re-seal. The
    /// next seal bakes the deletions into the new segment (its live(id) gather
    /// excludes them) and this is reset to empty. Live-tail ids (`>= seg.n_docs`)
    /// are NOT tombstoned — they are deleted directly out of the in-RAM `terms`
    /// tail. DEFAULTS empty; stays empty while no segment is attached (the in-RAM
    /// `terms` path needs no tombstone — a delete mutates `terms` directly).
    ///
    /// GENERALIZES to Set/Number/Text: each per-field index that drops its
    /// in-RAM inverted driver at seal (2h-2 SetIndex.elements, 2h-3
    /// NumberIndex.values, 2h-4 TextIndex.tokens) gains the identical
    /// `tombstones: RoaringBitmap`, records sealed-base deletes into it in
    /// `drop_eid`, subtracts it in the segment-ON branch of its posting accessor,
    /// and clears it at re-seal. Same shape, same four touch-points.
    pub(crate) tombstones: RoaringBitmap,
}

/// Result of walking keyword posting buckets in lexical field-sort order.
/// `Unavailable` is fail-closed: a torn segment falls back to the generic
/// bounded top-k path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeywordBucketWalk {
    Completed,
    Stopped,
    Unavailable,
}

impl KeywordIndex {
    /// The doc's keyword for a per-doc PREDICATE point lookup. When a sealed
    /// segment is attached it serves ids in its covered range `[0..n_docs)`
    /// (the live tail keeps ids `>= n_docs`); otherwise — and always when no segment
    /// is attached — this is exactly `self.forward.get(&id)`
    /// (cloned, since the segment path can only yield an owned `String`). The
    /// segment stores the exact UTF-8 bytes, so a hit equals the live `forward`
    /// entry; equality / membership compares are identical. A tombstoned sealed
    /// id skips its obsolete segment value and falls through to the live forward
    /// overlay: an update keeps the tombstone but supplies a replacement there,
    /// while a true delete has no live forward entry and remains absent.
    #[inline]
    pub(in crate::index) fn keyword_at(&self, id: u32) -> Option<String> {
        if let Some(value) = self.dense_forward.get(id as usize).and_then(|v| v.as_ref()) {
            return Some(value.clone());
        }
        if let Some(value) = self.forward.get(&id) {
            return Some(value.clone());
        }
        if self.tombstones.contains(id) {
            return None;
        }
        if let Some(seg) = &self.segment {
            if id < seg.n_docs() {
                return seg.keyword_at(id);
            }
        }
        None
    }

    #[inline]
    pub(crate) fn set_keyword(&mut self, id: u32, value: String) {
        let ix = id as usize;
        if self.dense_forward.len() <= ix {
            self.dense_forward.resize_with(ix + 1, || None);
        }
        self.dense_forward[ix] = Some(value);
    }

    #[inline]
    pub(crate) fn remove_keyword(&mut self, id: u32) -> Option<String> {
        let dense = self
            .dense_forward
            .get_mut(id as usize)
            .and_then(|slot| slot.take());
        dense.or_else(|| self.forward.remove(&id))
    }

    pub(crate) fn forward_len(&self) -> usize {
        self.dense_forward.iter().filter(|v| v.is_some()).count() + self.forward.len()
    }

    /// `true` once the forward payload for the sealed range has been dropped to
    /// disk (Phase 2f-1): the segment is attached AND the in-RAM `forward` map
    /// no longer holds sealed ids. After a production seal, EVERY forward read
    /// (predicate, value retrieval, drop) must route through `keyword_at`, not a
    /// raw `forward.get`, because the payload is on the mmap, not in RAM.
    #[inline]
    fn forward_dropped(&self) -> bool {
        self.segment.is_some()
    }

    /// UNIFIED inverted-index accessor (Phase 2h-1): term `value`'s posting
    /// bitmap on the ACTIVE source, composing the disk segment (sealed base ids
    /// `[0..seg.n_docs)`) with the in-RAM `terms` tail (ids added after the
    /// seal, or — when no segment is attached — the WHOLE inverted index).
    ///
    /// - segment OFF (no segment attached): returns `Cow::Borrowed` of
    ///   `terms[value]` — ZERO clone, byte-for-byte the old `k.terms.get(value)`
    ///   path that every Term/Terms/selectivity/planner site used to read.
    /// - segment ON: decodes the segment's stored postings for `value` (the
    ///   RAM `terms` index was DROPPED at seal) and UNIONs any live-tail
    ///   postings the runtime indexed back into `terms` for the same value. The
    ///   union is `Cow::Owned`. Both bases are ascending-docid RoaringBitmaps,
    ///   so the result is identical to what an un-dropped in-RAM `terms` held.
    ///
    /// `None` only when the term is absent from BOTH sources (so callers keep
    /// their `.unwrap_or_default()` empty-posting semantics).
    #[inline]
    pub(crate) fn term_postings(&self, value: &str) -> Option<std::borrow::Cow<'_, RoaringBitmap>> {
        if let Some(seg) = &self.segment {
            // Segment base, MINUS the query-time tombstone (base docids deleted
            // since the last seal — the on-disk postings can't be mutated, so the
            // delete is applied here). Then UNION the live-tail postings indexed
            // back into `terms` after the seal. Tombstones only ever hold base ids
            // (`< seg.n_docs`) and tail ids are `>= seg.n_docs`, so the subtraction
            // never touches the tail.
            let mut base = seg.keyword_postings(value).unwrap_or_default();
            if !self.tombstones.is_empty() {
                base -= &self.tombstones;
            }
            if let Some(t) = self.terms.get(value) {
                base |= t;
            }
            // Empty result ⇒ `None`, matching the in-RAM semantics where a
            // fully-deleted term is removed from `terms` (Term on a value with no
            // live doc yields `None`, never an empty posting set).
            return if base.is_empty() {
                None
            } else {
                Some(std::borrow::Cow::Owned(base))
            };
        }
        self.terms.get(value).map(std::borrow::Cow::Borrowed)
    }

    /// UNIFIED document-frequency accessor (Phase 2h-1): term `value`'s `df` on
    /// the ACTIVE source — the boolean planner's rarest-first selectivity input.
    /// segment OFF: the live `terms[value]` length. segment ON: the segment's
    /// CHEAP count-prefix df (no posting decode) PLUS any live-tail length. The
    /// sum can only over-count if the same id were in both base and tail, which
    /// the seal+tail split forbids (tail ids are `>= seg.n_docs`), so it is the
    /// exact union cardinality. 0 when the term is absent everywhere.
    ///
    /// TOMBSTONE NOTE (Phase 2h-1 FIX): this deliberately does NOT subtract the
    /// query-time `tombstones` (a delete-after-seal would leave the count-prefix
    /// df slightly HIGH for the affected term). `df` only drives the boolean
    /// planner's rarest-first clause ORDERING — never the result set, which comes
    /// from `term_postings` (which DOES subtract the tombstone). A small df
    /// over-count can at worst pick a marginally less-rare lead clause; the
    /// emitted documents are identical. Keeping df off the full posting decode is
    /// the whole point of the count-prefix, so the over-count is accepted.
    #[inline]
    pub(crate) fn term_df(&self, value: &str) -> u64 {
        if let Some(seg) = &self.segment {
            let base = seg.keyword_df(value).unwrap_or(0);
            let tail = self.terms.get(value).map(|p| p.len()).unwrap_or(0);
            return base + tail;
        }
        self.terms.get(value).map(|p| p.len()).unwrap_or(0)
    }

    /// Every distinct term with its LIVE posting bitmap, on the ACTIVE source —
    /// the unified enumeration that `unique_terms` and `duplicates` drive from so
    /// they stay correct after a seal drops the in-RAM `terms` driver (Phase 2h-1
    /// FIX). Terms with no live doc are EXCLUDED (the same invariant the in-RAM
    /// `terms` map keeps — `drop_eid` removes an emptied term).
    ///
    /// - segment OFF: clones each live `terms` entry verbatim (the bitmaps are
    ///   already free of deleted ids — a live-path delete mutated them in place).
    /// - segment ON: enumerate the segment dict (`keyword_terms_all`), subtract
    ///   the query-time `tombstones` from each base posting, UNION the live-tail
    ///   `terms` for the same value, and keep the term iff >=1 live doc remains.
    ///   Tail-only terms (indexed after the seal, absent from the dict) are folded
    ///   in too. The result MATCHES the in-RAM `terms` snapshot on the same data.
    pub(crate) fn live_terms(&self) -> BTreeMap<String, RoaringBitmap> {
        let Some(seg) = &self.segment else {
            return self.terms.clone();
        };
        let mut out: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        if let Some(dict) = seg.keyword_terms_all() {
            for (term, mut postings) in dict {
                if !self.tombstones.is_empty() {
                    postings -= &self.tombstones;
                }
                if let Some(tail) = self.terms.get(&term) {
                    postings |= tail;
                }
                if !postings.is_empty() {
                    out.insert(term, postings);
                }
            }
        }
        // Fold any live-tail-only terms the dict did not carry (indexed after the
        // seal). A term already present from the dict is left as the merged set.
        for (term, tail) in &self.terms {
            if !out.contains_key(term) {
                out.insert(term.clone(), tail.clone());
            }
        }
        out
    }

    /// Streams live keyword posting buckets in field-sort order without first
    /// materializing every `(term, posting)` pair. For a sealed field it
    /// merges the ordinal dictionary and the sorted live tail one entry at a
    /// time. Equal terms union the tail posting after tombstones are removed;
    /// tail-only terms retain their lexical place. Any torn ordinal entry is
    /// fail-closed and lets the caller take the generic exact fallback.
    pub(crate) fn visit_sorted_posting_buckets<F>(
        &self,
        descending: bool,
        mut visit: F,
    ) -> Result<KeywordBucketWalk>
    where
        F: FnMut(&RoaringBitmap) -> Result<bool>,
    {
        if let Some(segment) = &self.segment {
            let mut cursor = match segment.string_terms(descending) {
                Ok(cursor) => cursor,
                Err(_) => return Ok(KeywordBucketWalk::Unavailable),
            };
            let mut disk = match cursor.next() {
                Ok(term) => term,
                Err(_) => return Ok(KeywordBucketWalk::Unavailable),
            };
            if descending {
                let mut tail = self.terms.iter().rev().peekable();
                loop {
                    let (term, take_disk, take_tail) = match (disk.as_ref(), tail.peek()) {
                        (None, None) => break,
                        (Some(term), None) => (term.clone(), true, false),
                        (None, Some((term, _))) => ((*term).clone(), false, true),
                        (Some(disk_term), Some((tail_term, _))) => {
                            match disk_term.cmp(*tail_term) {
                                std::cmp::Ordering::Greater => (disk_term.clone(), true, false),
                                std::cmp::Ordering::Equal => (disk_term.clone(), true, true),
                                std::cmp::Ordering::Less => ((*tail_term).clone(), false, true),
                            }
                        }
                    };
                    let mut posting = if take_disk {
                        segment.keyword_postings(&term).unwrap_or_default()
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
                        disk = match cursor.next() {
                            Ok(term) => term,
                            Err(_) => return Ok(KeywordBucketWalk::Unavailable),
                        };
                    }
                    if !posting.is_empty() && !visit(&posting)? {
                        return Ok(KeywordBucketWalk::Stopped);
                    }
                }
            } else {
                let mut tail = self.terms.iter().peekable();
                loop {
                    let (term, take_disk, take_tail) = match (disk.as_ref(), tail.peek()) {
                        (None, None) => break,
                        (Some(term), None) => (term.clone(), true, false),
                        (None, Some((term, _))) => ((*term).clone(), false, true),
                        (Some(disk_term), Some((tail_term, _))) => {
                            match disk_term.cmp(*tail_term) {
                                std::cmp::Ordering::Less => (disk_term.clone(), true, false),
                                std::cmp::Ordering::Equal => (disk_term.clone(), true, true),
                                std::cmp::Ordering::Greater => ((*tail_term).clone(), false, true),
                            }
                        }
                    };
                    let mut posting = if take_disk {
                        segment.keyword_postings(&term).unwrap_or_default()
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
                        disk = match cursor.next() {
                            Ok(term) => term,
                            Err(_) => return Ok(KeywordBucketWalk::Unavailable),
                        };
                    }
                    if !posting.is_empty() && !visit(&posting)? {
                        return Ok(KeywordBucketWalk::Stopped);
                    }
                }
            }
            return Ok(KeywordBucketWalk::Completed);
        }

        let postings: Box<dyn Iterator<Item = &RoaringBitmap>> = if descending {
            Box::new(self.terms.iter().rev().map(|(_, posting)| posting))
        } else {
            Box::new(self.terms.values())
        };
        for posting in postings {
            if !visit(posting)? {
                return Ok(KeywordBucketWalk::Stopped);
            }
        }
        Ok(KeywordBucketWalk::Completed)
    }
}

/// Values whose live posting holds >= 2 docs — seeds the duplicates side-index
/// (`dup_values`) when an inverted map is rebuilt wholesale (snapshot restore);
/// the write/delete paths maintain it incrementally afterwards.
pub(crate) fn dup_values_of<K: Ord + Clone>(map: &BTreeMap<K, RoaringBitmap>) -> BTreeSet<K> {
    map.iter()
        .filter(|(_, set)| set.len() >= 2)
        .map(|(k, _)| k.clone())
        .collect()
}
