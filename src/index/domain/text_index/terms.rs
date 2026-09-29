//! The count of distinct terms with a live document, for /stats: O(1) from the
//! composed reader when nothing is pending, a memoized dictionary walk when
//! deletes are.

use crate::index::domain::fast_hash::FastHashSet;
use crate::index::domain::text_index::{LiveTermCache, TextIndex};
use crate::index::infrastructure::checkpoint_projection::text_projection;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::storage::note_staged_term_probes;

impl TextIndex {
    /// Count of distinct tokens with >=1 LIVE doc on the ACTIVE source — the
    /// segment-aware `unique_terms` count for a sealed Text field (Phase 2h-4
    /// FIX), so it stays correct after a seal drops the in-RAM `tokens` driver.
    ///
    /// - segment OFF (no segment attached): `self.tokens.len()`
    ///   — byte-for-byte the old value (an in-RAM delete already removed an emptied
    ///   token from `tokens`).
    /// - segment ON: enumerate the segment token DICT (`text_tokens_all`); a token
    ///   counts iff >=1 of its base docids is NOT tombstoned. Then fold any live-tail
    ///   token (indexed after the seal into `tokens`) the dict did not carry. With NO
    ///   tombstones every dict token has a live posting, so this equals the dict size
    ///   + the disjoint tail — the same distinct-token count an un-dropped `tokens`
    ///   map would hold. A FULLY-deleted token drops out, matching the in-RAM
    ///   semantics where `drop_eid` removes an emptied token.
    ///
    /// STAGED ROWS AND COST (#4246). The count is
    ///
    ///   `|live composed terms| + |(staged tokens U tail tokens) \ live composed terms|`
    ///
    /// and NEITHER half walks the composed dictionary. The first half is
    /// [`crate::persistence::infrastructure::composed_segment::ComposedSegmentReader::known_distinct_terms`],
    /// an O(1) number maintained at publication (see its own doc block) and
    /// exact whenever there are no pending deletes; the second folds one
    /// dictionary binary search per staged/tail token, and both of those sets
    /// are bounded by one checkpoint interval of writes. So `/stats` costs
    /// O(staged tokens + tail tokens), never O(dictionary) and never
    /// O(terms x staged rows) — the per-term `tok_postings` walk cost 2.83 s at
    /// 100k documents and the per-term posting decode that replaced it cost
    /// 4.81 s, both past the perf cell's 5 s deadline at 500k.
    ///
    /// EXACTNESS is not traded away for that. A fully-deleted token still drops
    /// out: pending deletes, or a composition step that could hide an older row,
    /// take the walking fallback below, memoized per `(reader, tombstone set)`
    /// in `live_term_cache` so repeated reads of an unchanged index pay for it
    /// once. The membership test used for the fold follows the same split — a
    /// dictionary lookup on the O(1) path, a composed posting decode with the
    /// tombstone filter otherwise. A TORN staged dictionary falls back to the
    /// composing `text_projection::live_term_count` walk, and a torn segment
    /// dictionary reports the staged-plus-tail fold alone, both fail-closed the
    /// same way the torn-segment branch below is.
    pub(crate) fn live_unique_tokens(&self) -> u64 {
        let composed = self.composed_live_term_count();
        // Tokens carried ONLY by staged rows or the live tail. Bounded by one
        // checkpoint interval of writes, so this set never grows with the
        // corpus. Every staged row repeats the corpus-wide tokens (an n-gram
        // field carries the same ~1.4k terms in each row), so the candidates
        // are deduplicated BEFORE the composed probe: one probe per distinct
        // token, never one per (row, token) pair.
        let mut candidates: FastHashSet<std::borrow::Cow<'_, str>> = FastHashSet::default();
        for row in self.staged_rows.values() {
            let reader = row.reader();
            let Some(count) = reader.keyword_ordinal_count() else {
                return text_projection::live_term_count(self).unwrap_or(0);
            };
            note_staged_term_probes(u64::from(count));
            for ordinal in 0..count {
                let Some(term) = reader.keyword_term_at_ordinal_cow(ordinal) else {
                    return text_projection::live_term_count(self).unwrap_or(0);
                };
                candidates.insert(term);
            }
        }
        for token in self.tokens.keys() {
            candidates.insert(std::borrow::Cow::Borrowed(token.as_str()));
        }
        let extra = candidates
            .iter()
            .filter(|token| !self.composed_has_live_term(token))
            .count() as u64;
        composed + extra
    }

    /// Whether the sealed composition still holds `token` with at least one
    /// live (non-tombstoned) document.
    ///
    /// With no pending deletes and an additive composition, dictionary
    /// membership IS liveness — every dictionary entry was written with a
    /// non-empty posting and nothing has hidden one since — so this is a
    /// binary search per layer with no posting decode. Otherwise it probes the
    /// newest layer holding the token first, decoding each posting only as far
    /// as its first live docid, and materializes nothing. A torn block keeps
    /// the token as pending — the same fail-closed answer a torn dictionary
    /// gets.
    fn composed_has_live_term(&self, token: &str) -> bool {
        let Some(seg) = &self.segment else {
            return false;
        };
        if self.tombstones.is_empty() && seg.known_distinct_terms().is_some() {
            return seg.has_dictionary_term(token);
        }
        seg.text_term_has_live_doc(token, &self.tombstones)
            .unwrap_or(false)
    }

    /// Distinct terms the sealed composition still holds with a live document.
    fn composed_live_term_count(&self) -> u64 {
        let Some(seg) = &self.segment else {
            return 0;
        };
        if self.tombstones.is_empty() {
            if let Some(known) = seg.known_distinct_terms() {
                return known;
            }
        }
        self.walked_composed_live_term_count(seg)
    }

    /// The fallback: ONE streaming dictionary walk, memoized against the exact
    /// reader and tombstone set it was measured on. A poisoned cache lock is
    /// recovered rather than propagated — this is a read-only stats accessor.
    fn walked_composed_live_term_count(&self, seg: &std::sync::Arc<ComposedSegmentReader>) -> u64 {
        let mut cache = self
            .live_term_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = cache.as_ref() {
            if entry
                .reader
                .upgrade()
                .is_some_and(|reader| std::sync::Arc::ptr_eq(&reader, seg))
                && entry.tombstones == self.tombstones
            {
                return entry.value;
            }
        }
        let Some(count) = seg.live_text_term_count(&self.tombstones) else {
            // Torn dictionary or posting block: discard the walk and report only
            // what the staged rows and the live tail can prove (never panic here).
            return 0;
        };
        *cache = Some(LiveTermCache {
            reader: std::sync::Arc::downgrade(seg),
            tombstones: self.tombstones.clone(),
            value: count,
        });
        count
    }
}
