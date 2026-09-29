//! A token's active postings for BM25 scoring: materialized, projected onto a
//! few candidates, or probed lazily, with the staged > live > sealed-minus-
//! tombstones precedence, and the token's live document frequency.

use std::cmp::Ordering as CmpOrdering;

use crate::index::domain::postings::{Postings, SparsePosting, TokPostings};
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::tok_probe::TokProbe;
use crate::persistence::infrastructure::composed_segment::TextPostingAt;
use crate::persistence::infrastructure::segment::codecs::SortedIdCursor;
use crate::storage::note_staged_term_probes;

impl TextIndex {
    /// A token's active postings for BM25 scoring: sealed base postings minus
    /// tombstones, union live overlays (including tail-only tokens). A live
    /// posting wins when it reuses a base id. `None` means no active postings.
    ///
    /// TOMBSTONE SUBTRACTION (Phase 2h-4 FIX, the crux): on the segment path the
    /// decoded base posting still holds any base docid deleted SINCE the last seal
    /// (the on-disk block is immutable; `drop_eid` only recorded the id in
    /// `tombstones`). Those ids are filtered OUT here — BEFORE the `TokPostings` is
    /// handed to the BM25 scan. Because EVERY downstream read (`df()`, `docids()`,
    /// `tfs()`, `tf(id)`) goes through this single composed posting, the effect is
    /// uniform across `eval_match` (Or + And) and `match_doc_score`:
    ///   • `df' = |posting − tombstones|` ⇒ the `idf` uses the live document
    ///     frequency, so it equals an in-RAM oracle's `idf`;
    ///   • a tombstoned docid is absent from `docids()`/`tf(id)` ⇒ it is never
    ///     scored and never enters the result set.
    /// Combined with the LIVE corpus scalars (`bm25_corpus`, already decremented by
    /// the delete), `n`/`avgdl`/`idf`/per-doc score are byte-identical to an oracle
    /// that physically removed the doc. A FULLY-tombstoned token (every base docid
    /// deleted) collapses to an empty posting ⇒ `None`, matching the in-RAM
    /// semantics where `drop_eid` removes an emptied token from `tokens`. Tombstones
    /// only ever hold base ids (`< seg.n_docs`), so this never touches a tail id.
    #[inline]
    pub(crate) fn tok_postings(&self, tok: &str) -> Option<TokPostings<'_>> {
        if self.staged_rows.is_empty() {
            return self.unstaged_tok_postings(tok);
        }
        note_staged_term_probes(self.staged_rows.len() as u64);
        let mut incoming = Vec::new();
        for (&id, row) in &self.staged_rows {
            if let Some(posting) = row.reader().text_postings_arc(tok) {
                incoming.push((id, posting.1[0]));
            }
        }
        let older = self.unstaged_tok_postings(tok);
        if incoming.is_empty() {
            return older;
        }
        let old_ids = older.as_ref().map_or(&[][..], |p| p.docids());
        let old_tfs = older.as_ref().map_or(&[][..], |p| p.tfs());
        let mut docids = Vec::with_capacity(old_ids.len() + incoming.len());
        let mut tfs = Vec::with_capacity(old_ids.len() + incoming.len());
        let mut prior = old_ids
            .iter()
            .copied()
            .zip(old_tfs.iter().copied())
            .peekable();
        for (id, tf) in incoming {
            while prior.peek().is_some_and(|(older, _)| *older < id) {
                let (id, tf) = prior.next().unwrap();
                docids.push(id);
                tfs.push(tf);
            }
            if prior.peek().is_some_and(|(older, _)| *older == id) {
                prior.next();
            }
            docids.push(id);
            tfs.push(tf);
        }
        for (id, tf) in prior {
            docids.push(id);
            tfs.push(tf);
        }
        Some(TokPostings::Combined { docids, tfs })
    }

    /// [`Self::tok_postings`] for a SMALL ascending candidate set (#4246). With
    /// a segment attached the sealed posting is never materialized for the
    /// call: a cold posting is streamed once for its df and the candidates'
    /// tfs (`ComposedSegmentReader::text_posting_at`), a resident one is
    /// probed by binary search, and the overlays are folded in with the same
    /// precedence as `tok_postings` — staged > live > (segment − tombstones),
    /// a live row overriding a tombstoned base id. `df` and every candidate's
    /// tf are therefore identical to `tok_postings` by construction, so BM25
    /// stays byte-identical; only ids outside `candidates` are absent from
    /// the returned streams. Falls back to `tok_postings` when the sparse
    /// route is not cheaper or not exact (no segment, torn map, dense base
    /// lacking the token).
    pub(crate) fn tok_postings_at(&self, tok: &str, candidates: &[u32]) -> Option<TokPostings<'_>> {
        let Some(seg) = &self.segment else {
            return self.tok_postings(tok);
        };
        debug_assert!(candidates.windows(2).all(|w| w[0] < w[1]));
        let mut staged: Vec<(u32, u32)> = Vec::new();
        if !self.staged_rows.is_empty() {
            note_staged_term_probes(self.staged_rows.len() as u64);
            for (&id, row) in &self.staged_rows {
                if let Some(posting) = row.reader().text_postings_arc(tok) {
                    staged.push((id, posting.1[0]));
                }
            }
        }
        let staged_ids: Vec<u32> = staged.iter().map(|&(id, _)| id).collect();
        let live = self.tokens.get(tok);
        let live_ids = live.map_or(&[][..], |p| &p.docids[..]);
        let staged_has = |id: u32| staged_ids.binary_search(&id).is_ok();
        let live_has = |id: u32| live.is_some_and(|p| p.tf(id).is_some());
        let (seg_df, seg_hits): (usize, Vec<(u32, u32)>) = {
            let mut staged_cur = SortedIdCursor::new(&staged_ids);
            let mut live_cur = SortedIdCursor::new(live_ids);
            let tombstones = &self.tombstones;
            match seg.text_posting_at(tok, candidates, |id| {
                tombstones.contains(id) || staged_cur.contains(id) || live_cur.contains(id)
            }) {
                Some(TextPostingAt::Sparse { df, hits }) => (df, hits),
                Some(TextPostingAt::Cached(resident)) => {
                    // Resident: count the overlay ids the posting also carries
                    // (each overlay is bounded by one checkpoint interval, the
                    // posting is not) and probe the candidates directly.
                    let (ids, tfs) = resident.as_ref();
                    let seg_has = |id: u32| ids.binary_search(&id).is_ok();
                    let mut hidden = staged_ids.iter().filter(|&&id| seg_has(id)).count();
                    hidden += live_ids
                        .iter()
                        .filter(|&&id| !staged_has(id) && seg_has(id))
                        .count();
                    hidden += tombstones
                        .iter()
                        .filter(|&id| !staged_has(id) && !live_has(id) && seg_has(id))
                        .count();
                    let hits = candidates
                        .iter()
                        .filter(|&&id| !tombstones.contains(id) || live_has(id) || staged_has(id))
                        .filter_map(|&id| ids.binary_search(&id).ok().map(|pos| (id, tfs[pos])))
                        .collect();
                    (ids.len() - hidden, hits)
                }
                None => return self.tok_postings(tok),
            }
        };
        let df =
            staged.len() + (live_ids.len() - count_common_sorted(live_ids, &staged_ids)) + seg_df;
        if df == 0 {
            return None;
        }
        let mut docids = Vec::with_capacity(candidates.len());
        let mut tfs = Vec::with_capacity(candidates.len());
        let mut seg_hits = seg_hits.into_iter().peekable();
        for &id in candidates {
            let tf = match staged.binary_search_by_key(&id, |&(id, _)| id) {
                Ok(pos) => Some(staged[pos].1),
                Err(_) => live.and_then(|p| p.tf(id)),
            }
            .or_else(|| {
                while seg_hits.peek().is_some_and(|&(hit, _)| hit < id) {
                    seg_hits.next();
                }
                match seg_hits.peek() {
                    Some(&(hit, tf)) if hit == id => Some(tf),
                    _ => None,
                }
            });
            if let Some(tf) = tf {
                docids.push(id);
                tfs.push(tf);
            }
        }
        Some(TokPostings::Sparse(std::sync::Arc::new(SparsePosting {
            df,
            docids,
            tfs,
        })))
    }

    fn unstaged_tok_postings(&self, tok: &str) -> Option<TokPostings<'_>> {
        if let Some(seg) = &self.segment {
            // Phase 2m: hold the cache-resident `Arc` directly. With NO pending
            // delete (the common warm path) this is a refcount bump — the
            // per-candidate `match_doc_score` `.tf(id)` probe then binary-searches
            // the SHARED streams with no re-decode and no per-call vector copy (the
            // `filtered_search` 25x disk fix). The cached posting is the RAW
            // immutable stream; results are byte-identical because the RARE
            // tombstone branch below subtracts the deleted base docids.
            let cached = seg.text_postings_arc(tok);
            let live = self.tokens.get(tok);
            if cached.is_none() {
                return live.map(TokPostings::Live);
            }
            let cached = cached.unwrap();
            if self.tombstones.is_empty() && live.is_none() {
                return Some(TokPostings::Segment(cached));
            }
            let (docids_all, tfs_all) = cached.as_ref();
            let live = live.map(|p| (&p.docids[..], &p.tfs[..]));
            let live_len = live.map_or(0, |(ids, _)| ids.len());
            let mut docids = Vec::with_capacity(docids_all.len() + live_len);
            let mut tfs = Vec::with_capacity(tfs_all.len() + live_len);
            let mut base_pos = 0;
            let mut live_pos = 0;
            while base_pos < docids_all.len() || live_pos < live_len {
                let base_id = docids_all.get(base_pos).copied();
                let live_id = live.and_then(|(ids, _)| ids.get(live_pos).copied());
                match (base_id, live_id) {
                    (Some(base_id), Some(live_id)) if base_id < live_id => {
                        if !self.tombstones.contains(base_id) {
                            docids.push(base_id);
                            tfs.push(tfs_all[base_pos]);
                        }
                        base_pos += 1;
                    }
                    (Some(base_id), Some(live_id)) if live_id < base_id => {
                        docids.push(live_id);
                        tfs.push(live.unwrap().1[live_pos]);
                        live_pos += 1;
                    }
                    (Some(base_id), Some(_)) => {
                        // A live posting overrides a reused base id, even when
                        // the base id is also tombstoned.
                        docids.push(base_id);
                        tfs.push(live.unwrap().1[live_pos]);
                        base_pos += 1;
                        live_pos += 1;
                    }
                    (Some(base_id), None) => {
                        if !self.tombstones.contains(base_id) {
                            docids.push(base_id);
                            tfs.push(tfs_all[base_pos]);
                        }
                        base_pos += 1;
                    }
                    (None, Some(live_id)) => {
                        docids.push(live_id);
                        tfs.push(live.unwrap().1[live_pos]);
                        live_pos += 1;
                    }
                    (None, None) => break,
                }
            }
            if docids.is_empty() {
                return None;
            }
            return Some(TokPostings::Combined { docids, tfs });
        }
        self.tokens.get(tok).map(TokPostings::Live)
    }

    /// A LAZY, non-materializing view of a token's active postings (perf fix:
    /// the 500k-hot-doc cold `match … op: "and"` regression). Unlike
    /// `tok_postings`/`unstaged_tok_postings`, this never allocates a merged
    /// `Vec<u32>` — it composes the segment's cached `Arc` (zero-copy), the
    /// live `Postings` (borrowed), and the staged-row overrides (a small
    /// `Vec` bounded by `staged_rows.len()`, NOT by corpus size) behind
    /// random-access `tf(id)` and a streaming `iter_active()`, with the exact
    /// same precedence `tok_postings` uses: staged > live > (segment minus
    /// tombstones). `eval_match`'s `And` branch uses this so an N-token
    /// intersection probes N-1 tokens with zero-allocation binary searches and
    /// streams only the DRIVING (rarest) token's postings.
    pub(crate) fn tok_probe(&self, tok: &str) -> TokProbe<'_> {
        let seg = self
            .segment
            .as_ref()
            .and_then(|seg| seg.text_postings_arc(tok));
        let live = self.tokens.get(tok);
        let mut staged = Vec::new();
        if !self.staged_rows.is_empty() {
            note_staged_term_probes(self.staged_rows.len() as u64);
            for (&id, row) in &self.staged_rows {
                if let Some(posting) = row.reader().text_postings_arc(tok) {
                    staged.push((id, posting.1[0]));
                }
            }
        }
        TokProbe {
            seg,
            live,
            staged,
            tombstones: &self.tombstones,
        }
    }

    /// A token's LIVE document frequency (`df`) on the active source — the
    /// segment's stored posting length MINUS any tombstoned base docid when
    /// sealed, else the live posting length. Used by `estimate_selectivity`; 0 (⇒
    /// `None`) for a token absent or fully deleted. Phase 2e-B; tombstone-aware in
    /// 2h-4.
    ///
    /// With NO pending deletes (`tombstones` empty) this is the CHEAP count-prefix
    /// df (no posting decode) — the common path, unchanged from 2e-B. When a delete
    /// has tombstoned base docids it routes through `tok_postings` (which subtracts
    /// the tombstone and returns `None` for a fully-deleted token), so the `df`
    /// reflects only LIVE docs — consistent with the `df'` the BM25 `idf` uses and
    /// with an in-RAM oracle. (`estimate_selectivity` only orders clauses, but
    /// keeping `df` live avoids a stale over-count after a delete-after-seal.)
    #[inline]
    pub(crate) fn tok_df(&self, tok: &str) -> Option<usize> {
        if !self.staged_rows.is_empty() {
            return self.tok_postings(tok).map(|posting| posting.df());
        }
        if let Some(seg) = &self.segment {
            if self.tombstones.is_empty() {
                // With no tombstones, live writes are post-seal tail ids and
                // cannot overlap the sealed base. Keep this path cheap by
                // combining the segment count with this token's live count.
                let df = seg.text_token_df(tok) + self.tokens.get(tok).map_or(0, Postings::df);
                return (df > 0).then_some(df);
            }
            // Compose the sealed base and every live overlay, including tail-only
            // tokens and reused base ids.
            return self.tok_postings(tok).map(|p| p.df());
        }
        self.tokens.get(tok).map(|p| p.df())
    }
}

/// Number of ids present in both ascending, duplicate-free slices.
pub(super) fn count_common_sorted(a: &[u32], b: &[u32]) -> usize {
    let (mut i, mut j, mut common) = (0usize, 0usize, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            CmpOrdering::Less => i += 1,
            CmpOrdering::Greater => j += 1,
            CmpOrdering::Equal => {
                common += 1;
                i += 1;
                j += 1;
            }
        }
    }
    common
}
