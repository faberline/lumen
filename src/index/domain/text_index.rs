//! A text field's inverted index: per-token postings with term frequencies, doc
//! lengths and the BM25 corpus scalars, over a sealed segment base plus the
//! live and staged overlays written since, with the tombstones that hide
//! deleted base docs until the next seal.

mod query;
mod terms;

#[cfg(test)]
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::postings::Postings;
use crate::index::domain::query::rank::MatchRankCache;
use crate::index::domain::token_set::TokenSet;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

#[cfg(test)]
thread_local! {
    // #4246 cost oracle: how many (term, staged row) pairs a read path
    // inspected. `/stats` must stay O(live terms + staged tokens); the
    // per-term staged scan it replaced was O(terms x staged_rows).
    static STAGED_TERM_PROBES: Cell<u64> = const { Cell::new(0) };
}

/// Count one inspection of a staged Text row on behalf of one term. Compiled
/// out entirely outside `cfg(test)`.
#[inline]
fn note_staged_term_probes(_probes: u64) {
    #[cfg(test)]
    STAGED_TERM_PROBES.with(|probes| probes.set(probes.get().saturating_add(_probes)));
}

#[cfg(test)]
fn reset_staged_term_probes() {
    STAGED_TERM_PROBES.with(|probes| probes.set(0));
}

#[cfg(test)]
fn staged_term_probes() -> u64 {
    STAGED_TERM_PROBES.with(Cell::get)
}

#[derive(Debug, Default)]
pub(in crate::index) struct TextIndex {
    /// Prepared rows are immutable disk payloads. Their local row is always 0.
    /// They remain live overlays until the matching checkpoint is published.
    ///
    /// DRAIN RULE (#4246). The only production drain is checkpoint
    /// publication: a published generation's `attach_live_checkpoint_delta`
    /// calls `retire_live_delta_overlay`, which removes every docid the
    /// capture absorbed, and a full-base `prepared` install replaces the index
    /// outright. Nothing else clears this map — `seal_to_segment` is a
    /// test/manual path — so the set is bounded by the writes admitted since
    /// the last publication (one `LUMEN_SNAPSHOT_SECS` interval), NOT by the
    /// corpus. Rows written WHILE a checkpoint runs are outside its barrier
    /// and belong to the next generation, so a single quiescent interval
    /// always returns this map to empty. Every read that folds these rows must
    /// therefore cost at most O(staged tokens) per call and never
    /// O(corpus terms x staged rows).
    pub(in crate::index) staged_rows:
        BTreeMap<u32, Arc<crate::index::infrastructure::staging::staged_text_row::StagedTextRow>>,
    /// token → flat docid-sorted postings (docid + tf).
    pub(in crate::index) tokens: BTreeMap<String, Postings>,
    /// Dense doc-len indexed by doc-id (zero may be an explicit empty value).
    /// Replaces a per-doc `forward` HashMap probe with a sequential Vec read on
    /// the hot loop.
    pub(in crate::index) lens: Vec<u32>,
    /// dense doc-id → distinct tokens emitted. Used ONLY by `drop_eid` /
    /// snapshots / coverage, so new writes avoid a per-doc HashMap insert.
    pub(in crate::index) distinct: Vec<Option<TokenSet>>,
    // Cold delta rows keep their stable runtime IDs without allocating a dense prefix.
    pub(in crate::index) delta_docs: FastHashMap<u32, (u32, TokenSet)>,
    pub(in crate::index) doc_count: u64,
    pub(in crate::index) total_doc_len: u64,
    pub(in crate::index) bytes: u64,
    /// Stage 2 disk-tier (Phase 2e-B): a sealed columnar mmap segment covering
    /// the WHOLE field for doc ids `[0..n_docs)` — a sorted token DICT + a
    /// parallel per-token STORED posting block (text tf is NOT rebuildable, so
    /// the inverted postings live on disk) + a fixed `u32[n_docs]` DocLen column
    /// + the BM25 corpus scalars in the header. When present, the BM25 scan
    /// (`eval_match` / `match_doc_score`) and `estimate_selectivity` read from
    /// the composed sealed-base plus live-overlay state. The `distinct` map
    /// retains explicit live overlays and is otherwise dropped with the sealed
    /// base. DEFAULTS to `None`; while it is `None` (nothing sealed) every read
    /// path is byte-for-byte the in-RAM path. Purely additive.
    pub(in crate::index) segment: Option<std::sync::Arc<ComposedSegmentReader>>,
    /// Hot BM25 rankings for unique-doc match shapes (single token, multi-token
    /// AND), cleared on any text mutation/seal so cached scores never cross
    /// corpus states. Each entry is lazily sorted only as far as the largest
    /// page requested so far — see `MatchRankCache`, the cold-500k-AND perf
    /// fix that replaces an eager full sort of every match on every cold query.
    pub(in crate::index) match_rank_cache:
        RwLock<FastHashMap<String, std::sync::Arc<Mutex<MatchRankCache>>>>,
    /// QUERY-TIME TOMBSTONE (Phase 2h-4): base docids `[0..seg.n_docs)` deleted
    /// SINCE the last seal. The inverted `tokens` postings AND the corpus-deriving
    /// `distinct` map were DROPPED to disk at seal, so `drop_eid` can no longer
    /// remove a sealed base id from the immutable on-disk posting blocks — instead
    /// it records the id here, decrements the LIVE corpus scalars
    /// (`doc_count`/`total_doc_len`), and the BM25 scan SUBTRACTS this set from
    /// every token's segment posting BEFORE computing `df` and BEFORE scoring. So
    /// `df' = |posting − tombstones|`, the `idf` uses `df'`, and a tombstoned doc
    /// is never scored — byte-identical to an in-RAM oracle that physically removed
    /// the doc. The next seal bakes the deletions into the new segment (its
    /// `live(id)` gather excludes them) and this is reset to empty. Live-tail ids
    /// (`>= seg.n_docs`) are NOT tombstoned — they are deleted directly out of the
    /// in-RAM `tokens`/`distinct` tail. DEFAULTS empty; stays empty while no
    /// segment is attached (the in-RAM `tokens` path needs no tombstone — a delete
    /// mutates `tokens` directly). Mirrors 2h-1/2h-2/2h-3's four touch-points.
    pub(in crate::index) tombstones: RoaringBitmap,
    /// Memoized composed live-term count (#4246). Only consulted when the
    /// composed reader cannot answer in O(1) — i.e. when there are pending
    /// deletes, or a compaction left the additive invariant behind. Keyed by
    /// the exact reader identity AND the exact tombstone set, so it is
    /// impossible for it to serve a stale number.
    pub(in crate::index) live_term_cache: Mutex<Option<LiveTermCache>>,
}

/// Memoized composed live-term count for one `(reader, tombstone set)` pair.
///
/// The reader is held by `Weak` so the entry can never alias a later reader at
/// a reused address, and the tombstone set is stored by value so a delete and
/// an un-delete between two reads can never look unchanged. Both are bounded by
/// the deletes taken since the last seal, never by the corpus.
#[derive(Debug)]
pub(in crate::index) struct LiveTermCache {
    reader: std::sync::Weak<ComposedSegmentReader>,
    tombstones: RoaringBitmap,
    value: u64,
}

impl TextIndex {
    pub(in crate::index) fn clear_match_rank_cache(&self) {
        if let Ok(mut cache) = self.match_rank_cache.write() {
            cache.clear();
        }
    }

    pub(in crate::index) fn doc_len(&self, id: u32) -> u32 {
        if let Some(row) = self.staged_rows.get(&id) {
            return row.doc_len();
        }
        if let Some((len, _)) = self.delta_docs.get(&id) {
            return *len;
        }
        // `distinct` is also the presence marker for a live overlay.  Check it
        // before the sealed base so a replacement of a base id uses its new
        // length, including an explicit empty value (Some(empty)).
        if self.distinct_at(id).is_some() {
            return self.lens.get(id as usize).copied().unwrap_or(0);
        }
        if let Some(seg) = &self.segment {
            if id < seg.n_docs() && self.tombstones.contains(id) {
                return 0;
            }
            return seg.text_doc_len(id);
        }
        self.lens.get(id as usize).copied().unwrap_or(0)
    }
    pub(in crate::index) fn distinct_at(&self, id: u32) -> Option<&TokenSet> {
        self.delta_docs
            .get(&id)
            .map(|(_, tokens)| tokens)
            .or_else(|| self.distinct.get(id as usize).and_then(Option::as_ref))
    }

    pub(in crate::index) fn take_distinct(&mut self, id: u32) -> Option<TokenSet> {
        if let Some((_, tokens)) = self.delta_docs.remove(&id) {
            return Some(tokens);
        }
        self.distinct
            .get_mut(id as usize)
            .and_then(|tokens| tokens.take())
    }

    /// Remove a current live overlay, if present. The presence marker is the
    /// `Some(TokenSet)` entry, so an explicit empty text value is removed here
    /// too. This must run before sealed-base tombstone handling for reused ids.
    pub(super) fn drop_live_overlay(&mut self, id: u32, eid: &str) -> Option<u64> {
        if let Some(row) = self.staged_rows.remove(&id) {
            let freed = row.indexed_bytes(eid);
            self.doc_count = self.doc_count.saturating_sub(1);
            self.total_doc_len = self.total_doc_len.saturating_sub(u64::from(row.doc_len()));
            self.bytes = self.bytes.saturating_sub(freed);
            return Some(freed);
        }
        let doc_len = self.doc_len(id);
        let tokens = self.take_distinct(id)?;
        let mut freed = 0u64;
        for tok in tokens.iter() {
            if let Some(p) = self.tokens.get_mut(tok) {
                if p.remove(id) {
                    freed += (tok.len() + eid.len()) as u64;
                }
                if p.docids.is_empty() {
                    self.tokens.remove(tok);
                }
            }
        }
        if let Some(len) = self.lens.get_mut(id as usize) {
            *len = 0;
        }
        self.doc_count = self.doc_count.saturating_sub(1);
        self.total_doc_len = self.total_doc_len.saturating_sub(doc_len as u64);
        self.bytes = self.bytes.saturating_sub(freed);
        Some(freed)
    }

    #[cfg(test)]
    pub(in crate::index) fn distinct_is_empty(&self) -> bool {
        self.delta_docs.is_empty() && self.distinct.iter().all(Option::is_none)
    }

    pub(in crate::index) fn distinct_iter(&self) -> impl Iterator<Item = (u32, &TokenSet)> {
        self.distinct
            .iter()
            .enumerate()
            .filter_map(|(id, tokens)| tokens.as_ref().map(|tokens| (id as u32, tokens)))
            .chain(
                self.delta_docs
                    .iter()
                    .map(|(&id, (_, tokens))| (id, tokens)),
            )
    }

    pub(super) fn distinct_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.distinct_iter().map(|(id, _)| id)
    }

    /// `(n, total_len)` for the BM25 corpus — ALWAYS the LIVE scalars
    /// (`self.doc_count`, `self.total_doc_len`), never the segment header (Phase
    /// 2h-4 FIX). On reopen the live scalars are INITIALIZED from the header
    /// (`open_from_segment`); the index path increments them on every doc; and
    /// `drop_eid` DECREMENTS them on a delete (including a sealed-base delete that
    /// only tombstones the immutable posting). The segment header is a base-only
    /// SEAL-TIME snapshot — using it would (a) keep a stale `N`/`avgdl` after a
    /// delete-after-seal (the crux this phase closes) and (b) undercount a
    /// post-seal live tail. The live scalars stay current across all three, so
    /// `n`/`avgdl` are byte-identical to an in-RAM oracle on the same op sequence.
    /// On a FIRST seal (no delete, no tail) the live scalars equal the header, so
    /// the BM25 score is unchanged from 2e-B. The `doc_count == 0` short-circuit in
    /// the caller mirrors the live path.
    pub(in crate::index) fn bm25_corpus(&self) -> (u64, u64) {
        (self.doc_count, self.total_doc_len)
    }

    /// The FULL token → postings map to seal, gathered from the ACTIVE source
    /// (Phase 2f-2 re-seal). On a FIRST seal (no segment) this is just a clone of
    /// the live `tokens`. On a RE-SEAL (a prior seal dropped `tokens` and put the
    /// base postings on the segment, while any docs indexed SINCE landed in the
    /// live `tokens` overlays), the base postings are decoded back out of the
    /// segment (text tf is STORED, so this round-trips bit-identically) and MERGED
    /// with the overlays. The merge keeps docids sorted and lets a live reused id
    /// override its sealed base posting. The result covers EVERY current docid,
    /// so a re-seal after a seal+drop+tail re-materializes the whole field.
    ///
    /// TOMBSTONE GC (Phase 2g-A): the prior-SEGMENT postings still hold a deleted
    /// base doc (the segment is immutable; `drop_eid` only touched the live
    /// `tokens`/`distinct`). `live(id)` is dropped-doc-aware (`false` for a base
    /// doc deleted from this field since the prior seal), so the merged base
    /// postings omit it. Empty postings (every docid of a token deleted) are
    /// dropped so the new segment never carries a zero-df token. The live
    /// overlays' `tokens` already exclude deletes, so they are merged after the
    /// tombstoned base has been filtered.
    pub(in crate::index) fn tokens_for_seal(
        &self,
        live: &dyn Fn(u32) -> bool,
    ) -> BTreeMap<String, Postings> {
        let Some(seg) = &self.segment else {
            return self
                .tokens
                .iter()
                .map(|(tok, p)| (tok.clone(), p.clone()))
                .collect();
        };
        let mut out: BTreeMap<String, Postings> = BTreeMap::new();
        if let Some(entries) = seg.text_tokens_all() {
            for (tok, docids, tfs) in entries {
                // Drop any docid no longer live (deleted from this field since the
                // prior seal) from the immutable base posting block.
                let mut kept_ids = Vec::with_capacity(docids.len());
                let mut kept_tfs = Vec::with_capacity(tfs.len());
                for (&id, &tf) in docids.iter().zip(tfs.iter()) {
                    if live(id) && !self.tombstones.contains(id) {
                        kept_ids.push(id);
                        kept_tfs.push(tf);
                    }
                }
                if !kept_ids.is_empty() {
                    out.insert(
                        tok,
                        Postings {
                            docids: kept_ids,
                            tfs: kept_tfs,
                        },
                    );
                }
            }
        }
        // Merge the live tail (docs indexed after the prior seal). Tail ids are
        // strictly greater than every base id, so appending keeps docids sorted.
        for (tok, p) in &self.tokens {
            let entry = out.entry(tok.clone()).or_default();
            for (&id, &tf) in p.docids.iter().zip(p.tfs.iter()) {
                entry.upsert(id, tf);
            }
        }
        out
    }

    /// The `(doc_count, total_doc_len)` BM25 corpus scalars to seal — the LIVE
    /// counters, which the index path increments on EVERY doc regardless of a
    /// sealed segment, so they are the FULL corpus (base + any post-seal tail).
    /// (The segment header's scalars are base-only and would undercount a re-seal,
    /// so they are deliberately NOT used here.) Phase 2f-2.
    fn corpus_for_seal(&self) -> (u64, u64) {
        (self.doc_count, self.total_doc_len)
    }

    /// The dense `u32[n_docs]` DocLen column to seal, covering EVERY current
    /// docid. Phase 2h-4 makes this SEGMENT-AWARE because the seal seam no longer
    /// keeps `lens` in RAM (it is dropped at seal — `doc_len()` reads the segment
    /// DocLen column for a sealed id): for a base id `< seg.n_docs` read the prior
    /// segment's DocLen column (`text_doc_len`); for a post-seal tail id read the
    /// live `self.lens`. Splitting the source is mandatory — the segment column is
    /// base-only (returns 0 for a tail id) and `self.lens` is now tail-only after a
    /// seal-and-drop (returns 0 for a base id), so each id MUST read from the side
    /// that holds it. On a FIRST seal (no segment) every id reads from `self.lens`,
    /// byte-identical to the old `self.lens` read.
    ///
    /// A NON-LIVE base id (deleted since the prior seal — its posting was
    /// tombstoned and its corpus length already decremented out of
    /// `corpus_for_seal`) MUST seal as DocLen 0, mirroring the in-RAM `drop_eid`
    /// (`set_doc_len(id, 0)`). The prior segment still carries the deleted doc's
    /// ORIGINAL nonzero length, so without the `live(id)` gate it would resurrect:
    /// (a) the new segment's DocLen column would re-introduce the deleted doc, and
    /// (b) `record_field_coverage` (which reads the segment presence bitset)
    /// would mark it as having written the field. Zeroing it keeps the new segment
    /// consistent with `tokens_for_seal` (postings GC'd) and `corpus_for_seal`
    /// (scalars decremented).
    fn lens_for_seal(&self, n_docs: u32, live: &dyn Fn(u32) -> bool) -> Vec<u32> {
        (0..n_docs)
            .map(|id| if live(id) { self.doc_len(id) } else { 0 })
            .collect()
    }

    /// The explicit text-field presence bits to seal. Presence is separate from
    /// token length so an explicit empty value remains covered after reopen.
    pub(in crate::index) fn present_for_seal(
        &self,
        n_docs: u32,
        live: &dyn Fn(u32) -> bool,
    ) -> Vec<bool> {
        (0..n_docs)
            .map(|id| {
                if !live(id) {
                    return false;
                }
                if self.staged_rows.contains_key(&id) || self.distinct_at(id).is_some() {
                    return true;
                }
                self.segment
                    .as_ref()
                    .is_some_and(|seg| seg.text_is_present(id))
            })
            .collect()
    }
}

#[cfg(test)]
pub(super) mod tests;
