//! BM25 scoring and the ranked match page: the per-token contribution every
//! match path shares, the bounded heap a top-k keeps, and one match query's
//! cached ranking, sorted only as far as the page asked for, with the
//! single-token ranking that fills it.

use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::Mutex;

use crate::index::domain::interner::Interner;
use crate::index::domain::text_index::TextIndex;
use crate::shared_kernel::types::query::MatchOp;

const BM25_K1: f32 = 1.2;
const BM25_B: f32 = 0.75;

/// One token's BM25 contribution for a single doc — the LITERAL expression
/// shared by every match path so the four scoring sites (the live/segment map
/// build in `eval_match`, its AND branch, and the `Vec` fast path
/// `eval_match_vec`) compute bit-identical f32s. Do NOT reassociate
/// `tf + K1*(1.0 - B + B*doc_len/avgdl)` or reorder the final `/`.
#[inline(always)]
pub(super) fn bm25_contrib(idf: f32, tf: f32, doc_len: f32, avgdl: f32) -> f32 {
    let denom = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_len / avgdl);
    idf * tf * (BM25_K1 + 1.0) / denom
}

#[inline]
pub(super) fn bm25_contrib_cached(
    cache: &mut Vec<(u32, u32, f32)>,
    idf: f32,
    tf: u32,
    doc_len: u32,
    avgdl: f32,
) -> f32 {
    for &(cached_tf, cached_len, score) in cache.iter() {
        if cached_tf == tf && cached_len == doc_len {
            return score;
        }
    }
    let score = bm25_contrib(idf, tf as f32, doc_len as f32, avgdl);
    if cache.len() < 32 {
        cache.push((tf, doc_len, score));
    }
    score
}

#[inline]
pub(super) fn text_doc_len_at(idx: &TextIndex, segment_doc_lens: Option<&[u32]>, id: u32) -> u32 {
    if idx.staged_rows.contains_key(&id) || idx.distinct_at(id).is_some() {
        return idx.doc_len(id);
    }
    segment_doc_lens
        .and_then(|lens| lens.get(id as usize).copied())
        .unwrap_or_else(|| idx.doc_len(id))
}

/// Heap entry for bounded top-k ranking. `BinaryHeap` keeps the greatest item at
/// the top, so this `Ord` intentionally makes the *worst* retained hit greatest:
/// lower score is worse; for an equal score, larger external_id is worse.
#[derive(Debug)]
pub(super) struct TopRankedHit {
    id: u32,
    score: f32,
    external_id: String,
}

impl PartialEq for TopRankedHit {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.score.to_bits() == other.score.to_bits()
            && self.external_id == other.external_id
    }
}

impl Eq for TopRankedHit {}

impl Ord for TopRankedHit {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.score.total_cmp(&other.score) {
            CmpOrdering::Less => CmpOrdering::Greater,
            CmpOrdering::Greater => CmpOrdering::Less,
            CmpOrdering::Equal => self.external_id.cmp(&other.external_id),
        }
    }
}

impl PartialOrd for TopRankedHit {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

#[inline]
pub(super) fn push_top_ranked(
    heap: &mut BinaryHeap<TopRankedHit>,
    k: usize,
    interner: &Interner,
    id: u32,
    score: f32,
) {
    if k == 0 {
        return;
    }
    if heap.len() < k {
        heap.push(TopRankedHit {
            id,
            score,
            external_id: interner.resolve(id).to_string(),
        });
        return;
    }

    let Some(worst) = heap.peek() else {
        return;
    };
    let better_than_worst = match score.total_cmp(&worst.score) {
        CmpOrdering::Greater => true,
        CmpOrdering::Less => false,
        CmpOrdering::Equal => interner.resolve(id) < worst.external_id.as_str(),
    };
    if better_than_worst {
        heap.pop();
        heap.push(TopRankedHit {
            id,
            score,
            external_id: interner.resolve(id).to_string(),
        });
    }
}

pub(super) fn finish_top_ranked(heap: BinaryHeap<TopRankedHit>) -> Vec<(u32, f32)> {
    let mut ranked = heap.into_vec();
    ranked.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(CmpOrdering::Equal)
            .then_with(|| a.external_id.cmp(&b.external_id))
    });
    ranked.into_iter().map(|h| (h.id, h.score)).collect()
}

pub(super) fn match_rank_key(op: MatchOp, tokens: &[String]) -> String {
    let mut key = match op {
        MatchOp::And => String::from("and"),
        MatchOp::Or => String::from("or"),
    };
    for token in tokens {
        key.push('\0');
        key.push_str(token);
    }
    key
}

/// Cached BM25 ranking for one match query (keyed by `match_rank_key`),
/// lazily extended to serve whatever page is actually asked for.
/// `entries[..sorted_len]` is fully sorted by `match_rank_cmp` (score desc,
/// then external id asc — the SAME order `build_and_ranked`/
/// `build_single_token_ranked` used to produce via an eager full `sort_by`);
/// `entries[sorted_len..]` holds the rest of the matches in arbitrary order.
/// Paging never needs the whole set sorted — only `offset + limit` — so a
/// cold query pays `select_nth_unstable_by` (O(n) average) to place that
/// boundary, then sorts only the prefix, instead of an O(n log n) sort of
/// every match (the other half of the cold-500k-AND perf fix, alongside the
/// zipper probe in `build_and_ranked`).
#[derive(Debug)]
pub(in crate::index) struct MatchRankCache {
    pub(super) entries: Vec<(u32, f32)>,
    pub(super) sorted_len: usize,
}

/// The ranking order every match path shares: score desc, then external id
/// asc as a deterministic tie-break. Byte-identical to the comparator the
/// old eager `sort_by` used.
#[inline]
fn match_rank_cmp(interner: &Interner, a: &(u32, f32), b: &(u32, f32)) -> CmpOrdering {
    b.1.partial_cmp(&a.1)
        .unwrap_or(CmpOrdering::Equal)
        .then_with(|| interner.resolve(a.0).cmp(interner.resolve(b.0)))
}

/// Extends `state.entries[..state.sorted_len]` to at least `want` sorted
/// entries (clamped to the total match count). A no-op when the prefix is
/// already that long. `select_nth_unstable_by` is safe to re-run over the
/// whole (possibly already-partitioned) vector for a larger `want`: it only
/// depends on `match_rank_cmp`, not on any prior partition, and the element
/// multiset is unchanged by an earlier partial partition/sort.
pub(super) fn ensure_sorted_prefix(state: &mut MatchRankCache, want: usize, interner: &Interner) {
    let want = want.min(state.entries.len());
    if want <= state.sorted_len {
        return;
    }
    if want > 0 && want < state.entries.len() {
        state
            .entries
            .select_nth_unstable_by(want - 1, |a, b| match_rank_cmp(interner, a, b));
    }
    state.entries[..want].sort_by(|a, b| match_rank_cmp(interner, a, b));
    state.sorted_len = want;
}

pub(super) fn cached_match_ranked_page(
    idx: &TextIndex,
    cache_key: &str,
    k: usize,
    interner: &Interner,
) -> Option<(Vec<(u32, f32)>, u64)> {
    let cell = idx
        .match_rank_cache
        .read()
        .ok()
        .and_then(|cache| cache.get(cache_key).cloned())?;
    let mut state = cell.lock().unwrap_or_else(|poison| poison.into_inner());
    let total = state.entries.len() as u64;
    let want = k.min(state.entries.len());
    ensure_sorted_prefix(&mut state, want, interner);
    Some((state.entries[..want].to_vec(), total))
}

/// Inserts a freshly built (unsorted) ranking into `idx.match_rank_cache` and
/// returns the requested page. The cache stores the RAW scored entries and
/// only sorts as far as `k` — see `MatchRankCache`.
pub(super) fn insert_match_rank_cache_and_page(
    idx: &TextIndex,
    cache_key: String,
    entries: Vec<(u32, f32)>,
    k: usize,
    interner: &Interner,
) -> (Vec<(u32, f32)>, u64) {
    let total = entries.len() as u64;
    let want = k.min(entries.len());
    let cell = std::sync::Arc::new(Mutex::new(MatchRankCache {
        entries,
        sorted_len: 0,
    }));
    if let Ok(mut cache) = idx.match_rank_cache.write() {
        cache.insert(cache_key, cell.clone());
    }
    let mut state = cell.lock().unwrap_or_else(|poison| poison.into_inner());
    ensure_sorted_prefix(&mut state, want, interner);
    (state.entries[..want].to_vec(), total)
}

pub(super) struct SingleTokenRankInput<'a> {
    pub(super) idx: &'a TextIndex,
    pub(super) docids: &'a [u32],
    pub(super) tfs: &'a [u32],
    pub(super) segment_doc_lens: Option<&'a [u32]>,
    pub(super) idf: f32,
    pub(super) avgdl: f32,
}

/// Builds the UNSORTED per-doc BM25 scores for the single-token match shape.
/// Sorting (when, and only as far as, a page needs it) happens later via
/// `MatchRankCache`/`ensure_sorted_prefix` — see that type's doc comment.
pub(super) fn build_single_token_ranked(input: SingleTokenRankInput<'_>) -> Vec<(u32, f32)> {
    let mut ranked = Vec::with_capacity(input.docids.len());
    let mut score_cache = Vec::with_capacity(4);
    for i in 0..input.docids.len() {
        let id = input.docids[i];
        let doc_len = text_doc_len_at(input.idx, input.segment_doc_lens, id);
        ranked.push((
            id,
            bm25_contrib_cached(
                &mut score_cache,
                input.idf,
                input.tfs[i],
                doc_len,
                input.avgdl,
            ),
        ));
    }
    ranked
}

#[cfg(test)]
pub(super) mod tests;
