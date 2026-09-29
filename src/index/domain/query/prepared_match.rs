//! A match clause scored per doc: its tokens resolved to postings once per
//! query, so each candidate costs a term-frequency lookup and no dictionary
//! search.

use std::collections::BinaryHeap;

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::analysis::tokenize;
use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::index::domain::postings::{SparsePosting, TokPostings};
use crate::index::domain::query::rank::{bm25_contrib_cached, finish_top_ranked, push_top_ranked};
use crate::index::domain::storage_error::StorageError;
use crate::index::domain::text_index::TextIndex;
use crate::shared_kernel::types::query::{MatchOp, QueryNode};

/// Per-doc BM25 for a `match` conjunct, identical to [`eval_match`]'s formula,
/// so a doc scored as a predicate gets the same contribution it would as a
/// materialized clause. `None` ⇒ the doc does not satisfy the match.
///
/// [`eval_match`]: crate::index::domain::query::text_match::eval_match
pub(super) fn match_doc_score(
    idx: &TextIndex,
    tokens: &[String],
    op: MatchOp,
    id: u32,
) -> Option<f32> {
    // Resolve the token→posting map ONCE, then score this doc. Single-doc callers
    // (the rare per-candidate predicate sites) get the same answer; the hot AND
    // bitmap loop instead builds a `PreparedMatch` ONCE and reuses it across all
    // candidates (see `score_prepared`) — that hoist is the `filtered_search` disk
    // fix (no per-candidate dict binary-search + cache fetch).
    let prepared = PreparedMatch::resolve(idx, tokens, op)?;
    prepared.score(idx, id)
}

/// A match clause with its per-token postings RESOLVED once (Phase 2m). On the
/// segment path each `TokPostings` holds the cache-resident posting `Arc`, so a
/// per-candidate score is a `.tf(id)` binary-search with NO dict lookup / cache
/// fetch / decode — the resolution (dict binary-search + cache get) is paid ONCE
/// per query instead of once per candidate doc, which is what blew up
/// `filtered_search` on disk (a wide candidate set × a re-resolve per doc).
pub(super) struct PreparedMatch<'a> {
    /// One entry per token: its resolved postings + precomputed idf. `None` for a
    /// token absent from the index (so AND can short-circuit, OR can skip).
    per_token: Vec<Option<(TokPostings<'a>, f32)>>,
    pub(super) op: MatchOp,
    n_tokens: usize,
    avgdl: f32,
}

impl<'a> PreparedMatch<'a> {
    const K1: f32 = 1.2;
    const B: f32 = 0.75;

    /// Resolve the postings for every token once. `None` when the corpus is empty
    /// or `tokens` is empty (matches the old `match_doc_score` early-outs exactly).
    pub(super) fn resolve(idx: &'a TextIndex, tokens: &[String], op: MatchOp) -> Option<Self> {
        let (corpus_n, corpus_total_len) = idx.bm25_corpus();
        if corpus_n == 0 || tokens.is_empty() {
            return None;
        }
        let n = corpus_n as f32;
        let avgdl = corpus_total_len as f32 / corpus_n as f32;
        let per_token = tokens
            .iter()
            .map(|tok| {
                idx.tok_postings(tok).map(|postings| {
                    let df = postings.df() as f32;
                    let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
                    (postings, idf)
                })
            })
            .collect();
        Some(PreparedMatch {
            per_token,
            op,
            n_tokens: tokens.len(),
            avgdl,
        })
    }

    /// [`Self::resolve`] for a SMALL candidate set (#4246): every token
    /// occurrence is resolved through [`TextIndex::tok_postings_at`] and
    /// memoized per distinct token, so a clause repeating a token pays one
    /// resolution. Scores are byte-identical to `resolve` — same corpus
    /// scalars, same `df`, same per-token order and float summation — because
    /// a sparse posting differs from the full one only in the ids it omits,
    /// none of which is a candidate. Empty `candidates` resolve to `None`:
    /// there is no doc to score.
    fn resolve_at(
        idx: &'a TextIndex,
        tokens: &[String],
        op: MatchOp,
        candidates: &[u32],
    ) -> Option<Self> {
        let (corpus_n, corpus_total_len) = idx.bm25_corpus();
        if corpus_n == 0 || tokens.is_empty() || candidates.is_empty() {
            return None;
        }
        let n = corpus_n as f32;
        let avgdl = corpus_total_len as f32 / corpus_n as f32;
        let mut memo: FastHashMap<&str, Option<std::sync::Arc<SparsePosting>>> =
            FastHashMap::default();
        let mut per_token = Vec::with_capacity(tokens.len());
        for tok in tokens {
            let resolved = match memo.get(tok.as_str()) {
                Some(Some(sparse)) => Some(TokPostings::Sparse(std::sync::Arc::clone(sparse))),
                Some(None) => None,
                None => {
                    let resolved = idx.tok_postings_at(tok, candidates);
                    match &resolved {
                        Some(TokPostings::Sparse(sparse)) => {
                            memo.insert(tok.as_str(), Some(std::sync::Arc::clone(sparse)));
                        }
                        None => {
                            memo.insert(tok.as_str(), None);
                        }
                        Some(_) => {}
                    }
                    resolved
                }
            };
            per_token.push(resolved.map(|postings| {
                let df = postings.df() as f32;
                let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
                (postings, idf)
            }));
        }
        Some(PreparedMatch {
            per_token,
            op,
            n_tokens: tokens.len(),
            avgdl,
        })
    }

    /// The planner's entry: `resolve_at` when the filter candidate set is
    /// small enough to have been projected (`Some`), `resolve` otherwise.
    pub(super) fn resolve_for(
        idx: &'a TextIndex,
        tokens: &[String],
        op: MatchOp,
        sparse_candidates: Option<&[u32]>,
    ) -> Option<Self> {
        match sparse_candidates {
            Some(candidates) => Self::resolve_at(idx, tokens, op, candidates),
            None => Self::resolve(idx, tokens, op),
        }
    }

    /// Score one doc against the pre-resolved postings — the identical BM25
    /// expression `match_doc_score` evaluated, just with the postings already in
    /// hand. Byte-identical result to the per-doc-resolved path.
    #[inline]
    pub(super) fn score(&self, idx: &TextIndex, id: u32) -> Option<f32> {
        let mut doc_len: Option<f32> = None;
        let mut score = 0.0f32;
        let mut matched = 0usize;
        for entry in &self.per_token {
            if let Some((postings, idf)) = entry {
                if let Some(tf) = postings.tf(id) {
                    let doc_len = *doc_len.get_or_insert_with(|| idx.doc_len(id) as f32);
                    let tf = tf as f32;
                    let denom = tf + Self::K1 * (1.0 - Self::B + Self::B * doc_len / self.avgdl);
                    score += idf * tf * (Self::K1 + 1.0) / denom;
                    matched += 1;
                }
            }
        }
        match self.op {
            MatchOp::And if matched == self.n_tokens => Some(score),
            MatchOp::Or if matched > 0 => Some(score),
            _ => None,
        }
    }

    pub(super) fn score_single_token_candidates_topk(
        &self,
        idx: &TextIndex,
        candidates: &RoaringBitmap,
        base: f32,
        interner: &Interner,
        k: usize,
    ) -> Option<(Vec<(u32, f32)>, u64)> {
        if self.n_tokens != 1 {
            return None;
        }
        let Some((postings, idf)) = self.per_token.first().and_then(|entry| entry.as_ref()) else {
            return Some((Vec::new(), 0));
        };

        // For one token, `AND` and `OR` have identical membership. Walk the
        // posting and candidate streams once instead of binary-searching the
        // posting for every filtered candidate.
        let docids = postings.docids();
        let tfs = postings.tfs();
        let mut cand = candidates.iter();
        let mut next = cand.next();
        let mut total = 0u64;
        let mut heap = BinaryHeap::with_capacity(k.min(candidates.len() as usize));
        let mut score_cache = Vec::with_capacity(4);
        for i in 0..docids.len() {
            let id = docids[i];
            loop {
                match next {
                    Some(c) if c < id => next = cand.next(),
                    Some(c) if c == id => {
                        let score = base
                            + bm25_contrib_cached(
                                &mut score_cache,
                                *idf,
                                tfs[i],
                                idx.doc_len(id),
                                self.avgdl,
                            );
                        total += 1;
                        push_top_ranked(&mut heap, k, interner, id, score);
                        next = cand.next();
                        break;
                    }
                    Some(_) => break,
                    None => return Some((finish_top_ranked(heap), total)),
                }
            }
        }
        Some((finish_top_ranked(heap), total))
    }
}

/// Hoist tokenization for a set of `match` conjuncts (once, not per doc).
pub(super) fn prep_matches<'a>(
    coll: &'a Collection,
    nodes: &[&QueryNode],
) -> Result<Vec<(&'a TextIndex, Vec<String>, MatchOp)>> {
    let mut out = Vec::with_capacity(nodes.len());
    for c in nodes {
        let QueryNode::Match(m) = c else { continue };
        let fi = coll
            .fields
            .get(&m.field)
            .ok_or_else(|| StorageError::UnknownField {
                collection: "<>".into(),
                field: m.field.clone(),
            })?;
        let FieldIndex::Text { analyzer, idx } = fi else {
            bail!(
                "match query is only valid on text fields (field `{}`)",
                m.field
            );
        };
        out.push((idx, tokenize::tokenize(&m.text, *analyzer), m.op));
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
