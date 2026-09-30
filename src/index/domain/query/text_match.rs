//! The match clause: BM25 over a text field into the scored-hit map, and the
//! bounded top-k form for a single token or a multi-token AND, where each doc
//! is scored once.

use std::collections::HashMap;

use anyhow::{bail, Result};

use crate::index::domain::analysis::tokenize;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::index::domain::postings::TokPostings;
use crate::index::domain::query::rank::{
    bm25_contrib, build_single_token_ranked, cached_match_ranked_page,
    insert_match_rank_cache_and_page, match_rank_key, SingleTokenRankInput,
};
use crate::index::domain::query::zip_cursor::{build_and_ranked, AndRankInput};
use crate::index::domain::query::ScoredHits;
use crate::index::domain::storage_error::StorageError;
use crate::index::domain::tok_probe::TokProbe;
use crate::shared_kernel::types::query::{MatchOp, MatchQuery};

#[inline]
/// BM25 over the text field. Implements the standard form:
///
/// ```text
/// score = Σ_t IDF(t) · TF(t,d) · (k1+1) / (TF(t,d) + k1 · (1 − b + b · |d| / avgdl))
/// ```
pub(super) fn eval_match(coll: &Collection, m: &MatchQuery) -> Result<ScoredHits> {
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
    let tokens = tokenize::tokenize(&m.text, *analyzer);
    // `doc_count` is the live corpus N; when a segment is attached it carries
    // the identical header scalar, so the short-circuit and `n` are unchanged.
    let (corpus_n, corpus_total_len) = idx.bm25_corpus();
    if tokens.is_empty() || corpus_n == 0 {
        return Ok(HashMap::new());
    }
    let n = corpus_n as f32;
    let avgdl = if corpus_n == 0 {
        1.0
    } else {
        corpus_total_len as f32 / corpus_n as f32
    };

    // Resolve a token's postings from either the live `tokens` map or the
    // attached segment (Phase 2e-B; text tf is STORED). The two sources return
    // the SAME `(docids, tfs)` u32 streams in the SAME ascending-docid order, so
    // every downstream `df`/`tf`/`doc_len`/`n`/`avgdl` -> f32 cast is bit-equal
    // and the literal BM25 expression yields identical bits on both paths.
    let resolve = |tok: &str| -> Option<TokPostings<'_>> { idx.tok_postings(tok) };

    // Score one token's BM25 contribution into `out` (`+=`, so a doc matching
    // several tokens accumulates). The BM25 expression is byte-identical to the
    // live path; only the postings SOURCE may differ (segment vs in-RAM). Do
    // NOT reassociate `tf + K1*(1.0 - B + B*doc_len/avgdl)`.
    let score_token = |tok: &str, out: &mut ScoredHits| {
        let Some(postings) = resolve(tok) else {
            return;
        };
        let df = postings.df() as f32;
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
        let (docids, tfs) = (postings.docids(), postings.tfs());
        for i in 0..docids.len() {
            let id = docids[i];
            let doc_len = idx.doc_len(id) as f32;
            let tf = tfs[i] as f32;
            *out.entry(id).or_insert(0.0) += bm25_contrib(idf, tf, doc_len, avgdl);
        }
    };

    Ok(match m.op {
        MatchOp::Or => {
            // Union: accumulate every token directly into one map — no
            // intermediate per-token maps to build and then merge. Pre-size to
            // the summed df so a high-df single term (the text_bm25 case) does
            // not pay ~15 power-of-two rehashes.
            let cap: usize = tokens
                .iter()
                .filter_map(|t| resolve(t).map(|p| p.df()))
                .sum();
            let mut acc: ScoredHits = HashMap::with_capacity(cap);
            for tok in &tokens {
                score_token(tok, &mut acc);
            }
            acc
        }
        MatchOp::And => {
            // Intersect via a streaming k-way merge over lazy per-token
            // cursors (`TokProbe`) — no per-token merged-postings
            // materialization (the 500k-hot-doc `match … op: "and"` perf
            // fix: the old path built a full `Vec<u32>`/`Vec<u32>` per token
            // via `tok_postings`, even for tokens only ever point-probed).
            // Any absent token ⇒ empty intersection. The per-doc score still
            // sums each token's BM25 contribution in ORIGINAL TOKEN ORDER
            // (the driver's own contribution reused from the SAME merge pass
            // instead of a redundant re-lookup), so the f32 result is
            // byte-identical to the old per-token merged-postings walk.
            let posts: Vec<TokProbe<'_>> = tokens.iter().map(|t| idx.tok_probe(t)).collect();
            if posts.iter().any(|p| p.definitely_absent()) {
                return Ok(HashMap::new());
            }
            let dfs: Vec<usize> = posts.iter().map(|p| p.active_len()).collect();
            let idfs: Vec<f32> = dfs
                .iter()
                .map(|&df| {
                    let df = df as f32;
                    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
                })
                .collect();
            let drive = (0..posts.len()).min_by_key(|&i| dfs[i]).unwrap_or(0);
            let mut acc: ScoredHits = HashMap::with_capacity(dfs[drive]);
            'docs: for (id, drive_tf) in posts[drive].iter_active() {
                let doc_len = idx.doc_len(id) as f32;
                let mut score = 0.0f32;
                for (k, p) in posts.iter().enumerate() {
                    let tf = if k == drive {
                        drive_tf
                    } else {
                        match p.tf(id) {
                            Some(tf) => tf,
                            None => continue 'docs,
                        }
                    };
                    score += bm25_contrib(idfs[k], tf as f32, doc_len, avgdl);
                }
                acc.insert(id, score);
            }
            acc
        }
    })
}

/// HashMap-free, bounded-top-k BM25 for the cases where every result docid is
/// UNIQUE: a single token (one posting list ⇒ each docid scored once) or a
/// multi-token `And` (driven from the rarest token ⇒ each driver docid emitted
/// once). For these the score is the SAME value `eval_match` would put in the
/// map, so streaming into a bounded heap drops the per-doc hash insert, the
/// map→Vec collect, and the full matched Vec partition/sort. Exact total is
/// still counted, but only `offset + limit` hits are retained.
///
/// Returns `None` to defer to `eval_match`'s map path when accumulation IS
/// required (multi-token `Or`: a doc matching several tokens must sum their
/// contributions). The emitted f32 bits are byte-identical to `eval_match`'s
/// map for the same query: both go through [`bm25_contrib`], in the same order,
/// and a unique-key `out.entry(id).or_insert(0.0) += c` equals `c` (single
/// token), while the `And` driver sums per token starting from `0.0f32` exactly
/// as the map branch does before its single `insert`.
pub(in crate::index) fn eval_match_topk(
    coll: &Collection,
    m: &MatchQuery,
    interner: &Interner,
    k: usize,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
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
    let tokens = tokenize::tokenize(&m.text, *analyzer);
    let (corpus_n, corpus_total_len) = idx.bm25_corpus();
    if tokens.is_empty() || corpus_n == 0 {
        return Ok(Some((Vec::new(), 0)));
    }
    let n = corpus_n as f32;
    let avgdl = if corpus_n == 0 {
        1.0
    } else {
        corpus_total_len as f32 / corpus_n as f32
    };

    // Only the unique-docid shapes use the Vec fast path. A multi-token `Or`
    // needs real accumulation (a doc may match >1 token) ⇒ defer to the map.
    if tokens.len() > 1 && m.op == MatchOp::Or {
        return Ok(None);
    }

    let resolve = |tok: &str| -> Option<TokPostings<'_>> { idx.tok_postings(tok) };

    if tokens.len() == 1 {
        // Single token (Or == And == the one posting list): each docid is
        // scored EXACTLY once, so `out.entry(id).or_insert(0.0) += c == c`.
        let cache_key = match_rank_key(m.op, &tokens);
        if let Some(page) = cached_match_ranked_page(idx, &cache_key, k, interner) {
            return Ok(Some(page));
        }
        let Some(postings) = resolve(&tokens[0]) else {
            return Ok(Some((Vec::new(), 0)));
        };
        let df = postings.df() as f32;
        let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
        let (docids, tfs) = (postings.docids(), postings.tfs());
        let segment_doc_lens = match &postings {
            TokPostings::Segment(_) => idx.segment.as_ref().and_then(|seg| seg.text_doc_lens()),
            _ => None,
        };
        let entries = build_single_token_ranked(SingleTokenRankInput {
            idx,
            docids,
            tfs,
            segment_doc_lens,
            idf,
            avgdl,
        });
        return Ok(Some(insert_match_rank_cache_and_page(
            idx, cache_key, entries, k, interner,
        )));
    }

    // Multi-token AND — same drive-from-rarest + per-token sum as the map
    // branch, but each surviving driver docid is pushed once (unique) instead
    // of `insert`ed. Byte-identical: the per-doc `score` starts at `0.0f32` and
    // accumulates `bm25_contrib` over the tokens in the SAME order. Uses the
    // lazy `TokProbe` (no per-token merged-postings materialization — the
    // 500k-hot-doc perf fix) exactly as `eval_match`'s `And` branch does.
    let posts: Vec<TokProbe<'_>> = tokens.iter().map(|t| idx.tok_probe(t)).collect();
    if posts.iter().any(|p| p.definitely_absent()) {
        return Ok(Some((Vec::new(), 0)));
    }
    let dfs: Vec<usize> = posts.iter().map(|p| p.active_len()).collect();
    let idfs: Vec<f32> = dfs
        .iter()
        .map(|&df| {
            let df = df as f32;
            ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
        })
        .collect();
    let cache_key = match_rank_key(m.op, &tokens);
    if let Some(page) = cached_match_ranked_page(idx, &cache_key, k, interner) {
        return Ok(Some(page));
    }
    let drive = (0..posts.len()).min_by_key(|&i| dfs[i]).unwrap_or(0);
    let entries = build_and_ranked(AndRankInput {
        idx,
        posts: &posts,
        idfs: &idfs,
        drive,
        drive_len: dfs[drive],
        avgdl,
    });
    Ok(Some(insert_match_rank_cache_and_page(
        idx, cache_key, entries, k, interner,
    )))
}
