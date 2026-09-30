//! Bounded top-k for an AND of filters and match clauses: the filters resolve
//! to a candidate set, each candidate is scored against the prepared matches,
//! and only the best k are kept, so a page never ranks every hit. A
//! single-token match under a keyword term and a number range scores straight
//! off the token's posting.

use std::collections::BinaryHeap;

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::analysis::tokenize;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::postings::TokPostings;
use crate::index::domain::query::prepared_match::{prep_matches, PreparedMatch};
use crate::index::domain::query::range::{range_bounds, sorted_bits_window};
use crate::index::domain::query::rank::{
    bm25_contrib_cached, finish_top_ranked, push_top_ranked, text_doc_len_at,
};
use crate::index::domain::query::selectivity::{
    is_predicable, plan_filter_candidates, SPARSE_CANDIDATE_MAX,
};
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::index::domain::text_index::TextIndex;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::QueryNode;

pub(in crate::index) fn eval_predicable_and_topk(
    coll: &Collection,
    q: &QueryNode,
    interner: &Interner,
    k: usize,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
    let QueryNode::And(children) = q else {
        return Ok(None);
    };
    let (nots, positives): (Vec<&QueryNode>, Vec<&QueryNode>) = children
        .iter()
        .partition(|c| matches!(c, QueryNode::Not(_)));
    if positives.is_empty()
        || !positives.iter().all(|c| is_predicable(c))
        || !nots.iter().all(|c| {
            let QueryNode::Not(inner) = c else {
                unreachable!()
            };
            is_predicable(inner)
        })
    {
        return Ok(None);
    }

    let filter_pos: Vec<&QueryNode> = positives
        .iter()
        .copied()
        .filter(|c| !matches!(c, QueryNode::Match(_)))
        .collect();
    let match_pos: Vec<&QueryNode> = positives
        .iter()
        .copied()
        .filter(|c| matches!(c, QueryNode::Match(_)))
        .collect();
    if filter_pos.is_empty() || match_pos.is_empty() {
        return Ok(None);
    }

    let (mut filter_nots, mut match_nots): (Vec<&QueryNode>, Vec<&QueryNode>) =
        (Vec::new(), Vec::new());
    for c in &nots {
        let QueryNode::Not(inner) = c else {
            unreachable!()
        };
        if matches!(&**inner, QueryNode::Match(_)) {
            match_nots.push(inner);
        } else {
            filter_nots.push(inner);
        }
    }

    let Some(plan) = plan_filter_candidates(coll, &filter_pos, &filter_nots, &match_pos)? else {
        return Ok(None);
    };
    let base = filter_pos.len() as f32 + nots.len() as f32;
    if nots.is_empty() && match_pos.len() == 1 {
        if let Some(topk) = eval_single_token_keyword_range_topk(
            coll,
            match_pos[0],
            &filter_pos,
            base,
            interner,
            k,
        )? {
            return Ok(Some(topk));
        }
    }

    let cand = plan.resolve(coll, &filter_pos, &filter_nots)?;

    let preps = prep_matches(coll, &match_pos)?;
    let not_preps = prep_matches(coll, &match_nots)?;
    // #4246: a small candidate set never materializes a posting.
    let sparse_cand: Option<Vec<u32>> =
        (cand.len() <= SPARSE_CANDIDATE_MAX).then(|| cand.iter().collect());
    let prepared: Vec<Option<PreparedMatch>> = preps
        .iter()
        .map(|(idx, toks, op)| PreparedMatch::resolve_for(idx, toks, *op, sparse_cand.as_deref()))
        .collect();
    let not_prepared: Vec<Option<PreparedMatch>> = not_preps
        .iter()
        .map(|(idx, toks, op)| PreparedMatch::resolve_for(idx, toks, *op, sparse_cand.as_deref()))
        .collect();

    if preps.len() == 1 && not_preps.is_empty() {
        if let Some(pm) = &prepared[0] {
            if let Some(topk) =
                pm.score_single_token_candidates_topk(preps[0].0, &cand, base, interner, k)
            {
                return Ok(Some(topk));
            }
        } else {
            return Ok(Some((Vec::new(), 0)));
        }
    }

    let mut total = 0u64;
    let mut heap = BinaryHeap::with_capacity(k.min(cand.len() as usize));
    'doc: for id in &cand {
        let mut score = base;
        for ((idx, _, _), pm) in preps.iter().zip(prepared.iter()) {
            let s = match pm {
                Some(pm) => pm.score(idx, id),
                None => None,
            };
            match s {
                Some(s) => score += s,
                None => continue 'doc,
            }
        }
        for ((idx, _, _), pm) in not_preps.iter().zip(not_prepared.iter()) {
            let s = match pm {
                Some(pm) => pm.score(idx, id),
                None => None,
            };
            if s.is_some() {
                continue 'doc;
            }
        }
        total += 1;
        push_top_ranked(&mut heap, k, interner, id, score);
    }

    Ok(Some((finish_top_ranked(heap), total)))
}

fn eval_single_token_keyword_range_topk(
    coll: &Collection,
    match_node: &QueryNode,
    filters: &[&QueryNode],
    base: f32,
    interner: &Interner,
    k: usize,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
    let QueryNode::Match(m) = match_node else {
        return Ok(None);
    };
    if filters.len() != 2 {
        return Ok(None);
    }

    let mut keyword: Option<(&str, &str, &KeywordIndex)> = None;
    let mut range: Option<(
        &NumberIndex,
        std::ops::Bound<SortableF64>,
        std::ops::Bound<SortableF64>,
    )> = None;
    for filter in filters {
        match filter {
            QueryNode::Term(t) => {
                let FieldValue::String(term) = &t.value else {
                    return Ok(None);
                };
                let Some(FieldIndex::Keyword(kidx)) = coll.fields.get(&t.field) else {
                    return Ok(None);
                };
                keyword = Some((t.field.as_str(), term.as_str(), kidx));
            }
            QueryNode::Range(r) => {
                let Some(FieldIndex::Number(nidx)) = coll.fields.get(&r.field) else {
                    return Ok(None);
                };
                let (lo, hi) = range_bounds(r)?;
                range = Some((nidx, lo, hi));
            }
            _ => return Ok(None),
        }
    }
    let (keyword_field, term, keyword_idx) = match keyword {
        Some(v) => v,
        None => return Ok(None),
    };
    let (range_idx, range_lo, range_hi) = match range {
        Some(v) => v,
        None => return Ok(None),
    };

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
    if tokens.len() != 1 {
        return Ok(None);
    }
    let (corpus_n, corpus_total_len) = idx.bm25_corpus();
    if corpus_n == 0 {
        return Ok(Some((Vec::new(), 0)));
    }
    let Some(postings) = idx.tok_postings(&tokens[0]) else {
        return Ok(Some((Vec::new(), 0)));
    };
    let df = postings.df() as f32;
    let n = corpus_n as f32;
    let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();
    let avgdl = corpus_total_len as f32 / corpus_n as f32;
    let segment_doc_lens = match &postings {
        TokPostings::Segment(_) => idx.segment.as_ref().and_then(|seg| seg.text_doc_lens()),
        _ => None,
    };

    let docs = range_idx.keyword_range_docs(keyword_field, term, keyword_idx);
    let window = sorted_bits_window(docs.as_slice(), &range_lo, &range_hi);
    if window.end.saturating_sub(window.start) > FILTERED_SEARCH_BITMAP_THRESHOLD {
        let candidates =
            range_idx.keyword_range_bitmap(keyword_field, term, keyword_idx, &range_lo, &range_hi);
        return Ok(Some(score_single_token_candidate_bitmap_topk(
            idx,
            &postings,
            idf,
            avgdl,
            segment_doc_lens,
            candidates.as_ref(),
            base,
            interner,
            k,
        )));
    }
    let mut total = 0u64;
    let mut heap = BinaryHeap::with_capacity(k.min(window.end.saturating_sub(window.start)));
    let mut score_cache = Vec::with_capacity(4);
    for &(_, id) in &docs[window] {
        let Some(tf) = postings.tf(id) else {
            continue;
        };
        let doc_len = text_doc_len_at(idx, segment_doc_lens, id);
        let score = base + bm25_contrib_cached(&mut score_cache, idf, tf, doc_len, avgdl);
        total += 1;
        push_top_ranked(&mut heap, k, interner, id, score);
    }

    Ok(Some((finish_top_ranked(heap), total)))
}

const FILTERED_SEARCH_BITMAP_THRESHOLD: usize = 1024;

fn score_single_token_candidate_bitmap_topk(
    idx: &TextIndex,
    postings: &TokPostings<'_>,
    idf: f32,
    avgdl: f32,
    segment_doc_lens: Option<&[u32]>,
    candidates: &RoaringBitmap,
    base: f32,
    interner: &Interner,
    k: usize,
) -> (Vec<(u32, f32)>, u64) {
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
                    let doc_len = text_doc_len_at(idx, segment_doc_lens, id);
                    let score =
                        base + bm25_contrib_cached(&mut score_cache, idf, tfs[i], doc_len, avgdl);
                    total += 1;
                    push_top_ranked(&mut heap, k, interner, id, score);
                    next = cand.next();
                    break;
                }
                Some(_) => break,
                None => return (finish_top_ranked(heap), total),
            }
        }
    }
    (finish_top_ranked(heap), total)
}
