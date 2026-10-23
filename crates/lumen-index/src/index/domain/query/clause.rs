//! One AND conjunct checked against one doc, the non-driving conjuncts applied
//! to a candidate, and a conjunction of filters evaluated to its doc bitmap.

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::analysis::tokenize;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::query::knn::eval_hamming;
use crate::index::domain::query::prepared_match::match_doc_score;
use crate::index::domain::query::range::{eval_range, in_keyword_range, in_range};
use crate::index::domain::query::range_conjunction::eval_high_cardinality_range_conjunction;
use crate::index::domain::query::selectivity::estimate_selectivity;
use crate::index::domain::query::terms::{
    eval_field_doc_union, eval_ids, eval_prefix, eval_term, eval_terms,
};
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::index::domain::text_index::TextIndex;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{MatchOp, QueryNode};

/// Does `id` satisfy `node`, and if so with what score contribution?
/// `Some(score)` = match (score added to the doc's AND total), `None` = no
/// match. Only valid for [`is_predicable`] nodes in embedded mode.
///
/// [`is_predicable`]: crate::index::domain::query::selectivity::is_predicable
pub(super) fn clause_matches(coll: &Collection, node: &QueryNode, id: u32) -> Result<Option<f32>> {
    let unknown = |field: &str| StorageError::UnknownField {
        collection: "<>".into(),
        field: field.to_string(),
    };
    Ok(match node {
        QueryNode::Term(t) => {
            let fi = coll.fields.get(&t.field).ok_or_else(|| unknown(&t.field))?;
            let hit = match (fi, &t.value) {
                (FieldIndex::Keyword(k), FieldValue::String(s)) => {
                    k.keyword_at(id).map(|v| &v == s).unwrap_or(false)
                }
                (FieldIndex::Number(n), FieldValue::Number(x)) => {
                    let key = SortableF64::new(*x)?;
                    n.number_at(id) == Some(key)
                }
                (FieldIndex::Set(s), FieldValue::String(el)) => s.set_contains(id, el),
                _ => bail!("term query type mismatch on field `{}`", t.field),
            };
            hit.then_some(1.0)
        }
        QueryNode::Terms(t) => {
            let fi = coll.fields.get(&t.field).ok_or_else(|| unknown(&t.field))?;
            let hit = match fi {
                FieldIndex::Keyword(k) => k.keyword_at(id).is_some_and(|v| {
                    t.values
                        .iter()
                        .any(|val| matches!(val, FieldValue::String(s) if *s == v))
                }),
                FieldIndex::Number(n) => n.number_at(id).is_some_and(|v| {
                    t.values.iter().any(|val| {
                        matches!(val, FieldValue::Number(x)
                            if SortableF64::new(*x).map(|k| k == v).unwrap_or(false))
                    })
                }),
                FieldIndex::Set(s) => s.set_contains_any(id, &t.values),
                _ => bail!("terms query type mismatch on field `{}`", t.field),
            };
            hit.then_some(1.0)
        }
        QueryNode::Prefix(q) => {
            let fi = coll.fields.get(&q.field).ok_or_else(|| unknown(&q.field))?;
            let FieldIndex::Keyword(index) = fi else {
                bail!(
                    "prefix query is only valid on keyword fields (field `{}`)",
                    q.field
                );
            };
            index
                .keyword_at(id)
                .is_some_and(|value| value.starts_with(&q.value))
                .then_some(1.0)
        }
        QueryNode::Range(r) => {
            let fi = coll.fields.get(&r.field).ok_or_else(|| unknown(&r.field))?;
            match fi {
                FieldIndex::Number(n) => match n.number_at(id) {
                    Some(v) if in_range(v, r)? => Some(1.0),
                    _ => None,
                },
                // #1307: keyword range as a per-doc predicate — same byte/
                // lexicographic comparison `eval_range`'s materialized path uses.
                FieldIndex::Keyword(k) => match k.keyword_at(id) {
                    Some(v) if in_keyword_range(&v, r)? => Some(1.0),
                    _ => None,
                },
                _ => bail!(
                    "range query is only valid on number or keyword fields (field `{}`)",
                    r.field
                ),
            }
        }
        QueryNode::Match(m) => {
            let fi = coll.fields.get(&m.field).ok_or_else(|| unknown(&m.field))?;
            let FieldIndex::Text { analyzer, idx } = fi else {
                bail!(
                    "match query is only valid on text fields (field `{}`)",
                    m.field
                );
            };
            let tokens = tokenize::tokenize(&m.text, *analyzer);
            match_doc_score(idx, &tokens, m.op, id)
        }
        // Exact hamming as a per-doc predicate: the same `hash_at` read
        // `eval_hamming` scans with, and the same score it gives a distance-0
        // hit, `(64 - 0) / 64`.
        QueryNode::Hamming(h) if h.max_distance == 0 => {
            let fi = coll.fields.get(&h.field).ok_or_else(|| unknown(&h.field))?;
            let FieldIndex::Hash(hidx) = fi else {
                bail!(
                    "hamming query is only valid on hash fields (field `{}`)",
                    h.field
                );
            };
            let query = parse_hash(&h.hash)?;
            (hidx.hash_at(id) == Some(query)).then_some(1.0)
        }
        _ => bail!("clause_matches called on a non-predicable node"),
    })
}

/// Apply the non-driver conjuncts of an AND to one doc: cheap forward filters
/// first (short-circuit on miss), then `match` BM25, then negations. Returns
/// `Some(total_score)` if the doc satisfies every conjunct, else `None`.
pub(super) fn apply_conjuncts(
    coll: &Collection,
    id: u32,
    base: f32,
    other_filters: &[&QueryNode],
    other_matches: &[(&TextIndex, Vec<String>, MatchOp)],
    not_inners: &[&QueryNode],
) -> Result<Option<f32>> {
    let mut score = base;
    for f in other_filters {
        match clause_matches(coll, f, id)? {
            Some(s) => score += s,
            None => return Ok(None),
        }
    }
    for (idx, toks, op) in other_matches {
        match match_doc_score(idx, toks, *op, id) {
            Some(s) => score += s,
            None => return Ok(None),
        }
    }
    for inner in not_inners {
        if clause_matches(coll, inner, id)?.is_some() {
            return Ok(None);
        }
        score += 1.0;
    }
    Ok(Some(score))
}

/// Evaluate a Term/Terms/Range conjunct to its doc bitmap (in-memory path).
pub(super) fn eval_filter_bitmap(coll: &Collection, node: &QueryNode) -> Result<RoaringBitmap> {
    match node {
        QueryNode::Term(t) => eval_term(coll, t),
        QueryNode::Terms(t) => eval_terms(coll, t),
        QueryNode::Prefix(q) => eval_prefix(coll, q),
        QueryNode::Ids(q) => eval_ids(coll, q),
        QueryNode::Range(r) => eval_range(coll, r),
        QueryNode::Exists(e) => eval_field_doc_union(coll, &e.field, 1),
        QueryNode::Duplicated(d) => {
            eval_field_doc_union(coll, &d.field, d.min_group_size.max(2) as u64)
        }
        // Exact hamming (`is_exact_hamming`): the hit set of the very scan
        // `eval_hamming` runs. Its constant 1.0 score is the caller's `base`
        // contribution, like every other filter here.
        QueryNode::Hamming(h) if h.max_distance == 0 => {
            Ok(eval_hamming(coll, h)?.into_keys().collect())
        }
        _ => bail!("eval_filter_bitmap called on a non-filter node"),
    }
}

pub(super) fn eval_filter_bitmap_conjunction(
    coll: &Collection,
    positives: &[&QueryNode],
    negatives: &[&QueryNode],
) -> Result<RoaringBitmap> {
    debug_assert!(!positives.is_empty());
    if let Some(cand) = eval_high_cardinality_range_conjunction(coll, positives, negatives)? {
        return Ok(cand);
    }

    let mut order = positives.to_vec();
    order.sort_by_key(|c| estimate_selectivity(coll, c));

    let mut cand = eval_filter_bitmap(coll, order[0])?;
    for c in order.iter().skip(1) {
        if cand.is_empty() {
            break;
        }
        cand &= &eval_filter_bitmap(coll, c)?;
    }
    for nf in negatives {
        if cand.is_empty() {
            break;
        }
        cand -= &eval_filter_bitmap(coll, nf)?;
    }
    Ok(cand)
}
