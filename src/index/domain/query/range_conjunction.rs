//! An AND of filters around a high-cardinality number range: the range is
//! checked per doc against a keyword or other filter's postings instead of
//! materializing its own.

use std::collections::BTreeSet;

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::query::clause::{clause_matches, eval_filter_bitmap};
use crate::index::domain::query::range::{range_bounds, sorted_bits_window, SortableBitsBounds};
use crate::index::domain::query::selectivity::estimate_selectivity;
use crate::index::domain::sortable_f64::{SortableF64, MISSING_SORTABLE_F64_BITS};
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::QueryNode;

fn range_distinct_count(coll: &Collection, node: &QueryNode) -> Result<Option<u64>> {
    let QueryNode::Range(r) = node else {
        return Ok(None);
    };
    let Some(FieldIndex::Number(n)) = coll.fields.get(&r.field) else {
        return Ok(None);
    };
    let (lo, hi) = range_bounds(r)?;
    Ok(Some(n.range_distinct_count(lo, hi)))
}

pub(super) fn eval_high_cardinality_range_conjunction(
    coll: &Collection,
    positives: &[&QueryNode],
    negatives: &[&QueryNode],
) -> Result<Option<RoaringBitmap>> {
    if positives.len() < 2 {
        return Ok(None);
    }
    if positives.len() == 2 && negatives.is_empty() {
        let range_ix = positives
            .iter()
            .position(|node| matches!(node, QueryNode::Range(_)));
        if let Some(range_ix) = range_ix {
            let QueryNode::Range(range_query) = positives[range_ix] else {
                unreachable!()
            };
            let Some(FieldIndex::Number(range_idx)) = coll.fields.get(&range_query.field) else {
                return Ok(None);
            };
            let (range_lo, range_hi) = range_bounds(range_query)?;
            let driver = positives[1 - range_ix];
            if let Some(out) = eval_range_with_keyword_driver_bitmap_dense(
                coll, driver, range_idx, &range_lo, &range_hi,
            )? {
                return Ok(Some(out));
            }
        }
    }
    let Some(range_ix) = select_high_cardinality_range(coll, positives)? else {
        return Ok(None);
    };
    let QueryNode::Range(range_query) = positives[range_ix] else {
        unreachable!()
    };
    let Some(FieldIndex::Number(range_idx)) = coll.fields.get(&range_query.field) else {
        return Ok(None);
    };
    let (range_lo, range_hi) = range_bounds(range_query)?;

    let mut rest: Vec<&QueryNode> = positives
        .iter()
        .copied()
        .enumerate()
        .filter(|(i, _)| *i != range_ix)
        .map(|(_, c)| c)
        .collect();
    rest.sort_by_key(|c| estimate_selectivity(coll, c));
    let range_sel = estimate_selectivity(coll, positives[range_ix]);
    if rest.len() == 1 {
        let rest_sel = estimate_selectivity(coll, rest[0]);
        if range_sel == u64::MAX || rest_sel.saturating_mul(4) < range_sel {
            if let Some(out) = eval_range_with_single_filter_driver(
                coll, rest[0], range_idx, &range_lo, &range_hi, negatives,
            )? {
                return Ok(Some(out));
            }
        }
    }
    let mut mask = eval_filter_bitmap(coll, rest[0])?;
    for c in rest.iter().skip(1) {
        if mask.is_empty() {
            break;
        }
        mask &= &eval_filter_bitmap(coll, c)?;
    }
    if range_sel != u64::MAX && mask.len().saturating_mul(4) >= range_sel {
        return Ok(None);
    }

    let mut out = RoaringBitmap::new();
    'doc: for id in &mask {
        if !range_idx.number_in_bounds(id, &range_lo, &range_hi) {
            continue;
        }
        for nf in negatives {
            if clause_matches(coll, nf, id)?.is_some() {
                continue 'doc;
            }
        }
        out.insert(id);
    }
    Ok(Some(out))
}

fn eval_range_with_keyword_driver_bitmap_dense(
    coll: &Collection,
    driver: &QueryNode,
    range_idx: &NumberIndex,
    range_lo: &std::ops::Bound<SortableF64>,
    range_hi: &std::ops::Bound<SortableF64>,
) -> Result<Option<RoaringBitmap>> {
    let (keyword_field, terms): (&str, Vec<&str>) = match driver {
        QueryNode::Term(t) => {
            let FieldValue::String(s) = &t.value else {
                return Ok(None);
            };
            (t.field.as_str(), vec![s.as_str()])
        }
        QueryNode::Terms(t) => {
            let mut terms = Vec::with_capacity(t.values.len());
            let mut seen = BTreeSet::new();
            for v in &t.values {
                let FieldValue::String(s) = v else {
                    return Ok(None);
                };
                if !seen.insert(s.as_str()) {
                    return Ok(None);
                }
                terms.push(s.as_str());
            }
            (t.field.as_str(), terms)
        }
        _ => return Ok(None),
    };
    let Some(FieldIndex::Keyword(kidx)) = coll.fields.get(keyword_field) else {
        return Ok(None);
    };

    let mut estimated = 0u64;
    for s in &terms {
        estimated += kidx.term_df(s);
    }
    if estimated < 8192 {
        return Ok(None);
    }

    let mut out = RoaringBitmap::new();
    for s in terms {
        let docs = range_idx.keyword_range_docs(keyword_field, s, kidx);
        if docs.is_empty() {
            continue;
        }
        let window = sorted_bits_window(docs.as_slice(), range_lo, range_hi);
        for &(_, id) in &docs[window] {
            out.insert(id);
        }
    }
    Ok(Some(out))
}

pub(super) fn select_high_cardinality_range(
    coll: &Collection,
    positives: &[&QueryNode],
) -> Result<Option<usize>> {
    let mut best_range: Option<(usize, u64)> = None;
    for (i, node) in positives.iter().enumerate() {
        if let Some(distinct) = range_distinct_count(coll, node)? {
            if distinct >= 1024
                && best_range
                    .map(|(_, best_distinct)| distinct > best_distinct)
                    .unwrap_or(true)
            {
                best_range = Some((i, distinct));
            }
        }
    }
    Ok(best_range.map(|(i, _)| i))
}

fn eval_range_with_single_filter_driver(
    coll: &Collection,
    driver: &QueryNode,
    range_idx: &NumberIndex,
    range_lo: &std::ops::Bound<SortableF64>,
    range_hi: &std::ops::Bound<SortableF64>,
    negatives: &[&QueryNode],
) -> Result<Option<RoaringBitmap>> {
    let mut out = RoaringBitmap::new();
    let supported = visit_filter_driver_postings(coll, driver, |posting| {
        append_range_filtered_posting(
            coll, &mut out, posting, range_idx, range_lo, range_hi, negatives,
        )
    })?;
    Ok(supported.then_some(out))
}

pub(super) fn visit_filter_driver_postings<F>(
    coll: &Collection,
    driver: &QueryNode,
    mut visit: F,
) -> Result<bool>
where
    F: FnMut(&RoaringBitmap) -> Result<()>,
{
    match driver {
        QueryNode::Term(t) => {
            let Some(fi) = coll.fields.get(&t.field) else {
                return Ok(false);
            };
            match (fi, &t.value) {
                (FieldIndex::Keyword(k), FieldValue::String(s)) => {
                    if let Some(posting) = k.term_postings(s) {
                        visit(posting.as_ref())?;
                    }
                }
                (FieldIndex::Number(n), FieldValue::Number(x)) => {
                    let key = SortableF64::new(*x)?;
                    if let Some(posting) = n.value_postings(key) {
                        visit(posting.as_ref())?;
                    }
                }
                (FieldIndex::Set(s), FieldValue::String(el)) => {
                    if let Some(posting) = s.element_postings(el) {
                        visit(posting.as_ref())?;
                    }
                }
                _ => return Ok(false),
            }
            Ok(true)
        }
        QueryNode::Terms(t) => {
            let Some(fi) = coll.fields.get(&t.field) else {
                return Ok(false);
            };
            for v in &t.values {
                match (fi, v) {
                    (FieldIndex::Keyword(k), FieldValue::String(s)) => {
                        if let Some(posting) = k.term_postings(s) {
                            visit(posting.as_ref())?;
                        }
                    }
                    (FieldIndex::Number(n), FieldValue::Number(x)) => {
                        let key = SortableF64::new(*x)?;
                        if let Some(posting) = n.value_postings(key) {
                            visit(posting.as_ref())?;
                        }
                    }
                    (FieldIndex::Set(s), FieldValue::String(el)) => {
                        if let Some(posting) = s.element_postings(el) {
                            visit(posting.as_ref())?;
                        }
                    }
                    _ => return Ok(false),
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn append_range_filtered_posting(
    coll: &Collection,
    out: &mut RoaringBitmap,
    posting: &RoaringBitmap,
    range_idx: &NumberIndex,
    range_lo: &std::ops::Bound<SortableF64>,
    range_hi: &std::ops::Bound<SortableF64>,
    negatives: &[&QueryNode],
) -> Result<()> {
    if negatives.is_empty() && range_idx.segment.is_none() {
        let bounds = SortableBitsBounds::new(range_lo, range_hi);
        for id in posting {
            let Some(bits) = range_idx.dense_forward.get(id as usize).copied() else {
                continue;
            };
            if bits != MISSING_SORTABLE_F64_BITS && bounds.contains(bits) {
                out.insert(id);
            }
        }
        return Ok(());
    }

    'doc: for id in posting {
        if !range_idx.number_in_bounds(id, range_lo, range_hi) {
            continue;
        }
        for nf in negatives {
            if clause_matches(coll, nf, id)?.is_some() {
                continue 'doc;
            }
        }
        out.insert(id);
    }
    Ok(())
}
