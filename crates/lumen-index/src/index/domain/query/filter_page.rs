//! The first page and total of a filter-only query: an AND of term, terms and
//! range filters, with a high-cardinality range checked per doc against the
//! driver's postings and only the page's hits kept. The driver's postings must
//! be disjoint, so each doc counts once.

use std::collections::BTreeSet;

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::query::clause::{
    clause_matches, eval_filter_bitmap, eval_filter_bitmap_conjunction,
};
use crate::index::domain::query::range::{range_bounds, sorted_bits_window, SortableBitsBounds};
use crate::index::domain::query::range_conjunction::{
    select_high_cardinality_range, visit_filter_driver_postings,
};
use crate::index::domain::query::selectivity::estimate_selectivity;
use crate::index::domain::sortable_f64::{SortableF64, MISSING_SORTABLE_F64_BITS};
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::QueryNode;

fn eval_range_with_single_filter_driver_page(
    coll: &Collection,
    driver: &QueryNode,
    range_idx: &NumberIndex,
    range_lo: &std::ops::Bound<SortableF64>,
    range_hi: &std::ops::Bound<SortableF64>,
    negatives: &[&QueryNode],
    want: usize,
    score: f32,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
    if negatives.is_empty() {
        if let Some(out) = eval_range_with_keyword_terms_driver_page_dense(
            coll, driver, range_idx, range_lo, range_hi, want, score,
        )? {
            return Ok(Some(out));
        }
    }
    if !filter_driver_postings_are_disjoint(coll, driver)? {
        return Ok(None);
    }
    let mut page = Vec::with_capacity(want.min(1024));
    let mut total = 0u64;
    let supported = visit_filter_driver_postings(coll, driver, |posting| {
        append_range_filtered_posting_page(
            coll, &mut page, &mut total, posting, range_idx, range_lo, range_hi, negatives, want,
            score,
        )
    })?;
    Ok(supported.then_some((page, total)))
}

fn eval_range_with_keyword_terms_driver_page_dense(
    coll: &Collection,
    driver: &QueryNode,
    range_idx: &NumberIndex,
    range_lo: &std::ops::Bound<SortableF64>,
    range_hi: &std::ops::Bound<SortableF64>,
    want: usize,
    score: f32,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
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

    let mut page = Vec::with_capacity(want.min(1024));
    let mut total = 0u64;
    for s in terms {
        let docs = range_idx.keyword_range_docs(keyword_field, s, kidx);
        if docs.is_empty() {
            continue;
        }
        let window = sorted_bits_window(docs.as_slice(), range_lo, range_hi);
        total += window.end.saturating_sub(window.start) as u64;
        if page.len() < want {
            for &(_, id) in &docs[window] {
                page.push((id, score));
                if page.len() >= want {
                    break;
                }
            }
        }
    }
    Ok(Some((page, total)))
}

fn filter_driver_postings_are_disjoint(coll: &Collection, driver: &QueryNode) -> Result<bool> {
    match driver {
        QueryNode::Term(t) => Ok(matches!(
            (coll.fields.get(&t.field), &t.value),
            (Some(FieldIndex::Keyword(_)), FieldValue::String(_))
                | (Some(FieldIndex::Number(_)), FieldValue::Number(_))
                | (Some(FieldIndex::Set(_)), FieldValue::String(_))
        )),
        QueryNode::Terms(t) => {
            let Some(fi) = coll.fields.get(&t.field) else {
                return Ok(false);
            };
            match fi {
                FieldIndex::Keyword(_) => {
                    let mut seen = BTreeSet::new();
                    for v in &t.values {
                        let FieldValue::String(s) = v else {
                            return Ok(false);
                        };
                        if !seen.insert(s.as_str()) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                FieldIndex::Number(_) => {
                    let mut seen = BTreeSet::new();
                    for v in &t.values {
                        let FieldValue::Number(x) = v else {
                            return Ok(false);
                        };
                        if !seen.insert(SortableF64::new(*x)?) {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

fn append_range_filtered_posting_page(
    coll: &Collection,
    page: &mut Vec<(u32, f32)>,
    total: &mut u64,
    posting: &RoaringBitmap,
    range_idx: &NumberIndex,
    range_lo: &std::ops::Bound<SortableF64>,
    range_hi: &std::ops::Bound<SortableF64>,
    negatives: &[&QueryNode],
    want: usize,
    score: f32,
) -> Result<()> {
    if negatives.is_empty() && range_idx.segment.is_none() {
        let bounds = SortableBitsBounds::new(range_lo, range_hi);
        for id in posting {
            let Some(bits) = range_idx.dense_forward.get(id as usize).copied() else {
                continue;
            };
            if bits != MISSING_SORTABLE_F64_BITS && bounds.contains(bits) {
                *total += 1;
                if page.len() < want {
                    page.push((id, score));
                }
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
        *total += 1;
        if page.len() < want {
            page.push((id, score));
        }
    }
    Ok(())
}

fn eval_high_cardinality_range_filter_page(
    coll: &Collection,
    positives: &[&QueryNode],
    negatives: &[&QueryNode],
    want: usize,
    score: f32,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
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
            if let Some(out) = eval_range_with_keyword_terms_driver_page_dense(
                coll, driver, range_idx, &range_lo, &range_hi, want, score,
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
    if rest.len() != 1 {
        return Ok(None);
    }
    let range_sel = estimate_selectivity(coll, positives[range_ix]);
    let rest_sel = estimate_selectivity(coll, rest[0]);
    if range_sel != u64::MAX && rest_sel.saturating_mul(4) >= range_sel {
        return Ok(None);
    }

    eval_range_with_single_filter_driver_page(
        coll, rest[0], range_idx, &range_lo, &range_hi, negatives, want, score,
    )
}

pub(super) fn eval_filter_only_page(
    coll: &Collection,
    node: &QueryNode,
    want: usize,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
    let QueryNode::And(children) = node else {
        return Ok(None);
    };
    let (nots, positives): (Vec<&QueryNode>, Vec<&QueryNode>) = children
        .iter()
        .partition(|c| matches!(c, QueryNode::Not(_)));
    if positives.is_empty()
        || !positives.iter().all(|c| {
            matches!(
                c,
                QueryNode::Term(_) | QueryNode::Terms(_) | QueryNode::Range(_)
            )
        })
    {
        return Ok(None);
    }

    let mut filter_nots = Vec::new();
    for n in nots {
        let QueryNode::Not(inner) = n else {
            unreachable!()
        };
        if !matches!(
            &**inner,
            QueryNode::Term(_) | QueryNode::Terms(_) | QueryNode::Range(_)
        ) {
            return Ok(None);
        }
        filter_nots.push(&**inner);
    }

    let score = (positives.len() + filter_nots.len()) as f32;
    eval_high_cardinality_range_filter_page(coll, &positives, &filter_nots, want, score)
}

pub(super) fn eval_filter_only_bitmap(
    coll: &Collection,
    node: &QueryNode,
) -> Result<Option<(RoaringBitmap, f32)>> {
    match node {
        QueryNode::Term(_) | QueryNode::Terms(_) | QueryNode::Range(_) => {
            Ok(Some((eval_filter_bitmap(coll, node)?, 1.0)))
        }
        QueryNode::And(children) => {
            let (nots, positives): (Vec<&QueryNode>, Vec<&QueryNode>) = children
                .iter()
                .partition(|c| matches!(c, QueryNode::Not(_)));
            if positives.is_empty()
                || !positives.iter().all(|c| {
                    matches!(
                        c,
                        QueryNode::Term(_) | QueryNode::Terms(_) | QueryNode::Range(_)
                    )
                })
            {
                return Ok(None);
            }

            let mut filter_nots = Vec::new();
            for n in nots {
                let QueryNode::Not(inner) = n else {
                    unreachable!()
                };
                if !matches!(
                    &**inner,
                    QueryNode::Term(_) | QueryNode::Terms(_) | QueryNode::Range(_)
                ) {
                    return Ok(None);
                }
                filter_nots.push(&**inner);
            }

            let score = (positives.len() + filter_nots.len()) as f32;
            let cand = eval_filter_bitmap_conjunction(coll, &positives, &filter_nots)?;
            Ok(Some((cand, score)))
        }
        _ => Ok(None),
    }
}
