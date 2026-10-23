//! The sort planners: a sort walked bucket by bucket in field order, checking
//! the query per doc, and a number sort under one keyword term paged in the
//! number index's order for that term.

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::query::page_cursor::PageCursor;
use crate::index::domain::query::plan::PlanKind;
use crate::index::domain::query::predicate::query_predicate;
use crate::index::domain::query::sort::{
    compare_sort_tuples, is_after_sort_cursor, sort_field_kind, sort_score, sort_values_for_doc,
    SortAfter, SortFieldKind, SortValue,
};
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{QueryNode, SortOrder, SortSpec};
use crate::shared_kernel::types::search::SearchRequest;

pub(super) fn eval_number_sort_keyword_term_page(
    coll: &Collection,
    query: &QueryNode,
    sort_idx: &NumberIndex,
    descending: bool,
    want: usize,
    track_total: bool,
) -> Result<Option<(Vec<(u32, f32)>, u64)>> {
    let QueryNode::Term(t) = query else {
        return Ok(None);
    };
    let FieldValue::String(term) = &t.value else {
        return Ok(None);
    };
    let Some(FieldIndex::Keyword(kidx)) = coll.fields.get(&t.field) else {
        return Ok(None);
    };

    let docs = sort_idx.keyword_range_docs(t.field.as_str(), term, kidx);
    let mut page = Vec::with_capacity(want.min(1024));
    if descending {
        for &(bits, id) in docs.iter().rev().take(want) {
            page.push((id, SortableF64::from_bits(bits).to_f64() as f32));
        }
    } else {
        for &(bits, id) in docs.iter().take(want) {
            page.push((id, SortableF64::from_bits(bits).to_f64() as f32));
        }
    }
    let total = if track_total {
        docs.len() as u64
    } else {
        page.len() as u64
    };
    Ok(Some((page, total)))
}

pub(super) fn is_unbounded_range_on_field(query: &QueryNode, field: &str) -> bool {
    let QueryNode::Range(r) = query else {
        return false;
    };
    r.field == field && r.gt.is_none() && r.gte.is_none() && r.lt.is_none() && r.lte.is_none()
}

fn cursor_value_matches_kind(value: &SortValue, kind: SortFieldKind) -> bool {
    matches!(
        (value, kind),
        (SortValue::Number(_), SortFieldKind::Number)
            | (SortValue::Keyword(_), SortFieldKind::Keyword)
    )
}

pub(in crate::index) fn sort_after_for_request(
    coll: &Collection,
    sort: Option<&[SortSpec]>,
    parsed_cursor: &Option<PageCursor>,
) -> Result<Option<SortAfter>> {
    let Some(sort) = sort else {
        return Ok(None);
    };
    let Some(parsed_cursor) = parsed_cursor else {
        return Ok(None);
    };
    let candidate = match parsed_cursor {
        PageCursor::SortKeyset { bits, docid } if sort.len() == 1 => SortAfter {
            values: vec![SortValue::Number(*bits)],
            docid: *docid,
        },
        PageCursor::SortValuesKeyset { values, docid } => SortAfter {
            values: values.clone(),
            docid: *docid,
        },
        _ => return Ok(None),
    };
    if candidate.values.len() != sort.len() {
        return Ok(None);
    }
    for (value, spec) in candidate.values.iter().zip(sort) {
        let kind = sort_field_kind(coll, "<>", spec)?;
        if !cursor_value_matches_kind(value, kind) {
            return Ok(None);
        }
    }
    Ok(Some(candidate))
}

fn visit_sorted_bucket(
    coll: &Collection,
    req: &SearchRequest,
    sort: &[SortSpec],
    docs: &RoaringBitmap,
    after: Option<&SortAfter>,
    page: &mut Vec<(u32, f32)>,
    total: &mut u64,
) -> Result<bool> {
    let want = req.limit as usize;
    if sort.len() == 1 {
        for id in docs {
            if !query_predicate(coll, &req.query, id)? {
                continue;
            }
            let Some(values) = sort_values_for_doc(coll, sort, id)? else {
                continue;
            };
            if !is_after_sort_cursor(&values, id, after, sort) {
                continue;
            }
            *total += 1;
            if page.len() < want {
                page.push((id, sort_score(&values[0])));
            } else if !req.track_total {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    let mut bucket = Vec::new();
    for id in docs {
        if !query_predicate(coll, &req.query, id)? {
            continue;
        }
        let Some(values) = sort_values_for_doc(coll, sort, id)? else {
            continue;
        };
        bucket.push((values, id));
    }
    bucket.sort_by(|(av, aid), (bv, bid)| compare_sort_tuples(av, *aid, bv, *bid, sort));
    for (values, id) in bucket {
        if !is_after_sort_cursor(&values, id, after, sort) {
            continue;
        }
        *total += 1;
        if page.len() < want {
            page.push((id, sort_score(&values[0])));
        } else if !req.track_total {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn try_generic_sort_plan(
    coll: &Collection,
    req: &SearchRequest,
    sort: &[SortSpec],
    sort_after: Option<&SortAfter>,
) -> Result<Option<(Vec<(u32, f32)>, u64, PlanKind)>> {
    let want = req.limit as usize;
    let mut page: Vec<(u32, f32)> = Vec::with_capacity(want.min(1024));
    let mut total: u64 = 0;
    let first = &sort[0];
    match coll.fields.get(&first.field) {
        Some(FieldIndex::Number(n)) => {
            let values = n.sorted_values();
            match first.order {
                SortOrder::Asc => {
                    for (_v, docs) in values.iter() {
                        if !visit_sorted_bucket(
                            coll, req, sort, docs, sort_after, &mut page, &mut total,
                        )? {
                            break;
                        }
                    }
                }
                SortOrder::Desc => {
                    for (_v, docs) in values.iter().rev() {
                        if !visit_sorted_bucket(
                            coll, req, sort, docs, sort_after, &mut page, &mut total,
                        )? {
                            break;
                        }
                    }
                }
            }
        }
        Some(FieldIndex::Keyword(k)) => {
            let terms = k.live_terms();
            match first.order {
                SortOrder::Asc => {
                    for (_term, docs) in terms.iter() {
                        if !visit_sorted_bucket(
                            coll, req, sort, docs, sort_after, &mut page, &mut total,
                        )? {
                            break;
                        }
                    }
                }
                SortOrder::Desc => {
                    for (_term, docs) in terms.iter().rev() {
                        if !visit_sorted_bucket(
                            coll, req, sort, docs, sort_after, &mut page, &mut total,
                        )? {
                            break;
                        }
                    }
                }
            }
        }
        Some(_) => {
            return Err(StorageError::UnsupportedSort(format!(
                "field `{}` is not sortable; supported sort fields are number and keyword",
                first.field
            ))
            .into());
        }
        None => {
            return Err(StorageError::UnknownField {
                collection: "<>".into(),
                field: first.field.clone(),
            }
            .into());
        }
    }
    if !req.track_total {
        total = total.max(page.len() as u64);
    }
    Ok(Some((page, total, PlanKind::SortedField)))
}
