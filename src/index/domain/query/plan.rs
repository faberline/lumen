//! The page planner: a first page and its total for the shapes that can stop
//! early without materializing the whole match, or None for the general
//! evaluator.

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::query::filter_page::{eval_filter_only_bitmap, eval_filter_only_page};
use crate::index::domain::query::predicate::{query_has_knn, query_predicate};
use crate::index::domain::query::range::range_bounds;
use crate::index::domain::query::sort::{SortAfter, SortValue};
use crate::index::domain::query::sort_plan::{
    eval_number_sort_keyword_term_page, is_unbounded_range_on_field, try_generic_sort_plan,
};
use crate::index::domain::sortable_f64::{range_is_empty, SortableF64};
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{QueryNode, SortOrder};
use crate::shared_kernel::types::search::SearchRequest;

/// Planner page+total for shapes that can early-terminate without
/// materializing the full matched set. Embedded mode + first page only;
/// returns `None` to fall back to the materialize-and-rank path.
///
/// Result order for these shapes is the field-sort order (sort queries) or the
/// number-index order (standalone range) — NOT the score/eid order of the
/// fallback. For a constant-score filter the order is unspecified anyway, and
/// sort queries define their own order, so this is a correct page.
/// Which planner produced the page — decides the next-cursor encoding:
/// `SortedField` pages continue via keyset (sort-value bits + docid),
/// `Posting` pages keep the legacy offset cursor (posting/docid order has no
/// resumable sort key).
pub(crate) enum PlanKind {
    SortedField,
    Posting,
    /// The general evaluator's score-desc + external_id-asc ranking.
    ScoreRanked,
}

pub(crate) fn try_plan(
    coll: &Collection,
    req: &SearchRequest,
    offset: usize,
    sort_after: Option<&SortAfter>,
) -> Result<Option<(Vec<(u32, f32)>, u64, PlanKind)>> {
    if offset != 0 {
        return Ok(None);
    }
    let want = req.limit as usize;

    // ---- sort by a single number field ----
    if let Some(sort) = &req.sort {
        let [s] = sort.as_slice() else {
            return try_generic_sort_plan(coll, req, sort, sort_after);
        };
        let Some(FieldIndex::Number(n)) = coll.fields.get(&s.field) else {
            return try_generic_sort_plan(coll, req, sort, sort_after);
        };
        if query_has_knn(&req.query) {
            return Ok(None);
        }
        let number_sort_after = sort_after.and_then(|after| match after.values.as_slice() {
            [SortValue::Number(bits)] => Some((*bits, after.docid)),
            _ => None,
        });
        let descending = matches!(s.order, SortOrder::Desc);
        // Keyset continuation: a (v, id) pair is on the page iff it sits
        // strictly AFTER the cursor in walk order. Within an equal key the
        // walk emits docids ascending in BOTH directions, so the equal-key
        // remainder is `id > cursor_docid`.
        let after_bits = number_sort_after.map(|(bits, _)| bits);
        let past_cursor = |v_bits: u64, id: u32| -> bool {
            match number_sort_after {
                None => true,
                Some((k, d)) => {
                    if v_bits == k {
                        id > d
                    } else if descending {
                        v_bits < k
                    } else {
                        v_bits > k
                    }
                }
            }
        };
        if is_unbounded_range_on_field(&req.query, &s.field) {
            let mut page: Vec<(u32, f32)> = Vec::with_capacity(want.min(1024));
            let mut total: u64 = 0;
            if let Some(seg) = n.segment_ref() {
                n.sorted_walk_segment(seg.as_ref(), descending, after_bits, |v, id| {
                    if !past_cursor(SortableF64::new(v).map(|s| s.bits()).unwrap_or(0), id) {
                        return Ok(true);
                    }
                    total += 1;
                    if page.len() < want {
                        page.push((id, v as f32));
                    } else if !req.track_total {
                        return Ok(false);
                    }
                    Ok(true)
                })?;
            } else {
                let values = n.sorted_values();
                let after_key = number_sort_after.map(|(bits, _)| SortableF64::from_bits(bits));
                match s.order {
                    SortOrder::Asc => {
                        let range: Box<dyn Iterator<Item = (&SortableF64, &RoaringBitmap)>> =
                            match after_key {
                                Some(k) => Box::new(values.range(k..)),
                                None => Box::new(values.iter()),
                            };
                        'asc: for (v, docs) in range {
                            for id in docs {
                                if !past_cursor(v.bits(), id) {
                                    continue;
                                }
                                total += 1;
                                if page.len() < want {
                                    page.push((id, v.to_f64() as f32));
                                } else if !req.track_total {
                                    break 'asc;
                                }
                            }
                        }
                    }
                    SortOrder::Desc => {
                        let range: Box<dyn Iterator<Item = (&SortableF64, &RoaringBitmap)>> =
                            match after_key {
                                Some(k) => Box::new(values.range(..=k).rev()),
                                None => Box::new(values.iter().rev()),
                            };
                        'desc: for (v, docs) in range {
                            for id in docs {
                                if !past_cursor(v.bits(), id) {
                                    continue;
                                }
                                total += 1;
                                if page.len() < want {
                                    page.push((id, v.to_f64() as f32));
                                } else if !req.track_total {
                                    break 'desc;
                                }
                            }
                        }
                    }
                }
            }
            if !req.track_total {
                total = total.max(page.len() as u64);
            }
            return Ok(Some((page, total, PlanKind::SortedField)));
        }
        if number_sort_after.is_none() {
            if let Some(out) = eval_number_sort_keyword_term_page(
                coll,
                &req.query,
                n,
                descending,
                want,
                req.track_total,
            )? {
                return Ok(Some((out.0, out.1, PlanKind::SortedField)));
            }
        }
        let mut page: Vec<(u32, f32)> = Vec::with_capacity(want.min(1024));
        let mut total: u64 = 0;

        // SEGMENT ON (Phase 2m): walk the on-disk sorted-value index + the BOUNDED
        // per-value posting cache, MERGED with the live tail, in value order —
        // instead of materializing the whole-field `live_values()` BTreeMap on
        // EVERY query (the 525x sort regression). `sorted_walk_segment` subtracts
        // the tombstone per value (deleted base docids), unions the tail, and
        // emits `(value, docid)` in BYTE-IDENTICAL order to the in-RAM walk; the
        // per-doc `query_predicate` + page/early-term logic below is unchanged. The
        // walk short-circuits when `visit` returns `Ok(false)`, so a 10-hit page
        // only touches (and caches) the first few values' postings.
        if let Some(seg) = n.segment_ref() {
            let q = &req.query;
            let track_total = req.track_total;
            n.sorted_walk_segment(seg.as_ref(), descending, after_bits, |v, id| {
                if !past_cursor(SortableF64::new(v).map(|s| s.bits()).unwrap_or(0), id) {
                    return Ok(true);
                }
                if query_predicate(coll, q, id)? {
                    total += 1;
                    if page.len() < want {
                        page.push((id, v as f32));
                    } else if !track_total {
                        return Ok(false); // page full, no total needed → stop early
                    }
                }
                Ok(true)
            })?;
            if !req.track_total {
                total = total.max(page.len() as u64);
            }
            return Ok(Some((page, total, PlanKind::SortedField)));
        }

        // SEGMENT OFF (no segment attached): byte-for-byte the original
        // zero-clone walk over the in-RAM `values` BTreeMap. `sorted_values`
        // returns `Cow::Borrowed(&self.values)` here.
        // Walk docs in field-sorted order; emit those satisfying the query.
        // Score is the sort value (informational; ranking IS the walk order).
        // `track_total=false` lets us stop as soon as the page is full.
        let values = n.sorted_values();
        let after_key = number_sort_after.map(|(bits, _)| SortableF64::from_bits(bits));
        match s.order {
            SortOrder::Asc => {
                let range: Box<dyn Iterator<Item = (&SortableF64, &RoaringBitmap)>> =
                    match after_key {
                        Some(k) => Box::new(values.range(k..)),
                        None => Box::new(values.iter()),
                    };
                'asc: for (v, docs) in range {
                    for id in docs {
                        if !past_cursor(v.bits(), id) {
                            continue;
                        }
                        if query_predicate(coll, &req.query, id)? {
                            total += 1;
                            if page.len() < want {
                                page.push((id, v.to_f64() as f32));
                            } else if !req.track_total {
                                break 'asc;
                            }
                        }
                    }
                }
            }
            SortOrder::Desc => {
                let range: Box<dyn Iterator<Item = (&SortableF64, &RoaringBitmap)>> =
                    match after_key {
                        Some(k) => Box::new(values.range(..=k).rev()),
                        None => Box::new(values.iter().rev()),
                    };
                'desc: for (v, docs) in range {
                    for id in docs {
                        if !past_cursor(v.bits(), id) {
                            continue;
                        }
                        if query_predicate(coll, &req.query, id)? {
                            total += 1;
                            if page.len() < want {
                                page.push((id, v.to_f64() as f32));
                            } else if !req.track_total {
                                break 'desc;
                            }
                        }
                    }
                }
            }
        }
        if !req.track_total {
            total = total.max(page.len() as u64);
        }
        return Ok(Some((page, total, PlanKind::SortedField)));
    }

    // ---- no sort: standalone term — page = first `limit` of the posting,
    // total = posting length (no HashMap build / sort of the full posting). ----
    if let QueryNode::Term(t) = &req.query {
        // Keyword (2h-1) and Set (2h-2) route through the unified accessor;
        // Number stays by-reference. A `Cow` unifies the borrowed (segment OFF)
        // and owned (segment ON) postings so the page+total slice is one path.
        let posting: Option<std::borrow::Cow<RoaringBitmap>> =
            match (coll.fields.get(&t.field), &t.value) {
                (Some(FieldIndex::Keyword(k)), FieldValue::String(s)) => k.term_postings(s),
                // Phase 2h-3: Number exact-match through the unified accessor (segment
                // count-prefix index + tail when sealed, else the live posting).
                (Some(FieldIndex::Number(n)), FieldValue::Number(x)) => {
                    match SortableF64::new(*x) {
                        Ok(key) => n.value_postings(key),
                        Err(_) => return Ok(None),
                    }
                }
                (Some(FieldIndex::Set(s)), FieldValue::String(el)) => s.element_postings(el),
                _ => return Ok(None), // type mismatch → fall back (eval_term reports it)
            };
        let page: Vec<(u32, f32)> = posting
            .as_deref()
            .into_iter()
            .flat_map(|p| p.iter())
            .take(want)
            .map(|id| (id, 1.0))
            .collect();
        let total = posting.as_deref().map(|p| p.len()).unwrap_or(0);
        return Ok(Some((page, total, PlanKind::Posting)));
    }

    // ---- no sort: standalone range early-termination ----
    if let QueryNode::Range(r) = &req.query {
        let Some(FieldIndex::Number(n)) = coll.fields.get(&r.field) else {
            return Ok(None);
        };
        let (lo, hi) = range_bounds(r)?;
        // An empty/inverted range yields an empty page (and avoids the
        // `BTreeMap::range` panic on such bounds).
        if range_is_empty(lo, hi) {
            return Ok(Some((Vec::new(), 0, PlanKind::Posting)));
        }
        let mut page: Vec<(u32, f32)> = Vec::with_capacity(want.min(1024));
        let mut total: u64 = 0;
        if let Some(seg) = n.segment_ref() {
            let (page, total) =
                n.range_page_segment(seg.as_ref(), lo, hi, want, req.track_total)?;
            return Ok(Some((page, total, PlanKind::Posting)));
        }

        // Segment OFF: zero-clone range walk over the in-RAM `values` BTreeMap.
        let values = n.sorted_values();
        for (_v, docs) in values.range((lo, hi)) {
            // Exact total is a cheap sum of bucket lengths — no per-doc work.
            total += docs.len() as u64;
            if page.len() < want {
                for id in docs {
                    page.push((id, 1.0));
                    if page.len() >= want {
                        break;
                    }
                }
            } else if !req.track_total {
                break;
            }
        }
        return Ok(Some((page, total, PlanKind::Posting)));
    }

    // ---- no sort: filter-only AND/NOT-AND with exact total + first page ----
    //
    // The general evaluator materializes these constant-score shapes into a
    // HashMap and then ranks every matching doc. For a pure filter every score
    // is identical and result order is unspecified (same as standalone term /
    // range planners above), so the bitmap is already the answer set: return its
    // first page while preserving the exact total.
    if let Some((page, total)) = eval_filter_only_page(coll, &req.query, want)? {
        return Ok(Some((page, total, PlanKind::Posting)));
    }
    if let Some((bitmap, score)) = eval_filter_only_bitmap(coll, &req.query)? {
        let total = bitmap.len();
        let page = bitmap
            .into_iter()
            .take(want)
            .map(|id| (id, score))
            .collect();
        return Ok(Some((page, total, PlanKind::Posting)));
    }

    Ok(None)
}
