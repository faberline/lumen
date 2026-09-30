//! A search over one collection: the query bounds, the collapse and sort paths,
//! the planner's fast paths, general evaluation, and the page with its
//! next-page cursor.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::time::Instant;

use anyhow::{anyhow, bail, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::query::eval::eval_query;
use crate::index::domain::query::page_cursor::{
    make_cursor, make_score_cursor, make_sort_cursor, make_sort_values_cursor, parse_page_cursor,
    PageCursor,
};
use crate::index::domain::query::plan::{try_plan, PlanKind};
use crate::index::domain::query::predicate::{
    collapse_driver, query_has_has_child, query_is_constant_score, query_predicate,
};
use crate::index::domain::query::sort::{
    sort_score, sort_value_at, sort_values_for_doc, validate_sort_request, SortValue,
};
#[cfg(test)]
use crate::index::domain::query::sort_missing::MATERIALIZED_SORT_RETAINED_HIGH_WATER;
use crate::index::domain::query::sort_missing::{
    missing_heap_keys, try_missing_keyword_bitmap_plan, MissingSortHeapCandidate,
};
use crate::index::domain::query::sort_plan::sort_after_for_request;
use crate::index::domain::query::text_match::eval_match_topk;
use crate::index::domain::query::topk::eval_predicable_and_topk;
use crate::index::domain::query::{query_needs_universe, validate_query};
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::query::{QueryNode, SortMissing};
use crate::shared_kernel::types::search::{SearchHit, SearchRequest, SearchResponse};

impl Engine {
    pub fn search(&self, collection_id: &str, req: SearchRequest) -> Result<SearchResponse> {
        let start = Instant::now();
        // DoS guard: reject pathological query trees before evaluation.
        validate_query(&req.query)?;
        if req.offset != 0 && req.cursor.is_some() {
            return Err(StorageError::InvalidPagination(
                "offset and cursor cannot be combined".into(),
            )
            .into());
        }
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;
        validate_sort_request(coll, collection_id, &req)?;

        let interner = &coll.interner;
        let parsed_cursor = req.cursor.as_deref().and_then(parse_page_cursor);
        let native_offset = usize::try_from(req.offset).map_err(|_| {
            StorageError::InvalidPagination("offset does not fit this server platform".into())
        })?;
        let offset = match &parsed_cursor {
            Some(PageCursor::Offset(n)) => *n as usize,
            _ => native_offset,
        };
        // #180: a sort with any `missing: first|last` key is served by the
        // materialized missing-aware path below, which paginates with offset
        // correctly (it never early-terminates). The exclude-only path uses the
        // keyset planner and is the one the #179 guard protects.
        let sort_uses_missing = req
            .sort
            .as_deref()
            .is_some_and(|s| s.iter().any(|spec| spec.missing != SortMissing::Exclude));
        // #181: a sort whose query contains has_child also routes to the
        // materialized path — has_child resolves to a parent bitmap via
        // eval_query, then the parents are sorted by their parent fields.
        let sort_needs_materialize = sort_uses_missing
            || (req.sort.as_deref().is_some_and(|s| !s.is_empty())
                && (query_has_has_child(&req.query) || req.offset != 0));
        // #179: an offset cursor cannot drive a field-sorted (exclude) page.
        // `try_plan` bails for offset != 0, so a non-zero offset would silently
        // fall through to the score-ranked path and IGNORE `sort`. Reject instead
        // of returning a mis-ordered page: sequential sorted paging uses the
        // keyset cursor handed back in the response; random page-jumps use the
        // request's native `offset` field and the materialized path above.
        if matches!(parsed_cursor, Some(PageCursor::Offset(n)) if n != 0)
            && req.sort.as_deref().is_some_and(|s| !s.is_empty())
            && !sort_needs_materialize
        {
            return Err(StorageError::UnsupportedSort(
                "offset pagination cannot be combined with sort; use a keyset \
                 cursor (omit the cursor on page 1, then follow the returned \
                 cursor), or use the native offset field for a direct page jump"
                    .into(),
            )
            .into());
        }
        // A sort keyset only continues the single-number-field sorted planner;
        // a score keyset only continues score-ranked pages. A cursor that does
        // not match the request shape degrades to first-page semantics (caller
        // error — cursors are bound to the query that produced them).
        let sort_after = sort_after_for_request(coll, req.sort.as_deref(), &parsed_cursor)?;
        let score_after: Option<(f32, String)> = match &parsed_cursor {
            Some(PageCursor::ScoreKeyset { score_bits, eid }) if req.sort.is_none() => {
                Some((f32::from_bits(*score_bits), eid.clone()))
            }
            _ => None,
        };
        let limit = req.limit as usize;
        let cache_key = search_cache_key(&req)?;
        if let Some(mut cached) = coll.cached_search_response(&cache_key) {
            let el = start.elapsed();
            cached.took_ms = el.as_millis() as u64;
            cached.took_us = el.as_micros() as u64;
            self.metrics.observe_search(el);
            return Ok(cached);
        }

        // Collapse / field-collapse (group-by a keyword field): return ONE hit
        // per distinct value of `collapse`, scored by the MAX member score. The
        // full matched set is needed so groups are complete → this bypasses the
        // paged planner. `hit.external_id` is the collapse value; `total` is the
        // distinct-group count. Drives nested `group` search: filter the child
        // collection, collapse by `parent_row_id` → distinct matching parents
        // (correlation preserved because each child doc is one group element).
        if let Some(collapse_field) = &req.collapse {
            let fi = coll
                .fields
                .get(collapse_field)
                .ok_or_else(|| StorageError::UnknownField {
                    collection: collection_id.to_string(),
                    field: collapse_field.clone(),
                })?;
            let FieldIndex::Keyword(kidx) = fi else {
                bail!(
                    "collapse requires a keyword field (field `{}`)",
                    collapse_field
                );
            };

            // Early-termination: for a constant-score query (no match/knn) with
            // track_total off, drive from the cheapest leaf and collect distinct
            // collapse values until the page is full — never materializing the
            // full matched set (the difference between 37 ms and the floor at
            // 1M). Order is unspecified, like any constant-score filter page.
            if offset == 0 && !req.track_total && query_is_constant_score(&req.query) {
                if let Some(iter) = collapse_driver(coll, &req.query) {
                    let want = limit.max(1);
                    // Collapse values are owned `String` so the source can be the
                    // live `forward` map OR — after a Phase 2f-1 seal-and-drop —
                    // the segment (`keyword_at`), without a borrow tied to the
                    // dropped map. Default build is unaffected (same result set).
                    let mut seen: HashMap<String, ()> = HashMap::new();
                    let mut order: Vec<String> = Vec::with_capacity(want);
                    for doc in iter {
                        if order.len() >= want {
                            break;
                        }
                        if query_predicate(coll, &req.query, doc)? {
                            if let Some(val) = kidx.keyword_at(doc) {
                                if seen.insert(val.clone(), ()).is_none() {
                                    order.push(val);
                                }
                            }
                        }
                    }
                    let hits: Vec<SearchHit> = order
                        .into_iter()
                        .map(|val| SearchHit {
                            external_id: val,
                            score: 1.0,
                        })
                        .collect();
                    let total = hits.len() as u64; // lower bound (track_total=false)
                    let el = start.elapsed();
                    let took_ms = el.as_millis() as u64;
                    self.metrics.observe_search(el);
                    let response = SearchResponse {
                        hits,
                        total,
                        cursor: None,
                        took_ms,
                        took_us: el.as_micros() as u64,
                    };
                    coll.cache_search_response(cache_key.clone(), &response);
                    return Ok(response);
                }
            }

            let universe: BTreeSet<u32> = if query_needs_universe(&req.query) {
                coll.eid_fields.keys().copied().collect()
            } else {
                BTreeSet::new()
            };
            let scored = eval_query(coll, collection_id, &req.query, &universe, &state)?;
            // Group by the collapse value, keeping the max member score. Docs
            // with no value for the collapse field drop out (no group). Owned
            // `String` keys so the value source can be the segment (`keyword_at`)
            // after a Phase 2f-1 seal-and-drop; default build is unaffected.
            let mut groups: HashMap<String, f32> = HashMap::new();
            for (doc, score) in &scored {
                if let Some(val) = kidx.keyword_at(*doc) {
                    let slot = groups.entry(val).or_insert(f32::NEG_INFINITY);
                    if *score > *slot {
                        *slot = *score;
                    }
                }
            }
            let total = groups.len() as u64;
            // Rank groups by score desc, then value asc; partition top-k.
            let cmp = |a: &(String, f32), b: &(String, f32)| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            };
            let mut ranked: Vec<(String, f32)> = groups.into_iter().collect();
            let k = offset.saturating_add(limit);
            if k > 0 && ranked.len() > k {
                ranked.select_nth_unstable_by(k - 1, cmp);
                ranked.truncate(k);
            }
            ranked.sort_by(cmp);
            let hits: Vec<SearchHit> = ranked
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|(val, score)| SearchHit {
                    external_id: val.to_string(),
                    score,
                })
                .collect();
            let next_offset = offset + hits.len();
            let cursor = if req.offset == 0 && (next_offset as u64) < total {
                Some(make_cursor(next_offset))
            } else {
                None
            };
            let el = start.elapsed();
            let took_ms = el.as_millis() as u64;
            self.metrics.observe_search(el);
            let response = SearchResponse {
                hits,
                total,
                cursor,
                took_ms,
                took_us: el.as_micros() as u64,
            };
            coll.cache_search_response(cache_key.clone(), &response);
            return Ok(response);
        }

        // #180: materialized missing-aware sort. When any sort key is
        // first/last, include rows lacking that key's value (placed before/after
        // the present rows per the policy) and count them in an exact total; rows
        // missing an `exclude` key are dropped. The single-key keyword planner
        // streams posting buckets when safe; every other shape uses a bounded
        // top-k fallback and paginates by offset.
        if sort_needs_materialize {
            let sort = req
                .sort
                .as_deref()
                .expect("materialized-sort path has a sort");
            if let Some((hits, total)) =
                try_missing_keyword_bitmap_plan(coll, collection_id, &req, offset, sort, &state)?
            {
                let next_offset = offset + hits.len();
                let cursor = if req.offset == 0 && (next_offset as u64) < total {
                    Some(make_cursor(next_offset))
                } else {
                    None
                };
                let el = start.elapsed();
                let took_ms = el.as_millis() as u64;
                self.metrics.observe_search(el);
                let response = SearchResponse {
                    hits,
                    total,
                    cursor,
                    took_ms,
                    took_us: el.as_micros() as u64,
                };
                coll.cache_search_response(cache_key.clone(), &response);
                return Ok(response);
            }
            let universe: BTreeSet<u32> = if query_needs_universe(&req.query) {
                coll.eid_fields.keys().copied().collect()
            } else {
                BTreeSet::new()
            };
            let scored = eval_query(coll, collection_id, &req.query, &universe, &state)?;
            // Keep only the prefix required to answer the requested native
            // offset page. The worst-first heap retains exactly `offset +
            // limit` tuples; it never grows a compaction buffer above that
            // bound while still scanning every match for the exact total.
            let wanted = offset.saturating_add(limit);
            let mut included: BinaryHeap<MissingSortHeapCandidate> =
                BinaryHeap::with_capacity(wanted.min(1024));
            let mut total = 0u64;
            'docs: for (id, _score) in scored {
                let mut tuple = Vec::with_capacity(sort.len());
                for spec in sort {
                    let v = sort_value_at(coll, spec, id)?;
                    if v.is_none() && spec.missing == SortMissing::Exclude {
                        continue 'docs;
                    }
                    tuple.push(v);
                }
                total += 1;
                if wanted == 0 {
                    continue;
                }
                let candidate = MissingSortHeapCandidate {
                    id,
                    keys: missing_heap_keys(&tuple, sort),
                    tuple,
                    external_id: interner.resolve(id).to_string(),
                };
                if included.len() < wanted {
                    included.push(candidate);
                    #[cfg(test)]
                    MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| {
                        high_water.set(high_water.get().max(included.len() as u64));
                    });
                } else if included
                    .peek()
                    .is_some_and(|worst| candidate.cmp(worst) == CmpOrdering::Less)
                {
                    included.pop();
                    included.push(candidate);
                }
            }
            let mut included = included.into_vec();
            included.sort_unstable();
            let hits: Vec<SearchHit> = included
                .into_iter()
                .skip(offset)
                .take(limit)
                .map(|candidate| SearchHit {
                    external_id: candidate.external_id,
                    score: candidate
                        .tuple
                        .iter()
                        .find_map(|v| v.as_ref().map(sort_score))
                        .unwrap_or(0.0),
                })
                .collect();
            let next_offset = offset + hits.len();
            let cursor = if req.offset == 0 && (next_offset as u64) < total {
                Some(make_cursor(next_offset))
            } else {
                None
            };
            let el = start.elapsed();
            let took_ms = el.as_millis() as u64;
            self.metrics.observe_search(el);
            let response = SearchResponse {
                hits,
                total,
                cursor,
                took_ms,
                took_us: el.as_micros() as u64,
            };
            coll.cache_search_response(cache_key.clone(), &response);
            return Ok(response);
        }

        // Planner fast paths (first page): sort-by-field and standalone range
        // early-terminate / avoid materializing a wide clause, returning the
        // final page directly. Everything else falls through to the
        // materialize-and-rank path below (identical result to before).
        // A score keyset bypasses the planner and the bounded top-k fast paths
        // (they collect top-(k) of the WHOLE set; the keyset page is the top of
        // the strictly-after-cursor subset).
        let planned = if score_after.is_some() {
            None
        } else {
            try_plan(coll, &req, offset, sort_after.as_ref())?
        };

        let (page, total, plan_kind): (Vec<(u32, f32)>, u64, PlanKind) = match planned {
            Some(pt) => pt,
            None => {
                // Single-term (and multi-token AND) `match` fast path: score
                // into a bounded top-k heap, skipping the per-doc `HashMap`
                // insert, map→Vec collect, and full matched Vec partition below.
                // Each docid is scored exactly once on these shapes, so the f32
                // bits and the `total` (== unique-docid count) are identical to
                // the map path.
                // Anything else (nested bool, multi-token OR, knn/rrf, …) falls
                // through to the general `eval_query` map path unchanged.
                let mut ranked: Vec<(u32, f32)>;
                let total: u64;
                if score_after.is_some() {
                    // Keyset continuation: rank the strictly-after-cursor subset.
                    let universe: BTreeSet<u32> = if query_needs_universe(&req.query) {
                        coll.eid_fields.keys().copied().collect()
                    } else {
                        BTreeSet::new()
                    };
                    let scored = eval_query(coll, collection_id, &req.query, &universe, &state)?;
                    total = scored.len() as u64;
                    let (after_score, after_eid) = score_after.as_ref().unwrap();
                    ranked = scored
                        .into_iter()
                        .filter(|(id, s)| {
                            *s < *after_score
                                || (*s == *after_score
                                    && interner.resolve(*id) > after_eid.as_str())
                        })
                        .collect();
                } else if let QueryNode::Match(m) = &req.query {
                    if let Some((top, exact_total)) =
                        eval_match_topk(coll, m, interner, offset.saturating_add(limit))?
                    {
                        total = exact_total;
                        ranked = top;
                    } else {
                        let scored =
                            eval_query(coll, collection_id, &req.query, &BTreeSet::new(), &state)?;
                        total = scored.len() as u64;
                        ranked = scored.into_iter().collect();
                    }
                } else if let Some((top, exact_total)) = eval_predicable_and_topk(
                    coll,
                    &req.query,
                    interner,
                    offset.saturating_add(limit),
                )? {
                    total = exact_total;
                    ranked = top;
                } else {
                    // The full eid set ("universe") is only consumed by the `Not`
                    // branch of eval_query; build it only when the query needs it.
                    let universe: BTreeSet<u32> = if query_needs_universe(&req.query) {
                        coll.eid_fields.keys().copied().collect()
                    } else {
                        BTreeSet::new()
                    };
                    let scored = eval_query(coll, collection_id, &req.query, &universe, &state)?;
                    total = scored.len() as u64;
                    ranked = scored.into_iter().collect();
                }

                // Rank by score desc, then external_id asc (tie-break on the
                // resolved string, stable across snapshot rebuilds). Partition
                // the top-k to the front in O(n), then sort just that slice.
                let cmp = |a: &(u32, f32), b: &(u32, f32)| {
                    b.1.partial_cmp(&a.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| interner.resolve(a.0).cmp(interner.resolve(b.0)))
                };
                let k = offset.saturating_add(limit);
                if k > 0 && ranked.len() > k {
                    ranked.select_nth_unstable_by(k - 1, cmp);
                    ranked.truncate(k);
                }
                ranked.sort_by(cmp);
                let page = ranked.into_iter().skip(offset).take(limit).collect();
                (page, total, PlanKind::ScoreRanked)
            }
        };

        // Shared response building (resolve dense ids back to external_ids).
        let hits: Vec<SearchHit> = page
            .iter()
            .map(|(id, score)| SearchHit {
                external_id: interner.resolve(*id).to_string(),
                score: *score,
            })
            .collect();

        // Next-page cursor. Keyset-capable pages (sorted-field walks and
        // score-ranked pages) hand out a v2 keyset bound to the LAST hit, so
        // the next page SEEKS instead of skipping — deep pagination cost does
        // not grow with depth. Posting-order planner pages and requests that
        // arrived with a legacy offset cursor keep the offset scheme.
        let used_offset_cursor = matches!(parsed_cursor, Some(PageCursor::Offset(_)));
        let cursor = if hits.is_empty() {
            None
        } else if used_offset_cursor {
            let next_offset = offset + hits.len();
            if (next_offset as u64) < total {
                Some(make_cursor(next_offset))
            } else {
                None
            }
        } else {
            match plan_kind {
                PlanKind::SortedField if hits.len() == limit => {
                    let sort = req.sort.as_deref().expect("sorted plan has sort");
                    let (last_id, _) = *page.last().expect("non-empty page");
                    let values = sort_values_for_doc(coll, sort, last_id)?;
                    match (sort, values) {
                        ([spec], Some(values)) => match coll.fields.get(&spec.field) {
                            Some(FieldIndex::Number(_)) => match values.as_slice() {
                                [SortValue::Number(bits)] => Some(make_sort_cursor(*bits, last_id)),
                                _ => Some(make_sort_values_cursor(&values, last_id)),
                            },
                            _ => Some(make_sort_values_cursor(&values, last_id)),
                        },
                        (_, Some(values)) => Some(make_sort_values_cursor(&values, last_id)),
                        (_, None) => None,
                    }
                }
                PlanKind::ScoreRanked if hits.len() == limit => {
                    let last = hits.last().expect("non-empty hits");
                    Some(make_score_cursor(last.score, &last.external_id))
                }
                PlanKind::Posting => {
                    let next_offset = offset + hits.len();
                    if (next_offset as u64) < total {
                        Some(make_cursor(next_offset))
                    } else {
                        None
                    }
                }
                _ => None, // keyset page shorter than the limit → exhausted
            }
        };

        let el = start.elapsed();
        let took_ms = el.as_millis() as u64;
        self.metrics.observe_search(el);
        let response = SearchResponse {
            hits,
            total,
            cursor,
            took_ms,
            took_us: el.as_micros() as u64,
        };
        coll.cache_search_response(cache_key, &response);
        Ok(response)
    }
}

fn search_cache_key(req: &SearchRequest) -> Result<String> {
    serde_json::to_string(req).map_err(Into::into)
}

#[cfg(test)]
mod tests;
