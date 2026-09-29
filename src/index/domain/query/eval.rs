//! The general evaluator: every clause of a query tree evaluated to its scored
//! hits, an AND driven from its cheapest conjunct when the others can be
//! checked per doc, and a has_child clause resolved through the child
//! collection.

use std::collections::{BTreeSet, HashMap};

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::query::clause::apply_conjuncts;
use crate::index::domain::query::knn::{eval_hamming, eval_knn, eval_knn_filtered, eval_rrf};
use crate::index::domain::query::prepared_match::{prep_matches, PreparedMatch};
use crate::index::domain::query::range::eval_range;
use crate::index::domain::query::selectivity::{
    estimate_selectivity, is_predicable, plan_filter_candidates, SPARSE_CANDIDATE_MAX,
};
use crate::index::domain::query::terms::{
    eval_field_doc_union, eval_ids, eval_prefix, eval_term, eval_terms,
};
use crate::index::domain::query::text_match::eval_match;
use crate::index::domain::query::{constant_score, query_needs_universe, ScoredHits};
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::query::{HasChildQuery, KnnQuery, QueryNode};
use crate::storage::EngineState;

pub(crate) fn eval_query(
    coll: &Collection,
    collection_id: &str,
    q: &QueryNode,
    universe: &BTreeSet<u32>,
    state: &EngineState,
) -> Result<ScoredHits> {
    Ok(match q {
        QueryNode::Match(m) => eval_match(coll, m)?,
        QueryNode::Term(t) => constant_score(eval_term(coll, t)?),
        QueryNode::Terms(t) => constant_score(eval_terms(coll, t)?),
        QueryNode::Prefix(p) => constant_score(eval_prefix(coll, p)?),
        QueryNode::Ids(q) => constant_score(eval_ids(coll, q)?),
        QueryNode::Range(r) => constant_score(eval_range(coll, r)?),
        QueryNode::Knn(k) => eval_knn(coll, k)?,
        QueryNode::Hamming(hq) => eval_hamming(coll, hq)?,
        QueryNode::Exists(e) => constant_score(eval_field_doc_union(coll, &e.field, 1)?),
        QueryNode::Duplicated(d) => constant_score(eval_field_doc_union(
            coll,
            &d.field,
            d.min_group_size.max(2) as u64,
        )?),
        QueryNode::Rrf(r) => eval_rrf(coll, collection_id, r, universe, state)?,
        QueryNode::And(children) => {
            // A `Not` conjunct is a filter: `A AND NOT B = A \ B`.
            let (nots, positives): (Vec<&QueryNode>, Vec<&QueryNode>) = children
                .iter()
                .partition(|c| matches!(c, QueryNode::Not(_)));

            if positives.is_empty() && nots.is_empty() {
                return Ok(HashMap::new()); // empty AND → empty set
            }

            // Filter-correct kNN. If a `knn` clause is conjoined with anything
            // else, evaluate the NON-knn part to an allow-set first, then drive
            // each knn THROUGH it (allow-list) — so the result is the nearest-k
            // WITHIN the filtered set, not a post-filter over the global top-k
            // (which collapses recall the way pgvector does). knn is never
            // `is_predicable`, so without this it would fall through to the
            // materialize-and-intersect path and post-filter.
            if positives.iter().any(|c| matches!(c, QueryNode::Knn(_))) {
                let knn_qs: Vec<&KnnQuery> = positives
                    .iter()
                    .copied()
                    .filter_map(|c| match c {
                        QueryNode::Knn(k) => Some(k),
                        _ => None,
                    })
                    .collect();
                let rest_children: Vec<QueryNode> = children
                    .iter()
                    .filter(|c| !matches!(c, QueryNode::Knn(_)))
                    .cloned()
                    .collect();

                // Allow-set + base scores from every non-knn conjunct (filters,
                // matches, has_child, negations). No non-knn conjunct → the knn
                // is unconstrained (degenerate `And(knn)` / `And(knn, knn)`).
                let (allow, base_scores): (Option<RoaringBitmap>, ScoredHits) =
                    if rest_children.is_empty() {
                        (None, HashMap::new())
                    } else {
                        let rest = eval_query(
                            coll,
                            collection_id,
                            &QueryNode::And(rest_children),
                            universe,
                            state,
                        )?;
                        let bm: RoaringBitmap = rest.keys().copied().collect();
                        (Some(bm), rest)
                    };

                // Each knn driven through the allow-set; intersect multiple knns.
                let mut knn_acc: Option<ScoredHits> = None;
                for kq in &knn_qs {
                    let hits = match &allow {
                        Some(bm) => eval_knn_filtered(coll, kq, bm)?,
                        None => eval_knn(coll, kq)?,
                    };
                    knn_acc = Some(match knn_acc {
                        None => hits,
                        Some(prev) => prev
                            .into_iter()
                            .filter_map(|(id, s)| hits.get(&id).map(|h| (id, s + h)))
                            .collect(),
                    });
                }
                let knn_hits = knn_acc.unwrap_or_default();

                // AND-combine: a hit must satisfy both the knn and the non-knn
                // part; score = base (non-knn contributions) + knn similarity.
                return Ok(match &allow {
                    None => knn_hits,
                    Some(_) => {
                        let mut out = ScoredHits::new();
                        for (id, kscore) in knn_hits {
                            if let Some(base) = base_scores.get(&id) {
                                out.insert(id, base + kscore);
                            }
                        }
                        out
                    }
                });
            }

            // Planner fast path. Evaluate the AND without materializing a wide
            // clause:
            //   * ≥1 filter conjunct (term/terms/range) → INTERSECT their
            //     RoaringBitmaps (compressed-SIMD AND, smallest first), subtract
            //     filter negations, then score `match` conjuncts over the small
            //     candidate set;
            //   * match-only positives → drive from the cheapest match
            //     (bulk-scored), predicate-filter the rest.
            // Score = sum of every conjunct's contribution → byte-identical to
            // the fallback.
            let predicable = !positives.is_empty()
                && positives.iter().all(|c| is_predicable(c))
                && nots.iter().all(|c| {
                    let QueryNode::Not(inner) = c else {
                        unreachable!()
                    };
                    is_predicable(inner)
                });

            if predicable {
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

                // Drive from the globally cheapest POSITIVE. Cheapest is a filter
                // → intersect filter bitmaps, then score matches over the small
                // candidate. Cheapest is a match (e.g. a rare keyword vs a wide
                // range — `price 1000-5000 AND name has "手機殼"`) → drive from
                // that match's scored posting and apply the filters as per-doc
                // predicates, so the match is never scored over a wide filter's
                // worth of docs.
                if let Some(plan) =
                    plan_filter_candidates(coll, &filter_pos, &filter_nots, &match_pos)?
                {
                    let cand = plan.resolve(coll, &filter_pos, &filter_nots)?;
                    // Each filter / negation contributes a constant 1.0; matches
                    // add BM25 on top and gate membership.
                    let base = filter_pos.len() as f32 + nots.len() as f32;
                    let preps = prep_matches(coll, &match_pos)?;
                    let not_preps = prep_matches(coll, &match_nots)?;
                    // #4246: a small candidate set never materializes a posting.
                    let sparse_cand: Option<Vec<u32>> =
                        (cand.len() <= SPARSE_CANDIDATE_MAX).then(|| cand.iter().collect());
                    // Phase 2m: RESOLVE each match's per-token postings ONCE (dict
                    // binary-search + cache fetch paid here, not per candidate doc).
                    // On disk this is THE `filtered_search` fix — the candidate set
                    // can be thousands of docs and the old per-doc `match_doc_score`
                    // re-resolved every token's posting for each, decoding/cloning a
                    // wide posting thousands of times. The prepared postings borrow
                    // the field index for the whole `'doc` loop. Byte-identical
                    // scores; only the resolution is hoisted.
                    let prepared: Vec<Option<PreparedMatch>> = preps
                        .iter()
                        .map(|(idx, toks, op)| {
                            PreparedMatch::resolve_for(idx, toks, *op, sparse_cand.as_deref())
                        })
                        .collect();
                    let not_prepared: Vec<Option<PreparedMatch>> = not_preps
                        .iter()
                        .map(|(idx, toks, op)| {
                            PreparedMatch::resolve_for(idx, toks, *op, sparse_cand.as_deref())
                        })
                        .collect();
                    let mut acc = ScoredHits::new();
                    'doc: for id in &cand {
                        let mut score = base;
                        for ((idx, _, _), pm) in preps.iter().zip(prepared.iter()) {
                            // `None` prepared ⇒ the corpus/tokens were empty, which
                            // `match_doc_score` reported as no-match → skip the doc.
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
                        acc.insert(id, score);
                    }
                    acc
                } else {
                    // Match-only positives: drive from the cheapest match
                    // (materialized + bulk-scored), then predicate-filter.
                    let driver_ix = match_pos
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, c)| estimate_selectivity(coll, c))
                        .map(|(i, _)| i)
                        .expect("match_pos non-empty when filter_pos empty");
                    let driver = match_pos[driver_ix];
                    let others: Vec<&QueryNode> = match_pos
                        .iter()
                        .copied()
                        .enumerate()
                        .filter(|(i, _)| *i != driver_ix)
                        .map(|(_, c)| c)
                        .collect();
                    let other_matches = prep_matches(coll, &others)?;
                    let not_inners: Vec<&QueryNode> = nots
                        .iter()
                        .copied()
                        .map(|n| match n {
                            QueryNode::Not(inner) => &**inner,
                            _ => unreachable!(),
                        })
                        .collect();
                    // Drive from the cheapest match; apply the OTHER matches AND
                    // all filters as per-doc predicates (filters contribute their
                    // constant score via clause_matches inside apply_conjuncts).
                    let scored = eval_query(coll, collection_id, driver, universe, state)?;
                    let mut acc = ScoredHits::new();
                    for (id, base) in scored {
                        if let Some(s) = apply_conjuncts(
                            coll,
                            id,
                            base,
                            &filter_pos,
                            &other_matches,
                            &not_inners,
                        )? {
                            acc.insert(id, s);
                        }
                    }
                    acc
                }
            } else {
                // Fallback: materialize-and-intersect.
                let mut pos = positives.iter();
                let mut acc = match pos.next() {
                    Some(first) => eval_query(coll, collection_id, first, universe, state)?,
                    // All-negative AND: start from the universe, then trim.
                    None => constant_score(universe.iter().cloned().collect()),
                };
                for c in pos {
                    let other = eval_query(coll, collection_id, c, universe, state)?;
                    acc = acc
                        .into_iter()
                        .filter_map(|(eid, score)| other.get(&eid).map(|s| (eid, score + s)))
                        .collect();
                    if acc.is_empty() {
                        break;
                    }
                }
                for n in &nots {
                    if acc.is_empty() {
                        break;
                    }
                    let QueryNode::Not(inner) = n else {
                        unreachable!()
                    };
                    let exclude = eval_query(coll, collection_id, inner, universe, state)?;
                    acc.retain(|eid, _| !exclude.contains_key(eid));
                    for s in acc.values_mut() {
                        *s += 1.0;
                    }
                }
                acc
            }
        }
        QueryNode::Or(children) => {
            let mut acc: ScoredHits = HashMap::new();
            for c in children {
                for (eid, score) in eval_query(coll, collection_id, c, universe, state)? {
                    *acc.entry(eid).or_insert(0.0) += score;
                }
            }
            acc
        }
        QueryNode::Not(child) => {
            let inner = eval_query(coll, collection_id, child, universe, state)?;
            universe
                .iter()
                .filter(|id| !inner.contains_key(*id))
                .map(|id| (*id, 1.0))
                .collect()
        }
        QueryNode::HasChild(hc) => eval_has_child(coll, hc, state)?,
    })
}

/// Evaluate a `has_child` clause: run its sub-query on the child collection,
/// map each matching child's `field` (= parent external_id) to a PARENT docid,
/// and return that set as a constant-score result — so it composes under
/// and/or/not in the PARENT query like any other clause. Within-element
/// correlation holds because one child doc is one group element.
fn eval_has_child(
    parent: &Collection,
    hc: &HasChildQuery,
    state: &EngineState,
) -> Result<ScoredHits> {
    let child = state
        .collections
        .get(&hc.collection)
        .ok_or_else(|| StorageError::CollectionNotFound(hc.collection.clone()))?;
    let FieldIndex::Keyword(kidx) =
        child
            .fields
            .get(&hc.field)
            .ok_or_else(|| StorageError::UnknownField {
                collection: hc.collection.clone(),
                field: hc.field.clone(),
            })?
    else {
        bail!(
            "has_child `field` must be a keyword field (`{}` in `{}`)",
            hc.field,
            hc.collection
        );
    };
    let child_universe: BTreeSet<u32> = if query_needs_universe(&hc.query) {
        child.eid_fields.keys().copied().collect()
    } else {
        BTreeSet::new()
    };
    let matched = eval_query(child, &hc.collection, &hc.query, &child_universe, state)?;
    // Distinct parent external_ids → PARENT docids → constant-score set.
    let mut parents = RoaringBitmap::new();
    for (child_doc, _) in &matched {
        // Route through `keyword_at` (segment for sealed ids after a Phase 2f-1
        // seal-and-drop, else the live `forward` tail) so the parent-id lookup
        // survives a dropped forward payload. Default build is unaffected.
        if let Some(pid) = kidx.keyword_at(*child_doc) {
            if let Some(parent_doc) = parent.interner.id(&pid) {
                parents.insert(parent_doc);
            }
        }
    }
    Ok(constant_score(parents))
}
