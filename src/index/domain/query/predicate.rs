//! A query tree as a per-doc predicate for the sort walk, the shape checks that
//! decide whether a sorted or collapsed search may take the planner, and the
//! term or range leaf that drives an early-terminating collapse.

use anyhow::{bail, Result};

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::query::clause::clause_matches;
use crate::index::domain::query::range::range_bounds;
use crate::index::domain::query::selectivity::estimate_selectivity;
use crate::index::domain::query::terms::{eval_field_doc_union, eval_ids};
use crate::index::domain::sortable_f64::SortableF64;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::QueryNode;

/// Boolean: does `id` satisfy `node`? (Score ignored — used by sort-walk.)
pub(crate) fn query_predicate(coll: &Collection, node: &QueryNode, id: u32) -> Result<bool> {
    Ok(match node {
        QueryNode::Term(_)
        | QueryNode::Terms(_)
        | QueryNode::Prefix(_)
        | QueryNode::Range(_)
        | QueryNode::Match(_) => clause_matches(coll, node, id)?.is_some(),
        // #182: ids is a direct membership test against the resolved bitmap.
        QueryNode::Ids(q) => eval_ids(coll, q)?.contains(id),
        QueryNode::And(cs) => {
            for c in cs {
                if !query_predicate(coll, c, id)? {
                    return Ok(false);
                }
            }
            true
        }
        QueryNode::Or(cs) => {
            for c in cs {
                if query_predicate(coll, c, id)? {
                    return Ok(true);
                }
            }
            false
        }
        QueryNode::Not(c) => !query_predicate(coll, c, id)?,
        QueryNode::Knn(_) => bail!("knn cannot be evaluated as a per-doc predicate"),
        QueryNode::Rrf(_) => bail!("rrf cannot be evaluated as a per-doc predicate"),
        QueryNode::HasChild(_) => bail!("has_child cannot be evaluated as a per-doc predicate"),
        QueryNode::Hamming(_) => bail!("hamming cannot be evaluated as a per-doc predicate"),
        // Exists/Duplicated test membership in the field's doc-union; the sort-walk
        // runs over a bounded candidate set so the recompute stays cheap.
        QueryNode::Exists(e) => eval_field_doc_union(coll, &e.field, 1)?.contains(id),
        QueryNode::Duplicated(d) => {
            eval_field_doc_union(coll, &d.field, d.min_group_size.max(2) as u64)?.contains(id)
        }
    })
}

pub(super) fn query_has_knn(node: &QueryNode) -> bool {
    match node {
        QueryNode::Knn(_) => true,
        // rrf produces a fused relevance ranking, not a sort-walkable predicate —
        // treat it like knn so the sort fast-path bails to the general evaluator.
        QueryNode::Rrf(_) => true,
        QueryNode::And(cs) | QueryNode::Or(cs) => cs.iter().any(query_has_knn),
        QueryNode::Not(c) => query_has_knn(c),
        _ => false,
    }
}

/// #181: does the query tree contain a `has_child` clause anywhere? Such a query
/// must be sorted via the materialized path (eval_query resolves the join),
/// never the per-doc keyset planner.
pub(crate) fn query_has_has_child(node: &QueryNode) -> bool {
    match node {
        QueryNode::HasChild(_) => true,
        QueryNode::And(cs) | QueryNode::Or(cs) => cs.iter().any(query_has_has_child),
        QueryNode::Not(c) => query_has_has_child(c),
        _ => false,
    }
}

/// Constant-score = no scored clause anywhere (no `match`, no `knn`). All
/// matches score equally, so collapse can early-terminate (any `limit` groups).
pub(crate) fn query_is_constant_score(node: &QueryNode) -> bool {
    match node {
        // HasChild produces a constant-score bitmap, but collapse early-term
        // can't DRIVE through it (no per-doc predicate) → treat as non-constant
        // so a query containing it takes the full collapse path.
        QueryNode::Match(_)
        | QueryNode::Knn(_)
        | QueryNode::HasChild(_)
        | QueryNode::Hamming(_)
        | QueryNode::Rrf(_) => false,
        QueryNode::And(cs) | QueryNode::Or(cs) => cs.iter().all(query_is_constant_score),
        QueryNode::Not(c) => query_is_constant_score(c),
        _ => true,
    }
}

/// Iterate the docs of a Term/Range by reference (no clone). Used to drive
/// early-terminating collapse without materializing the clause.
fn term_or_range_iter<'a>(
    coll: &'a Collection,
    node: &QueryNode,
) -> Option<Box<dyn Iterator<Item = u32> + 'a>> {
    match node {
        QueryNode::Term(t) => {
            // Keyword (2h-1) and Set (2h-2) route through the unified accessor: a
            // Borrowed posting (segment OFF) iterates by reference exactly as
            // before; an Owned posting (segment ON — RAM driver dropped) is
            // consumed by an owning iterator so the box can outlive the Cow.
            match (coll.fields.get(&t.field), &t.value) {
                (Some(FieldIndex::Keyword(k)), FieldValue::String(s)) => {
                    Some(match k.term_postings(s) {
                        Some(std::borrow::Cow::Borrowed(set)) => Box::new(set.iter()),
                        Some(std::borrow::Cow::Owned(set)) => Box::new(set.into_iter()),
                        None => Box::new(std::iter::empty()),
                    })
                }
                (Some(FieldIndex::Set(s)), FieldValue::String(el)) => {
                    Some(match s.element_postings(el) {
                        Some(std::borrow::Cow::Borrowed(set)) => Box::new(set.iter()),
                        Some(std::borrow::Cow::Owned(set)) => Box::new(set.into_iter()),
                        None => Box::new(std::iter::empty()),
                    })
                }
                (Some(FieldIndex::Number(n)), FieldValue::Number(x)) => {
                    // Phase 2h-3: Number exact-match through the unified accessor —
                    // Borrowed (segment OFF) iterates by reference; Owned (segment
                    // ON) consumes the decoded sorted-value+tail union.
                    let Ok(key) = SortableF64::new(*x) else {
                        return None;
                    };
                    Some(match n.value_postings(key) {
                        Some(std::borrow::Cow::Borrowed(set)) => Box::new(set.iter()),
                        Some(std::borrow::Cow::Owned(set)) => Box::new(set.into_iter()),
                        None => Box::new(std::iter::empty()),
                    })
                }
                _ => None,
            }
        }
        QueryNode::Range(r) => {
            let Some(FieldIndex::Number(n)) = coll.fields.get(&r.field) else {
                return None;
            };
            let (lo, hi) = range_bounds(r).ok()?;
            // Phase 2h-3: range through the unified accessor. Segment OFF this is
            // the same `values.range` union (now materialized once); segment ON it
            // is the on-disk binary-search union (+ tail, − tombstones). The result
            // is an owned bitmap, so its `into_iter` drives the collapse.
            Some(Box::new(n.range_postings(lo, hi).into_iter()))
        }
        _ => None,
    }
}

/// Pick the cheapest Term/Range leaf to drive an early-terminating collapse.
/// `And` → its cheapest such child; a top-level Term/Range → itself; otherwise
/// `None` (Or/Not/scored queries fall back to the full collapse path).
pub(crate) fn collapse_driver<'a>(
    coll: &'a Collection,
    query: &'a QueryNode,
) -> Option<Box<dyn Iterator<Item = u32> + 'a>> {
    match query {
        QueryNode::Term(_) | QueryNode::Range(_) => term_or_range_iter(coll, query),
        QueryNode::And(children) => children
            .iter()
            .filter(|c| matches!(c, QueryNode::Term(_) | QueryNode::Range(_)))
            .min_by_key(|c| estimate_selectivity(coll, c))
            .and_then(|c| term_or_range_iter(coll, c)),
        _ => None,
    }
}
