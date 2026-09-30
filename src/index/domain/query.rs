//! Query evaluation over one collection: the bounds a query tree must pass
//! before it runs, and the doc id to score map every clause evaluates to.
//! Non-text leaves score a constant 1.0, a match scores BM25, and the
//! combinators sum their children's scores.

mod clause;
pub(crate) mod eval;
pub(crate) mod filter_page;
mod knn;
pub(in crate::index) mod page_cursor;
pub(crate) mod plan;
pub(crate) mod predicate;
pub(crate) mod prepared_match;
pub(crate) mod range;
pub(crate) mod range_conjunction;
pub(crate) mod rank;
mod selectivity;
pub(crate) mod sort;
pub(in crate::index) mod sort_missing;
pub(crate) mod sort_plan;
pub(crate) mod terms;
pub(crate) mod text_match;
pub(crate) mod topk;
pub(crate) mod zip_cursor;

use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::query::QueryNode;

/// Map of `external_id` → score. Non-text leaves return a constant
/// score of `1.0`; only `match` leaves emit BM25 scores. Combinators
/// sum child scores so an AND over (match, term) ranks documents whose
/// text relevance is highest among the eligible set.
///
/// Backed by a `HashMap` (not `BTreeMap`): the final page is re-sorted by
/// `(−score, external_id)` in `search`, so iteration order here is
/// irrelevant, and O(1) inserts beat O(log n) for the large matched sets a
/// broad query produces.
type ScoredHits = HashMap<u32, f32>;

/// Query-shape safety bounds (DoS guards). A query is validated before
/// evaluation so a pathological tree can never exhaust the stack (deep
/// nesting) or CPU/memory (very wide trees / huge fan-outs).
const MAX_QUERY_DEPTH: usize = 32;
const MAX_QUERY_NODES: usize = 1024;
const MAX_TERMS_VALUES: usize = 1024;
const MAX_KNN_K: u32 = 10_000;

/// Reject pathological queries with a clear error. Traversal is **iterative**
/// (explicit stack) so validating a deeply-nested tree cannot itself overflow
/// the stack. Bounds: nesting depth, total node count, `terms` fan-out, `knn` k.
pub fn validate_query(root: &QueryNode) -> std::result::Result<(), StorageError> {
    let mut stack: Vec<(&QueryNode, usize)> = vec![(root, 1)];
    let mut nodes = 0usize;
    while let Some((node, depth)) = stack.pop() {
        nodes += 1;
        if depth > MAX_QUERY_DEPTH {
            return Err(StorageError::QueryTooComplex(format!(
                "nesting depth exceeds {MAX_QUERY_DEPTH}"
            )));
        }
        if nodes > MAX_QUERY_NODES {
            return Err(StorageError::QueryTooComplex(format!(
                "node count exceeds {MAX_QUERY_NODES}"
            )));
        }
        match node {
            QueryNode::And(children) | QueryNode::Or(children) => {
                for c in children {
                    stack.push((c, depth + 1));
                }
            }
            QueryNode::Not(child) => stack.push((child, depth + 1)),
            QueryNode::HasChild(hc) => stack.push((&hc.query, depth + 1)),
            QueryNode::Rrf(r) => {
                for c in &r.queries {
                    stack.push((c, depth + 1));
                }
            }
            QueryNode::Terms(t) if t.values.len() > MAX_TERMS_VALUES => {
                return Err(StorageError::QueryTooComplex(format!(
                    "terms value count {} exceeds {MAX_TERMS_VALUES}",
                    t.values.len()
                )));
            }
            QueryNode::Knn(k) if k.k > MAX_KNN_K => {
                return Err(StorageError::QueryTooComplex(format!(
                    "knn k={} exceeds {MAX_KNN_K}",
                    k.k
                )));
            }
            QueryNode::Prefix(p) if p.value.is_empty() => {
                return Err(StorageError::QueryTooComplex(
                    "prefix value must not be empty".into(),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether evaluating `q` actually needs the full eid universe. It is used in
/// exactly two spots: the `Not` arm (a negation reached top-level, via `Or`,
/// or as a `Not`'s inner) and an all-negative `And`. A `Not` that is a direct
/// conjunct of an `And` is applied as a *filter* and needs no universe, so the
/// common `And[positive…, Not[…]]` shape skips the O(N) clone. This mirrors
/// `eval_query` exactly — keep the two in sync.
pub(crate) fn query_needs_universe(q: &QueryNode) -> bool {
    match q {
        QueryNode::Not(_) => true,
        QueryNode::Or(children) => children.iter().any(query_needs_universe),
        QueryNode::Rrf(r) => r.queries.iter().any(query_needs_universe),
        QueryNode::And(children) => {
            let (nots, positives): (Vec<&QueryNode>, Vec<&QueryNode>) = children
                .iter()
                .partition(|c| matches!(c, QueryNode::Not(_)));
            if positives.is_empty() {
                // Empty AND → empty set (no universe); all-negative AND needs it.
                return !nots.is_empty();
            }
            positives.iter().any(|c| query_needs_universe(c))
                || nots.iter().any(|n| match n {
                    // The Not is a filter; only its inner can pull in a universe.
                    QueryNode::Not(inner) => query_needs_universe(inner),
                    _ => false,
                })
        }
        _ => false,
    }
}

fn constant_score(set: RoaringBitmap) -> ScoredHits {
    set.into_iter().map(|id| (id, 1.0)).collect()
}
