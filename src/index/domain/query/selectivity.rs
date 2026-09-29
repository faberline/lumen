//! Which AND conjuncts the planner can check per doc, how many docs each
//! matches without materializing it, and which filter drives an AND of filters
//! and matches.

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::analysis::tokenize;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::query::clause::eval_filter_bitmap_conjunction;
use crate::index::domain::query::range::range_bounds;
use crate::index::domain::sortable_f64::SortableF64;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::query::{MatchOp, QueryNode};

// ---------------------------------------------------------------------------
// Query planner — filter-as-pruning for AND
//
// The materialize-and-intersect form of `And` evaluates every conjunct in full
// (e.g. a 16k-doc `range`) and then intersects. That is O(largest matched-set)
// even when the *combined* selectivity is tiny. The planner instead drives the
// AND from its cheapest conjunct and checks the others as per-doc predicates
// against the forward maps — so a wide clause is never materialized. The result
// (the matched set AND each doc's summed score) is identical to the
// materialized form; only the cost differs. Embedded mode only (the forward
// maps the predicates read are populated when there is no LSM backend).
// ---------------------------------------------------------------------------

/// Conjuncts the planner can check as a per-doc predicate. Anything else
/// (nested And/Or, Knn) forces the materialize-and-intersect fallback.
pub(crate) fn is_predicable(node: &QueryNode) -> bool {
    matches!(
        node,
        QueryNode::Term(_)
            | QueryNode::Terms(_)
            | QueryNode::Prefix(_)
            | QueryNode::Ids(_)
            | QueryNode::Range(_)
            | QueryNode::Match(_)
            | QueryNode::Exists(_)
            | QueryNode::Duplicated(_)
    ) || is_exact_hamming(node)
}

/// An exact `hamming` (`max_distance == 0`) is a point filter: it matches only
/// the docs whose hash is bit-equal to the query, and [`eval_hamming`] scores
/// such a hit `(64 - 0) / 64 == 1.0` — the same constant every filter conjunct
/// contributes on the AND fast path. So the planner may treat it as a filter
/// (bitmap driver or per-doc predicate) with byte-identical scores, instead of
/// forcing the materialize-and-intersect fallback, where an
/// `and[hamming, match(<hundreds of ngram tokens>)]` evaluates the whole
/// `match` over the corpus to intersect it with one doc (#4246: the 500k
/// durable readback timed out on exactly that shape). A fuzzy hamming
/// (`max_distance > 0`) scores by distance and stays on the fallback, where it
/// keeps its graded score.
///
/// [`eval_hamming`]: crate::index::domain::query::knn::eval_hamming
pub(crate) fn is_exact_hamming(node: &QueryNode) -> bool {
    matches!(node, QueryNode::Hamming(h) if h.max_distance == 0)
}

/// Cheap upper bound on how many docs a positive conjunct matches, WITHOUT
/// materializing it — used to pick the conjunct to drive the AND from. Reads
/// only posting/bucket lengths. Returns `u64::MAX` for shapes we don't drive
/// from (so a leaf is always preferred).
pub(crate) fn estimate_selectivity(coll: &Collection, node: &QueryNode) -> u64 {
    match node {
        // #182: the id list size is a cheap upper bound on matched docs — a
        // small ids clause is a good (selective) driver for the AND.
        QueryNode::Ids(q) => q.values.len() as u64,
        QueryNode::Term(t) => match (coll.fields.get(&t.field), &t.value) {
            (Some(FieldIndex::Keyword(k)), FieldValue::String(s)) => {
                // Phase 2h-1: df from the active source (segment count-prefix
                // when sealed, else live posting length) — keeps rarest-first
                // clause ordering cheap with no posting decode.
                k.term_df(s)
            }
            (Some(FieldIndex::Number(n)), FieldValue::Number(x)) => SortableF64::new(*x)
                .ok()
                // Phase 2h-3: df from the active source (segment count-prefix when
                // sealed, else live posting length) — rarest-first ordering cheap.
                .map(|key| n.value_df(key))
                .unwrap_or(0),
            (Some(FieldIndex::Set(s)), FieldValue::String(el)) => {
                // Phase 2h-2: df from the active source (segment count-prefix when
                // sealed, else live posting length) — rarest-first ordering cheap.
                s.element_df(el)
            }
            _ => u64::MAX,
        },
        QueryNode::Terms(t) => match coll.fields.get(&t.field) {
            Some(FieldIndex::Keyword(k)) => t
                .values
                .iter()
                .map(|v| match v {
                    FieldValue::String(s) => k.term_df(s),
                    _ => 0,
                })
                .sum(),
            Some(FieldIndex::Set(s)) => t
                .values
                .iter()
                .map(|v| match v {
                    FieldValue::String(el) => s.element_df(el),
                    _ => 0,
                })
                .sum(),
            _ => u64::MAX,
        },
        QueryNode::Prefix(_) => u64::MAX,
        QueryNode::Range(r) => match coll.fields.get(&r.field) {
            // Phase 2h-3: range df from the active source (segment count-prefix
            // sum when sealed, else the live range posting-length sum).
            Some(FieldIndex::Number(n)) => match range_bounds(r) {
                Ok((lo, hi)) => n.range_df(lo, hi),
                Err(_) => u64::MAX,
            },
            _ => u64::MAX,
        },
        QueryNode::Match(m) => match coll.fields.get(&m.field) {
            Some(FieldIndex::Text { analyzer, idx }) => {
                let toks = tokenize::tokenize(&m.text, *analyzer);
                // df from the active source: the sealed segment's stored posting
                // length when attached (Phase 2e-B), else the live posting df.
                let dfs = toks.iter().map(|t| idx.tok_df(t).map(|d| d as u64));
                match m.op {
                    // AND: a doc needs every token, so ≤ the rarest token's df.
                    MatchOp::And => dfs.map(|d| d.unwrap_or(0)).min().unwrap_or(0),
                    // OR: ≤ the sum of dfs.
                    MatchOp::Or => dfs.map(|d| d.unwrap_or(0)).sum(),
                }
            }
            _ => u64::MAX,
        },
        // An exact hamming is a 64-bit point lookup (see `is_exact_hamming`):
        // its expected df is 1, so it drives the AND — one linear hash scan to
        // a tiny candidate set — rather than a wide `match` sibling whose
        // rarest-token df is the corpus. A prior, not a measured count.
        QueryNode::Hamming(h) if h.max_distance == 0 => 1,
        // Don't drive an AND from these. Exists/Duplicated would need a full
        // value-union scan to size, so they're not chosen as the cheap driver
        // either — a cheaper sibling clause (term/range) drives, then they filter.
        QueryNode::Knn(_)
        | QueryNode::And(_)
        | QueryNode::Or(_)
        | QueryNode::Not(_)
        | QueryNode::HasChild(_)
        | QueryNode::Hamming(_)
        | QueryNode::Rrf(_)
        | QueryNode::Exists(_)
        | QueryNode::Duplicated(_) => u64::MAX,
    }
}

/// Largest filter candidate set the `and[filter, match]` planner resolves
/// sparsely (#4246). Below it every match token is resolved through
/// [`TextIndex::tok_postings_at`] — one streamed pass per distinct token —
/// instead of materializing each token's full posting; above it the posting
/// is reused across enough candidates to be worth holding.
///
/// [`TextIndex::tok_postings_at`]: crate::index::domain::text_index::TextIndex::tok_postings_at
pub(crate) const SPARSE_CANDIDATE_MAX: u64 = 64;

pub(crate) enum FilterCandidatePlan {
    Ready(RoaringBitmap),
    // Keep bitmap construction lazy so the keyword/range top-k fast path can
    // still return a page without first materializing the whole filter set.
    Deferred,
}

impl FilterCandidatePlan {
    pub(crate) fn resolve(
        self,
        coll: &Collection,
        filters: &[&QueryNode],
        filter_nots: &[&QueryNode],
    ) -> Result<RoaringBitmap> {
        match self {
            Self::Ready(cand) => Ok(cand),
            Self::Deferred => eval_filter_bitmap_conjunction(coll, filters, filter_nots),
        }
    }
}

/// Choose the filter driver without materializing wide text postings merely
/// to estimate their size. A filter estimated to match at most one row is
/// evaluated first; if its ACTUAL set fits the sparse resolver, no text
/// estimate is needed. A nonempty text match cannot have a smaller estimate.
/// Exact Hamming's estimate is only a prior, so hash collisions must pass this
/// cardinality check too. All other cases retain the original estimates and
/// rarest-positive planning, including its score accumulation order.
pub(crate) fn plan_filter_candidates(
    coll: &Collection,
    filters: &[&QueryNode],
    filter_nots: &[&QueryNode],
    matches: &[&QueryNode],
) -> Result<Option<FilterCandidatePlan>> {
    let Some(filter_sel) = filters.iter().map(|c| estimate_selectivity(coll, c)).min() else {
        return Ok(None);
    };
    let mut candidates = None;
    if filter_sel <= 1 {
        let cand = eval_filter_bitmap_conjunction(coll, filters, filter_nots)?;
        if cand.len() <= SPARSE_CANDIDATE_MAX {
            return Ok(Some(FilterCandidatePlan::Ready(cand)));
        }
        candidates = Some(cand);
    }
    let match_sel = matches.iter().map(|c| estimate_selectivity(coll, c)).min();
    if match_sel.is_some_and(|sel| sel < filter_sel) {
        return Ok(None);
    }
    Ok(Some(match candidates {
        Some(cand) => FilterCandidatePlan::Ready(cand),
        None => FilterCandidatePlan::Deferred,
    }))
}
