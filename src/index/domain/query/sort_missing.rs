//! A keyword sort with missing values placed first or last: the query evaluated
//! once to a bitmap, then the keyword's posting buckets streamed in field
//! order, with each bucket's selection bounded by the page.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeSet, BinaryHeap};

use anyhow::Result;
use roaring::RoaringBitmap;

use crate::index::domain::collection::Collection;
use crate::index::domain::engine_state::EngineState;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::index::domain::keyword_index::KeywordBucketWalk;
use crate::index::domain::query::clause::eval_filter_bitmap;
use crate::index::domain::query::eval::eval_query;
use crate::index::domain::query::predicate::query_has_has_child;
use crate::index::domain::query::query_needs_universe;
use crate::index::domain::query::sort::{compare_sort_value, SortValue};
use crate::index::domain::query::terms::field_presence_bitmap;
use crate::shared_kernel::types::query::{QueryNode, SortMissing, SortOrder, SortSpec};
use crate::shared_kernel::types::search::{SearchHit, SearchRequest};
#[cfg(test)]
use crate::storage::MATERIALIZED_SORT_COMPARISONS;

/// One normalized field-sort component for the bounded missing-aware heap.
/// Its ordering is the public sort ordering: smaller is a better result. A
/// `BinaryHeap` then exposes the worst retained result at `peek()`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MissingHeapKey {
    MissingFirst,
    PresentAsc(SortValue),
    PresentDesc(SortValue),
    MissingLast,
}

impl MissingHeapKey {
    fn rank(&self) -> u8 {
        match self {
            Self::MissingFirst => 0,
            Self::PresentAsc(_) | Self::PresentDesc(_) => 1,
            Self::MissingLast => 2,
        }
    }
}

impl Ord for MissingHeapKey {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        let rank = self.rank().cmp(&other.rank());
        if rank != CmpOrdering::Equal {
            return rank;
        }
        match (self, other) {
            (Self::PresentAsc(left), Self::PresentAsc(right)) => compare_sort_value(left, right),
            (Self::PresentDesc(left), Self::PresentDesc(right)) => {
                compare_sort_value(left, right).reverse()
            }
            // Every candidate uses the same sort specs, so equal-position
            // present keys always have the same direction. Keep this stable if
            // a malformed internal caller violates that invariant.
            (Self::PresentAsc(_), Self::PresentDesc(_)) => CmpOrdering::Less,
            (Self::PresentDesc(_), Self::PresentAsc(_)) => CmpOrdering::Greater,
            _ => CmpOrdering::Equal,
        }
    }
}

impl PartialOrd for MissingHeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MissingSortHeapCandidate {
    pub(crate) id: u32,
    pub(crate) tuple: Vec<Option<SortValue>>,
    pub(crate) keys: Vec<MissingHeapKey>,
    pub(crate) external_id: String,
}

impl Ord for MissingSortHeapCandidate {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        #[cfg(test)]
        MATERIALIZED_SORT_COMPARISONS.with(|comparisons| {
            comparisons.set(comparisons.get().saturating_add(1));
        });
        self.keys
            .cmp(&other.keys)
            .then_with(|| self.external_id.cmp(&other.external_id))
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for MissingSortHeapCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

pub(crate) fn missing_heap_keys(
    tuple: &[Option<SortValue>],
    sort: &[SortSpec],
) -> Vec<MissingHeapKey> {
    tuple
        .iter()
        .zip(sort)
        .map(|(value, spec)| match value {
            Some(value) if matches!(spec.order, SortOrder::Asc) => {
                MissingHeapKey::PresentAsc(value.clone())
            }
            Some(value) => MissingHeapKey::PresentDesc(value.clone()),
            None if matches!(spec.missing, SortMissing::First) => MissingHeapKey::MissingFirst,
            None => MissingHeapKey::MissingLast,
        })
        .collect()
}

/// Retains the lexically smallest external ids from one equal-key posting
/// bucket. The heap is bounded by `wanted`, which matters for low-cardinality
/// keyword values such as booleans: sorting that whole bucket would recreate
/// the all-match latency cliff for a small requested page.
fn select_docids_by_external_id<I>(docids: I, wanted: usize, interner: &Interner) -> Vec<u32>
where
    I: Iterator<Item = u32>,
{
    if wanted == 0 {
        return Vec::new();
    }
    let mut heap: BinaryHeap<(String, u32)> = BinaryHeap::with_capacity(wanted.min(1024));
    for id in docids {
        let candidate = (interner.resolve(id).to_string(), id);
        if heap.len() < wanted {
            heap.push(candidate);
            continue;
        }
        let replace = heap
            .peek()
            .is_some_and(|largest| candidate.cmp(largest) == CmpOrdering::Less);
        if replace {
            heap.pop();
            heap.push(candidate);
        }
    }
    let mut selected = heap.into_vec();
    selected.sort_unstable();
    selected.into_iter().map(|(_, id)| id).collect()
}

/// Bitmap-only query shapes. Their result score is the same constant `1.0`
/// that `eval_query` would assign, so the missing-keyword sort planner can
/// avoid building its score HashMap. Complex/scored/boolean shapes deliberately
/// return `None` and keep the general evaluator as the exact fallback.
fn constant_filter_bitmap_for_missing_sort(
    coll: &Collection,
    query: &QueryNode,
) -> Result<Option<RoaringBitmap>> {
    match query {
        QueryNode::Term(_)
        | QueryNode::Terms(_)
        | QueryNode::Prefix(_)
        | QueryNode::Ids(_)
        | QueryNode::Range(_)
        | QueryNode::Exists(_)
        | QueryNode::Duplicated(_) => Ok(Some(eval_filter_bitmap(coll, query)?)),
        _ => Ok(None),
    }
}

/// Plans one keyword field with `missing:first|last`. It evaluates the query
/// once into a bitmap, then streams keyword posting buckets in field order.
/// For `missing:last`, a normal page stops once `offset + limit` rows are
/// selected; it never comparison-sorts the high-cardinality tail. For
/// `missing:first`, discovering the leading missing group necessarily scans
/// matches, but selection inside that group remains bounded by page size.
///
pub(crate) fn try_missing_keyword_bitmap_plan(
    coll: &Collection,
    collection_id: &str,
    req: &SearchRequest,
    offset: usize,
    sort: &[SortSpec],
    state: &EngineState,
) -> Result<Option<(Vec<SearchHit>, u64)>> {
    let [spec] = sort else {
        return Ok(None);
    };
    if !matches!(spec.missing, SortMissing::First | SortMissing::Last) {
        return Ok(None);
    }
    if query_has_has_child(&req.query) {
        return Ok(None);
    }
    let Some(FieldIndex::Keyword(keyword)) = coll.fields.get(&spec.field) else {
        return Ok(None);
    };

    let matched = match constant_filter_bitmap_for_missing_sort(coll, &req.query)? {
        Some(bitmap) => bitmap,
        None => {
            let universe: BTreeSet<u32> = if query_needs_universe(&req.query) {
                coll.eid_fields.keys().copied().collect()
            } else {
                BTreeSet::new()
            };
            eval_query(coll, collection_id, &req.query, &universe, state)?
                .keys()
                .copied()
                .collect()
        }
    };
    let total = matched.len() as u64;
    let wanted = offset.saturating_add(req.limit as usize);
    let interner = &coll.interner;
    // `eid_fields` is the authoritative field-presence census. Derive both
    // groups with bitmap operations instead of probing `keyword_at` for every
    // match, which would turn a high-cardinality page into random segment reads.
    let field_presence = field_presence_bitmap(coll, &spec.field);
    let mut present = matched.clone();
    present &= &field_presence;
    let mut missing = matched.clone();
    missing -= &field_presence;
    let mut ordered: Vec<(u32, bool)> = Vec::with_capacity(wanted.min(1024));

    if spec.missing == SortMissing::First {
        ordered.extend(
            select_docids_by_external_id(missing.iter(), wanted, interner)
                .into_iter()
                .map(|id| (id, false)),
        );
        if missing.len() as usize >= wanted {
            let hits = ordered
                .into_iter()
                .skip(offset)
                .take(req.limit as usize)
                .map(|(id, present)| SearchHit {
                    external_id: interner.resolve(id).to_string(),
                    score: if present { 1.0 } else { 0.0 },
                })
                .collect();
            return Ok(Some((hits, total)));
        }
    }

    let walk =
        keyword.visit_sorted_posting_buckets(matches!(spec.order, SortOrder::Desc), |posting| {
            let mut bucket = posting.clone();
            bucket &= &present;
            if bucket.is_empty() {
                return Ok(true);
            }
            let room = wanted.saturating_sub(ordered.len());
            if room == 0 {
                return Ok(false);
            }
            ordered.extend(
                select_docids_by_external_id(bucket.iter(), room, interner)
                    .into_iter()
                    .map(|id| (id, true)),
            );
            Ok(ordered.len() < wanted)
        })?;
    if matches!(walk, KeywordBucketWalk::Unavailable) {
        return Ok(None);
    }

    // `missing:last` needs the absent group only when the field-ordered
    // buckets did not already fill the requested offset+page window. Reaching
    // this point implies the walk completed, so the bitmap subtraction is exact.
    if spec.missing == SortMissing::Last && ordered.len() < wanted {
        if !matches!(walk, KeywordBucketWalk::Completed) {
            return Ok(None);
        }
        let room = wanted.saturating_sub(ordered.len());
        ordered.extend(
            select_docids_by_external_id(missing.iter(), room, interner)
                .into_iter()
                .map(|id| (id, false)),
        );
    }

    let hits = ordered
        .into_iter()
        .skip(offset)
        .take(req.limit as usize)
        .map(|(id, present)| SearchHit {
            external_id: interner.resolve(id).to_string(),
            score: if present { 1.0 } else { 0.0 },
        })
        .collect();
    Ok(Some((hits, total)))
}
