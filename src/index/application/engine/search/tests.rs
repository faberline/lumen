//! Engine::search end to end: term, match and range queries, the result cache,
//! the Exists and Duplicated leaves, the scoring order, query evaluation and
//! validation, sorted and keyset pagination, the offset cursor a sort rejects,
//! and a has_child query under a sort.

use crate::shared_kernel::types::search::SearchHit;

fn score_of(hits: &[SearchHit], eid: &str) -> f32 {
    hits.iter()
        .find(|h| h.external_id == eid)
        .unwrap_or_else(|| panic!("eid {eid} not in hits"))
        .score
}

mod eval;
mod exists_duplicated;
mod has_child_sort;
mod offset_sort_guard;
mod pagination;
mod queries;
mod range;
mod scoring;
