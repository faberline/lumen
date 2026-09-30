//! Engine::search end to end: term, match and range queries, the result cache,
//! the Exists and Duplicated leaves, the scoring order, query evaluation and
//! validation, and sorted and keyset pagination.

use crate::shared_kernel::types::search::SearchHit;

fn score_of(hits: &[SearchHit], eid: &str) -> f32 {
    hits.iter()
        .find(|h| h.external_id == eid)
        .unwrap_or_else(|| panic!("eid {eid} not in hits"))
        .score
}

mod eval;
mod exists_duplicated;
mod pagination;
mod queries;
mod range;
mod scoring;
