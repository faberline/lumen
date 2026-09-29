//! Scatter one search across shard engines and merge the shard pages into one
//! globally ranked page with a global cursor.

use std::cmp::Ordering;
use std::time::Instant;

use anyhow::Result;
use rayon::prelude::*;

use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::{
    query::SortOrder,
    search::{SearchHit, SearchRequest, SearchResponse},
};

/// Query sealed/local shards in parallel and merge the top page into the same
/// response shape as a single-engine search.
///
/// `sort_value` resolves numeric sort keys for returned hits. It exists because
/// [`SearchHit`] intentionally carries only `(external_id, score)` today; a
/// production sharded router can resolve values from shard-local metadata, while
/// the scale bench derives deterministic corpus values without widening the
/// public response type.
pub fn search_shards_parallel<S, F, K>(
    collection_id: &str,
    req: SearchRequest,
    shards: &[S],
    search: F,
    sort_value: K,
) -> Result<SearchResponse>
where
    S: Sync,
    F: Fn(&S, &str, SearchRequest) -> Result<SearchResponse> + Sync,
    K: Fn(&SearchHit, &str) -> Option<f64> + Sync,
{
    let start = Instant::now();
    let offset = search_request_offset(&req)?;
    let limit = req.limit as usize;
    let mut shard_req = req.clone();
    shard_req.offset = 0;
    shard_req.cursor = None;
    shard_req.limit = offset.saturating_add(limit).min(u32::MAX as usize) as u32;

    let shard_results: Vec<_> = shards
        .par_iter()
        .map(|shard| search(shard, collection_id, shard_req.clone()))
        .collect();

    let mut responses = Vec::with_capacity(shard_results.len());
    for result in shard_results {
        responses.push(result?);
    }

    Ok(merge_shard_search_responses(
        &req,
        responses,
        start.elapsed().as_micros() as u64,
        sort_value,
    ))
}

pub fn merge_shard_search_responses<K>(
    req: &SearchRequest,
    responses: impl IntoIterator<Item = SearchResponse>,
    took_us: u64,
    sort_value: K,
) -> SearchResponse
where
    K: Fn(&SearchHit, &str) -> Option<f64>,
{
    let offset = req
        .cursor
        .as_deref()
        .and_then(parse_cursor)
        .map(|offset| offset as usize)
        .unwrap_or_else(|| usize::try_from(req.offset).unwrap_or(usize::MAX));
    let limit = req.limit as usize;
    let mut hits = Vec::new();
    let mut total = 0u64;
    for resp in responses {
        total += resp.total;
        hits.extend(resp.hits);
    }

    if let Some(sort) = &req.sort {
        hits.sort_by(|a, b| {
            for spec in sort {
                let ord = match (sort_value(a, &spec.field), sort_value(b, &spec.field)) {
                    (Some(av), Some(bv)) => av.partial_cmp(&bv).unwrap_or(Ordering::Equal),
                    _ => Ordering::Equal,
                };
                let ord = match spec.order {
                    SortOrder::Asc => ord,
                    SortOrder::Desc => ord.reverse(),
                };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            a.external_id.cmp(&b.external_id)
        });
    } else {
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.external_id.cmp(&b.external_id))
        });
    }

    let page: Vec<_> = hits.into_iter().skip(offset).take(limit).collect();
    let next_offset = offset + page.len();
    let cursor = if (next_offset as u64) < total {
        Some(make_cursor(next_offset))
    } else {
        None
    };

    SearchResponse {
        hits: page,
        total,
        cursor,
        took_ms: took_us / 1000,
        took_us,
    }
}

fn make_cursor(offset: usize) -> String {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
    STANDARD_NO_PAD.encode(format!("{{\"offset\":{offset}}}"))
}

fn parse_cursor(s: &str) -> Option<u64> {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
    let raw = STANDARD_NO_PAD.decode(s).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    v.get("offset")?.as_u64()
}

pub(crate) fn search_request_offset(req: &SearchRequest) -> Result<usize> {
    if req.offset != 0 && req.cursor.is_some() {
        return Err(
            StorageError::InvalidPagination("offset and cursor cannot be combined".into()).into(),
        );
    }
    match req.cursor.as_deref().and_then(parse_cursor) {
        Some(offset) => usize::try_from(offset).map_err(|_| {
            StorageError::InvalidPagination(
                "cursor offset does not fit this server platform".into(),
            )
            .into()
        }),
        None => usize::try_from(req.offset).map_err(|_| {
            StorageError::InvalidPagination("offset does not fit this server platform".into())
                .into()
        }),
    }
}

#[cfg(test)]
mod tests;
