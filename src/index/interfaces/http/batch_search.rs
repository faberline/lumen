//! msearch-style batch search over several collections, with its per-item
//! authorization errors.

use anyhow::Result;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use futures::StreamExt;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{
    batch_search_storage_error, enforce_read_consistency, read_consistency_from, ApiErr, AppState,
};
use crate::index::interfaces::http::search::run_local_search;
use crate::index::interfaces::http::AUTHORIZATION_CONCURRENCY;
use crate::shared_kernel::types::search::{
    BatchSearchRequest, BatchSearchResponse, BatchSearchResult, MAX_BATCH_SEARCH_SIZE,
};

#[cfg(doc)]
use crate::index::interfaces::http::query_method::collections_query_dispatch;

/// msearch-style batch search: N independent `(collection, SearchRequest)`
/// items in one HTTP request, fanned out concurrently. `collections:search`
/// is one literal path segment (AIP-136 custom-method syntax), so it
/// registers directly in axum next to `/collections` and
/// `/collections/{collection_id}` without any capture ambiguity.
///
/// One item failing (e.g. an unknown collection) never fails the batch —
/// the batch-level status stays 200 and that item's [`BatchSearchResult`]
/// carries the error. Only a malformed body or an over-limit batch returns
/// 400. Cursors, sort, and collapse all stay per-item: there is no merged
/// cursor and no cross-collection score merging.
#[utoipa::path(
    post,
    path = "/collections:search",
    tag = "Query",
    request_body = BatchSearchRequest,
    responses(
        (status = 200, description = "Per-item results, same order and length as `searches`", body = BatchSearchResponse),
        (status = 400, description = "Malformed body or batch size over the limit", body = ApiError)
    )
)]
pub(crate) async fn batch_search(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Json(req): Json<BatchSearchRequest>,
) -> Result<Json<BatchSearchResponse>, ApiErr> {
    Ok(Json(batch_search_core(&state, &auth, &headers, req).await?))
}

/// Shared implementation behind `POST /collections:search` and its `QUERY
/// /collections` twin ([`collections_query_dispatch`], epic #1296 R1: every
/// QUERY endpoint keeps a POST twin — same handler, identical response).
pub(super) async fn batch_search_core(
    state: &AppState,
    auth: &AuthContext,
    headers: &HeaderMap,
    req: BatchSearchRequest,
) -> Result<BatchSearchResponse, ApiErr> {
    if req.searches.len() > MAX_BATCH_SEARCH_SIZE {
        return Err(ApiErr::new(
            StatusCode::BAD_REQUEST,
            "batch_too_large",
            format!(
                "batch has {} items, max is {MAX_BATCH_SEARCH_SIZE}",
                req.searches.len()
            ),
        ));
    }
    let consistency = read_consistency_from(headers);
    enforce_read_consistency(state, consistency)?;
    // `buffered`, not `buffer_unordered`: the response contract is that
    // `results` matches `searches` in order and length, and each item's
    // authorization now costs a SubjectAccessReview, so the bound matters as
    // much as the concurrency does.
    let results: Vec<BatchSearchResult> = futures::stream::iter(req.searches)
        .map(|item| {
            let state = state.clone();
            let auth = auth.clone();
            let headers = headers.clone();
            async move {
                if let Err(e) = auth.ensure(&item.collection, Role::Read).await {
                    return batch_search_auth_error(e);
                }
                let result = if let Some(router) = &state.routed {
                    router
                        .search(&item.collection, item.request, &headers)
                        .await
                } else {
                    run_local_search(&state, &item.collection, item.request).await
                };
                match result {
                    Ok(response) => BatchSearchResult::Ok { response },
                    Err(e) => batch_search_storage_error(e),
                }
            }
        })
        .buffered(AUTHORIZATION_CONCURRENCY)
        .collect()
        .await;
    Ok(BatchSearchResponse { results })
}

/// Classify one batch item's auth rejection into a
/// [`BatchSearchResult::Error`].
///
/// The per-item envelope reuses [`AuthErr::wire`], so a batch item and a
/// single-collection request report a denial — or an unanswered
/// SubjectAccessReview — with the same code and the same wording. One item's
/// failure never fails the batch: the caller may legitimately hold read on
/// some of the collections it asked about and not others.
fn batch_search_auth_error(
    e: crate::access::application::authorization::AuthErr,
) -> BatchSearchResult {
    let (_, code, message) = e.wire();
    BatchSearchResult::Error {
        code: code.to_string(),
        message,
    }
}
