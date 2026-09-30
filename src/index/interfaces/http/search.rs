//! Single-collection search: one collection's search and its exhaustive
//! search:all export, and the local search both run on the blocking executor.

use std::sync::Arc;

use anyhow::Result;
use axum::extract::{Extension, Path, State};
use axum::http::HeaderMap;
use axum::response::Json;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{enforce_read_consistency, read_consistency_from, ApiErr, AppState};
use crate::shared_kernel::types::search::{
    SearchAllRequest, SearchAllResponse, SearchRequest, SearchResponse,
};

#[cfg(doc)]
use crate::index::interfaces::http::query_method::collection_id_query_dispatch;

#[utoipa::path(
    post,
    path = "/collections/{collection_id}/search",
    tag = "Query",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = SearchRequest,
    responses((status = 200, description = "Search hits", body = SearchResponse))
)]
pub(crate) async fn search(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Json(req): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiErr> {
    Ok(Json(
        search_core(&state, &auth, &headers, &collection_id, req).await?,
    ))
}

/// Export every matching external id in one explicit full-materialization
/// request. The local engine evaluates the request while holding one read-lock
/// snapshot; routed deployments collect one independently consistent snapshot
/// per shard and intentionally do not claim a cross-shard transaction.
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/search:all",
    tag = "Query",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = SearchAllRequest,
    responses(
        (status = 200, description = "All matching external ids; materializes the complete result set", body = SearchAllResponse),
        (status = 400, description = "Invalid query or unsupported sort", body = ApiError)
    )
)]
pub(crate) async fn search_all(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Json(req): Json<SearchAllRequest>,
) -> Result<Json<SearchAllResponse>, ApiErr> {
    let response = search_core(
        &state,
        &auth,
        &headers,
        &collection_id,
        SearchRequest {
            query: req.query,
            limit: u32::MAX,
            offset: 0,
            cursor: None,
            routing_key: req.routing_key,
            sort: req.sort,
            track_total: true,
            collapse: None,
        },
    )
    .await?;
    Ok(Json(SearchAllResponse {
        external_ids: response
            .hits
            .into_iter()
            .map(|hit| hit.external_id)
            .collect(),
        total: response.total,
        took_ms: response.took_ms,
        took_us: response.took_us,
    }))
}

/// Shared implementation behind `POST /collections/{collection_id}/search`
/// and its `QUERY /collections/{collection_id}` twin
/// ([`collection_id_query_dispatch`], epic #1296 R1: every QUERY endpoint
/// keeps a POST twin — same handler, identical response). Consults
/// `state.routed` first (#1398 R1) — a routed deployment scatters/forwards
/// by ownership; every other deployment falls through to `search_backend`
/// unchanged.
pub(super) async fn search_core(
    state: &AppState,
    auth: &AuthContext,
    headers: &HeaderMap,
    collection_id: &str,
    req: SearchRequest,
) -> Result<SearchResponse, ApiErr> {
    auth.ensure(collection_id, Role::Read).await?;
    let consistency = read_consistency_from(headers);
    enforce_read_consistency(state, consistency)?;
    if let Some(router) = &state.routed {
        return router
            .search(collection_id, req, headers)
            .await
            .map_err(ApiErr::from);
    }
    run_local_search(state, collection_id, req)
        .await
        .map_err(ApiErr::from)
}

/// Runs a local synchronous backend outside the Tokio reactor. Keep this one
/// helper shared by single-search and batch-search handlers so batch items
/// cannot bypass the readiness-preserving boundary.
pub(super) async fn run_local_search(
    state: &AppState,
    collection_id: &str,
    req: SearchRequest,
) -> Result<SearchResponse> {
    let backend = Arc::clone(&state.search_backend);
    let collection_id = collection_id.to_string();
    state
        .search_executor
        .run(move || backend.search(&collection_id, req))
        .await
}
