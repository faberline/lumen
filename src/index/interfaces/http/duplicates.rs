//! Duplicate detection within one collection, served from the local shard.

use anyhow::Result;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;

use crate::access::application::authorization::{AuthContext, Role};
use crate::app::http::{api_err::ApiErr, app_state::AppState, guards::read_consistency_from};
use crate::shared_kernel::types::search::{DuplicatesRequest, DuplicatesResponse};

#[utoipa::path(
    post,
    path = "/collections/{collection_id}/duplicates",
    tag = "Query",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = DuplicatesRequest,
    responses((status = 200, description = "Duplicate groups", body = DuplicatesResponse))
)]
/// Local-shard only, deliberately not wired to `state.routed` (#1398 known
/// gap, #1442 R6): `Engine::duplicates` filters by `min_group_size` *before*
/// any cross-shard merge could happen, so scatter-then-merge would silently
/// miss a true cross-shard group (e.g. one copy per shard under
/// `min_group_size: 2`) — a correctness regression, not a routing gap. A
/// correct cross-shard implementation needs unfiltered per-shard candidate
/// groups from `storage.rs`, out of scope here. #1442 R6 closes the "silent
/// wrong answer" gap this left in routed multi-shard mode: rather than
/// unchanged pre-#1398 behavior (silently answering from local-shard data
/// only, missing cross-shard duplicate groups with no indication), a routed
/// deployment now rejects with a distinct, non-retryable error so a caller
/// can tell "not supported here" from "no duplicates found".
pub(crate) async fn duplicates(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Json(req): Json<DuplicatesRequest>,
) -> Result<Json<DuplicatesResponse>, ApiErr> {
    auth.ensure(&collection_id, Role::Read).await?;
    let _consistency = read_consistency_from(&headers);
    if state.routed.is_some() {
        return Err(ApiErr::new(
            StatusCode::NOT_IMPLEMENTED,
            "duplicates_not_routed",
            "duplicate detection is local-shard-only and does not merge across shards; not \
             supported in routed multi-shard mode (#1442 R6)"
                .to_string(),
        ));
    }
    Ok(Json(
        state
            .engine
            .duplicates(&collection_id, req)
            .map_err(ApiErr::from)?,
    ))
}
