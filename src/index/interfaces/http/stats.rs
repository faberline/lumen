//! One collection's stats, read on the blocking executor.

use std::sync::Arc;

use anyhow::Result;
use axum::extract::{Extension, Path, State};
use axum::response::Json;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{ApiErr, AppState};
use crate::shared_kernel::types::stats::StatsResponse;

#[utoipa::path(
    get,
    path = "/collections/{collection_id}/stats",
    tag = "Query",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    responses((status = 200, description = "Collection stats", body = StatsResponse))
)]
pub(crate) async fn stats(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(collection_id): Path<String>,
) -> Result<Json<StatsResponse>, ApiErr> {
    auth.ensure(&collection_id, Role::Read).await?;
    // `Engine::stats` holds the state read lock and walks every live term of
    // every field, including one checkpoint interval of un-absorbed staged
    // Text rows (#4246). That is CPU-bound Engine work, so it goes through the
    // same bounded blocking bridge the search handlers use rather than
    // stalling the reactor worker that also serves `/healthz` (see the rule
    // above `BlockingSearchExecutor`).
    let engine = Arc::clone(&state.engine);
    let requested = collection_id.clone();
    Ok(Json(
        state
            .search_executor
            .run(move || engine.stats(&requested))
            .await
            .map_err(ApiErr::from)?,
    ))
}

#[cfg(test)]
mod tests;
