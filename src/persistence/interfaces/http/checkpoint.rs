//! The on-demand durable checkpoint (#1389) and the planned-restart HNSW cache
//! seal, with the stable error envelopes a refused seal answers with.

use anyhow::Result;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Serialize;

use crate::access::application::authorization::{AuthContext, Role};
use crate::app::http::{api_err::ApiErr, app_state::AppState, guards::enforce_storage_writable};
use crate::ingest::application::write_coordinator::errors::{RestartRequired, StorageFullError};
use crate::persistence::application::ports::checkpoint_sink::{
    HnswCacheSealInvalidated, HnswCacheSealUnavailable,
};

#[cfg(doc)]
use crate::persistence::application::ports::checkpoint_sink::CheckpointSink;

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct HnswCacheSealMutationStamp {
    epoch: u64,
    apply_revision: u64,
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct HnswCacheSealResponse {
    sealed: bool,
    cache_fields: usize,
    durability: &'static str,
    mutation_stamp: HnswCacheSealMutationStamp,
}

/// `POST /admin/checkpoint` (#1389): force a synchronous durability
/// checkpoint of the live engine state and return only once it is committed.
/// The reshard driver's cutover calls this on every shard it just migrated
/// data into or evicted data from, so `/admin/reshard:apply`/`:evict`'s
/// mutations — which bypass `WriteCoordinator`/the AOF — reach durability
/// before the driver triggers the cutover rolling restart, instead of
/// depending on the next periodic `LUMEN_SNAPSHOT_SECS` tick. `persisted:
/// false` means no durable store is configured on this node (nothing to
/// lose on restart, e.g. dev mode); a production/operator deployment with
/// segment persistence configured always reports `true` on success. See
/// [`CheckpointSink`].
#[utoipa::path(
    post,
    path = "/admin/checkpoint",
    tag = "Admin",
    responses(
        (status = 200, description = "Checkpoint committed (or vacuously satisfied if no durable store is configured)", body = serde_json::Value),
        (status = 400, description = "Checkpoint write failed", body = ApiError)
    )
)]
pub(crate) async fn admin_checkpoint(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    // The concrete checkpoint sink acquires the shared checkpoint permit. An
    // API-level permit here can deadlock behind a queued exclusive restore.
    enforce_storage_writable(&state)?;
    let persisted = state
        .checkpoint
        .checkpoint_now()
        .await
        .map_err(ApiErr::from)?;
    tracing::info!(
        target: "lumen.audit",
        event = "admin_checkpoint",
        subject = auth.subject().unwrap_or("anonymous"),
        persisted,
    );
    Ok(Json(serde_json::json!({ "persisted": persisted })))
}

/// `POST /admin/restart:seal-hnsw-cache`: publish the optional HNSW graph
/// bytes that a planned restart may use after it replays the authoritative
/// checkpoint and AOF. It takes no request body. The response is deliberately
/// strict because the performance harness treats any missing field as a failed
/// restart preparation rather than guessing that a cache is usable.
#[utoipa::path(
    post,
    path = "/admin/restart:seal-hnsw-cache",
    tag = "Admin",
    responses(
        (status = 200, description = "HNSW graph cache sealed at a durable mutation boundary", body = HnswCacheSealResponse),
        (status = 401, description = "Authentication required", body = ApiError),
        (status = 403, description = "Missing admin role", body = ApiError),
        (status = 409, description = "No current HNSW graph is sealable or the graph changed during sealing", body = ApiError),
        (status = 500, description = "HNSW cache sealing failed", body = ApiError),
        (status = 503, description = "Restart required", body = ApiError),
        (status = 507, description = "Node storage is full", body = ApiError)
    )
)]
pub(crate) async fn admin_restart_seal_hnsw_cache(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<HnswCacheSealResponse>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    enforce_storage_writable(&state)?;
    let receipt = state
        .checkpoint
        .seal_hnsw_graph_cache()
        .await
        .map_err(hnsw_cache_seal_api_error)?;
    tracing::info!(
        target: "lumen.audit",
        event = "admin_restart_seal_hnsw_cache",
        subject = auth.subject().unwrap_or("anonymous"),
        cache_fields = receipt.cache_fields,
        durability = receipt.durability.as_str(),
        mutation_epoch = receipt.mutation_epoch,
        mutation_apply_revision = receipt.mutation_apply_revision,
    );
    Ok(Json(HnswCacheSealResponse {
        sealed: true,
        cache_fields: receipt.cache_fields,
        durability: receipt.durability.as_str(),
        mutation_stamp: HnswCacheSealMutationStamp {
            epoch: receipt.mutation_epoch,
            apply_revision: receipt.mutation_apply_revision,
        },
    }))
}

fn hnsw_cache_seal_api_error(error: anyhow::Error) -> ApiErr {
    if error.downcast_ref::<HnswCacheSealUnavailable>().is_some() {
        return ApiErr::new(
            StatusCode::CONFLICT,
            "planned_restart_cache_unavailable",
            error.to_string(),
        );
    }
    if error.downcast_ref::<HnswCacheSealInvalidated>().is_some() {
        return ApiErr::new(
            StatusCode::CONFLICT,
            "planned_restart_cache_invalidated",
            error.to_string(),
        );
    }
    if error.downcast_ref::<RestartRequired>().is_some() {
        return ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "restart_required",
            error.to_string(),
        );
    }
    if crate::ingest::application::write_coordinator::errors::is_storage_full(&error)
        || error.downcast_ref::<StorageFullError>().is_some()
    {
        return ApiErr::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "storage_full",
            error.to_string(),
        );
    }
    ApiErr::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "planned_restart_cache_failed",
        error.to_string(),
    )
}

#[cfg(test)]
mod tests;
