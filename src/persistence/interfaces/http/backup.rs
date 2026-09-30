//! Backup and restore: the whole engine's state exported as one JSON snapshot
//! or written to a local directory, and restored from a snapshot.

use anyhow::Result;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{enforce_storage_writable, ApiErr, AppState};
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::persistence::infrastructure::backup_sink::{BackupSink, LocalFsSink};

/// Dump the entire engine state as a single JSON document.
#[utoipa::path(
    get,
    path = "/admin/backup",
    tag = "Admin",
    responses(
        (status = 200, description = "Full engine snapshot as JSON", body = serde_json::Value),
        (status = 403, description = "Missing admin role", body = ApiError)
    )
)]
pub(crate) async fn backup(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<SnapshotV1>, ApiErr> {
    // Cluster-wide admin op: needs admin on wildcard.
    auth.ensure_admin(Role::Admin).await?;
    tracing::info!(
        target: "lumen.audit",
        event = "backup_started",
        subject = auth.subject().unwrap_or("anonymous"),
    );
    Ok(Json(state.engine.snapshot().map_err(ApiErr::from)?))
}

#[derive(Debug, Deserialize)]
pub(crate) struct LocalBackupRequest {
    /// Filesystem path the snapshot will be written into.
    path: String,
    /// Key prefix; the file will be named `{prefix}-{unix_seconds}.json`.
    #[serde(default = "default_backup_prefix")]
    prefix: String,
}

fn default_backup_prefix() -> String {
    "lumen-backup".into()
}

/// Snapshot the engine and persist it via a `LocalFsSink`. Returns the
/// final key the sink chose. The path is created if missing.
#[utoipa::path(
    post,
    path = "/admin/backup/local",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Snapshot written; sink identity and object key", body = serde_json::Value),
        (status = 400, description = "Invalid local sink path", body = ApiError),
        (status = 403, description = "Missing admin role", body = ApiError)
    )
)]
pub(crate) async fn backup_to_local(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<LocalBackupRequest>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    let snap = state.engine.snapshot().map_err(ApiErr::from)?;
    let payload = serde_json::to_vec(&snap)
        .map_err(|e| ApiErr::new(StatusCode::INTERNAL_SERVER_ERROR, "encode", e.to_string()))?;
    let sink = LocalFsSink::new(&req.path, &req.prefix)
        .map_err(|e| ApiErr::new(StatusCode::BAD_REQUEST, "bad_sink", e.to_string()))?;
    let key = sink
        .put(std::time::SystemTime::now(), &payload)
        .map_err(|e| ApiErr::new(StatusCode::INTERNAL_SERVER_ERROR, "sink_put", e.to_string()))?;
    tracing::info!(
        target: "lumen.audit",
        event = "backup_local",
        subject = auth.subject().unwrap_or("anonymous"),
        sink = %sink.identity(),
        key = %key,
        bytes = payload.len(),
    );
    Ok(Json(serde_json::json!({
        "sink": sink.identity(),
        "key": key,
        "bytes": payload.len(),
    })))
}

/// Restore the engine from a snapshot dump produced by `/admin/backup`.
/// Replaces all existing state.
#[utoipa::path(
    post,
    path = "/admin/restore",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 204, description = "Engine state replaced from the snapshot"),
        (status = 403, description = "Missing admin role", body = ApiError),
        (status = 500, description = "Restore failed", body = ApiError),
        (status = 503, description = "Restore temporarily unavailable", body = ApiError),
        (status = 422, description = "Malformed or incompatible snapshot", body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn restore(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(snap): Json<SnapshotV1>,
) -> Result<StatusCode, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    enforce_storage_writable(&state)?;
    state
        .restore_sink
        .restore(snap)
        .await
        .map_err(ApiErr::from)?;
    tracing::info!(
        target: "lumen.audit",
        event = "restore_applied",
        subject = auth.subject().unwrap_or("anonymous"),
    );
    Ok(StatusCode::NO_CONTENT)
}
