//! The collection lifecycle handlers: list, create and drop a collection, and
//! drop one of its fields.

use anyhow::Result;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use futures::{StreamExt, TryStreamExt};
use serde::Deserialize;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{enforce_storage_writable, ApiErr, AppState};
use crate::index::application::engine::collections::DropOutcome;
use crate::index::interfaces::http::AUTHORIZATION_CONCURRENCY;
use crate::shared_kernel::types::schema::{CreateCollectionRequest, CreateCollectionResponse};

#[utoipa::path(
    get,
    path = "/collections",
    tag = "Collections",
    responses((status = 200, description = "List collection IDs", body = [String]))
)]
pub(crate) async fn list_collections(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
) -> Result<Json<Vec<String>>, ApiErr> {
    let all = state.engine.list_collections().map_err(ApiErr::from)?;
    // Filter to what the caller can actually read. Each id is its own
    // SubjectAccessReview, so the checks go out concurrently — but bounded, or
    // one list request against a fleet with thousands of collections becomes
    // thousands of simultaneous apiserver calls.
    //
    // A denial removes the collection from the listing. An *unanswered* check
    // does not: silently dropping it would tell the caller the collection does
    // not exist on the strength of an apiserver outage, and a wrong listing is
    // harder to notice than a 503.
    let visible: Vec<Option<String>> = futures::stream::iter(all)
        .map(|id| {
            let auth = &auth;
            async move {
                match auth.ensure(&id, Role::Read).await {
                    Ok(()) => Ok(Some(id)),
                    Err(crate::access::application::authorization::AuthErr::Forbidden {
                        ..
                    }) => Ok(None),
                    Err(e) => Err(ApiErr::from(e)),
                }
            }
        })
        .buffered(AUTHORIZATION_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(Json(visible.into_iter().flatten().collect()))
}

#[utoipa::path(
    put,
    path = "/collections/{collection_id}",
    tag = "Collections",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = CreateCollectionRequest,
    responses(
        (status = 200, description = "Collection created", body = CreateCollectionResponse),
        (status = 400, description = "Invalid schema",     body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn create_collection(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Json(req): Json<CreateCollectionRequest>,
) -> Result<Json<CreateCollectionResponse>, ApiErr> {
    auth.ensure(&collection_id, Role::Admin).await?;
    enforce_storage_writable(&state)?;
    // #2496: fan create_collection out across every physical shard when
    // routed — a collection created against only one shard left every other
    // shard unable to serve a write that hashed there, matching the
    // `index`/`delete_external_id`/`replace_docs` routed-or-local pattern.
    let resp = if let Some(router) = &state.routed {
        router
            .create_collection(collection_id.clone(), req, &headers)
            .await
            .map_err(ApiErr::from)?
    } else {
        state
            .write_backend
            .create_collection(collection_id.clone(), req)
            .await
            .map_err(ApiErr::from)?
    };
    tracing::info!(
        target: "lumen.audit",
        event = "collection_create_or_extend",
        subject = auth.subject().unwrap_or("anonymous"),
        collection_id = %collection_id,
        version = resp.version,
        fields = resp.fields_count,
    );
    Ok(Json(resp))
}

#[derive(Debug, Deserialize)]
pub(crate) struct DropQuery {
    #[serde(default)]
    force: bool,
}

#[utoipa::path(
    delete,
    path = "/collections/{collection_id}",
    tag = "Collections",
    params(
        ("collection_id" = String, Path, description = "Collection namespace"),
        ("force" = Option<bool>, Query, description = "Skip the soft-delete grace window")
    ),
    responses(
        (status = 202, description = "Soft-deleted (grace window)"),
        (status = 204, description = "Physically dropped"),
        (status = 404, description = "Unknown collection"),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn drop_collection(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Query(q): Query<DropQuery>,
) -> Result<StatusCode, ApiErr> {
    auth.ensure(&collection_id, Role::Admin).await?;
    enforce_storage_writable(&state)?;
    // #2496: same routed-or-local fan-out as `create_collection` above.
    let outcome = if let Some(router) = &state.routed {
        router
            .drop_collection(collection_id.clone(), q.force, &headers)
            .await
            .map_err(ApiErr::from)?
    } else {
        state
            .write_backend
            .drop_collection(collection_id.clone(), q.force)
            .await
            .map_err(ApiErr::from)?
    };
    let phase = match outcome {
        DropOutcome::NotFound => {
            return Err(ApiErr::not_found(format!(
                "collection not found: {collection_id}"
            )));
        }
        DropOutcome::Marked => "marked",
        DropOutcome::AlreadyMarked => "already_marked",
        DropOutcome::Physical => "physical",
    };
    tracing::info!(
        target: "lumen.audit",
        event = "collection_drop",
        phase,
        subject = auth.subject().unwrap_or("anonymous"),
        collection_id = %collection_id,
    );
    // Soft-delete returns 202 Accepted so callers can tell it's still
    // in the grace window; physical / already-marked return 204.
    Ok(match outcome {
        DropOutcome::Marked => StatusCode::ACCEPTED,
        _ => StatusCode::NO_CONTENT,
    })
}

#[utoipa::path(
    delete,
    path = "/collections/{collection_id}/fields/{field_name}",
    tag = "Collections",
    params(
        ("collection_id" = String, Path, description = "Collection namespace"),
        ("field_name"    = String, Path, description = "Field to drop")
    ),
    responses(
        (status = 200, description = "Field dropped; new schema version", body = serde_json::Value),
        (status = 404, description = "Unknown collection or field",       body = ApiError)
    )
)]
pub(crate) async fn drop_field(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path((collection_id, field_name)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure(&collection_id, Role::Admin).await?;
    let version = state
        .write_backend
        .drop_field(collection_id.clone(), field_name.clone())
        .await
        .map_err(ApiErr::from)?;
    tracing::info!(
        target: "lumen.audit",
        event = "field_drop",
        subject = auth.subject().unwrap_or("anonymous"),
        collection_id = %collection_id,
        field_name = %field_name,
        version,
    );
    Ok(Json(serde_json::json!({
        "collection_id": collection_id,
        "field_name": field_name,
        "version": version,
    })))
}
