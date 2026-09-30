//! The document delete handlers: one document or one of its fields, every
//! document in a collection (truncate), and a batch of documents (unindex).

use anyhow::Result;
use axum::extract::{Extension, FromRequest, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;
use serde::Deserialize;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{
    enforce_collection_write_fence, enforce_storage_writable, enforce_write_fence, ApiErr, AppState,
};
use crate::shared_kernel::types::document::{
    validate_batch_unindex_docs_request, BatchUnindexDocsRequest,
};

#[derive(Debug, Deserialize)]
pub(crate) struct DeleteQuery {
    field: Option<String>,
}

#[deprecated(
    note = "use DELETE /collections/{collection_id}/docs/{external_id} for complete indexed-row deletion"
)]
#[utoipa::path(
    delete,
    path = "/collections/{collection_id}/index/{external_id}",
    tag = "Index",
    params(
        ("collection_id" = String, Path, description = "Collection namespace"),
        ("external_id"   = String, Path, description = "Caller-owned identifier"),
        ("field"         = Option<String>, Query, description = "Restrict deletion to one field")
    ),
    responses(
        (status = 204, description = "Deleted"),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn delete_external_id(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path((collection_id, external_id)): Path<(String, String)>,
    Query(q): Query<DeleteQuery>,
) -> Result<StatusCode, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;
    enforce_storage_writable(&state)?;
    enforce_write_fence(&state, &collection_id, &external_id)?;
    if let Some(router) = &state.routed {
        router
            .delete(collection_id.clone(), external_id, q.field, &headers)
            .await
            .map_err(ApiErr::from)?;
    } else {
        state
            .write_backend
            .delete(collection_id.clone(), external_id, q.field)
            .await
            .map_err(ApiErr::from)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Deletes the complete indexed row for one caller-owned external id. Lumen
/// indexes caller-owned fields only; source-record hydration stays with the
/// caller.
#[utoipa::path(
    delete,
    operation_id = "delete_doc",
    path = "/collections/{collection_id}/docs/{external_id}",
    tag = "Index",
    params(
        ("collection_id" = String, Path, description = "Collection namespace"),
        ("external_id"   = String, Path, description = "Caller-owned identifier")
    ),
    responses(
        (status = 204, description = "Deleted"),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn delete_doc(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path((collection_id, external_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;
    enforce_storage_writable(&state)?;
    enforce_write_fence(&state, &collection_id, &external_id)?;
    if let Some(router) = &state.routed {
        router
            .delete(collection_id.clone(), external_id, None, &headers)
            .await
            .map_err(ApiErr::from)?;
    } else {
        state
            .write_backend
            .delete(collection_id.clone(), external_id, None)
            .await
            .map_err(ApiErr::from)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Empty all indexed documents while retaining the collection declaration.
///
/// A successful response means this physical shard has durably swapped to the
/// empty document state.  In routed mode each shard has that same boundary,
/// but callers can observe a mixed cross-shard window while the fan-out runs.
#[utoipa::path(
    post,
    operation_id = "truncate_docs",
    path = "/collections/{collection_id}/docs:truncate",
    tag = "Index",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    responses(
        (status = 204, description = "Documents truncated; schema is unchanged"),
        (status = 503, description = "Reshard fence or routed shard unavailable", body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn truncate_docs(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    request: Request,
) -> Result<StatusCode, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;
    // A no-body custom method must not silently accept a stale `request_id`
    // or another future selector.  Read at most one byte so a malformed large
    // body cannot become an allocation-based denial of service.
    let body = axum::body::to_bytes(request.into_body(), 1)
        .await
        .map_err(|_| {
            ApiErr::new(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "truncate takes no request body",
            )
        })?;
    if !body.is_empty() {
        return Err(ApiErr::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "truncate takes no request body",
        ));
    }
    enforce_storage_writable(&state)?;
    // A truncate covers every document bucket.  It cannot safely pass a
    // reshard cutover fence merely because it has no single external id.
    enforce_collection_write_fence(&state)?;
    if let Some(router) = &state.routed {
        router
            .truncate_docs(collection_id, &headers)
            .await
            .map_err(ApiErr::from)?;
    } else {
        state
            .write_backend
            .truncate_docs(collection_id)
            .await
            .map_err(ApiErr::from)?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Remove complete indexed rows for a bounded caller-supplied id list.
///
/// Validation happens before storage admission, reshard fences, routing, and
/// durable publish.  Routed requests partition the list by current ownership;
/// each nonempty physical shard receives one atomic durable command.
#[utoipa::path(
    post,
    operation_id = "batch_unindex_docs",
    path = "/collections/{collection_id}/docs:unindex",
    tag = "Index",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = BatchUnindexDocsRequest,
    responses(
        (status = 204, description = "Documents unindexed"),
        (status = 400, description = "Malformed body, empty/duplicate ids, or more than 1000 ids", body = ApiError),
        (status = 503, description = "Reshard fence or routed shard unavailable", body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn unindex_docs(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    request: Request,
) -> Result<StatusCode, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;

    // Axum's default JSON rejection may use several client-error statuses
    // depending on the failure source.  This fixed command promises 400 for
    // every malformed JSON/body-shape case, before any write fence or route.
    let Json(req) = Json::<BatchUnindexDocsRequest>::from_request(request, &state)
        .await
        .map_err(|error| {
            ApiErr::new(
                StatusCode::BAD_REQUEST,
                "bad_request",
                format!("invalid docs:unindex body: {error}"),
            )
        })?;
    validate_batch_unindex_docs_request(&req).map_err(|error| {
        ApiErr::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("invalid docs:unindex body: {error}"),
        )
    })?;

    enforce_storage_writable(&state)?;
    for external_id in &req.external_ids {
        enforce_write_fence(&state, &collection_id, external_id)?;
    }
    if let Some(router) = &state.routed {
        router
            .unindex_docs(collection_id, req, &headers)
            .await
            .map_err(ApiErr::from)?;
    } else {
        state
            .write_backend
            .unindex_docs(collection_id, req)
            .await
            .map_err(ApiErr::from)?;
    }
    Ok(StatusCode::NO_CONTENT)
}
