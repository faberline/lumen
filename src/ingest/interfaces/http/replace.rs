//! Full-replacement upserts: the docs:replace batch and its single-document
//! sugar.

use anyhow::Result;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{enforce_storage_writable, enforce_write_fence, ApiErr, AppState};
use crate::shared_kernel::types::document::{
    ReplaceDocBody, ReplaceDocItem, ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse,
    MAX_BATCH_REPLACE_SIZE,
};

/// Batch full-replacement upsert: each item's `fields` becomes the doc's
/// entire indexed state, implicitly deleting any declared schema field the
/// doc has today but that is absent from `fields`. `docs:replace` is one
/// literal path segment (AIP-136 custom-method syntax) appended after
/// `{collection_id}`, so it registers directly in axum next to
/// `/collections/{collection_id}/docs/{external_id}` without any capture
/// ambiguity — collection ids are validated to reject `:`.
///
/// PUT is deliberate: this is idempotent full replacement (plus optional
/// doc-level last-write-wins), so replaying the same request converges to
/// the same state. Own the *complete* row for a doc? Use `docs:replace`.
/// Own only *some* fields and want to add/update those without touching
/// the rest? Use `POST .../index` instead.
///
/// One bad item (unknown field, type mismatch) never fails the batch — the
/// batch-level status stays 200 and that item's [`ReplaceDocResult`]
/// carries the error. Only a malformed body or an over-limit batch returns
/// 400.
#[utoipa::path(
    put,
    path = "/collections/{collection_id}/docs:replace",
    tag = "Index",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = ReplaceDocsRequest,
    responses(
        (status = 200, description = "Per-item results, same order and length as `docs`", body = ReplaceDocsResponse),
        (status = 400, description = "Malformed body or batch size over the limit", body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn replace_docs(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Json(req): Json<ReplaceDocsRequest>,
) -> Result<Json<ReplaceDocsResponse>, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;
    enforce_storage_writable(&state)?;
    if req.docs.len() > MAX_BATCH_REPLACE_SIZE {
        return Err(ApiErr::new(
            StatusCode::BAD_REQUEST,
            "batch_too_large",
            format!(
                "batch has {} items, max is {MAX_BATCH_REPLACE_SIZE}",
                req.docs.len()
            ),
        ));
    }
    for doc in &req.docs {
        enforce_write_fence(&state, &collection_id, &doc.external_id)?;
    }
    let resp = replace_docs_routed_or_local(&state, &headers, collection_id, req).await?;
    Ok(Json(resp))
}

/// Shared write-backend/router branch for [`replace_docs`] and
/// [`replace_doc`] (its single-doc sugar).
async fn replace_docs_routed_or_local(
    state: &AppState,
    headers: &HeaderMap,
    collection_id: String,
    req: ReplaceDocsRequest,
) -> Result<ReplaceDocsResponse, ApiErr> {
    if let Some(router) = &state.routed {
        router
            .replace_docs(collection_id, req, headers)
            .await
            .map_err(ApiErr::from)
    } else {
        state
            .write_backend
            .replace_docs(collection_id, req)
            .await
            .map_err(ApiErr::from)
    }
}

/// Single-resource sugar over `docs:replace`: exactly the one-item batch
/// `{"docs": [{"external_id": ..., "version": ..., "fields": {...}}]}`,
/// unwrapped back into a bare [`ReplaceDocResult`]. See [`replace_docs`]
/// for the full-replacement / doc-level LWW semantics — the batch-level
/// status stays 200 here too; a bad item comes back as
/// `{"status":"error",...}` in the body rather than as an HTTP error.
#[utoipa::path(
    put,
    path = "/collections/{collection_id}/docs/{external_id}",
    tag = "Index",
    params(
        ("collection_id" = String, Path, description = "Collection namespace"),
        ("external_id"   = String, Path, description = "Caller-owned identifier")
    ),
    request_body = ReplaceDocBody,
    responses(
        (status = 200, description = "Replacement result for this doc", body = ReplaceDocResult),
        (status = 400, description = "Malformed body", body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn replace_doc(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path((collection_id, external_id)): Path<(String, String)>,
    Json(body): Json<ReplaceDocBody>,
) -> Result<Json<ReplaceDocResult>, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;
    enforce_storage_writable(&state)?;
    enforce_write_fence(&state, &collection_id, &external_id)?;
    let req = ReplaceDocsRequest {
        docs: vec![ReplaceDocItem {
            external_id,
            version: body.version,
            fields: body.fields,
        }],
    };
    let resp = replace_docs_routed_or_local(&state, &headers, collection_id, req).await?;
    let result = resp.results.into_iter().next().ok_or_else(|| {
        ApiErr::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "no result for single-doc replace".to_string(),
        )
    })?;
    Ok(Json(result))
}
