//! Indexing: the batch index handler and the streaming NDJSON bulk reindex.

use anyhow::Result;
use axum::extract::{Extension, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Json;

use crate::access::application::authorization::{AuthContext, Role};
use crate::app::http::{
    api_err::ApiErr,
    app_state::AppState,
    guards::{enforce_storage_writable, enforce_write_fence},
};
use crate::shared_kernel::types::document::{
    IndexItem, IndexRequest, IndexResponse, MAX_INDEX_BATCH_SIZE,
};

/// At most [`MAX_INDEX_BATCH_SIZE`] items per request; a longer batch is
/// rejected with 400 before any item runs.
#[deprecated(note = "use PUT /collections/{collection_id}/docs:replace for complete indexed rows")]
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/index",
    tag = "Index",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body = IndexRequest,
    responses(
        (status = 200, description = "Items indexed",     body = IndexResponse),
        (status = 400, description = "Batch size over the limit", body = ApiError),
        (status = 404, description = "Unknown collection", body = ApiError),
        (status = 422, description = "Type mismatch",      body = ApiError),
        (status = 507, description = "Node in ENOSPC degraded read-only mode (#2516)", body = ApiError)
    )
)]
pub(crate) async fn index(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    headers: HeaderMap,
    Path(collection_id): Path<String>,
    Json(req): Json<IndexRequest>,
) -> Result<Json<IndexResponse>, ApiErr> {
    auth.ensure(&collection_id, Role::Write).await?;
    enforce_storage_writable(&state)?;
    if req.items.len() > MAX_INDEX_BATCH_SIZE {
        return Err(ApiErr::new(
            StatusCode::BAD_REQUEST,
            "batch_too_large",
            format!(
                "batch has {} items, max is {MAX_INDEX_BATCH_SIZE}",
                req.items.len()
            ),
        ));
    }
    for item in &req.items {
        enforce_write_fence(&state, &collection_id, &item.external_id)?;
    }
    let resp = if let Some(router) = &state.routed {
        router
            .index(collection_id.clone(), req, &headers)
            .await
            .map_err(ApiErr::from)?
    } else {
        state
            .write_backend
            .index(collection_id.clone(), req)
            .await
            .map_err(ApiErr::from)?
    };
    Ok(Json(resp))
}

/// Streaming bulk-reindex endpoint.
///
/// Body is NDJSON of `IndexItem` records (one per line). Response is
/// an NDJSON stream of progress events:
///
/// ```text
/// {"event":"progress","indexed_total":1000,"batch_indexed":1000,"elapsed_ms":42}
/// {"event":"progress","indexed_total":2000,"batch_indexed":1000,"elapsed_ms":85}
/// ...
/// {"event":"done","indexed_total":2473,"elapsed_ms":210}
/// ```
///
/// Errors are surfaced as `{"event":"error","line":N,"message":"..."}`
/// inline; the stream continues so partial progress is observable.
///
/// Rejected outright in routed multi-shard mode (#1442 R6): the spawned
/// batch loop below writes through `state.write_backend` directly, bypassing
/// both `state.routed`'s per-item shard ownership and `enforce_write_fence`
/// (the same per-item reshard-cutover pause every other write path
/// observes). Routing each streamed item by ownership and fencing it
/// individually, inside a detached `tokio::spawn` task that already streams
/// its own NDJSON response back, is a materially bigger change than this
/// bounded hardening pass — an accepted, documented fallback per R6's own
/// scope rather than a half-routed implementation that could silently
/// mis-shard or skip the write fence.
#[utoipa::path(
    post,
    path = "/collections/{collection_id}/reindex/stream",
    tag = "Index",
    params(("collection_id" = String, Path, description = "Collection namespace")),
    request_body(content = String, description = "NDJSON of IndexItem records, one per line"),
    responses(
        (status = 200, description = "NDJSON stream of progress events, terminated by a done event"),
        (status = 501, description = "Not supported in routed multi-shard mode (#1442 R6)", body = ApiError)
    )
)]
pub(crate) async fn reindex_stream(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Path(collection_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiErr> {
    use axum::body::Body;
    use std::time::Instant;
    use tokio::sync::mpsc;

    auth.ensure(&collection_id, Role::Write).await?;
    if state.routed.is_some() {
        return Err(ApiErr::new(
            StatusCode::NOT_IMPLEMENTED,
            "reindex_stream_not_routed",
            "streaming bulk reindex bypasses per-item shard ownership and the write fence; not \
             supported in routed multi-shard mode, use POST .../index instead (#1442 R6)"
                .to_string(),
        ));
    }

    const BATCH_SIZE: usize = 1_000;
    let (tx, rx) = mpsc::channel::<Result<axum::body::Bytes, std::io::Error>>(16);
    let writer = state.write_backend.clone();
    let collection = collection_id.clone();

    tokio::spawn(async move {
        let started = Instant::now();
        let mut batch: Vec<IndexItem> = Vec::with_capacity(BATCH_SIZE);
        let mut indexed_total = 0u64;
        let send = |tx: &mpsc::Sender<_>, line: serde_json::Value| {
            let mut s = line.to_string();
            s.push('\n');
            let bytes = axum::body::Bytes::from(s.into_bytes());
            tx.try_send(Ok::<_, std::io::Error>(bytes))
        };

        for (lineno, raw) in body.split(|&b| b == b'\n').enumerate() {
            let line = raw.trim_ascii();
            if line.is_empty() {
                continue;
            }
            let item: IndexItem = match serde_json::from_slice(line) {
                Ok(i) => i,
                Err(e) => {
                    let _ = send(
                        &tx,
                        serde_json::json!({
                            "event": "error",
                            "line": lineno + 1,
                            "message": e.to_string(),
                        }),
                    );
                    continue;
                }
            };
            batch.push(item);

            if batch.len() >= BATCH_SIZE {
                let drained = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                let batch_start = Instant::now();
                match writer
                    .index(
                        collection.clone(),
                        IndexRequest {
                            items: drained,
                            request_id: None,
                        },
                    )
                    .await
                {
                    Ok(r) => {
                        indexed_total += r.indexed as u64;
                        let _ = send(
                            &tx,
                            serde_json::json!({
                                "event": "progress",
                                "indexed_total": indexed_total,
                                "batch_indexed": r.indexed,
                                "elapsed_ms": started.elapsed().as_millis() as u64,
                                "batch_elapsed_ms": batch_start.elapsed().as_millis() as u64,
                            }),
                        );
                    }
                    Err(e) => {
                        let _ = send(
                            &tx,
                            serde_json::json!({
                                "event": "error",
                                "line": lineno + 1,
                                "message": e.to_string(),
                            }),
                        );
                    }
                }
            }
        }

        // Final flush of whatever's left in the batch.
        if !batch.is_empty() {
            let batch_start = Instant::now();
            if let Ok(r) = writer
                .index(
                    collection.clone(),
                    IndexRequest {
                        items: batch,
                        request_id: None,
                    },
                )
                .await
            {
                indexed_total += r.indexed as u64;
                let _ = send(
                    &tx,
                    serde_json::json!({
                        "event": "progress",
                        "indexed_total": indexed_total,
                        "batch_indexed": r.indexed,
                        "elapsed_ms": started.elapsed().as_millis() as u64,
                        "batch_elapsed_ms": batch_start.elapsed().as_millis() as u64,
                    }),
                );
            }
        }

        let _ = send(
            &tx,
            serde_json::json!({
                "event": "done",
                "indexed_total": indexed_total,
                "elapsed_ms": started.elapsed().as_millis() as u64,
            }),
        );

        tracing::info!(
            target: "lumen.audit",
            event = "reindex_stream_done",
            subject = auth.subject().unwrap_or("anonymous"),
            collection_id = %collection,
            indexed_total,
            elapsed_ms = started.elapsed().as_millis() as u64,
        );
    });

    let stream =
        futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) });
    let resp = axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .map_err(|e| {
            ApiErr::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "stream_init",
                e.to_string(),
            )
        })?;
    Ok(resp)
}
