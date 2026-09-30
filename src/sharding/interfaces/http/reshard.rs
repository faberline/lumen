//! The reshard admin verbs (#1380): batch apply, bucket-scoped export and
//! evict, and `/admin/reshard:prune` (#1457 R1), the final migration pass's
//! independently chunked authoritative-replace scope. The sharding domain's
//! primitives (`bucket_moves`, `snapshot_reshard_batches`,
//! `snapshot_reshard_prune_chunks`) emit bounded `ReshardBatch` and
//! `ReshardPruneChunk` units for checkpointed migration; these verbs are the
//! wire surface that moves them, and persistence's `/admin/checkpoint` (#1389)
//! makes their mutations survive the cutover restart the driver triggers.

use std::collections::BTreeSet;

use anyhow::Result;
use axum::extract::{Extension, State};
use axum::response::Json;
use serde::Deserialize;

use crate::access::application::authorization::{AuthContext, Role};
use crate::app::http::{
    api_err::ApiErr, app_state::AppState, guards::acquire_direct_mutation_permit,
};
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::sharding::domain::reshard_batch::ReshardBatch;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;

#[cfg(doc)]
use crate::index::application::engine::Engine;

/// `POST /admin/reshard:apply`: additively merge one [`ReshardBatch`] into
/// the live engine (upsert semantics for the batch's documents; never a
/// full replace, unlike `/admin/restore`). Idempotent — a retried batch
/// (operator resume after a checkpoint) converges to the same query-visible
/// state; see [`Engine::apply_reshard_batch`].
#[utoipa::path(
    post,
    path = "/admin/reshard:apply",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Batch merged additively (safe to retry)", body = serde_json::Value),
        (status = 400, description = "Malformed batch or snapshot version mismatch", body = ApiError)
    )
)]
pub(crate) async fn reshard_apply(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(batch): Json<ReshardBatch>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    let _mutation_permit = acquire_direct_mutation_permit(&state).await?;
    let outcome = state
        .engine
        .apply_reshard_batch(batch.snapshot, None)
        .map_err(ApiErr::from)?;
    tracing::info!(
        target: "lumen.audit",
        event = "reshard_batch_applied",
        subject = auth.subject().unwrap_or("anonymous"),
        bucket = batch.bucket,
        from_shard = batch.from_shard,
        to_shard = batch.to_shard,
        from_map_version = batch.from_map_version,
        to_map_version = batch.to_map_version,
        collections_touched = outcome.collections_touched,
        documents_upserted = outcome.documents_upserted,
        documents_pruned = outcome.documents_pruned,
    );
    Ok(Json(serde_json::json!({
        "collections_touched": outcome.collections_touched,
        "documents_upserted": outcome.documents_upserted,
        "documents_pruned": outcome.documents_pruned,
    })))
}

/// `POST /admin/reshard:prune`: accumulate one [`ReshardPruneChunk`] of the
/// final migration pass's authoritative "keep" set for one `(bucket,
/// collection_id)` pair, and prune once every chunk has arrived (#1457 R1).
/// Unlike `/admin/reshard:apply` (purely additive), this verb is what makes
/// the final pass authoritative for the buckets it copies: a document
/// deleted on the source during the split is absent from the accumulated
/// keep set and is pruned here instead of surviving as a stale copy from an
/// earlier additive pass. Idempotent per chunk (safe to retry after a 413)
/// and as a whole group (safe to re-send every chunk after a driver
/// restart); see [`Engine::apply_reshard_prune_chunk`].
#[utoipa::path(
    post,
    path = "/admin/reshard:prune",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Chunk accumulated; pruned once every chunk of its group has arrived", body = serde_json::Value),
        (status = 400, description = "Malformed chunk", body = ApiError)
    )
)]
pub(crate) async fn reshard_prune(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(chunk): Json<crate::sharding::domain::prune_chunk::ReshardPruneChunk>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    let _mutation_permit = acquire_direct_mutation_permit(&state).await?;
    let to_map_version = chunk.to_map_version;
    let bucket = chunk.bucket;
    let collection_id = chunk.collection_id.clone();
    let chunk_index = chunk.chunk_index;
    let total_chunks = chunk.total_chunks;
    let outcome = state
        .engine
        .apply_reshard_prune_chunk(chunk)
        .map_err(ApiErr::from)?;
    if outcome.complete {
        tracing::info!(
            target: "lumen.audit",
            event = "reshard_prune_applied",
            subject = auth.subject().unwrap_or("anonymous"),
            bucket,
            to_map_version,
            collection_id = collection_id.as_str(),
            chunk_index,
            total_chunks,
            documents_pruned = outcome.documents_pruned,
        );
    }
    Ok(Json(serde_json::json!({
        "complete": outcome.complete,
        "documents_pruned": outcome.documents_pruned,
    })))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ScopedBackupRequest {
    /// Same `virtual_bucket_count` the caller's [`VirtualBucketShardMap`]
    /// uses — must match what `snapshot_reshard_batches` was/will be called
    /// with so bucket membership agrees.
    virtual_bucket_count: u32,
    /// Only documents whose bucket is in this set are included.
    buckets: BTreeSet<u32>,
}

/// `POST /admin/backup:scoped`: like `GET /admin/backup`, but restricted to
/// documents routed to the requested virtual buckets — a source shard can
/// export just the buckets that are moving instead of a full-engine dump.
/// Bucket membership is computed with the same hash `reshard::
/// snapshot_reshard_batches` uses ([`crate::reshard::snapshot_bucket_subset`]),
/// so an export and a later-computed batch can never disagree.
#[utoipa::path(
    post,
    path = "/admin/backup:scoped",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "SnapshotV1 restricted to the requested virtual buckets", body = serde_json::Value),
        (status = 400, description = "Invalid virtual_bucket_count", body = ApiError)
    )
)]
pub(crate) async fn backup_scoped(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ScopedBackupRequest>,
) -> Result<Json<SnapshotV1>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    let full = state.engine.snapshot().map_err(ApiErr::from)?;
    let scoped = crate::sharding::domain::snapshot_subset::snapshot_bucket_subset(
        &full,
        req.virtual_bucket_count,
        &req.buckets,
    )
    .map_err(ApiErr::from)?;
    tracing::info!(
        target: "lumen.audit",
        event = "backup_scoped",
        subject = auth.subject().unwrap_or("anonymous"),
        virtual_bucket_count = req.virtual_bucket_count,
        buckets = req.buckets.len(),
    );
    Ok(Json(scoped))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReshardEvictRequest {
    /// This shard's physical index in `assignments`.
    shard: u32,
    /// The newer map version being cut over to; carried for audit logging.
    map_version: u64,
    /// `bucket -> physical shard` assignment for the newer map. Its length
    /// is the virtual bucket count.
    assignments: Vec<u32>,
    physical_shard_count: u32,
}

/// `POST /admin/reshard:evict`: source-side post-cutover eviction. Given a
/// newer virtual-bucket map and this shard's index within it, removes
/// exactly the documents whose bucket no longer routes to this shard —
/// nothing else. A separate, explicitly-invoked step; never implicit in
/// `/admin/reshard:apply` or `/admin/backup*`. Idempotent — a document
/// already evicted by a prior call no longer matches and is skipped.
#[utoipa::path(
    post,
    path = "/admin/reshard:evict",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Documents no longer owned by this shard removed", body = serde_json::Value),
        (status = 400, description = "Invalid virtual bucket map", body = ApiError)
    )
)]
pub(crate) async fn reshard_evict(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ReshardEvictRequest>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    let _mutation_permit = acquire_direct_mutation_permit(&state).await?;
    let map =
        VirtualBucketShardMap::new(req.map_version, req.assignments, req.physical_shard_count)
            .map_err(ApiErr::from)?;
    let outcome = state
        .engine
        .evict_not_owned(&map, req.shard)
        .map_err(ApiErr::from)?;
    tracing::info!(
        target: "lumen.audit",
        event = "reshard_evict",
        subject = auth.subject().unwrap_or("anonymous"),
        shard = req.shard,
        map_version = req.map_version,
        collections_touched = outcome.collections_touched,
        documents_evicted = outcome.documents_evicted,
    );
    Ok(Json(serde_json::json!({
        "collections_touched": outcome.collections_touched,
        "documents_evicted": outcome.documents_evicted,
    })))
}
