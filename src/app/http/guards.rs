//! The checks handlers run before they reach the engine: read consistency
//! against this pod's cluster state, the reshard write fence, the
//! restart-required and storage-full states, and the direct-mutation permit.

use std::sync::atomic::Ordering;

use anyhow::Result;
use axum::http::{HeaderMap, StatusCode};

use crate::app::http::api_err::ApiErr;
use crate::app::http::app_state::AppState;
use crate::replication::domain::cluster_state::ReadConsistency;
use crate::replication::domain::raft_role::RaftRole;

#[cfg(doc)]
use crate::app::http::write_fence::WriteFence;
// enforce_write_fence's docs link the deprecated `index` and `delete_external_id`.
#[cfg(doc)]
#[allow(deprecated)]
use crate::ingest::interfaces::http::delete::delete_external_id;
#[cfg(doc)]
#[allow(deprecated)]
use crate::ingest::interfaces::http::index::index;
#[cfg(doc)]
use crate::ingest::interfaces::http::replace::{replace_doc, replace_docs};

pub(crate) fn read_consistency_from(headers: &HeaderMap) -> ReadConsistency {
    ReadConsistency::from_header(
        headers
            .get("x-read-consistency")
            .and_then(|h| h.to_str().ok()),
    )
}

/// Enforces a resolved `x-read-consistency` against this pod's live
/// per-shard cluster state (`AppState::cluster`) before a read reaches the
/// local engine (#1310).
///
/// Standalone and legacy external-log builds (`state.cluster` is `None`)
/// have exactly one authoritative copy per shard, so every consistency
/// level is trivially satisfied there — this is a no-op, matching today's
/// behavior unchanged. Primary-replica mode (`state.cluster` is `Some`) is
/// the only place a request's resolved [`ReadConsistency`] can actually
/// diverge from what gets served:
/// - [`ReadConsistency::Any`] is unconstrained.
/// - [`ReadConsistency::Leader`] only succeeds on the pod that currently
///   holds `RaftRole::Leader` for this shard; lumen has no read-forwarding
///   surface, so a non-leader replica rejects the request rather than
///   silently serving a possibly-stale local copy.
/// - [`ReadConsistency::Bounded`] succeeds on the leader (never stale) or
///   on a follower/learner whose `replication_lag_ms` is at or under the
///   requested bound; a replica over the bound rejects rather than
///   silently serving a stale read. In `lumen serve --wal raft`, a
///   follower/learner's `replication_lag_ms` is the conservative "unknown"
///   sentinel (`u64::MAX`) — `RaftHost` doesn't expose a peer-timing RPC
///   today, so `Bounded` on a non-leader replica always rejects rather than
///   report a fabricated lag figure (see `spawn_cluster_state_poller` in
///   `src/bin/lumen/serve/raft.rs`, #1349).
pub(crate) fn enforce_read_consistency(
    state: &AppState,
    consistency: ReadConsistency,
) -> Result<(), ApiErr> {
    let Some(cluster) = state.cluster.as_ref() else {
        return Ok(());
    };
    match consistency {
        ReadConsistency::Any => Ok(()),
        ReadConsistency::Leader => {
            if cluster.role() == RaftRole::Leader {
                return Ok(());
            }
            Err(match cluster.leader_peer() {
                Some(leader) => ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "read_consistency_not_leader",
                    format!(
                        "replica `{}` is not the shard {} leader (current leader is `{}`); \
                         leader-consistency reads must reach it",
                        cluster.pod_name, cluster.shard_index, leader.pod_name
                    ),
                ),
                None => ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "read_consistency_no_leader",
                    format!(
                        "shard {} has no reachable leader; leader-consistency reads cannot be satisfied",
                        cluster.shard_index
                    ),
                ),
            })
        }
        ReadConsistency::Bounded(bound_ms) => {
            if cluster.role() == RaftRole::Leader {
                return Ok(());
            }
            let lag_ms = cluster.replication_lag_ms.load(Ordering::Relaxed);
            if lag_ms <= bound_ms {
                Ok(())
            } else {
                Err(ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "read_consistency_lag_exceeded",
                    format!(
                        "replica `{}` lag {lag_ms}ms exceeds bounded({bound_ms}ms) consistency",
                        cluster.pod_name
                    ),
                ))
            }
        }
    }
}

/// Reject a write whose `(collection_id, external_id)` routes to a
/// currently-fenced virtual bucket (#1396 R2). Checked in [`index`],
/// [`replace_docs`], [`replace_doc`], and [`delete_external_id`] — every
/// write path a reshard's final migration pass must observe a converged
/// snapshot of.
///
/// `delete_external_id` is fenced too (#1458 R2): an earlier revision left
/// DELETE exempt on the theory that `apply_reshard_batch`'s
/// authoritative-subset `replace_ids` scoping (see
/// [`crate::sharding::domain::reshard_batch::snapshot_reshard_batches`]'s `replace_mode` and
/// [`crate::index::application::engine::Engine::apply_reshard_batch`]'s `replace` parameter)
/// already closes the resurrection gap for a delete acked *before* the
/// final pass's scoped-backup read. That leaves a delete racing strictly
/// inside the sub-window between that read and the same pass's eviction
/// uncovered — fencing DELETE like every other write closes it fully, at
/// the ordinary cost (a retryable 503) of any write to a fenced bucket. See
/// the #1396 R2 write-fence module doc in `app::http::write_fence`.
pub(crate) fn enforce_write_fence(
    state: &AppState,
    collection_id: &str,
    external_id: &str,
) -> Result<(), ApiErr> {
    if let Some(bucket) = state.write_fence.blocks(collection_id, external_id) {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "bucket_write_paused",
            format!(
                "virtual bucket {bucket} is paused for an in-progress reshard cutover; retry shortly"
            ),
        ));
    }
    Ok(())
}

pub(crate) fn enforce_collection_write_fence(state: &AppState) -> Result<(), ApiErr> {
    if state.write_fence.blocks_any() {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "bucket_write_paused",
            "a collection-wide write is paused for an in-progress reshard cutover; retry shortly",
        ));
    }
    Ok(())
}

/// #2516: sticky ENOSPC degraded read-only mode. Called first (before any
/// other check) by every mutating admin/data-plane handler — `index`,
/// `docs:replace` (both `replace_docs` and `replace_doc`), delete,
/// create/drop collection, and admin restore — so a node that has already
/// taken a genuine ENOSPC hit on its durable write path (see
/// `crate::ingest::application::write_coordinator::errors::is_storage_full` /
/// `Metrics::mark_storage_degraded`) fast-fails every subsequent mutating
/// request with `507 Insufficient Storage` instead of re-attempting (and
/// re-failing) the same durable write. Deliberately a pure gauge read: no
/// I/O, so this never itself contributes to a full disk. Reads/search/health
/// are exempt — they keep serving while degraded (see the `readyz`
/// discussion in this issue's report: a degraded node still answers reads).
pub(crate) fn enforce_storage_writable(state: &AppState) -> Result<(), ApiErr> {
    if state.writer.restart_required() {
        return Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "restart_required",
            "node observed an unresolved durability boundary; restart this Lumen process before retrying any mutation"
                .to_string(),
        ));
    }
    if state.engine.metrics().is_storage_degraded() {
        return Err(ApiErr::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "storage_full",
            "node is in degraded read-only mode: local storage reported ENOSPC on a durable \
             write path; retry once the periodic re-probe clears it, or restart the pod once \
             space has been freed"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn acquire_direct_mutation_permit(
    state: &AppState,
) -> Result<Option<tokio::sync::OwnedRwLockReadGuard<()>>, ApiErr> {
    enforce_storage_writable(state)?;
    let Some(gate) = state.writer.mutation_gate() else {
        return Ok(None);
    };
    gate.shared().await.map(Some).map_err(ApiErr::from)
}
