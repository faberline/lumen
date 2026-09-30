//! `/admin/reshard:fence`, which arms or clears the bounded write pause the
//! reshard driver's cutover holds over the virtual buckets its final migration
//! pass copies (#1396 R2).

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Result;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Deserialize;

use crate::access::application::authorization::{AuthContext, Role};
use crate::api::{ApiErr, AppState};

#[cfg(doc)]
use crate::api::WriteFence;
#[cfg(doc)]
use crate::sharding::interfaces::http::reshard::ScopedBackupRequest;

fn default_fence_ttl_secs() -> u64 {
    300
}

/// Upper bound on `ReshardFenceRequest::ttl_secs` (#1443 R3): well above any
/// real driver tick, but small enough that `Instant::now().checked_add` never
/// overflows and a malformed/malicious admin request can never arm an
/// effectively-permanent write pause.
const MAX_FENCE_TTL_SECS: u64 = 3600;

#[derive(Debug, Deserialize)]
pub(crate) struct ReshardFenceRequest {
    /// Same `virtual_bucket_count` the caller's map uses, matching
    /// [`ScopedBackupRequest`]'s convention.
    virtual_bucket_count: u32,
    /// Buckets to pause writes on. An empty set explicitly clears any
    /// currently-armed fence, independent of its deadline.
    buckets: BTreeSet<u32>,
    /// How long the pause stays armed if never explicitly cleared (a
    /// crashed-driver backstop; see [`WriteFence`]'s doc). Defaults to 300s —
    /// generous relative to one driver tick (`DRIVER_POLL_INTERVAL`, 20s in
    /// `reshard_driver.rs`) plus a full migration-pass HTTP round trip, while
    /// still bounded well under any operator-visible SLO.
    #[serde(default = "default_fence_ttl_secs")]
    ttl_secs: u64,
}

/// `POST /admin/reshard:fence`: arm or clear a bounded write pause on a set
/// of virtual buckets (#1396 R2). The reshard driver's cutover
/// (`service_k8s::reshard_driver::advance_catching_up`) arms this over exactly
/// the buckets its final `CatchingUp` migration pass is about to copy,
/// immediately before that pass, and clears it (`buckets: []`) once the
/// pass/evict/checkpoint/cutover sequence finishes — on success or on
/// `Blocked`. A write to a fenced bucket is rejected with `503
/// bucket_write_paused` rather than silently dropped or applied against a
/// map that is about to change; see [`WriteFence`] for the crash-safety
/// (TTL) argument.
#[utoipa::path(
    post,
    path = "/admin/reshard:fence",
    tag = "Admin",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Fence armed or cleared", body = serde_json::Value),
        (status = 400, description = "Invalid virtual_bucket_count", body = ApiError)
    )
)]
pub(crate) async fn reshard_fence(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ReshardFenceRequest>,
) -> Result<Json<serde_json::Value>, ApiErr> {
    auth.ensure_admin(Role::Admin).await?;
    if req.virtual_bucket_count == 0 {
        return Err(ApiErr::new(
            StatusCode::BAD_REQUEST,
            "invalid_virtual_bucket_count",
            "virtual_bucket_count must be > 0",
        ));
    }
    if req.buckets.is_empty() {
        state.write_fence.clear();
        // #2475: publish the same clear on `/metrics` so
        // `render::prometheus_rule`'s `LumenReshardWorkflowStalled` alert
        // (which reads `lumen_reshard_fence_active`) reflects it too.
        state.engine.metrics().set_reshard_fence_active(false);
        tracing::info!(
            target: "lumen.audit",
            event = "reshard_fence_cleared",
            subject = auth.subject().unwrap_or("anonymous"),
        );
    } else {
        // #1443 R3: reject a nonsensical TTL as 400 rather than letting
        // `WriteFence::arm`'s `Instant::now() + ttl` overflow — `0` would
        // arm-then-immediately-expire (never actually pausing anything, a
        // silent no-op the caller would wrongly believe closed the write
        // window), and anything above the generous upper bound is either a
        // malformed request or would arm an effectively-permanent pause.
        if req.ttl_secs == 0 || req.ttl_secs > MAX_FENCE_TTL_SECS {
            return Err(ApiErr::new(
                StatusCode::BAD_REQUEST,
                "invalid_ttl_secs",
                format!(
                    "ttl_secs must be in 1..={MAX_FENCE_TTL_SECS}, got {}",
                    req.ttl_secs
                ),
            ));
        }
        if !state.write_fence.arm(
            req.virtual_bucket_count,
            req.buckets.clone(),
            Duration::from_secs(req.ttl_secs),
        ) {
            // Unreachable in practice now that ttl_secs is bounded above,
            // but `arm` still reports overflow explicitly (#1443 R3) rather
            // than panicking — surface it as the same 400 shape instead of a
            // silently-unarmed 200.
            return Err(ApiErr::new(
                StatusCode::BAD_REQUEST,
                "invalid_ttl_secs",
                "ttl_secs would overflow the fence deadline",
            ));
        }
        // #2475: `reshard_fence_armed_unixtime` lets the alert distinguish
        // a fence still mid-`CatchingUp`-pass from one the driver never
        // came back to clear.
        state.engine.metrics().set_reshard_fence_active(true);
        tracing::info!(
            target: "lumen.audit",
            event = "reshard_fence_armed",
            subject = auth.subject().unwrap_or("anonymous"),
            virtual_bucket_count = req.virtual_bucket_count,
            buckets = req.buckets.len(),
            ttl_secs = req.ttl_secs,
        );
    }
    Ok(Json(serde_json::json!({
        "armed": !req.buckets.is_empty(),
        "buckets": req.buckets,
    })))
}
