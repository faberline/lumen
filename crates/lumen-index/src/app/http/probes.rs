//! The admin probes ApiDoc documents: `/healthz`, `/readyz` and `/metrics`,
//! which service-http serves, so only their OpenAPI metadata is here, and
//! `/version` and `/debug/cluster`, which the router's admin sub-router serves.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::app::http::app_state::AppState;
use crate::index::domain::capability_report::{binary_capabilities, CapabilityReport};
use crate::shared_kernel::types::cluster_state_view::ClusterStateView;
use crate::shared_kernel::types::raft_role::RaftRole;

/// The compatibility version this binary speaks. It is not the crate version
/// and does not change with a release.
pub const EFFECTIVE_COMPATIBILITY_VERSION: &str = "0.1";

/// Effective compatibility version.
pub fn effective_compatibility_version() -> &'static str {
    EFFECTIVE_COMPATIBILITY_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct VersionResponse {
    pub version: String,
    pub git_sha: String,
    pub built_at: String,
    pub binary_capabilities: Vec<String>,
    pub active_capabilities: Vec<String>,
    pub effective_compatibility_version: String,
}

#[utoipa::path(
    get,
    path = "/metrics",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Prometheus text-format metrics", body = String))
)]
/// OpenAPI metadata for the shared `/metrics` implementation in service-http.
#[allow(dead_code)]
async fn metrics(
    State(state): State<AppState>,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let body = state.engine.metrics().render();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

#[utoipa::path(
    get,
    path = "/debug/cluster",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Cluster state snapshot", body = ClusterStateView))
)]
pub(super) async fn debug_cluster(State(state): State<AppState>) -> Json<ClusterStateView> {
    let view = match state.cluster.as_ref() {
        Some(c) => c.snapshot(),
        None => ClusterStateView {
            pod_name: "local".into(),
            shard_index: 0,
            replica_index: 0,
            role: RaftRole::Leader,
            peers: vec![],
            applied_index: 0,
            leader_term: 0,
            replication_lag_ms: 0,
        },
    };
    Json(view)
}

#[utoipa::path(
    get,
    path = "/healthz",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Process is alive", body = String))
)]
/// OpenAPI metadata for the shared `/healthz` implementation in service-http.
#[allow(dead_code)]
async fn healthz() -> &'static str {
    "ok"
}

#[utoipa::path(
    get,
    path = "/version",
    tag = "Admin",
    security(()),
    responses((status = 200, description = "Build provenance and compatibility capabilities", body = VersionResponse))
)]
/// Build provenance and compatibility capabilities. `version` is the crate
/// version; `git_sha` and `built_at` are stamped by `build.rs` and degrade to
/// "unknown" outside a git checkout. `binary_capabilities`,
/// `active_capabilities`, and `effective_compatibility_version` distinguish
/// binary support, active cluster capability, and compatibility version.
pub(super) async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        git_sha: option_env!("LUMEN_GIT_SHA")
            .unwrap_or("unknown")
            .to_string(),
        built_at: option_env!("LUMEN_BUILT_AT")
            .unwrap_or("unknown")
            .to_string(),
        binary_capabilities: binary_capabilities(),
        active_capabilities: CapabilityReport::standalone().active_capabilities(),
        effective_compatibility_version: effective_compatibility_version().to_string(),
    })
}

#[utoipa::path(
    get,
    path = "/readyz",
    tag = "Admin",
    security(()),
    responses(
        (status = 200, description = "Engine ready"),
        (status = 503, description = "Not ready")
    )
)]
/// OpenAPI metadata for the shared `/readyz` implementation in service-http.
#[allow(dead_code)]
async fn readyz(State(state): State<AppState>) -> (StatusCode, &'static str) {
    if state.writer.restart_required() {
        (StatusCode::SERVICE_UNAVAILABLE, "restart required")
    } else if state.engine.is_draining() {
        (StatusCode::SERVICE_UNAVAILABLE, "draining")
    } else {
        (StatusCode::OK, "ok")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_compatibility_version() {
        assert_eq!(effective_compatibility_version(), "0.1");
    }
}
