//! The admin probes ApiDoc documents: `/healthz`, `/readyz` and `/metrics`,
//! which service-http serves, so only their OpenAPI metadata is here, and
//! `/version` and `/debug/cluster`, which the router's admin sub-router serves.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::app::http::app_state::AppState;
use crate::shared_kernel::types::cluster_state_view::ClusterStateView;
use crate::shared_kernel::types::raft_role::RaftRole;

pub const SEARCH_FACETS_V1: &str = "search_facets_v1";
pub const BINARY_CAPABILITIES: &[&str] = &[SEARCH_FACETS_V1];
pub const EFFECTIVE_COMPATIBILITY_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Capabilities supported by this binary.
pub fn binary_capabilities() -> Vec<String> {
    BINARY_CAPABILITIES.iter().map(|&s| s.to_string()).collect()
}

/// Effective compatibility version.
pub fn effective_compatibility_version() -> &'static str {
    EFFECTIVE_COMPATIBILITY_VERSION
}

/// Individual member capability report within a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberCapabilityReport {
    pub member_id: String,
    pub capabilities: Vec<String>,
    pub is_stale: bool,
}

impl MemberCapabilityReport {
    pub fn new(member_id: impl Into<String>, capabilities: Vec<String>, is_stale: bool) -> Self {
        Self {
            member_id: member_id.into(),
            capabilities,
            is_stale,
        }
    }
}

/// Capability report representing active capabilities converged across members.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapabilityReport {
    active_capabilities: Vec<String>,
}

impl CapabilityReport {
    /// Construct a capability report from expected members and member reports.
    ///
    /// If there is no evidence of convergence (e.g. expected members is empty,
    /// reports are missing, any member report is stale, or member capabilities are mixed),
    /// no capabilities are active and an empty capability set is returned.
    pub fn from_member_reports(
        expected_members: &[String],
        reports: &[MemberCapabilityReport],
    ) -> Self {
        if expected_members.is_empty() || reports.is_empty() {
            return Self {
                active_capabilities: Vec::new(),
            };
        }

        let mut reports_by_id = std::collections::HashMap::new();
        for r in reports {
            reports_by_id.insert(&r.member_id, r);
        }

        for expected in expected_members {
            match reports_by_id.get(expected) {
                None => {
                    return Self {
                        active_capabilities: Vec::new(),
                    };
                }
                Some(report) => {
                    if report.is_stale {
                        return Self {
                            active_capabilities: Vec::new(),
                        };
                    }
                }
            }
        }

        let binary = binary_capabilities();
        let mut active = Vec::new();
        for cap in &binary {
            let all_support = expected_members.iter().all(|expected| {
                if let Some(r) = reports_by_id.get(expected) {
                    r.capabilities.contains(cap)
                } else {
                    false
                }
            });
            if all_support {
                active.push(cap.clone());
            }
        }

        Self {
            active_capabilities: active,
        }
    }

    /// Standalone mode: the single local node is its own converged member.
    pub fn standalone() -> Self {
        let member = "standalone".to_string();
        Self::from_member_reports(
            &[member.clone()],
            &[MemberCapabilityReport::new(
                member,
                binary_capabilities(),
                false,
            )],
        )
    }

    pub fn active_capabilities(&self) -> Vec<String> {
        self.active_capabilities.clone()
    }
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
    fn test_binary_capabilities_contains_search_facets_v1() {
        let caps = binary_capabilities();
        assert!(caps.contains(&SEARCH_FACETS_V1.to_string()));
    }

    #[test]
    fn test_effective_compatibility_version() {
        assert_eq!(effective_compatibility_version(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_standalone_active_equals_binary() {
        let report = CapabilityReport::standalone();
        assert_eq!(report.active_capabilities(), binary_capabilities());
    }

    #[test]
    fn test_binary_supported_but_not_active_when_no_convergence_evidence() {
        // Binary supports capability, but expected member report is missing
        let report = CapabilityReport::from_member_reports(&["member-1".to_string()], &[]);
        assert!(report.active_capabilities().is_empty());

        let report_empty = CapabilityReport::from_member_reports(&[], &[]);
        assert!(report_empty.active_capabilities().is_empty());
    }

    #[test]
    fn test_stale_or_missing_member_report_produces_no_active() {
        // Missing member report: expected 2 members, only 1 provided
        let report_missing = CapabilityReport::from_member_reports(
            &["member-1".to_string(), "member-2".to_string()],
            &[MemberCapabilityReport::new(
                "member-1",
                binary_capabilities(),
                false,
            )],
        );
        assert!(report_missing.active_capabilities().is_empty());

        // Stale member report
        let report_stale = CapabilityReport::from_member_reports(
            &["member-1".to_string()],
            &[MemberCapabilityReport::new(
                "member-1",
                binary_capabilities(),
                true,
            )],
        );
        assert!(report_stale.active_capabilities().is_empty());
    }

    #[test]
    fn test_mixed_member_report_produces_no_active() {
        // Mixed: member-1 has search_facets_v1, member-2 does not
        let report_mixed = CapabilityReport::from_member_reports(
            &["member-1".to_string(), "member-2".to_string()],
            &[
                MemberCapabilityReport::new("member-1", binary_capabilities(), false),
                MemberCapabilityReport::new("member-2", vec![], false),
            ],
        );
        assert!(report_mixed.active_capabilities().is_empty());
    }

    #[test]
    fn test_negative_control_mixed_and_stale_never_activates_search_facets_v1() {
        // Negative control: feed mixed and stale inputs to active calculation.
        // Assert search_facets_v1 is NOT in active_capabilities.
        // If the implementation were changed to directly copy the binary capabilities list,
        // this test MUST fail!
        let mixed = CapabilityReport::from_member_reports(
            &["node-a".to_string(), "node-b".to_string()],
            &[
                MemberCapabilityReport::new("node-a", vec![SEARCH_FACETS_V1.to_string()], false),
                MemberCapabilityReport::new("node-b", vec![], false),
            ],
        );
        assert!(
            !mixed
                .active_capabilities()
                .contains(&SEARCH_FACETS_V1.to_string()),
            "search_facets_v1 must not be active on mixed cluster"
        );

        let stale = CapabilityReport::from_member_reports(
            &["node-a".to_string()],
            &[MemberCapabilityReport::new(
                "node-a",
                vec![SEARCH_FACETS_V1.to_string()],
                true,
            )],
        );
        assert!(
            !stale
                .active_capabilities()
                .contains(&SEARCH_FACETS_V1.to_string()),
            "search_facets_v1 must not be active on stale member report"
        );
    }
}
