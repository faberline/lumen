//! Pure domain rules for the compatibility capabilities a Lumen binary supports
//! and the capabilities that are active across the serving members.
//!
//! No I/O, no HTTP and no schema types live here. `active` is derived only from
//! convergence evidence, so a binary that supports a capability is not
//! reported as active until every expected member proves it.

use std::collections::HashMap;

/// The first facet capability.
pub const SEARCH_FACETS_V1: &str = "search_facets_v1";

/// Capabilities supported by this binary.
pub const BINARY_CAPABILITIES: &[&str] = &[SEARCH_FACETS_V1];

/// Capabilities supported by this binary, as owned strings.
pub fn binary_capabilities() -> Vec<String> {
    BINARY_CAPABILITIES.iter().map(|&s| s.to_string()).collect()
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
    /// No capability is active unless every expected member has exactly one
    /// fresh report. Any of these leaves the set empty: no expected members,
    /// no reports, a missing or stale report, two reports for the same member
    /// (stale and fresh, or conflicting), or a member that lacks a capability.
    pub fn from_member_reports(
        expected_members: &[String],
        reports: &[MemberCapabilityReport],
    ) -> Self {
        let none = Self {
            active_capabilities: Vec::new(),
        };
        if expected_members.is_empty() || reports.is_empty() {
            return none;
        }

        let mut reports_by_id = HashMap::new();
        for r in reports {
            // A second report for one member is ambiguous: refuse all activation.
            if reports_by_id.insert(&r.member_id, r).is_some() {
                return none;
            }
        }

        for expected in expected_members {
            match reports_by_id.get(expected) {
                None => return none,
                Some(report) if report.is_stale => return none,
                Some(_) => {}
            }
        }

        let active = binary_capabilities()
            .into_iter()
            .filter(|cap| {
                expected_members.iter().all(|expected| {
                    reports_by_id
                        .get(expected)
                        .is_some_and(|r| r.capabilities.contains(cap))
                })
            })
            .collect();

        Self {
            active_capabilities: active,
        }
    }

    /// Standalone mode: the single local node is its own converged member.
    pub fn standalone() -> Self {
        let member = "standalone".to_string();
        let expected = [member.clone()];
        Self::from_member_reports(
            &expected,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_capabilities_contains_search_facets_v1() {
        let caps = binary_capabilities();
        assert!(caps.contains(&SEARCH_FACETS_V1.to_string()));
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
    fn test_stale_then_fresh_report_for_one_member_produces_no_active() {
        let expected = ["member-1".to_string()];
        let stale_then_fresh = CapabilityReport::from_member_reports(
            &expected,
            &[
                MemberCapabilityReport::new("member-1", binary_capabilities(), true),
                MemberCapabilityReport::new("member-1", binary_capabilities(), false),
            ],
        );
        assert!(
            !stale_then_fresh
                .active_capabilities()
                .contains(&SEARCH_FACETS_V1.to_string()),
            "a stale report followed by a fresh one must not activate"
        );

        let fresh_then_stale = CapabilityReport::from_member_reports(
            &expected,
            &[
                MemberCapabilityReport::new("member-1", binary_capabilities(), false),
                MemberCapabilityReport::new("member-1", binary_capabilities(), true),
            ],
        );
        assert!(fresh_then_stale.active_capabilities().is_empty());
    }

    #[test]
    fn test_conflicting_reports_for_one_member_produce_no_active() {
        let expected = ["member-1".to_string()];
        let conflicting = CapabilityReport::from_member_reports(
            &expected,
            &[
                MemberCapabilityReport::new("member-1", binary_capabilities(), false),
                MemberCapabilityReport::new("member-1", vec![], false),
            ],
        );
        assert!(
            !conflicting
                .active_capabilities()
                .contains(&SEARCH_FACETS_V1.to_string()),
            "two conflicting reports for one member must not activate"
        );
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
