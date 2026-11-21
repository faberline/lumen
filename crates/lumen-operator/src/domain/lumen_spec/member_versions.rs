//! Member versions of a `Lumen` fleet (#248 slice 1a): a pure summary of the
//! version reports the members sent, for the status subresource. No I/O.

use std::collections::BTreeMap;

use crate::domain::lumen_spec::status::LumenMemberStatus;

/// A member report older than this many seconds is `Stale`: still listed with
/// its last values, but never treated as current.
pub const MEMBER_REPORT_STALE_AFTER_SECS: u64 = 120;

/// One member's version report, as cached by the reconcile loop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberVersionReport {
    pub name: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub compatibility_version: String,
    pub observed_at_secs: u64,
}

/// The per-member list and the effective compatibility version of a fleet.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct MembersSummary {
    pub members: Vec<LumenMemberStatus>,
    pub effective_compatibility_version: Option<String>,
}

/// Summarize the members named in `expected`, in that order.
///
/// The effective compatibility version is the lowest `compatibility_version`
/// among the members that have a report (`Reported` or `Stale`), so a missing
/// or stale member never raises it. Any unparsable version makes it `None`.
pub fn summarize_members(
    expected: &[String],
    reports: &[MemberVersionReport],
    now_secs: u64,
) -> MembersSummary {
    // The newest report per name wins.
    let mut newest: BTreeMap<&str, &MemberVersionReport> = BTreeMap::new();
    for report in reports {
        let replace = match newest.get(report.name.as_str()) {
            Some(current) => report.observed_at_secs > current.observed_at_secs,
            None => true,
        };
        if replace {
            newest.insert(report.name.as_str(), report);
        }
    }

    let mut members = Vec::with_capacity(expected.len());
    let mut lowest: Option<(Vec<u64>, &str)> = None;
    let mut unparsable = false;
    for name in expected {
        let Some(report) = newest.get(name.as_str()) else {
            members.push(LumenMemberStatus {
                name: name.clone(),
                version: String::new(),
                capabilities: Vec::new(),
                compatibility_version: String::new(),
                state: "Missing".to_string(),
            });
            continue;
        };
        let stale =
            now_secs.saturating_sub(report.observed_at_secs) > MEMBER_REPORT_STALE_AFTER_SECS;
        members.push(LumenMemberStatus {
            name: name.clone(),
            version: report.version.clone(),
            capabilities: report.capabilities.clone(),
            compatibility_version: report.compatibility_version.clone(),
            state: if stale { "Stale" } else { "Reported" }.to_string(),
        });
        match parse_dotted(&report.compatibility_version) {
            Some(parts) => {
                let keep = match &lowest {
                    Some((best, _)) => parts < *best,
                    None => true,
                };
                if keep {
                    lowest = Some((parts, report.compatibility_version.as_str()));
                }
            }
            None => unparsable = true,
        }
    }

    let effective_compatibility_version = if unparsable {
        None
    } else {
        lowest.map(|(_, version)| version.to_string())
    };
    MembersSummary {
        members,
        effective_compatibility_version,
    }
}

/// `"0.10"` becomes `[0, 10]`, so it compares above `"0.9"`.
fn parse_dotted(value: &str) -> Option<Vec<u64>> {
    value
        .split('.')
        .map(|part| part.parse::<u64>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_000_000;

    fn report(
        name: &str,
        version: &str,
        compat: &str,
        observed_at_secs: u64,
    ) -> MemberVersionReport {
        MemberVersionReport {
            name: name.to_string(),
            version: version.to_string(),
            capabilities: vec!["search".to_string()],
            compatibility_version: compat.to_string(),
            observed_at_secs,
        }
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn mixed_n_and_n_minus_one_lists_both_versions() {
        let reports = vec![
            report("s-0", "0.7.0", "0.2", NOW),
            report("s-1", "0.6.9", "0.1", NOW),
        ];
        let summary = summarize_members(&names(&["s-0", "s-1"]), &reports, NOW);
        assert_eq!(summary.members.len(), 2);
        assert!(summary.members.iter().all(|m| m.state == "Reported"));
        assert_eq!(summary.members[0].version, "0.7.0");
        assert_eq!(summary.members[1].version, "0.6.9");
        assert_eq!(
            summary.effective_compatibility_version,
            Some("0.1".to_string())
        );
    }

    #[test]
    fn missing_report_keeps_effective_version_at_the_lower_value() {
        let reports = vec![report("s-0", "0.7.0", "0.2", NOW)];
        let summary = summarize_members(&names(&["s-0", "s-1"]), &reports, NOW);
        let missing = &summary.members[1];
        assert_eq!(missing.name, "s-1");
        assert_eq!(missing.state, "Missing");
        assert_eq!(missing.version, "");
        assert_eq!(missing.compatibility_version, "");
        assert!(missing.capabilities.is_empty());
        assert_eq!(
            summary.effective_compatibility_version,
            Some("0.2".to_string())
        );
    }

    #[test]
    fn stale_report_stays_listed_and_keeps_its_lower_value() {
        let reports = vec![
            report("s-0", "0.7.0", "0.2", NOW),
            report(
                "s-1",
                "0.6.9",
                "0.1",
                NOW - MEMBER_REPORT_STALE_AFTER_SECS - 1,
            ),
        ];
        let summary = summarize_members(&names(&["s-0", "s-1"]), &reports, NOW);
        let stale = &summary.members[1];
        assert_eq!(stale.state, "Stale");
        assert_eq!(stale.version, "0.6.9");
        assert_eq!(stale.compatibility_version, "0.1");
        assert_eq!(
            summary.effective_compatibility_version,
            Some("0.1".to_string())
        );
    }

    #[test]
    fn dotted_versions_compare_numerically() {
        let reports = vec![
            report("s-0", "1.0.0", "0.10", NOW),
            report("s-1", "1.0.0", "0.9", NOW),
        ];
        let summary = summarize_members(&names(&["s-0", "s-1"]), &reports, NOW);
        assert_eq!(
            summary.effective_compatibility_version,
            Some("0.9".to_string())
        );
    }

    #[test]
    fn unparsable_compatibility_version_gives_no_effective_version() {
        let reports = vec![report("s-0", "0.7.0", "abc", NOW)];
        let summary = summarize_members(&names(&["s-0"]), &reports, NOW);
        assert_eq!(summary.effective_compatibility_version, None);
    }

    #[test]
    fn no_reports_gives_no_effective_version() {
        let summary = summarize_members(&names(&["s-0", "s-1"]), &[], NOW);
        assert!(summary.members.iter().all(|m| m.state == "Missing"));
        assert_eq!(summary.effective_compatibility_version, None);
    }

    #[test]
    fn duplicate_reports_use_the_newest() {
        let reports = vec![
            report("s-0", "0.6.0", "0.1", NOW - 5),
            report("s-0", "0.7.0", "0.2", NOW),
        ];
        let summary = summarize_members(&names(&["s-0"]), &reports, NOW);
        assert_eq!(summary.members[0].version, "0.7.0");
        assert_eq!(
            summary.effective_compatibility_version,
            Some("0.2".to_string())
        );
    }
}
