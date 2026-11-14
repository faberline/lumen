//! The performance release thresholds of the lumen-acceptance runner (#264).
//! The 30-minute run itself is not here: this phase reads the evidence file that
//! the performance run writes to `--artifact-dir` and fails when a threshold is missed.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::run::Phase;

pub(super) const EVIDENCE_FILE: &str = "perf-evidence.json";

/// What one release case measured. Written by the performance run, read here.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(super) struct Evidence {
    pub(super) commit: String,
    pub(super) cpu_limit: String,
    pub(super) memory_limit: String,
    pub(super) duration_s: f64,
    pub(super) requested_document_ops_per_s: f64,
    pub(super) document_ops_per_s: f64,
    pub(super) combined_qps: f64,
    pub(super) p99_ms: f64,
    pub(super) max_request_ms: f64,
    pub(super) errors: u64,
    pub(super) timeouts: u64,
    pub(super) drain_s: f64,
    pub(super) peak_rss_bytes: u64,
    pub(super) checkpoint_count: u64,
    pub(super) merge_count: u64,
}

#[derive(Debug, Clone)]
pub(super) struct Thresholds {
    pub(super) duration_s: f64,
    pub(super) cpu_limit: &'static str,
    pub(super) memory_limit: &'static str,
    pub(super) min_combined_qps: f64,
    pub(super) max_p99_ms: f64,
    pub(super) max_request_ms: f64,
    pub(super) min_document_ops_per_s: f64,
    pub(super) min_load_fraction: f64,
    pub(super) max_drain_s: f64,
    pub(super) max_peak_rss_bytes: u64,
}

impl Thresholds {
    /// The release values.
    pub(super) fn release() -> Self {
        Thresholds {
            duration_s: 1800.0,
            cpu_limit: "2.5",
            memory_limit: "16Gi",
            min_combined_qps: 10.0,
            max_p99_ms: 1000.0,
            max_request_ms: 5000.0,
            min_document_ops_per_s: 100.0,
            min_load_fraction: 0.95,
            max_drain_s: 60.0,
            max_peak_rss_bytes: 12 * 1024 * 1024 * 1024,
        }
    }
}

/// Every threshold that `e` misses, one readable line each. Empty means it passes.
/// `expected_commit` is the `--commit` of this run; a different `e.commit` is stale.
pub(super) fn evaluate(e: &Evidence, t: &Thresholds, expected_commit: &str) -> Vec<String> {
    let mut misses = Vec::new();
    if e.commit != expected_commit {
        misses.push(format!(
            "stale: result is for {}, run is for {expected_commit}",
            e.commit
        ));
    }
    if e.duration_s < t.duration_s {
        misses.push(format!(
            "duration: {}s is below {}s",
            e.duration_s, t.duration_s
        ));
    }
    if e.cpu_limit != t.cpu_limit || e.memory_limit != t.memory_limit {
        misses.push(format!(
            "limits: cpu {} and memory {} must be {} and {}",
            e.cpu_limit, e.memory_limit, t.cpu_limit, t.memory_limit
        ));
    }
    if e.combined_qps < t.min_combined_qps {
        misses.push(format!(
            "qps: {} is below {}",
            e.combined_qps, t.min_combined_qps
        ));
    }
    if e.p99_ms > t.max_p99_ms {
        misses.push(format!("p99: {} ms is above {} ms", e.p99_ms, t.max_p99_ms));
    }
    if e.max_request_ms > t.max_request_ms {
        misses.push(format!(
            "slow request: {} ms is above {} ms",
            e.max_request_ms, t.max_request_ms
        ));
    }
    if e.document_ops_per_s < t.min_document_ops_per_s {
        misses.push(format!(
            "document ops: {} per s is below {}",
            e.document_ops_per_s, t.min_document_ops_per_s
        ));
    }
    if e.document_ops_per_s < t.min_load_fraction * e.requested_document_ops_per_s {
        misses.push(format!(
            "load: {} per s is below {} of the requested {} per s",
            e.document_ops_per_s, t.min_load_fraction, e.requested_document_ops_per_s
        ));
    }
    if e.errors > 0 {
        misses.push(format!("errors: {}", e.errors));
    }
    if e.timeouts > 0 {
        misses.push(format!("timeouts: {}", e.timeouts));
    }
    if e.drain_s > t.max_drain_s {
        misses.push(format!(
            "drain: {} s is above {} s",
            e.drain_s, t.max_drain_s
        ));
    }
    if e.peak_rss_bytes > t.max_peak_rss_bytes {
        misses.push(format!(
            "rss: {} bytes is above {} bytes",
            e.peak_rss_bytes, t.max_peak_rss_bytes
        ));
    }
    if e.checkpoint_count == 0 {
        misses.push("no checkpoint: none observed in counters or events".to_string());
    }
    if e.merge_count == 0 {
        misses.push("no merge: none observed in counters or events".to_string());
    }
    misses
}

pub(super) struct PerformanceThresholds {
    pub(super) expected_commit: String,
}

impl Phase for PerformanceThresholds {
    fn name(&self) -> &str {
        "performance_thresholds"
    }

    fn run(&self, _endpoint: &str, artifact_dir: &Path) -> Result<()> {
        let path = artifact_dir.join(EVIDENCE_FILE);
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("missing artifact {EVIDENCE_FILE}"))?;
        let evidence: Evidence = serde_json::from_str(&text)
            .with_context(|| format!("{EVIDENCE_FILE} is not valid evidence"))?;
        let misses = evaluate(&evidence, &Thresholds::release(), &self.expected_commit);
        if !misses.is_empty() {
            bail!("{}", misses.join("; "));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = include_str!("fixtures/perf_evidence_good.json");
    const GIB: u64 = 1024 * 1024 * 1024;

    fn good() -> Evidence {
        serde_json::from_str(GOOD).expect("fixture parses")
    }

    fn failures(e: &Evidence) -> Vec<String> {
        evaluate(e, &Thresholds::release(), "abc1234")
    }

    fn assert_only(e: &Evidence, prefix: &str) {
        let lines = failures(e);
        assert_eq!(lines.len(), 1, "expected one failure, got {lines:?}");
        assert!(
            lines[0].starts_with(prefix),
            "expected prefix `{prefix}`, got `{}`",
            lines[0]
        );
    }

    #[test]
    fn good_fixture_passes() {
        assert!(failures(&good()).is_empty(), "{:?}", failures(&good()));
    }

    #[test]
    fn short_seeded_run_passes_with_short_thresholds() {
        let mut e = good();
        e.duration_s = 2.0;
        let t = Thresholds {
            duration_s: 2.0,
            ..Thresholds::release()
        };
        assert!(evaluate(&e, &t, "abc1234").is_empty());
    }

    #[test]
    fn release_values_are_exact() {
        let t = Thresholds::release();
        assert_eq!(t.duration_s, 1800.0);
        assert_eq!(t.cpu_limit, "2.5");
        assert_eq!(t.memory_limit, "16Gi");
        assert_eq!(t.min_combined_qps, 10.0);
        assert_eq!(t.max_p99_ms, 1000.0);
        assert_eq!(t.max_request_ms, 5000.0);
        assert_eq!(t.min_document_ops_per_s, 100.0);
        assert_eq!(t.min_load_fraction, 0.95);
        assert_eq!(t.max_drain_s, 60.0);
        assert_eq!(t.max_peak_rss_bytes, 12 * GIB);
    }

    #[test]
    fn low_qps_fails() {
        let mut e = good();
        e.combined_qps = 9.9;
        assert_only(&e, "qps:");
    }

    #[test]
    fn high_p99_fails() {
        let mut e = good();
        e.p99_ms = 1000.1;
        assert_only(&e, "p99:");
    }

    #[test]
    fn slow_request_fails() {
        let mut e = good();
        e.max_request_ms = 5000.1;
        assert_only(&e, "slow request:");
    }

    #[test]
    fn low_document_ops_fails() {
        // The reduced input rate case: the load is fully served but too small.
        let mut e = good();
        e.requested_document_ops_per_s = 90.0;
        e.document_ops_per_s = 90.0;
        assert_only(&e, "document ops:");
    }

    #[test]
    fn under_95_percent_of_requested_fails() {
        let mut e = good();
        e.requested_document_ops_per_s = 200.0;
        e.document_ops_per_s = 150.0;
        assert_only(&e, "load:");
    }

    #[test]
    fn any_error_fails() {
        let mut e = good();
        e.errors = 1;
        assert_only(&e, "errors:");
    }

    #[test]
    fn any_timeout_fails() {
        let mut e = good();
        e.timeouts = 1;
        assert_only(&e, "timeouts:");
    }

    #[test]
    fn long_drain_fails() {
        let mut e = good();
        e.drain_s = 60.1;
        assert_only(&e, "drain:");
    }

    #[test]
    fn high_rss_fails() {
        let mut e = good();
        e.peak_rss_bytes = 12 * GIB + 1;
        assert_only(&e, "rss:");
    }

    #[test]
    fn short_duration_fails() {
        let mut e = good();
        e.duration_s = 1799.0;
        assert_only(&e, "duration:");
    }

    #[test]
    fn wrong_limits_fail() {
        let mut e = good();
        e.cpu_limit = "2".into();
        assert_only(&e, "limits:");
    }

    #[test]
    fn disabled_checkpoint_fails() {
        let mut e = good();
        e.checkpoint_count = 0;
        assert_only(&e, "no checkpoint:");
    }

    #[test]
    fn disabled_merge_fails() {
        let mut e = good();
        e.merge_count = 0;
        assert_only(&e, "no merge:");
    }

    #[test]
    fn stale_result_fails() {
        let lines = evaluate(&good(), &Thresholds::release(), "other999");
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].starts_with("stale:"), "{lines:?}");
    }

    #[test]
    fn boundary_values_pass() {
        let mut e = good();
        e.combined_qps = 10.0;
        e.p99_ms = 1000.0;
        e.max_request_ms = 5000.0;
        e.requested_document_ops_per_s = 100.0;
        e.document_ops_per_s = 100.0;
        e.drain_s = 60.0;
        e.peak_rss_bytes = 12 * GIB;
        assert!(failures(&e).is_empty(), "{:?}", failures(&e));
    }

    #[test]
    fn phase_passes_with_good_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(EVIDENCE_FILE), GOOD).expect("write");
        let phase = PerformanceThresholds {
            expected_commit: "abc1234".into(),
        };
        assert!(phase.run("http://x", dir.path()).is_ok());
    }

    #[test]
    fn phase_fails_on_missing_artifact() {
        let dir = tempfile::tempdir().expect("tempdir");
        let phase = PerformanceThresholds {
            expected_commit: "abc1234".into(),
        };
        let err = phase.run("http://x", dir.path()).expect_err("missing");
        assert!(format!("{err:#}").contains("perf-evidence.json"), "{err:#}");
    }

    #[test]
    fn phase_fails_on_invalid_json() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join(EVIDENCE_FILE), "{}").expect("write");
        let phase = PerformanceThresholds {
            expected_commit: "abc1234".into(),
        };
        let err = phase.run("http://x", dir.path()).expect_err("invalid");
        assert!(format!("{err:#}").contains("not valid evidence"), "{err:#}");
    }

    #[test]
    fn phase_error_lists_the_misses() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut value: serde_json::Value = serde_json::from_str(GOOD).expect("json");
        value["errors"] = serde_json::json!(3);
        value["merge_count"] = serde_json::json!(0);
        std::fs::write(dir.path().join(EVIDENCE_FILE), value.to_string()).expect("write");
        let phase = PerformanceThresholds {
            expected_commit: "abc1234".into(),
        };
        let text = format!(
            "{:#}",
            phase.run("http://x", dir.path()).expect_err("misses")
        );
        assert!(text.contains("errors:"), "{text}");
        assert!(text.contains("no merge:"), "{text}");
    }
}
