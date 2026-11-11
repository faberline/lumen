//! The retained report (`report.json`) and the one-row-per-phase terminal summary.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Mode {
    Functional,
    Performance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum PhaseStatus {
    Passed,
    Failed,
    Missing,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct PhaseResult {
    pub(super) name: String,
    pub(super) status: PhaseStatus,
    pub(super) detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct Report {
    pub(super) mode: Mode,
    pub(super) tested_commit: String,
    pub(super) image_digest: String,
    pub(super) environment: String,
    pub(super) cpu_limit: String,
    pub(super) memory_limit: String,
    pub(super) seed: u64,
    /// Complete document operations per second. `0.0` in this slice.
    pub(super) load_achieved: f64,
    pub(super) checkpoint_count: u64,
    pub(super) merge_count: u64,
    /// Name of the first phase whose status is not `passed`, else `null`.
    pub(super) failing_phase: Option<String>,
    pub(super) cleanup_complete: bool,
    pub(super) phases: Vec<PhaseResult>,
}

/// Writes `dir/report.json` and returns its path.
pub(super) fn write_report(dir: &Path, report: &Report) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("report.json");
    let text = serde_json::to_string_pretty(report)?;
    std::fs::write(&path, text)?;
    Ok(path)
}

/// One line per phase, then the report path.
pub(super) fn summary(report: &Report, report_path: &Path) -> String {
    let mut lines: Vec<String> = report
        .phases
        .iter()
        .map(|phase| {
            let word = match phase.status {
                PhaseStatus::Passed => "passed",
                PhaseStatus::Failed => "failed",
                PhaseStatus::Missing => "missing",
            };
            format!("{:<28} {}", phase.name, word)
        })
        .collect();
    lines.push(format!("report: {}", report_path.display()));
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(phases: Vec<PhaseResult>) -> Report {
        Report {
            mode: Mode::Functional,
            tested_commit: "unknown".into(),
            image_digest: "unknown".into(),
            environment: "unknown".into(),
            cpu_limit: "unknown".into(),
            memory_limit: "unknown".into(),
            seed: 1,
            load_achieved: 0.0,
            checkpoint_count: 0,
            merge_count: 0,
            failing_phase: None,
            cleanup_complete: true,
            phases,
        }
    }

    #[test]
    fn report_has_every_required_field() {
        let value = serde_json::to_value(sample(Vec::new())).expect("serializes");
        let object = value.as_object().expect("object");
        for key in [
            "tested_commit",
            "image_digest",
            "environment",
            "cpu_limit",
            "memory_limit",
            "seed",
            "load_achieved",
            "checkpoint_count",
            "merge_count",
            "failing_phase",
        ] {
            assert!(object.contains_key(key), "missing key {key}");
        }
    }

    #[test]
    fn summary_has_one_row_per_phase_and_the_report_path() {
        let report = sample(vec![
            PhaseResult {
                name: "endpoint_configured".into(),
                status: PhaseStatus::Passed,
                detail: String::new(),
            },
            PhaseResult {
                name: "artifact:trace.json".into(),
                status: PhaseStatus::Missing,
                detail: "file not found".into(),
            },
        ]);
        let text = summary(&report, Path::new("/tmp/a/report.json"));
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[2].starts_with("report: "));
    }
}
