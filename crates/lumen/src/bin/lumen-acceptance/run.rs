//! Runs the phase list, checks the required artifacts and the cleanup step, then
//! writes the report.

use std::path::Path;

use anyhow::{bail, Result};

use crate::cli::RunArgs;
use crate::lifecycle::CollectionLifecycle;
use crate::report::{summary, write_report, Mode, PhaseResult, PhaseStatus, Report};

pub(super) trait Phase {
    fn name(&self) -> &str;
    /// A phase passes when this returns `Ok`.
    fn run(&self, endpoint: &str, artifact_dir: &Path) -> Result<()>;
}

pub(super) trait Cleanup {
    fn run(&self) -> Result<()>;
}

pub(super) struct Outcome {
    pub(super) report: Report,
    pub(super) passed: bool,
}

pub(super) fn run_phases(
    mode: Mode,
    args: &RunArgs,
    phases: &[Box<dyn Phase>],
    required_artifacts: &[&str],
    cleanup: &dyn Cleanup,
) -> Result<Outcome> {
    let mut results: Vec<PhaseResult> = Vec::new();
    for phase in phases {
        let result = match phase.run(&args.endpoint, &args.artifact_dir) {
            Ok(()) => PhaseResult {
                name: phase.name().to_string(),
                status: PhaseStatus::Passed,
                detail: String::new(),
            },
            Err(err) => PhaseResult {
                name: phase.name().to_string(),
                status: PhaseStatus::Failed,
                detail: format!("{err:#}"),
            },
        };
        results.push(result);
    }

    let cleanup_complete = cleanup.run().is_ok();

    for name in required_artifacts {
        if !args.artifact_dir.join(name).is_file() {
            results.push(PhaseResult {
                name: format!("artifact:{name}"),
                status: PhaseStatus::Missing,
                detail: "file not found".to_string(),
            });
        }
    }

    let failing_phase = results
        .iter()
        .find(|result| result.status != PhaseStatus::Passed)
        .map(|result| result.name.clone());
    let passed = failing_phase.is_none() && cleanup_complete;

    Ok(Outcome {
        report: Report {
            mode,
            tested_commit: args.commit.clone(),
            image_digest: args.image_digest.clone(),
            environment: args.environment.clone(),
            cpu_limit: args.cpu_limit.clone(),
            memory_limit: args.memory_limit.clone(),
            seed: args.seed,
            load_achieved: 0.0,
            // Real phases fill these counters in later slices.
            checkpoint_count: 0,
            merge_count: 0,
            failing_phase,
            cleanup_complete,
            phases: results,
        },
        passed,
    })
}

/// The one built-in phase of this slice: the endpoint argument is set.
struct EndpointConfigured;

impl Phase for EndpointConfigured {
    fn name(&self) -> &str {
        "endpoint_configured"
    }

    fn run(&self, endpoint: &str, _artifact_dir: &Path) -> Result<()> {
        if endpoint.trim().is_empty() {
            bail!("--endpoint is empty");
        }
        Ok(())
    }
}

/// This slice has nothing to clean up, so cleanup always completes.
struct NoCleanup;

impl Cleanup for NoCleanup {
    fn run(&self) -> Result<()> {
        Ok(())
    }
}

/// The default phase names of a mode.
fn default_phase_names(mode: Mode) -> Vec<&'static str> {
    match mode {
        Mode::Functional => vec!["endpoint_configured", "collection_lifecycle"],
        Mode::Performance => vec!["endpoint_configured"],
    }
}

/// The phase list for a run: the mode's default list, or the names given by `--phases`.
fn phase_list(mode: Mode, args: &RunArgs) -> Result<Vec<Box<dyn Phase>>> {
    let names: Vec<String> = match &args.phases {
        None => default_phase_names(mode)
            .into_iter()
            .map(str::to_string)
            .collect(),
        Some(raw) => raw
            .split(',')
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect(),
    };
    if names.is_empty() {
        bail!("--phases names no phase; supported: endpoint_configured, collection_lifecycle, performance_thresholds");
    }
    names
        .iter()
        .map(|name| match name.as_str() {
            "endpoint_configured" => Ok(Box::new(EndpointConfigured) as Box<dyn Phase>),
            "performance_thresholds" => Ok(Box::new(crate::perf_gate::PerformanceThresholds {
                expected_commit: args.commit.clone(),
            }) as Box<dyn Phase>),
            "collection_lifecycle" => Ok(Box::new(CollectionLifecycle) as Box<dyn Phase>),
            other => bail!(
                "unknown phase `{other}`; supported: endpoint_configured, collection_lifecycle, performance_thresholds"
            ),
        })
        .collect()
}

/// Runs one mode, writes `report.json` and prints the summary. Returns whether the run passed.
pub(super) fn execute(mode: Mode, args: RunArgs) -> Result<bool> {
    let phases = phase_list(mode, &args)?;
    let outcome = run_phases(mode, &args, &phases, &[], &NoCleanup)?;
    let report_path = write_report(&args.artifact_dir, &outcome.report)?;
    println!("{}", summary(&outcome.report, &report_path));
    Ok(outcome.passed)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakePhase {
        name: &'static str,
        ok: bool,
    }

    impl Phase for FakePhase {
        fn name(&self) -> &str {
            self.name
        }
        fn run(&self, _endpoint: &str, _artifact_dir: &Path) -> Result<()> {
            if self.ok {
                Ok(())
            } else {
                bail!("fake failure")
            }
        }
    }

    struct FakeCleanup {
        ok: bool,
    }

    impl Cleanup for FakeCleanup {
        fn run(&self) -> Result<()> {
            if self.ok {
                Ok(())
            } else {
                bail!("cleanup failure")
            }
        }
    }

    fn phases(items: &[(&'static str, bool)]) -> Vec<Box<dyn Phase>> {
        items
            .iter()
            .map(|&(name, ok)| Box::new(FakePhase { name, ok }) as Box<dyn Phase>)
            .collect()
    }

    fn args(dir: &Path) -> RunArgs {
        RunArgs {
            endpoint: "http://fake.invalid".into(),
            artifact_dir: dir.to_path_buf(),
            commit: "unknown".into(),
            image_digest: "unknown".into(),
            environment: "unknown".into(),
            cpu_limit: "unknown".into(),
            memory_limit: "unknown".into(),
            seed: 1,
            phases: None,
        }
    }

    #[test]
    fn passing_phase_list_exits_zero() {
        let dir = tempfile::tempdir().expect("tempdir");
        let list = phases(&[("a", true), ("b", true)]);
        let outcome = run_phases(
            Mode::Functional,
            &args(dir.path()),
            &list,
            &[],
            &FakeCleanup { ok: true },
        )
        .expect("runs");
        assert!(outcome.passed);
        assert_eq!(outcome.report.failing_phase, None);
        assert_eq!(outcome.report.phases.len(), 2);
        assert!(outcome
            .report
            .phases
            .iter()
            .all(|p| p.status == PhaseStatus::Passed));
        assert!(outcome.report.cleanup_complete);
    }

    #[test]
    fn failing_phase_is_named_and_later_phases_still_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let list = phases(&[("a", true), ("b", false), ("c", true)]);
        let outcome = run_phases(
            Mode::Functional,
            &args(dir.path()),
            &list,
            &[],
            &FakeCleanup { ok: true },
        )
        .expect("runs");
        assert!(!outcome.passed);
        assert_eq!(outcome.report.failing_phase.as_deref(), Some("b"));
        assert_eq!(outcome.report.phases.len(), 3);
        assert_eq!(outcome.report.phases[2].name, "c");
        assert_eq!(outcome.report.phases[2].status, PhaseStatus::Passed);
        assert!(outcome.report.phases[1].detail.contains("fake failure"));
    }

    #[test]
    fn missing_artifact_fails_the_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let list = phases(&[("a", true)]);
        let outcome = run_phases(
            Mode::Functional,
            &args(dir.path()),
            &list,
            &["trace.json"],
            &FakeCleanup { ok: true },
        )
        .expect("runs");
        assert!(!outcome.passed);
        assert_eq!(
            outcome.report.failing_phase.as_deref(),
            Some("artifact:trace.json")
        );
        let missing = outcome.report.phases.last().expect("artifact result");
        assert_eq!(missing.status, PhaseStatus::Missing);

        // Negative check in the same test: with the file present, the run passes.
        std::fs::write(dir.path().join("trace.json"), b"{}").expect("write artifact");
        let outcome = run_phases(
            Mode::Functional,
            &args(dir.path()),
            &list,
            &["trace.json"],
            &FakeCleanup { ok: true },
        )
        .expect("runs");
        assert!(outcome.passed);
    }

    #[test]
    fn incomplete_cleanup_fails_the_run() {
        let dir = tempfile::tempdir().expect("tempdir");
        let list = phases(&[("a", true)]);
        let outcome = run_phases(
            Mode::Functional,
            &args(dir.path()),
            &list,
            &[],
            &FakeCleanup { ok: false },
        )
        .expect("runs");
        assert!(!outcome.passed);
        assert!(!outcome.report.cleanup_complete);
    }

    #[test]
    fn functional_default_list_has_both_phases() {
        assert_eq!(
            default_phase_names(Mode::Functional),
            ["endpoint_configured", "collection_lifecycle"]
        );
    }

    #[test]
    fn execute_writes_report_json_with_all_fields() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut run_args = args(dir.path());
        run_args.phases = Some("endpoint_configured".into());
        let ok = execute(Mode::Functional, run_args).expect("executes");
        assert!(ok);
        let text = std::fs::read_to_string(dir.path().join("report.json")).expect("report.json");
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(value["mode"], "functional");
        assert_eq!(value["tested_commit"], "unknown");
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
            assert!(value.get(key).is_some(), "missing key {key}");
        }
    }

    #[test]
    fn empty_endpoint_fails_the_built_in_phase() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut run_args = args(dir.path());
        run_args.endpoint = "  ".into();
        let ok = execute(Mode::Performance, run_args).expect("executes");
        assert!(!ok);
        let text = std::fs::read_to_string(dir.path().join("report.json")).expect("report.json");
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(value["failing_phase"], "endpoint_configured");
    }

    #[test]
    fn performance_thresholds_phase_is_wired() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut run_args = args(dir.path());
        run_args.phases = Some("performance_thresholds".into());
        run_args.commit = "abc1234".into();
        let ok = execute(Mode::Performance, run_args.clone()).expect("executes");
        assert!(!ok, "no evidence file, so the run must fail");
        let text = std::fs::read_to_string(dir.path().join("report.json")).expect("report.json");
        let value: serde_json::Value = serde_json::from_str(&text).expect("json");
        assert_eq!(value["failing_phase"], "performance_thresholds");

        let good = include_str!("fixtures/perf_evidence_good.json");
        std::fs::write(dir.path().join("perf-evidence.json"), good).expect("write");
        let ok = execute(Mode::Performance, run_args).expect("executes");
        assert!(ok, "the good fixture must pass");
    }

    #[test]
    fn unknown_phase_name_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut run_args = args(dir.path());
        run_args.phases = Some("nope".into());
        let err = execute(Mode::Functional, run_args).expect_err("unknown phase");
        assert!(format!("{err:#}").contains("unknown phase"));
    }
}
