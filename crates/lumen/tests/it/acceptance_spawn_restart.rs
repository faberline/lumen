#![cfg(feature = "acceptance")]
//! The `lumen-acceptance` runner in `--spawn` mode and in `--endpoint` mode.

use std::process::Command;

use serde_json::Value;

fn acceptance() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lumen-acceptance"))
}

fn read_report(dir: &std::path::Path) -> Value {
    let text = std::fs::read_to_string(dir.join("report.json")).expect("report.json");
    serde_json::from_str(&text).expect("report json")
}

#[test]
fn spawn_runs_phases_and_cleans_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let status = acceptance()
        .args([
            "functional",
            "--spawn",
            env!("CARGO_BIN_EXE_lumen"),
            "--artifact-dir",
        ])
        .arg(dir.path())
        .args(["--phases", "endpoint_configured"])
        .status()
        .expect("runs");
    assert!(status.success(), "spawn run must pass");

    let report = read_report(dir.path());
    assert_eq!(report["cleanup_complete"], true);
    assert!(report["failing_phase"].is_null());
    assert!(
        !dir.path().join("spawn-data").exists(),
        "the cleanup step removes the spawn data dir"
    );
}

#[test]
fn endpoint_mode_keeps_working() {
    let dir = tempfile::tempdir().expect("tempdir");
    let status = acceptance()
        .args([
            "functional",
            "--endpoint",
            "http://127.0.0.1:9",
            "--artifact-dir",
        ])
        .arg(dir.path())
        .args(["--phases", "endpoint_configured"])
        .status()
        .expect("runs");
    assert!(status.success(), "endpoint run must pass");

    let report = read_report(dir.path());
    let phases = report["phases"].as_array().expect("phases");
    assert_eq!(phases.len(), 1);
    assert_eq!(phases[0]["name"], "endpoint_configured");
    assert_eq!(phases[0]["status"], "passed");
}

#[test]
fn spawn_fails_when_the_binary_is_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let status = acceptance()
        .args([
            "functional",
            "--spawn",
            "/nonexistent/lumen",
            "--artifact-dir",
        ])
        .arg(dir.path())
        .args(["--phases", "endpoint_configured"])
        .status()
        .expect("runs");
    assert!(!status.success(), "a missing binary must fail the run");
}

#[test]
fn spawn_restart_persistence_passes_and_cleans_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let status = acceptance()
        .args([
            "functional",
            "--spawn",
            env!("CARGO_BIN_EXE_lumen"),
            "--artifact-dir",
        ])
        .arg(dir.path())
        .args(["--phases", "restart_persistence"])
        .status()
        .expect("runs");
    assert!(status.success(), "restart_persistence must pass");

    let report = read_report(dir.path());
    let phases = report["phases"].as_array().expect("phases");
    assert_eq!(phases.len(), 1);
    assert_eq!(phases[0]["name"], "restart_persistence");
    assert_eq!(phases[0]["status"], "passed");
    assert_eq!(report["cleanup_complete"], true);
    assert!(
        !dir.path().join("spawn-data").exists(),
        "the cleanup step removes the spawn data dir"
    );
}

#[test]
fn spawn_with_the_default_phase_list_includes_restart_persistence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let status = acceptance()
        .args([
            "functional",
            "--spawn",
            env!("CARGO_BIN_EXE_lumen"),
            "--artifact-dir",
        ])
        .arg(dir.path())
        .status()
        .expect("runs");
    assert!(status.success(), "the default spawn run must pass");

    let report = read_report(dir.path());
    let phases = report["phases"].as_array().expect("phases");
    assert_eq!(phases.len(), 5);
    assert_eq!(phases[4]["name"], "restart_persistence");
    assert!(phases.iter().all(|phase| phase["status"] == "passed"));
    assert_eq!(report["cleanup_complete"], true);
}

#[test]
fn endpoint_mode_rejects_restart_persistence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let status = acceptance()
        .args([
            "functional",
            "--endpoint",
            "http://127.0.0.1:9",
            "--artifact-dir",
        ])
        .arg(dir.path())
        .args(["--phases", "restart_persistence"])
        .status()
        .expect("runs");
    assert!(
        !status.success(),
        "restart_persistence needs --spawn, so endpoint mode must fail"
    );
}
