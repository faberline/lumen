//! Selection of GKE suites for pull requests (issue #457).
//!
//! `scripts/select-gke-suites.sh` reads changed paths from stdin and prints the
//! GKE suites that they need. `ci.yml` uses it so that a pull request starts
//! only the suites its diff can affect.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

fn select(input: &str) -> String {
    let mut child = Command::new("bash")
        .arg("scripts/select-gke-suites.sh")
        .current_dir(root())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn scripts/select-gke-suites.sh");
    child
        .stdin
        .take()
        .expect("stdin of the selector")
        .write_all(input.as_bytes())
        .expect("write paths to the selector");
    let out = child.wait_with_output().expect("wait for the selector");
    assert!(
        out.status.success(),
        "selector failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .expect("selector stdout is UTF-8")
        .trim()
        .to_string()
}

#[test]
fn docs_only_selects_nothing() {
    assert_eq!(
        select("docs/gke.md\nREADME.md\nclients/openapi.json\n"),
        "[]"
    );
}

#[test]
fn operator_area_selects_operator() {
    assert_eq!(
        select("crates/lumen-operator/src/lib.rs\nk8s/operator/crd.yaml\n"),
        r#"["operator"]"#
    );
}

#[test]
fn overlay_area_selects_overlay() {
    assert_eq!(
        select(
            "k8s/gke-test-job/overlay/kustomization.yaml\ncrates/lumen/tests/it/e2e/overlay.rs\n"
        ),
        r#"["overlay"]"#
    );
}

#[test]
fn operator_and_overlay_select_both() {
    assert_eq!(
        select("crates/lumen-operator/src/lib.rs\nk8s/gke-test-job/overlay/kustomization.yaml\n"),
        r#"["overlay","operator"]"#
    );
}

#[test]
fn engine_change_selects_both() {
    assert_eq!(
        select("crates/lumen-index/src/lib.rs\n"),
        r#"["overlay","operator"]"#
    );
}

#[test]
fn workflow_change_selects_both() {
    assert_eq!(
        select(".github/workflows/ci.yml\n"),
        r#"["overlay","operator"]"#
    );
}

#[test]
fn unknown_path_selects_both() {
    assert_eq!(select("somewhere/new.file\n"), r#"["overlay","operator"]"#);
}

#[test]
fn selector_script_change_selects_both() {
    assert_eq!(
        select("scripts/select-gke-suites.sh\n"),
        r#"["overlay","operator"]"#
    );
}

#[test]
fn empty_input_selects_both() {
    assert_eq!(select(""), r#"["overlay","operator"]"#);
}

#[test]
fn docs_plus_cargo_lock_is_not_skipped() {
    // Negative control: a selector that ignores everything would print [].
    assert_eq!(
        select("docs/gke.md\nCargo.lock\n"),
        r#"["overlay","operator"]"#
    );
}

/// Text of `ci` between the line `from` and the line `to` (both must occur).
fn job_text<'a>(ci: &'a str, from: &str, to: &str) -> &'a str {
    let start = ci
        .find(from)
        .unwrap_or_else(|| panic!("no {from:?} in ci.yml"));
    let rest = &ci[start..];
    let end = rest
        .find(to)
        .unwrap_or_else(|| panic!("no {to:?} after {from:?} in ci.yml"));
    &rest[..end]
}

#[test]
fn ci_yml_uses_the_selection() {
    let ci = std::fs::read_to_string(root().join(".github/workflows/ci.yml"))
        .expect("read .github/workflows/ci.yml");
    assert!(ci.contains("scripts/select-gke-suites.sh"));
    assert!(ci.contains("steps.select.outputs.suites"));
    assert!(ci.contains("needs.e2e-client.outputs.suites != '[]'"));

    let client = job_text(&ci, "\n  e2e-client:\n", "\n  e2e-gke:\n");
    assert!(client.contains("pull-requests: read"));
    assert!(client.contains("packages: write"));

    let gke = job_text(&ci, "\n  e2e-gke:\n", "\n  e2e-native:\n");
    assert!(gke.contains("if: ${{ needs.e2e-client.outputs.suites != '[]' }}"));
}
