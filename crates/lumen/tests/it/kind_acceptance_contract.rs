//! The Kind overlay acceptance script keeps its teardown, its single image
//! build, its durable overlay shape and its report check (#570).

use std::path::{Path, PathBuf};
use std::process::Command;

const SCRIPT: &str = "scripts/kind-acceptance.sh";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../")).join(relative)
}

fn repo_file(relative: &str) -> String {
    let path = repo_path(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn script_has_guaranteed_teardown_and_one_build() {
    let text = repo_file(SCRIPT);
    for needle in [
        "trap cleanup EXIT",
        "kind delete cluster",
        "kind load docker-image",
        "--restart-cmd",
        "kind get clusters",
        "--features acceptance",
    ] {
        assert!(text.contains(needle), "{SCRIPT} must contain `{needle}`");
    }
    assert_eq!(
        text.matches("docker build").count(),
        1,
        "{SCRIPT} must build the image exactly once"
    );
    assert!(
        !text.contains("--spawn"),
        "{SCRIPT} must not use --spawn: the cluster owns the process"
    );
}

#[test]
fn script_passes_bash_syntax_check() {
    let status = Command::new("bash")
        .arg("-n")
        .arg(repo_path(SCRIPT))
        .status()
        .expect("bash runs");
    assert!(status.success(), "bash -n {SCRIPT} must pass");

    // Negative control: the syntax check must really reject broken shell.
    let bad = std::env::temp_dir().join("kind_acceptance_bad.sh");
    std::fs::write(&bad, "if then\n").expect("write bad script");
    let bad_status = Command::new("bash")
        .arg("-n")
        .arg(&bad)
        .status()
        .expect("bash runs");
    let _ = std::fs::remove_file(&bad);
    assert!(!bad_status.success(), "bash -n must reject `if then`");
}

#[test]
fn script_fails_when_a_required_phase_is_absent() {
    let text = repo_file(SCRIPT);
    for phase in ["restart_persistence", "checkpoint_and_merge"] {
        assert!(
            text.contains(phase),
            "{SCRIPT} must require the report phase `{phase}`"
        );
    }
    assert!(
        text.contains(r#"index("restart_persistence")"#),
        "{SCRIPT} must check the phase names with jq index()"
    );
}

#[test]
fn overlay_has_the_durable_shape() {
    let base = "k8s/overlays/kind-acceptance";

    let kustomization = repo_file(&format!("{base}/kustomization.yaml"));
    assert!(
        kustomization.contains("../dev"),
        "overlay must build on ../dev"
    );
    assert!(
        kustomization.contains("REPLACE_ME"),
        "the tracked overlay keeps REPLACE_ME; the script sets the tag"
    );

    let deployment = repo_file(&format!("{base}/lumen-deployment.yaml"));
    for needle in ["LUMEN_WAL", "embedded", "/var/lib/lumen/data"] {
        assert!(
            deployment.contains(needle),
            "lumen-deployment.yaml must contain `{needle}`"
        );
    }
    assert!(
        !deployment.contains("LUMEN_AUTH"),
        "../dev already adds LUMEN_AUTH; the patch must not add it again"
    );

    let pvc = repo_file(&format!("{base}/data-pvc.yaml"));
    assert!(
        pvc.contains("lumen-data"),
        "data-pvc.yaml must name lumen-data"
    );
    assert!(
        !pvc.contains("storageClassName"),
        "Kind uses its default storage class; do not name dynamic-rwo"
    );
}

#[test]
fn operator_mode_is_refused_before_any_cluster_work() {
    let output = Command::new("bash")
        .arg(repo_path(SCRIPT))
        .args(["--mode", "operator"])
        .current_dir(repo_path(""))
        .output()
        .expect("bash runs");
    assert_eq!(
        output.status.code(),
        Some(2),
        "operator mode must exit 2; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("is not supported yet"),
        "stderr must say the mode is not supported yet, got: {stderr}"
    );
}
