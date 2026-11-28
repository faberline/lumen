//! The Kind acceptance script keeps its teardown, its single image build, its
//! durable overlay shape, its report check (#570), and its overlay and
//! operator modes with the combined report (#571). No test here needs a
//! network or a cluster.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const SCRIPT: &str = "scripts/kind-acceptance.sh";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../")).join(relative)
}

fn repo_file(relative: &str) -> String {
    let path = repo_path(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Runs `--combine-only` on canned runner reports. `operator` is the text of
/// the operator runner report file (`null` means a skipped mode), `topology`
/// the text of `<dir>/operator/topology.json` when it is written.
fn run_combine(
    overlay: &str,
    operator: &str,
    topology: Option<&str>,
    tag: &str,
) -> (Option<i32>, Value) {
    if Command::new("jq").arg("--version").output().is_err() {
        panic!("jq is required");
    }
    let dir = std::env::temp_dir().join(format!("kind_acceptance_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("operator")).expect("create the artifact dir");
    std::fs::write(dir.join("overlay.json"), overlay).expect("write the overlay report");
    std::fs::write(dir.join("operator.json"), operator).expect("write the operator report");
    if let Some(text) = topology {
        std::fs::write(dir.join("operator/topology.json"), text).expect("write the topology");
    }
    let output = Command::new("bash")
        .arg(repo_path(SCRIPT))
        .arg("--combine-only")
        .arg(dir.join("overlay.json"))
        .arg(dir.join("operator.json"))
        .arg("--artifact-dir")
        .arg(&dir)
        .current_dir(repo_path(""))
        .output()
        .expect("bash runs");
    let report_text = std::fs::read_to_string(dir.join("report.json")).unwrap_or_else(|_| {
        panic!(
            "no report.json written; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    let report: Value = serde_json::from_str(&report_text).expect("report.json is JSON");
    let _ = std::fs::remove_dir_all(&dir);
    (output.status.code(), report)
}

const GOOD_OVERLAY: &str = r#"{"image_digest":"sha256:aa","environment":"x","phases":[{"name":"restart_persistence","status":"passed"},{"name":"checkpoint_and_merge","status":"passed"}]}"#;
const GOOD_OPERATOR: &str = r#"{"image_digest":"sha256:aa","environment":"x","phases":[{"name":"restart_persistence","status":"passed"},{"name":"checkpoint_and_merge","status":"passed"}]}"#;
const GOOD_TOPOLOGY: &str = r#"{"statefulset":"lumen","replicas":1,"ready_replicas":1,"pvcs":["raft-lumen-0"],"cr_phase":"Ready","cr_ready_replicas":1}"#;

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
fn unknown_mode_is_refused_before_any_cluster_work() {
    let output = Command::new("bash")
        .arg(repo_path(SCRIPT))
        .args(["--mode", "nonsense"])
        .current_dir(repo_path(""))
        .output()
        .expect("bash runs");
    assert_eq!(
        output.status.code(),
        Some(2),
        "an unknown mode must exit 2; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("is not supported"),
        "stderr must say the mode is not supported, got: {stderr}"
    );
}

#[test]
fn script_names_both_modes_and_jq() {
    let text = repo_file(SCRIPT);
    for needle in [
        "--mode",
        "operator",
        "all",
        "jq",
        "--combine-only",
        "kind-acceptance-operator",
        "__restart_operator",
        "topology.json",
        "lumens.lumen.dev",
        "app.kubernetes.io/component=server",
    ] {
        assert!(text.contains(needle), "{SCRIPT} must contain `{needle}`");
    }
    assert_eq!(
        text.matches("docker build").count(),
        1,
        "{SCRIPT} must build the image exactly once"
    );
}

#[test]
fn combine_only_passes_two_good_reports() {
    let (code, report) = run_combine(GOOD_OVERLAY, GOOD_OPERATOR, Some(GOOD_TOPOLOGY), "pass");
    assert_eq!(code, Some(0), "two good reports must pass: {report}");
    assert_eq!(report["passed"], true);
    assert_eq!(report["cleanup_complete"], true);
    assert_eq!(report["image_identity"], "sha256:aa");
    assert_eq!(report["modes"]["operator"]["topology"]["replicas"], 1);
    assert!(report["modes"]["overlay"]["phases"].is_array());
}

#[test]
fn combine_only_fails_when_a_required_phase_is_missing() {
    let operator = r#"{"image_digest":"sha256:aa","phases":[{"name":"restart_persistence","status":"passed"}]}"#;
    let (code, report) = run_combine(GOOD_OVERLAY, operator, Some(GOOD_TOPOLOGY), "phase");
    assert_ne!(
        code,
        Some(0),
        "a missing phase must exit non-zero: {report}"
    );
    assert_eq!(report["passed"], false);
}

#[test]
fn combine_only_fails_when_the_image_identity_differs() {
    let operator = r#"{"image_digest":"sha256:bb","phases":[{"name":"restart_persistence","status":"passed"},{"name":"checkpoint_and_merge","status":"passed"}]}"#;
    let (code, report) = run_combine(GOOD_OVERLAY, operator, Some(GOOD_TOPOLOGY), "image");
    assert_ne!(
        code,
        Some(0),
        "a different image identity must exit non-zero: {report}"
    );
    assert_eq!(report["passed"], false);
}

#[test]
fn combine_only_fails_when_a_mode_is_skipped() {
    let (code, report) = run_combine(GOOD_OVERLAY, "null", None, "skip");
    assert_ne!(code, Some(0), "a skipped mode must exit non-zero: {report}");
    assert_eq!(report["passed"], false);
    assert!(report["modes"]["operator"].is_null());
}

#[test]
fn operator_overlay_has_the_operator_shape() {
    let base = "k8s/overlays/kind-acceptance-operator";

    let kustomization = repo_file(&format!("{base}/kustomization.yaml"));
    assert!(
        kustomization.contains("- ../../operator"),
        "the operator overlay must build on ../../operator"
    );
    assert!(
        !kustomization.contains("../../../operator"),
        "../../../operator is outside the repository layout of this overlay"
    );
    assert!(kustomization.contains("REPLACE_ME"));
    assert!(kustomization.contains("namespace: lumen"));
    assert!(kustomization.contains("Never"));
    assert!(
        kustomization.contains("- namespace.yaml"),
        "the operator overlay must create Namespace lumen itself"
    );

    let namespace = repo_file(&format!("{base}/namespace.yaml"));
    assert!(namespace.contains("kind: Namespace"));
    assert!(namespace.contains("name: lumen"));

    let cr = repo_file(&format!("{base}/lumen-cr.yaml"));
    for needle in [
        "kind: Lumen",
        "name: lumen",
        "shardCount: 1",
        "replicasPerShard: 1",
        "voterCount: 1",
        "auth: disabled",
        "imagePullPolicy: Never",
        "raftStorage: 1Gi",
        "kubernetes.io/os: linux",
    ] {
        assert!(cr.contains(needle), "lumen-cr.yaml must contain `{needle}`");
    }
    assert!(
        !cr.contains("raftStorageClass"),
        "Kind uses its default storage class; do not name raftStorageClass"
    );
}
