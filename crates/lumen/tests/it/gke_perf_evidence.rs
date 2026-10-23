//! The GKE durable performance matrix (.github/workflows/gke-perf.yml) keeps a
//! receipt only as marked base64 lines in the client pod log of each aw
//! evidence artifact. scripts/gke-perf/extract-receipts.py takes them back
//! out, and must fail closed on any log that is not one clean receipt block
//! for the called cell.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use base64::Engine as _;
use serde_yaml::Value;
use sha2::{Digest, Sha256};

const CELL: &str = "index-1000-hnsw-cpu";
const POD: &str = "lumen-perf-cell-x7k2p";

fn root() -> PathBuf {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")).to_path_buf()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The lines scripts/gke-perf/cell.sh prints for one block.
fn block(kind: &str, cell: &str, payload: &[u8]) -> Vec<String> {
    block_declaring(kind, cell, payload, &sha256_hex(payload), payload.len())
}

fn block_declaring(kind: &str, cell: &str, payload: &[u8], sha: &str, bytes: usize) -> Vec<String> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
    let mut lines = vec![format!(
        "LUMEN-PERF-{kind}-BEGIN cell={cell} sha256={sha} bytes={bytes}"
    )];
    lines.extend(
        encoded
            .as_bytes()
            .chunks(76)
            .map(|chunk| String::from_utf8(chunk.to_vec()).unwrap()),
    );
    lines.push(format!("LUMEN-PERF-{kind}-END cell={cell}"));
    lines
}

/// `kubectl logs --all-containers --prefix --timestamps` framing.
fn prefixed(lines: &[String], pod: &str, container: &str) -> String {
    lines
        .iter()
        .map(|line| format!("[pod/{pod}/{container}] 2026-10-15T01:02:03.456789012Z {line}\n"))
        .collect()
}

fn client_log(lines: &[String]) -> String {
    prefixed(lines, POD, "cell")
}

fn receipt() -> Vec<u8> {
    br#"{"schema_version":1}"#.repeat(40)
}

struct Run {
    code: i32,
    stderr: String,
    receipts: BTreeMap<String, Vec<u8>>,
    failures: Vec<String>,
}

/// Lay out one artifact for CELL with `files`, then extract `cells`.
fn extract(files: &[(&str, String)], cells: &str) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let evidence = dir.path().join("evidence");
    let artifact = evidence.join(format!("gke-test-evidence-99-1-{CELL}"));
    fs::create_dir_all(&artifact).unwrap();
    for (name, text) in files {
        fs::write(artifact.join(name), text).unwrap();
    }
    let output = dir.path().join("out");
    let failures = dir.path().join("fail");
    fs::create_dir(&output).unwrap();
    fs::create_dir(&failures).unwrap();
    let result = Command::new("python3")
        .arg("-I")
        .arg(root().join("scripts/gke-perf/extract-receipts.py"))
        .arg("--evidence-dir")
        .arg(&evidence)
        .args(["--run-id", "99", "--run-attempt", "1", "--cells", cells])
        .arg("--output-dir")
        .arg(&output)
        .arg("--failures-dir")
        .arg(&failures)
        .output()
        .expect("run extract-receipts.py");
    let receipts = fs::read_dir(&output)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_str().unwrap().to_string();
            (name, fs::read(&path).unwrap())
        })
        .collect();
    let mut failure_names: Vec<String> = fs::read_dir(&failures)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_str().unwrap().to_string())
        .collect();
    failure_names.sort();
    Run {
        code: result.status.code().unwrap(),
        stderr: String::from_utf8_lossy(&result.stderr).into_owned(),
        receipts,
        failures: failure_names,
    }
}

fn passing_log() -> String {
    let mut lines = vec!["running 1 test".to_string(), "test ... ok".to_string()];
    lines.extend(block("RECEIPT", CELL, &receipt()));
    lines.push(format!("QUALIFY {CELL} exit=0 duration_s=2400"));
    client_log(&lines)
}

fn log_name() -> String {
    format!("logs-{POD}.txt")
}

#[test]
fn extracts_the_one_receipt_of_the_called_cell() {
    let server = prefixed(
        &block("RECEIPT", "replace-1-flat-cpu", b"x"),
        "lumen-perf-0",
        "lumen",
    );
    let run = extract(
        &[
            (&log_name(), passing_log()),
            ("logs-lumen-perf-0.txt", server),
        ],
        CELL,
    );
    assert_eq!(run.code, 0, "{}", run.stderr);
    assert_eq!(
        run.receipts,
        BTreeMap::from([(format!("{CELL}.json"), receipt())])
    );
    assert!(run.failures.is_empty());
}

#[test]
fn refuses_any_log_that_is_not_one_clean_receipt() {
    let good = block("RECEIPT", CELL, &receipt());
    let mut tampered = good.clone();
    tampered[2] = "!!!!".to_string();
    let mut unclosed = good.clone();
    unclosed.pop();
    let foreign_inside = format!(
        "{}garbage\n{}",
        client_log(&good[..2]),
        client_log(&good[2..])
    );
    let other_container = format!(
        "{}{}{}",
        client_log(&good[..2]),
        prefixed(&good[2..3], POD, "sidecar"),
        client_log(&good[3..])
    );
    let mut receipt_and_failure = good.clone();
    receipt_and_failure.extend(block("FAILURE", CELL, b"tar"));
    let cases = [
        ("missing receipt", client_log(&["test failed".to_string()])),
        (
            "two receipts",
            client_log(&[good.clone(), good.clone()].concat()),
        ),
        (
            "wrong cell",
            client_log(&block("RECEIPT", "index-1-flat-cpu", &receipt())),
        ),
        (
            "wrong sha256",
            client_log(&block_declaring(
                "RECEIPT",
                CELL,
                &receipt(),
                &"0".repeat(64),
                receipt().len(),
            )),
        ),
        (
            "wrong byte count",
            client_log(&block_declaring(
                "RECEIPT",
                CELL,
                &receipt(),
                &sha256_hex(&receipt()),
                3,
            )),
        ),
        ("unclosed", client_log(&unclosed)),
        ("tampered line", client_log(&tampered)),
        ("foreign line inside", foreign_inside),
        ("other container inside", other_container),
        (
            "malformed marker",
            client_log(&["LUMEN-PERF-RECEIPT-BEGIN cell=x".to_string()]),
        ),
        ("receipt and failure", client_log(&receipt_and_failure)),
    ];
    for (case, log) in cases {
        let run = extract(&[(&log_name(), log)], CELL);
        assert_eq!(run.code, 1, "{case}: {}", run.stderr);
        assert!(run.receipts.is_empty(), "{case}");
        assert!(
            run.stderr.contains(&format!("refused: {CELL}")),
            "{case}: {}",
            run.stderr
        );
    }
}

#[test]
fn keeps_the_failure_evidence_of_a_failed_cell() {
    let run = extract(
        &[(
            &log_name(),
            client_log(&block("FAILURE", CELL, b"tar bytes")),
        )],
        CELL,
    );
    assert_eq!(run.code, 1, "{}", run.stderr);
    assert!(run.receipts.is_empty());
    assert_eq!(run.failures, [format!("{CELL}-failure.tar.gz")]);
}

#[test]
fn refuses_a_restarted_or_duplicated_client() {
    let previous = format!("logs-{POD}-previous.txt");
    let run = extract(
        &[(&log_name(), passing_log()), (&previous, passing_log())],
        CELL,
    );
    assert_eq!(run.code, 1, "{}", run.stderr);
    assert!(run.stderr.contains("restarted"), "{}", run.stderr);
    let run = extract(
        &[
            (&log_name(), passing_log()),
            ("logs-lumen-perf-cell-other.txt", passing_log()),
        ],
        CELL,
    );
    assert_eq!(run.code, 1, "{}", run.stderr);
    assert!(run.receipts.is_empty());
}

#[test]
fn a_missing_artifact_fails_only_its_cell() {
    let run = extract(
        &[(&log_name(), passing_log())],
        &format!("{CELL},replace-1-flat-cpu"),
    );
    assert_eq!(run.code, 1, "{}", run.stderr);
    assert_eq!(
        run.receipts.keys().collect::<Vec<_>>(),
        [&format!("{CELL}.json")]
    );
    assert!(
        run.stderr.contains("refused: replace-1-flat-cpu"),
        "{}",
        run.stderr
    );
}

#[test]
fn refuses_bad_cell_arguments() {
    for cells in [
        "index-2-gpu",
        "index-1000-hnsw-cpu,index-1000-hnsw-cpu",
        "",
        " , ",
    ] {
        let run = extract(&[(&log_name(), passing_log())], cells);
        assert_eq!(run.code, 2, "{cells:?}: {}", run.stderr);
        assert!(run.receipts.is_empty(), "{cells:?}");
    }
}

/// The workflow runs up to three cells at a time through the aw contract, each
/// in its own lane and namespace, with the folders and image names this
/// repository ships.
#[test]
fn workflow_runs_cells_in_lanes_through_aw() {
    let source = fs::read_to_string(root().join(".github/workflows/gke-perf.yml")).unwrap();
    let workflow: Value = serde_yaml::from_str(&source).unwrap();
    let cell = &workflow["jobs"]["cell"];
    assert_eq!(
        cell["uses"],
        "faberline/aw/.github/workflows/gke-test.yml@main"
    );
    // At most max_nodes of the perf pools in faberline/aw pools.yaml.
    assert_eq!(cell["strategy"]["max-parallel"], 3);
    assert_eq!(workflow["concurrency"]["group"], "lumen-gke-perf");
    assert_eq!(workflow["concurrency"]["cancel-in-progress"], false);
    assert_eq!(cell["strategy"]["fail-fast"], false);
    let with = &cell["with"];
    for folder in ["overlay", "test-job"] {
        let path = root().join(with[folder].as_str().unwrap());
        assert!(path.join("kustomization.yaml").is_file(), "{folder}");
    }
    assert_eq!(with["test-image-name"], "lumen-perf-client");
    assert_eq!(with["evidence-suffix"], "${{ matrix.cell }}");
    assert_eq!(with["lane"], "${{ matrix.cell }}");
    assert_eq!(with["namespace"], "lumen-perf-${{ matrix.cell }}");
    let env = with["test-env"].as_str().unwrap();
    for key in [
        "LUMEN_PERF_CELL",
        "LUMEN_PERF_ENDPOINT",
        "LUMEN_PERF_BATCH",
        "LUMEN_PERF_BACKEND",
        "LUMEN_PERF_IMAGE",
        "LUMEN_PERF_REPOSITORY",
        "LUMEN_PERF_RUN_ID",
        "LUMEN_PERF_RUN_ATTEMPT",
        "LUMEN_PERF_COMMIT",
    ] {
        assert_eq!(env.matches(&format!("{key}=")).count(), 1, "{key}");
    }
    for forbidden in ["CARGO_PROFILE_", "LUMEN_PERF_DIAGNOSTIC", "TOKEN", "SECRET"] {
        assert!(!env.contains(forbidden), "{forbidden}");
    }
    let job: Value = serde_yaml::from_str(
        &fs::read_to_string(root().join("k8s/gke-perf-job/job.yaml")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        job["spec"]["template"]["spec"]["containers"][0]["image"],
        "lumen-perf-client"
    );
    assert_eq!(job["spec"]["backoffLimit"], 0);
}
