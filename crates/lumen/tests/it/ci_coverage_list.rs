//! The least-covered list of the CI `coverage-list` check run (faberline/lumen#562).
//!
//! `scripts/ci/coverage_list.py` lists the files below 100 percent line
//! coverage of a `main` run. These tests pin its output and the wiring of the
//! job `test-all-features` in `.github/workflows/ci.yml`.

use serde_json::{json, Value};
use serde_yaml::Value as Yaml;
use std::{fs, path::PathBuf, process::Command};

const SHA: &str = "a1b2c3d4e5f60718293a4b5c6d7e8f9012345678";

fn root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

/// A `cargo llvm-cov --json --summary-only` document. Each entry is
/// `(name, lines, covered)`; the file name is under `/w/`.
fn llvm_summary(files: &[(&str, u64, u64)]) -> Value {
    let files: Vec<Value> = files
        .iter()
        .map(|(name, count, covered)| {
            let percent = if *count == 0 {
                100.0
            } else {
                *covered as f64 / *count as f64 * 100.0
            };
            json!({
                "filename": format!("/w/{name}"),
                "summary": {"lines": {"count": count, "covered": covered, "percent": percent}}
            })
        })
        .collect();
    json!({"data": [{"files": files}]})
}

struct Run {
    code: i32,
    payload: Option<Value>,
    stderr: String,
}

/// Runs `scripts/ci/coverage_list.py` in a temporary directory. `config` is
/// the text of the `aw.toml`; `report` is written as `coverage-report.json`
/// and passed with `--report`; `limit` is passed with `--limit`.
fn run(summary: &Value, config: &str, report: Option<&Value>, limit: Option<&str>) -> Run {
    let dir = tempfile::tempdir().expect("temp dir");
    let summary_path = dir.path().join("summary.json");
    let config_path = dir.path().join("aw.toml");
    let payload_path = dir.path().join("payload.json");
    fs::write(&summary_path, summary.to_string()).expect("summary is written");
    fs::write(&config_path, config).expect("config is written");

    let mut command = Command::new("python3");
    command
        .arg("scripts/ci/coverage_list.py")
        .arg("--summary")
        .arg(&summary_path)
        .args(["--root", "/w", "--sha", SHA, "--config"])
        .arg(&config_path)
        .arg("--payload")
        .arg(&payload_path)
        .current_dir(root());
    if let Some(report) = report {
        let report_path = dir.path().join("coverage-report.json");
        fs::write(&report_path, report.to_string()).expect("report is written");
        command.arg("--report").arg(&report_path);
    }
    if let Some(limit) = limit {
        command.args(["--limit", limit]);
    }

    let out = command.output().expect("python3 starts");
    let payload = fs::read_to_string(&payload_path)
        .ok()
        .map(|text| serde_json::from_str(&text).expect("payload is JSON"));
    Run {
        code: out.status.code().expect("the script exits with a code"),
        payload,
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn summary_of(payload: &Value) -> String {
    payload["output"]["summary"]
        .as_str()
        .expect("output.summary is text")
        .to_owned()
}

fn table_of(payload: &Value) -> String {
    payload["output"]["text"]
        .as_str()
        .expect("output.text is text")
        .to_owned()
}

fn table_rows(table: &str) -> Vec<&str> {
    table
        .lines()
        .filter(|line| line.starts_with("| `"))
        .collect()
}

#[test]
fn list_names_the_sha_and_sorts_the_least_covered_first() {
    let input = llvm_summary(&[
        ("a.rs", 10, 5),
        ("b.rs", 10, 10),
        ("c.rs", 10, 8),
        ("d.rs", 100, 50),
    ]);
    let run = run(&input, "", None, None);
    assert_eq!(run.code, 0, "stderr: {}", run.stderr);
    let payload = run.payload.expect("the payload is written");
    assert_eq!(payload["name"], "coverage-list");
    assert_eq!(payload["head_sha"], SHA);
    assert_eq!(payload["status"], "completed");

    let summary = summary_of(&payload);
    assert!(summary.contains(SHA), "summary names the sha: {summary}");
    assert!(
        summary.contains("files below 100 percent: 3"),
        "summary counts three files: {summary}"
    );

    // d.rs and a.rs are both 50 percent; d.rs has more uncovered lines, so it
    // comes first. c.rs (80 percent) comes last. b.rs (100 percent) is absent.
    let table = table_of(&payload);
    let d = table.find("`d.rs`").expect("d.rs is listed");
    let a = table.find("`a.rs`").expect("a.rs is listed");
    let c = table.find("`c.rs`").expect("c.rs is listed");
    assert!(d < a && a < c, "rows are out of order: {table}");
    assert!(
        !table.contains("b.rs"),
        "a file at 100 percent is not listed"
    );

    let rows = table_rows(&table);
    assert!(
        rows[0].starts_with("| `d.rs` | 50") && rows[0].ends_with("| 50 |"),
        "d.rs row: {}",
        rows[0]
    );
    assert!(
        rows[1].starts_with("| `a.rs` | 50") && rows[1].ends_with("| 5 |"),
        "a.rs row: {}",
        rows[1]
    );
}

#[test]
fn list_is_cut_at_the_limit_and_says_how_many_are_left() {
    let names: Vec<String> = (0..25).map(|i| format!("f{i:02}.rs")).collect();
    let files: Vec<(&str, u64, u64)> = names.iter().map(|name| (name.as_str(), 10, 9)).collect();
    let input = llvm_summary(&files);

    let default = run(&input, "", None, None);
    assert_eq!(default.code, 0, "stderr: {}", default.stderr);
    let payload = default.payload.expect("the payload is written");
    assert_eq!(table_rows(&table_of(&payload)).len(), 20);
    let summary = summary_of(&payload);
    assert!(summary.contains("files below 100 percent: 25"), "{summary}");
    assert!(summary.contains("not listed: 5 more files"), "{summary}");

    let wide = run(&input, "", None, Some("30"));
    assert_eq!(wide.code, 0, "stderr: {}", wide.stderr);
    let payload = wide.payload.expect("the payload is written");
    assert_eq!(table_rows(&table_of(&payload)).len(), 25);
    assert!(!summary_of(&payload).contains("not listed"));
}

#[test]
fn list_totals_equal_the_report() {
    let input = llvm_summary(&[
        ("a.rs", 10, 5),
        ("b.rs", 10, 10),
        ("c.rs", 10, 8),
        ("d.rs", 100, 50),
    ]);
    // 73 of 130 lines is 56.1538... which is 56.15 cut to two decimals.
    let matching = json!({"measured": 56.15, "covered": 73, "lines": 130, "files": 4});
    let ok = run(&input, "", Some(&matching), None);
    assert_eq!(ok.code, 0, "stderr: {}", ok.stderr);
    assert!(ok.payload.is_some(), "a matching report writes the payload");

    let differing = json!({"measured": 56.16, "covered": 73, "lines": 130, "files": 4});
    let bad = run(&input, "", Some(&differing), None);
    assert_eq!(bad.code, 1, "a differing report must fail");
    assert!(bad.stderr.contains("measured"), "stderr: {}", bad.stderr);
    assert!(
        bad.payload.is_none(),
        "a differing report writes no payload"
    );
}

#[test]
fn list_honours_the_excludes_of_aw_toml() {
    let input = llvm_summary(&[("gen/x.rs", 10, 0), ("a.rs", 10, 5)]);
    let config = "[[standards.coverage.exclude]]\npath = \"^gen/\"\nreason = \"generated\"\n";
    let run = run(&input, config, None, None);
    assert_eq!(run.code, 0, "stderr: {}", run.stderr);
    let payload = run.payload.expect("the payload is written");
    let summary = summary_of(&payload);
    assert!(
        summary.contains("of 10 lines"),
        "only a.rs is counted: {summary}"
    );
    assert!(summary.contains("files below 100 percent: 1"), "{summary}");
    let table = table_of(&payload);
    assert!(table.contains("`a.rs`"), "a.rs is listed: {table}");
    assert!(
        !table.contains("gen/x.rs"),
        "an excluded file is not listed"
    );
}

#[test]
fn list_is_complete_when_everything_is_covered() {
    let input = llvm_summary(&[("a.rs", 10, 10)]);
    let run = run(&input, "", None, None);
    assert_eq!(run.code, 0, "stderr: {}", run.stderr);
    let payload = run.payload.expect("the payload is written");
    assert!(summary_of(&payload).contains("files below 100 percent: 0"));
    assert!(table_rows(&table_of(&payload)).is_empty());
}

/// True only when `ci.yml` publishes the list as the spec says: the job
/// `test-all-features` holds `checks: write` beside `contents: read`, its
/// last step publishes on a push to `main` with the report, the step before
/// it is the aw coverage action with `target/coverage.json` unchanged, the
/// workflow level keeps only `contents: read`, and no other job has
/// `checks: write`.
fn wired(source: &str) -> bool {
    let Ok(workflow) = serde_yaml::from_str::<Yaml>(source) else {
        return false;
    };
    let job = &workflow["jobs"]["test-all-features"];
    let perms = &job["permissions"];
    if perms.as_mapping().map(|m| m.len()) != Some(2)
        || perms["contents"].as_str() != Some("read")
        || perms["checks"].as_str() != Some("write")
    {
        return false;
    }
    let Some(steps) = job["steps"].as_sequence() else {
        return false;
    };
    if steps.len() < 2 {
        return false;
    }
    let coverage = &steps[steps.len() - 2];
    let publish = &steps[steps.len() - 1];

    let coverage_ok = coverage["uses"]
        .as_str()
        .is_some_and(|uses| uses.starts_with("faberline/aw/.github/actions/coverage@"))
        && coverage["with"]["summary"].as_str() == Some("target/coverage.json");
    let publish_ok = publish["run"].as_str().is_some_and(|run| {
        run.contains("scripts/ci/coverage_list.py")
            && run.contains("check-runs")
            && run.contains("--report")
    }) && publish["if"].as_str().is_some_and(|cond| {
        cond.contains("github.event_name == 'push'") && cond.contains("refs/heads/main")
    });
    let workflow_ok = workflow["permissions"].as_mapping().map(|m| m.len()) == Some(1)
        && workflow["permissions"]["contents"].as_str() == Some("read");
    let others_ok = workflow["jobs"].as_mapping().is_some_and(|jobs| {
        jobs.iter().all(|(id, job)| {
            id.as_str() == Some("test-all-features") || job["permissions"]["checks"].is_null()
        })
    });

    coverage_ok && publish_ok && workflow_ok && others_ok
}

fn ci_workflow() -> String {
    fs::read_to_string(root().join(".github/workflows/ci.yml")).expect("ci.yml is readable")
}

#[test]
fn ci_publishes_the_list_only_from_main_with_checks_write() {
    let source = ci_workflow();
    assert!(
        wired(&source),
        "ci.yml does not publish the coverage list as the spec says"
    );

    // Negative control: without `checks: write` the same check fails.
    let from = "      checks: write\n";
    assert_eq!(source.matches(from).count(), 1, "the checks line is unique");
    let changed = source.replacen(from, "", 1);
    assert!(!wired(&changed), "a job without checks: write passed");
}
