//! `lumen migrate search-request` through the real binary: stdin or `--in` in,
//! stdout, stderr, files and exit status out. The command is offline.

use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::Value;

const MATCH: &str = r#"{"match":{"field":"bio","text":"rust"}}"#;

fn run(args: &[&str], stdin: &str) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lumen"));
    command
        .args(["migrate", "search-request"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn lumen");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    child.wait_with_output().expect("wait for lumen")
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

fn stderr_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stderr).expect("stderr is the report JSON")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("lumen-migrate-{name}-{}.json", std::process::id()))
}

#[test]
fn legacy_track_total_true_is_exact() {
    let input = format!(r#"{{"query":{MATCH},"track_total":true}}"#);
    let output = run(&[], &input);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(output.stdout.ends_with(b"\n"));
    assert_eq!(stdout_json(&output)["track_total"], "exact");
    assert_eq!(stderr_json(&output)["changes"][0]["to"], "exact");
}

#[test]
fn legacy_track_total_false_is_none() {
    let input = format!(r#"{{"query":{MATCH},"track_total":false}}"#);
    let output = run(&[], &input);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(stdout_json(&output)["track_total"], "none");
    assert_eq!(stderr_json(&output)["changes"][0]["to"], "none");
}

#[test]
fn track_total_bool_is_not_left_in_the_output() {
    // Negative control: a bool left as-is would print `"track_total": true`.
    for value in ["true", "false"] {
        let input = format!(r#"{{"query":{MATCH},"track_total":{value}}}"#);
        let output = run(&[], &input);
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        assert!(!text.contains("\"track_total\": true"), "{text}");
        assert!(!text.contains("\"track_total\": false"), "{text}");
    }
}

#[test]
fn sort_missing_with_the_legacy_default_exclude_is_refused() {
    let input = format!(r#"{{"query":{MATCH},"sort":[{{"field":"age"}}]}}"#);
    let output = run(&[], &input);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let err = stderr(&output);
    assert!(err.contains("refused:"), "{err}");
    assert!(err.contains(r#"sort[0] field "age""#), "{err}");
    assert!(err.contains(r#""first" or "last""#), "{err}");
}

#[test]
fn sort_missing_last_is_converted_without_a_change_row() {
    let input = format!(
        r#"{{"query":{MATCH},"sort":[{{"field":"age","order":"desc","missing":"last"}}]}}"#
    );
    let output = run(&[], &input);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(stdout_json(&output)["sort"][0]["missing"], "last");
    assert_eq!(stderr_json(&output)["changes"], serde_json::json!([]));
}

#[test]
fn unsupported_input_exits_nonzero_with_empty_stdout() {
    let input = r#"{"query":{"term":{"field":"a","value":"x"}}}"#;
    let output = run(&[], input);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        stderr(&output).contains("not supported by this version of the tool"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn invalid_json_exits_nonzero() {
    let output = run(&[], "not json");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        stderr(&output).contains("input is not valid JSON"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn in_out_and_report_files() {
    let input_path = temp_path("in-ok");
    let out_path = temp_path("out-ok");
    let report_path = temp_path("report-ok");
    std::fs::write(
        &input_path,
        format!(r#"{{"query":{MATCH},"track_total":true}}"#),
    )
    .unwrap();

    let output = run(
        &[
            "--in",
            input_path.to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
            "--report",
            report_path.to_str().unwrap(),
        ],
        "",
    );
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
    let request: Value =
        serde_json::from_str(&std::fs::read_to_string(&out_path).unwrap()).unwrap();
    assert_eq!(request["track_total"], "exact");
    let report: Value =
        serde_json::from_str(&std::fs::read_to_string(&report_path).unwrap()).unwrap();
    assert_eq!(report["changes"][0]["path"], "track_total");
    assert_eq!(report["refusals"], serde_json::json!([]));

    for path in [input_path, out_path, report_path] {
        let _ = std::fs::remove_file(path);
    }
}

#[test]
fn a_refused_request_writes_no_out_file() {
    let out_path = temp_path("out-refused");
    let _ = std::fs::remove_file(&out_path);
    let output = run(
        &["--out", out_path.to_str().unwrap()],
        r#"{"query":{"term":{}}}"#,
    );
    assert!(!output.status.success());
    assert!(!out_path.exists());
}

#[test]
fn two_runs_are_byte_identical() {
    let input = format!(r#"{{"track_total":false,"query":{MATCH},"limit":3}}"#);
    let first = run(&[], &input);
    let second = run(&[], &input);
    assert!(first.status.success(), "stderr: {}", stderr(&first));
    assert_eq!(first.stdout, second.stdout);
    assert_eq!(first.stderr, second.stderr);
}

#[test]
fn migrate_help_lists_search_request() {
    let output = Command::new(env!("CARGO_BIN_EXE_lumen"))
        .args(["migrate", "--help"])
        .output()
        .expect("run lumen migrate --help");
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("search-request"));
}
