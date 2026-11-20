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

/// The change row of the report that names `path`.
fn change(output: &Output, path: &str) -> Value {
    stderr_json(output)["changes"]
        .as_array()
        .expect("changes is an array")
        .iter()
        .find(|row| row["path"] == path)
        .cloned()
        .unwrap_or(Value::Null)
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
    assert_eq!(change(&output, "track_total")["to"], "exact");
}

#[test]
fn legacy_track_total_false_is_none() {
    let input = format!(r#"{{"query":{MATCH},"track_total":false}}"#);
    let output = run(&[], &input);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(stdout_json(&output)["track_total"], "none");
    assert_eq!(change(&output, "track_total")["to"], "none");
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
    assert_eq!(
        stderr_json(&output)["changes"],
        serde_json::json!([{"path": "query", "from": "query", "to": "scoring"}])
    );
}

#[test]
fn unsupported_input_exits_nonzero_with_empty_stdout() {
    let input = format!(r#"{{"query":{MATCH},"cursor":"x"}}"#);
    let output = run(&[], &input);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        stderr(&output).contains("a 0.4 cursor is not portable"),
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
    assert_eq!(report["changes"][0]["path"], "query");
    assert_eq!(report["changes"][1]["path"], "track_total");
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
        &format!(r#"{{"query":{MATCH},"sort":[{{"field":"age"}}]}}"#),
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

const TERM: &str = r#"{"term":{"field":"status","value":"active"}}"#;
const RANGE: &str = r#"{"range":{"field":"age","gte":18}}"#;

#[test]
fn scoring_only_query_is_written_as_scoring() {
    let output = run(&[], &format!(r#"{{"query":{MATCH}}}"#));
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let request = stdout_json(&output);
    assert!(request.get("scoring").is_some());
    assert!(request.get("query").is_none());
    assert!(request.get("filter").is_none());
}

#[test]
fn filter_only_query_is_written_as_filter() {
    let output = run(&[], &format!(r#"{{"query":{TERM}}}"#));
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let request = stdout_json(&output);
    assert!(request.get("filter").is_some());
    assert!(request.get("scoring").is_none());
    assert!(request.get("query").is_none());
}

#[test]
fn and_of_a_mix_is_split_in_the_output() {
    let output = run(&[], &format!(r#"{{"query":{{"and":[{MATCH},{TERM}]}}}}"#));
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let request = stdout_json(&output);
    assert_eq!(
        request["scoring"],
        serde_json::from_str::<Value>(MATCH).unwrap()
    );
    assert_eq!(
        request["filter"],
        serde_json::from_str::<Value>(TERM).unwrap()
    );
    assert_eq!(
        stderr_json(&output)["changes"][0],
        serde_json::json!({"path": "query", "from": "query", "to": "scoring+filter"})
    );
}

#[test]
fn or_of_a_mix_is_refused() {
    let output = run(&[], &format!(r#"{{"query":{{"or":[{MATCH},{TERM}]}}}}"#));
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let err = stderr(&output);
    assert!(err.contains("refused:"), "{err}");
    assert!(
        err.contains(r#"query: "or" mixes scoring and filter nodes"#),
        "{err}"
    );
}

#[test]
fn not_of_a_mix_is_refused() {
    let output = run(
        &[],
        &format!(r#"{{"query":{{"not":{{"and":[{MATCH},{TERM}]}}}}}}"#),
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        stderr(&output).contains(r#"query: "not" mixes"#),
        "{}",
        stderr(&output)
    );
}

#[test]
fn refusal_report_names_the_node_path() {
    let report_path = temp_path("report-refused");
    let out_path = temp_path("out-refused-path");
    let _ = std::fs::remove_file(&out_path);
    let input = format!(r#"{{"query":{{"and":[{MATCH},{{"or":[{MATCH},{TERM}]}}]}}}}"#);
    let output = run(
        &[
            "--report",
            report_path.to_str().unwrap(),
            "--out",
            out_path.to_str().unwrap(),
        ],
        &input,
    );
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let report: Value =
        serde_json::from_str(&std::fs::read_to_string(&report_path).unwrap()).unwrap();
    assert_eq!(report["changes"], serde_json::json!([]));
    let first = report["refusals"][0].as_str().unwrap();
    assert!(first.starts_with("query.and[1]:"), "{first}");
    assert!(!out_path.exists());
    let _ = std::fs::remove_file(report_path);
}

#[test]
fn nested_mixed_and_is_split() {
    let input = format!(r#"{{"query":{{"and":[{MATCH},{{"and":[{TERM},{RANGE}]}}]}}}}"#);
    let output = run(&[], &input);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let request = stdout_json(&output);
    assert_eq!(
        request["scoring"],
        serde_json::from_str::<Value>(MATCH).unwrap()
    );
    assert_eq!(
        request["filter"],
        serde_json::from_str::<Value>(&format!(r#"{{"and":[{TERM},{RANGE}]}}"#)).unwrap()
    );
}

#[test]
fn or_of_a_mix_is_never_silently_split() {
    // Negative control: a silent split would print `scoring` or `filter`.
    let output = run(&[], &format!(r#"{{"query":{{"or":[{MATCH},{TERM}]}}}}"#));
    assert!(!output.status.success());
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(!text.contains("\"scoring\""), "{text}");
    assert!(!text.contains("\"filter\""), "{text}");
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

const OR_MIX: &str = r#"{"or":[{"match":{"field":"bio","text":"rust"}},{"term":{"field":"status","value":"active"}}]}"#;

/// The three-item batch of the unit test `batch_of_three_all_convert`.
fn three_item_batch(middle_query: &str) -> String {
    format!(
        r#"{{"searches":[{{"collection":"a","query":{MATCH},"track_total":true}},{{"collection":"b","query":{middle_query},"limit":5}},{{"collection":"c","query":{{"and":[{MATCH},{TERM}]}},"collapse":"k"}}]}}"#
    )
}

#[test]
fn legacy_cursor_is_refused() {
    let input = format!(r#"{{"query":{MATCH},"cursor":"abc"}}"#);
    let output = run(&[], &input);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let text = stderr(&output);
    assert!(text.contains("refused:"), "{text}");
    assert!(text.contains("a 0.4 cursor is not portable"), "{text}");
}

#[test]
fn collapse_string_is_converted() {
    let input = format!(r#"{{"query":{MATCH},"collapse":"category"}}"#);
    let output = run(&[], &input);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        stdout_json(&output)["collapse"],
        serde_json::json!({"field": "category"})
    );
    assert_eq!(change(&output, "collapse")["path"], "collapse");
}

#[test]
fn duplicates_request_is_refused_with_the_note() {
    let output = run(&[], r#"{"field":"email"}"#);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let text = stderr(&output);
    assert!(text.contains("terms facet"), "{text}");
    assert!(text.contains("term filter"), "{text}");
}

#[test]
fn batch_of_three_converts() {
    let output = run(&[], &three_item_batch(TERM));
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let request = stdout_json(&output);
    let items = request["searches"]
        .as_array()
        .expect("searches is an array");
    assert_eq!(items.len(), 3);
    assert_eq!(items[2]["collapse"], serde_json::json!({"field": "k"}));
    let text = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(text.ends_with('\n'), "{text}");
    assert!(!text.contains("\"query\""), "{text}");
}

#[test]
fn batch_with_a_refused_item_names_the_index() {
    let output = run(&[], &three_item_batch(OR_MIX));
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let text = stderr(&output);
    assert!(text.contains("searches[1].query"), "{text}");
    assert!(text.contains("refused:"), "{text}");
}

#[test]
fn a_refused_batch_item_is_never_dropped_silently() {
    let out_refused = temp_path("out-batch-refused");
    let out_fixed = temp_path("out-batch-fixed");
    let _ = std::fs::remove_file(&out_refused);
    let _ = std::fs::remove_file(&out_fixed);

    let refused = run(
        &["--out", out_refused.to_str().unwrap()],
        &three_item_batch(OR_MIX),
    );
    assert!(!refused.status.success());
    assert!(!out_refused.exists());

    let fixed = run(
        &["--out", out_fixed.to_str().unwrap()],
        &three_item_batch(TERM),
    );
    assert!(fixed.status.success(), "stderr: {}", stderr(&fixed));
    let request: Value =
        serde_json::from_str(&std::fs::read_to_string(&out_fixed).unwrap()).unwrap();
    assert_eq!(request["searches"].as_array().unwrap().len(), 3);

    for path in [out_refused, out_fixed] {
        let _ = std::fs::remove_file(path);
    }
}

#[test]
fn batch_refusal_report_names_each_index() {
    let report_path = temp_path("report-batch-refused");
    let input = format!(
        r#"{{"searches":[{{"collection":"a","query":{OR_MIX}}},{{"collection":"b","query":{TERM}}},{{"collection":"c","query":{MATCH},"cursor":"x"}}]}}"#
    );
    let output = run(&["--report", report_path.to_str().unwrap()], &input);
    assert!(!output.status.success());
    let report: Value =
        serde_json::from_str(&std::fs::read_to_string(&report_path).unwrap()).unwrap();
    let refusals = report["refusals"].as_array().expect("refusals is an array");
    assert_eq!(refusals.len(), 2);
    assert!(refusals[0].as_str().unwrap().starts_with("searches[0]."));
    assert!(refusals[1].as_str().unwrap().starts_with("searches[2]."));
    assert_eq!(report["changes"], serde_json::json!([]));
    let _ = std::fs::remove_file(report_path);
}
