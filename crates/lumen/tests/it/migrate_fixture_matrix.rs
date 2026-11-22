//! Compatibility-window matrix: one real `lumen migrate` run for each offline
//! row of the "Compatibility window" table in docs/migration-search-v2.md.
//! The rows without a tool are runtime behaviour and are only pinned here.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};

const GUIDE: &str = include_str!("../../../../docs/migration-search-v2.md");

struct Row {
    guide_row: &'static str,
    tool: Option<&'static str>,
    input: &'static str,
    refused: bool,
    /// Substrings that a refused run must print on stderr.
    expect: &'static [&'static str],
}

const MATRIX: [Row; 10] = [
    Row {
        guide_row: "Unknown search fields",
        tool: Some("search-request"),
        input: r#"{"query":{"match":{"field":"bio","text":"rust"}},"bogus":1}"#,
        refused: true,
        expect: &["unknown field \"bogus\""],
    },
    Row {
        guide_row: "Mixed-version requests",
        tool: None,
        input: "",
        refused: false,
        expect: &[],
    },
    Row {
        guide_row: "Numeric schema",
        tool: Some("collection-schema"),
        input: r#"{"fields":{"age":{"type":"number"}}}"#,
        refused: false,
        expect: &[],
    },
    Row {
        guide_row: "Multi-value schema",
        tool: Some("collection-schema"),
        input: r#"{"fields":{"tags":{"type":"set"}}}"#,
        refused: false,
        expect: &[],
    },
    Row {
        guide_row: "Total request",
        tool: Some("search-request"),
        input: r#"{"query":{"match":{"field":"bio","text":"rust"}},"track_total":true}"#,
        refused: false,
        expect: &[],
    },
    Row {
        guide_row: "Total response",
        tool: None,
        input: "",
        refused: false,
        expect: &[],
    },
    Row {
        guide_row: "Cursor mismatch",
        tool: Some("search-request"),
        input: r#"{"query":{"match":{"field":"bio","text":"rust"}},"cursor":"abc"}"#,
        refused: true,
        expect: &["a 0.4 cursor is not portable"],
    },
    Row {
        guide_row: "Collapse",
        tool: Some("search-request"),
        input: r#"{"query":{"match":{"field":"bio","text":"rust"}},"collapse":"category"}"#,
        refused: false,
        expect: &[],
    },
    Row {
        guide_row: "Duplicate discovery",
        tool: Some("search-request"),
        input: r#"{"field":"email"}"#,
        refused: true,
        expect: &["terms facet", "term filter"],
    },
    Row {
        guide_row: "Managed capability",
        tool: None,
        input: "",
        refused: false,
        expect: &[],
    },
];

fn run(tool: &str, stdin: &str, cwd: &Path) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_lumen"))
        .args(["migrate", tool])
        .current_dir(cwd)
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn lumen");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    child.wait_with_output().expect("wait for lumen")
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("lumen-matrix-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn slug(name: &str) -> String {
    name.replace(' ', "-").to_lowercase()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The first cell of every data row of the "Compatibility window" table.
fn guide_rows() -> Vec<String> {
    let mut in_table = false;
    let mut rows = Vec::new();
    for line in GUIDE.lines() {
        if line == "## Compatibility window" {
            in_table = true;
            continue;
        }
        if in_table && line.starts_with("## ") {
            break;
        }
        if !in_table || !line.starts_with('|') {
            continue;
        }
        let first = line.split('|').nth(1).unwrap_or("").trim();
        if first.is_empty() || first.starts_with('-') || first == "Surface" {
            continue;
        }
        rows.push(first.to_string());
    }
    rows
}

fn guide_rows_missing(guide: &[String], matrix: &[&str]) -> Vec<String> {
    guide
        .iter()
        .filter(|row| !matrix.contains(&row.as_str()))
        .cloned()
        .collect()
}

fn check_refused(row: &Row, output: &Output) {
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}: stderr: {}",
        row.guide_row,
        stderr(output)
    );
    assert!(
        output.stdout.is_empty(),
        "{}: stdout must be empty",
        row.guide_row
    );
    let err = stderr(output);
    assert!(err.contains("refused:"), "{}: stderr: {err}", row.guide_row);
    for needle in row.expect {
        assert!(
            err.contains(needle),
            "{}: missing {needle:?} in: {err}",
            row.guide_row
        );
    }
}

fn check_converted(row: &Row, output: &Output) {
    let stdout: Value = serde_json::from_slice(&output.stdout).expect("stdout is JSON");
    match row.guide_row {
        "Numeric schema" => {
            assert_eq!(
                stdout["schema"]["fields"]["age"],
                json!({"type": "float64"})
            );
            assert_eq!(
                stdout["report"]["conversions"],
                json!([{"field": "age", "from": "number", "to": "float64"}])
            );
            assert_eq!(stdout["report"]["rebuild_required"], json!(["age"]));
            assert!(
                output.stderr.is_empty(),
                "collection-schema writes no stderr"
            );
        }
        "Multi-value schema" => {
            assert_eq!(
                stdout["schema"]["fields"]["tags"],
                json!({"type": "keyword", "multi": true})
            );
            assert_eq!(
                stdout["report"]["conversions"][0]["to"],
                json!("keyword+multi")
            );
            assert!(
                output.stderr.is_empty(),
                "collection-schema writes no stderr"
            );
        }
        "Total request" => {
            assert_eq!(stdout["track_total"], json!("exact"));
            let report: Value = serde_json::from_slice(&output.stderr).expect("stderr is JSON");
            let changes = report["changes"].as_array().expect("changes array");
            assert!(
                changes
                    .iter()
                    .any(|c| c["path"] == "track_total" && c["from"] == true && c["to"] == "exact"),
                "changes: {report}"
            );
        }
        "Collapse" => assert_eq!(stdout["collapse"], json!({"field": "category"})),
        other => panic!("no check for converted row {other}"),
    }
}

#[test]
fn matrix_covers_every_guide_row() {
    let names: Vec<&str> = MATRIX.iter().map(|row| row.guide_row).collect();
    assert_eq!(guide_rows().len(), 10);
    assert!(guide_rows_missing(&guide_rows(), &names).is_empty());
}

#[test]
fn matrix_gap_is_detected() {
    let names: Vec<&str> = MATRIX
        .iter()
        .map(|row| row.guide_row)
        .filter(|name| *name != "Collapse")
        .collect();
    assert_eq!(
        guide_rows_missing(&guide_rows(), &names),
        vec!["Collapse".to_string()]
    );
}

#[test]
fn every_tool_row_has_the_expected_exit_code_and_report() {
    for row in MATRIX.iter().filter(|row| row.tool.is_some()) {
        let tool = row.tool.expect("tool row");
        let dir = temp_dir(&format!("report-{}", slug(row.guide_row)));
        let output = run(tool, row.input, &dir);
        if row.refused {
            check_refused(row, &output);
        } else {
            assert_eq!(
                output.status.code(),
                Some(0),
                "{}: stderr: {}",
                row.guide_row,
                stderr(&output)
            );
            assert!(
                output.stdout.ends_with(b"\n"),
                "{}: no final newline",
                row.guide_row
            );
            check_converted(row, &output);
        }
    }
}

#[test]
fn every_tool_row_is_byte_identical_on_a_second_run() {
    for row in MATRIX.iter().filter(|row| row.tool.is_some()) {
        let tool = row.tool.expect("tool row");
        let dir = temp_dir(&format!("twice-{}", slug(row.guide_row)));
        let first = run(tool, row.input, &dir);
        let second = run(tool, row.input, &dir);
        assert_eq!(first.stdout, second.stdout, "{} stdout", row.guide_row);
        assert_eq!(first.stderr, second.stderr, "{} stderr", row.guide_row);
        assert_eq!(
            first.status.code(),
            second.status.code(),
            "{} exit code",
            row.guide_row
        );
    }
}

#[test]
fn every_tool_row_writes_no_file_and_needs_no_network() {
    for row in MATRIX.iter().filter(|row| row.tool.is_some()) {
        let tool = row.tool.expect("tool row");
        let dir = temp_dir(&format!("nofile-{}", slug(row.guide_row)));
        // The proxy points at a closed port: a run that connects would fail.
        let output = run(tool, row.input, &dir);
        assert!(
            output.status.code() == Some(0) || row.refused,
            "{}: stderr: {}",
            row.guide_row,
            stderr(&output)
        );
        if row.refused {
            assert!(output.stdout.is_empty(), "{} stdout", row.guide_row);
        }
        let entries = std::fs::read_dir(&dir).expect("read temp dir").count();
        assert_eq!(entries, 0, "{} wrote a file", row.guide_row);
    }
}

#[test]
fn rows_without_a_tool_are_runtime_behaviour() {
    let without: Vec<&str> = MATRIX
        .iter()
        .filter(|row| row.tool.is_none())
        .map(|row| row.guide_row)
        .collect();
    assert_eq!(
        without,
        vec![
            "Mixed-version requests",
            "Total response",
            "Managed capability"
        ]
    );
    assert_eq!(MATRIX.iter().filter(|row| row.tool.is_some()).count(), 7);
}
