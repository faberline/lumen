//! `docs/coverage-exceptions.md` names every `#[ignore]` test in `crates/lumen`
//! with its command and CI job, lists the 20 least covered files, and keeps
//! `line` in `aw.toml` from being lowered.
//!
//! The ignore scan is a line scan: a line whose trimmed text starts with
//! `#[ignore` names the next `fn` within the following 8 lines. A string
//! literal that contains `#[ignore` starts with `"` after trimming, so it does
//! not match.

use std::fs;
use std::path::{Path, PathBuf};

const DOC: &str = "docs/coverage-exceptions.md";

fn repo_root() -> PathBuf {
    // The lumen crate lives in crates/lumen; the repository root is two up.
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        if name == "target" || name == ".git" {
            continue;
        }
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// `(file relative to crates/lumen, fn name)` for every `#[ignore]` test.
fn ignored_tests(root: &Path) -> Vec<(String, String)> {
    let crate_dir = root.join("crates/lumen");
    let mut files = Vec::new();
    walk(&crate_dir, &mut files);
    let mut found = Vec::new();
    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        let rel = file
            .strip_prefix(&crate_dir)
            .unwrap_or(&file)
            .to_string_lossy()
            .replace('\\', "/");
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if !line.trim_start().starts_with("#[ignore") {
                continue;
            }
            let end = (i + 9).min(lines.len());
            for next in &lines[i + 1..end] {
                let mut t = next.trim_start();
                t = t.strip_prefix("pub ").unwrap_or(t);
                t = t.strip_prefix("async ").unwrap_or(t);
                if let Some(rest) = t.strip_prefix("fn ") {
                    let name = rest.split('(').next().unwrap_or("").trim().to_string();
                    found.push((rel.clone(), name));
                    break;
                }
            }
        }
    }
    found
}

/// The text under `heading` up to the next `## ` heading.
fn section<'a>(doc: &'a str, heading: &str) -> &'a str {
    let body = doc.split(heading).nth(1).unwrap_or("");
    body.split("\n## ").next().unwrap_or(body)
}

/// Tests that have no row under `## Ignored tests` with both their name in
/// backticks and their file path on the same line.
fn missing_from_doc(tests: &[(String, String)], doc: &str) -> Vec<String> {
    let rows = section(doc, "## Ignored tests");
    tests
        .iter()
        .filter(|(file, name)| {
            !rows
                .lines()
                .any(|l| l.contains(&format!("`{name}`")) && l.contains(file.as_str()))
        })
        .map(|(_, name)| name.clone())
        .collect()
}

#[test]
fn every_ignored_test_is_listed_in_the_exceptions_doc() {
    let root = repo_root();
    let doc = fs::read_to_string(root.join(DOC)).expect("read docs/coverage-exceptions.md");
    let missing = missing_from_doc(&ignored_tests(&root), &doc);
    assert!(
        missing.is_empty(),
        "ignored tests missing from {DOC}: {missing:?}"
    );
}

#[test]
fn the_ignore_scan_is_not_vacuous() {
    let tests = ignored_tests(&repo_root());
    assert!(
        tests.len() >= 30,
        "the scan found {} ignored tests, expected at least 30",
        tests.len()
    );
    for name in [
        "index_throughput_floor",
        "scale_proof_reopen_rss_is_bounded",
    ] {
        assert!(
            tests.iter().any(|(_, n)| n == name),
            "the scan missed {name}"
        );
    }
}

#[test]
fn the_doc_lists_the_twenty_least_covered_files() {
    let doc = fs::read_to_string(repo_root().join(DOC)).expect("read docs/coverage-exceptions.md");
    let rows: Vec<&str> = section(&doc, "## Least covered files")
        .lines()
        .filter(|l| l.trim_start().starts_with('|'))
        .skip(2) // header and separator
        .collect();
    assert_eq!(
        rows.len(),
        20,
        "expected 20 data rows, found {}",
        rows.len()
    );
    for row in rows {
        let cells = row.trim().trim_matches('|').split('|').count();
        assert_eq!(cells, 4, "row must have 4 cells: {row}");
    }
}

#[test]
fn line_in_aw_toml_is_not_lowered() {
    let toml = fs::read_to_string(repo_root().join("aw.toml")).expect("read aw.toml");
    let mut in_section = false;
    let mut value = None;
    for raw in toml.lines() {
        let t = raw.trim();
        if t.starts_with('[') {
            in_section = t == "[standards.coverage]";
            continue;
        }
        if in_section {
            if let Some((key, val)) = t.split_once('=') {
                if key.trim() == "line" {
                    value = val.trim().parse::<f64>().ok();
                }
            }
        }
    }
    let line = value.expect("aw.toml has [standards.coverage] line = <number>");
    assert!(
        line >= 87.34,
        "line in aw.toml is {line}, it must not be lowered below 87.34"
    );
}

#[test]
fn a_missing_test_is_reported() {
    let doc = "## Ignored tests\n| `index_throughput_floor` | tests/it/perf_gate.rs | c | j |";
    let tests = vec![
        (
            "tests/it/perf_gate.rs".to_string(),
            "index_throughput_floor".to_string(),
        ),
        (
            "tests/it/write_qps.rs".to_string(),
            "write_qps_bench".to_string(),
        ),
    ];
    assert_eq!(
        missing_from_doc(&tests, doc),
        vec!["write_qps_bench".to_string()]
    );
}
