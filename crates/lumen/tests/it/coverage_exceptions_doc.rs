//! `docs/coverage-exceptions.md` must list every `#[ignore]` test under
//! `crates/lumen`, the twenty least covered files, and keep the coverage floor.
//!
//! The scan reads each `.rs` file under `crates/lumen` and takes a line whose
//! trimmed text starts with the ignore attribute. Lines inside string literals
//! start with a quote after trimming, so they do not match.

use std::fs;
use std::path::{Path, PathBuf};

const DOC: &str = "docs/coverage-exceptions.md";

/// The coverage floor the test pins. A later raise passes; a lowering fails.
const LINE_FLOOR: f64 = 87.34;

fn repo_root() -> PathBuf {
    // The lumen crate lives in crates/lumen; the repository root is two up.
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

/// Every `.rs` file under `dir`, skipping build output and VCS metadata.
fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = entry.file_name();
            if matches!(name.to_string_lossy().as_ref(), "target" | ".git") {
                continue;
            }
            walk(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// The identifier after the first `fn ` on the line, if any.
fn fn_name(line: &str) -> Option<String> {
    let pos = line.find("fn ")?;
    let name: String = line[pos + 3..]
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Every ignored test as (file relative to `crates/lumen`, function name).
fn ignored_tests(root: &Path) -> Vec<(String, String)> {
    let lumen = root.join("crates/lumen");
    let mut files = Vec::new();
    walk(&lumen, &mut files);
    files.sort();

    let mut found = Vec::new();
    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        let rel = file
            .strip_prefix(&lumen)
            .expect("file is under crates/lumen")
            .to_string_lossy()
            .replace('\\', "/");
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if !line.trim_start().starts_with("#[ignore") {
                continue;
            }
            if let Some(name) = lines.iter().skip(i + 1).take(8).find_map(|l| fn_name(l)) {
                found.push((rel.clone(), name));
            }
        }
    }
    found
}

/// The names of the tests that have no table row under `## Ignored tests`
/// with both the test name in backticks and the file path.
fn missing_from_doc(tests: &[(String, String)], doc: &str) -> Vec<String> {
    let section = doc.split("## Ignored tests").nth(1).unwrap_or("");
    tests
        .iter()
        .filter(|(file, name)| {
            let needle = format!("`{name}`");
            !section
                .lines()
                .any(|l| l.starts_with('|') && l.contains(&needle) && l.contains(file.as_str()))
        })
        .map(|(_, name)| name.clone())
        .collect()
}

/// The data rows of the table under `## Least covered files`.
fn least_covered_rows(doc: &str) -> Vec<&str> {
    let section = doc.split("## Least covered files").nth(1).unwrap_or("");
    let section = section.split("\n## ").next().unwrap_or("");
    section
        .lines()
        .filter(|l| l.starts_with('|'))
        .filter(|l| !l.starts_with("| File |") && !l.starts_with("|---"))
        .collect()
}

/// The `line` value under `[standards.coverage]` in aw.toml.
fn coverage_line_value(toml: &str) -> Option<f64> {
    let mut in_section = false;
    for raw in toml.lines() {
        let line = raw.trim();
        if line.starts_with('[') {
            in_section = line == "[standards.coverage]";
            continue;
        }
        if !in_section {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            if key.trim() == "line" {
                return value.trim().parse().ok();
            }
        }
    }
    None
}

fn read_doc(root: &Path) -> String {
    fs::read_to_string(root.join(DOC)).unwrap_or_else(|err| panic!("read {DOC}: {err}"))
}

#[test]
fn every_ignored_test_is_listed_in_the_exceptions_doc() {
    let root = repo_root();
    let doc = read_doc(&root);
    let missing = missing_from_doc(&ignored_tests(&root), &doc);
    assert!(
        missing.is_empty(),
        "these #[ignore] tests are missing from {DOC} under `## Ignored tests`: {missing:?}"
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
            "the scan did not find `{name}`"
        );
    }
}

#[test]
fn the_doc_lists_the_twenty_least_covered_files() {
    let doc = read_doc(&repo_root());
    let rows = least_covered_rows(&doc);
    assert_eq!(
        rows.len(),
        20,
        "{DOC} must list exactly 20 least covered files, found {}",
        rows.len()
    );
    for row in rows {
        let cells = row.trim().trim_matches('|').split('|').count();
        assert_eq!(cells, 4, "row must have 4 cells: {row}");
    }
}

#[test]
fn line_in_aw_toml_is_not_lowered() {
    let root = repo_root();
    let toml = fs::read_to_string(root.join("aw.toml")).expect("read aw.toml");
    let value = coverage_line_value(&toml).expect("`line` under [standards.coverage] in aw.toml");
    assert!(
        value >= LINE_FLOOR,
        "aw.toml `line` is {value}, below the floor {LINE_FLOOR}"
    );
}

#[test]
fn a_missing_test_is_reported() {
    let doc = "## Ignored tests\n| `index_throughput_floor` | tests/it/perf_gate.rs | c | j |";
    let tests = [
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
