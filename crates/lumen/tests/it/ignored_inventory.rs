//! Keeps docs/ignored-tests.md equal to the ignored tests in the source.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const KINDS: [&str; 5] = [
    "contract owner",
    "needs environment",
    "performance gate",
    "report only",
    "measurement harness",
];

/// `(path relative to the repository root, test function name)`.
type Entry = (String, String);

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root exists")
}

/// The identifier after the first `fn ` on a line.
fn fn_name(line: &str) -> Option<String> {
    let after = line.split("fn ").nth(1)?;
    let name: String = after
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

fn collect_source_entries(dir: &Path, root: &Path, out: &mut BTreeSet<Entry>) {
    let Ok(read) = fs::read_dir(dir) else {
        return;
    };
    for entry in read.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            collect_source_entries(&path, root, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let Ok(text) = fs::read_to_string(&path) else {
                continue;
            };
            let lines: Vec<&str> = text.lines().collect();
            for (i, line) in lines.iter().enumerate() {
                if !line.trim_start().starts_with("#[ignore") {
                    continue;
                }
                let name = lines[i + 1..]
                    .iter()
                    .find(|l| l.contains("fn "))
                    .and_then(|l| fn_name(l));
                if let Some(name) = name {
                    let rel = path
                        .strip_prefix(root)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.insert((rel, name));
                }
            }
        }
    }
}

fn source_entries(root: &Path) -> BTreeSet<Entry> {
    let mut out = BTreeSet::new();
    collect_source_entries(&root.join("crates"), root, &mut out);
    out
}

/// Table rows of the inventory, header and separator skipped, cells trimmed
/// of spaces and backticks: `[File, Test, Kind, Reason, CI job]`.
fn doc_rows(doc: &str) -> Vec<Vec<String>> {
    doc.lines()
        .filter(|l| l.trim_start().starts_with('|'))
        .filter_map(|l| {
            let cells: Vec<String> = l
                .trim()
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim().trim_matches('`').to_string())
                .collect();
            if cells.len() < 5 || cells[0] == "File" || cells[0].starts_with("---") {
                return None;
            }
            Some(cells)
        })
        .collect()
}

fn doc_entries(doc: &str) -> BTreeSet<Entry> {
    doc_rows(doc)
        .into_iter()
        .map(|cells| (cells[0].clone(), cells[1].clone()))
        .collect()
}

/// Human-readable differences; empty when the two sets are equal.
fn differences(source: &BTreeSet<Entry>, doc: &BTreeSet<Entry>) -> Vec<String> {
    let mut out = Vec::new();
    for (path, name) in source.difference(doc) {
        out.push(format!(
            "in source, missing from docs/ignored-tests.md: {path} {name}"
        ));
    }
    for (path, name) in doc.difference(source) {
        out.push(format!(
            "in docs/ignored-tests.md, not in source: {path} {name}"
        ));
    }
    out
}

fn read_doc(root: &Path) -> String {
    let path = root.join("docs/ignored-tests.md");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn ignored_tests_doc_matches_source() {
    let root = repo_root();
    let source = source_entries(&root);
    assert!(!source.is_empty(), "the scan found no ignored tests");
    let doc = doc_entries(&read_doc(&root));
    let diff = differences(&source, &doc);
    assert!(
        diff.is_empty(),
        "ignored-test inventory is out of date:\n{}",
        diff.join("\n")
    );
}

#[test]
fn ignored_tests_doc_rows_are_complete() {
    let rows = doc_rows(&read_doc(&repo_root()));
    assert!(!rows.is_empty(), "the inventory has no rows");
    let bad: Vec<String> = rows
        .iter()
        .filter(|cells| !KINDS.contains(&cells[2].as_str()) || cells[4].is_empty())
        .map(|cells| {
            format!(
                "{} {}: kind `{}`, CI job `{}`",
                cells[0], cells[1], cells[2], cells[4]
            )
        })
        .collect();
    assert!(
        bad.is_empty(),
        "rows need an allowed Kind and a CI job:\n{}",
        bad.join("\n")
    );
}

#[test]
fn ignored_inventory_detects_a_missing_row() {
    let mut source = BTreeSet::new();
    source.insert(("x.rs".to_string(), "y".to_string()));
    source.insert(("a.rs".to_string(), "b".to_string()));
    let mut doc = BTreeSet::new();
    doc.insert(("a.rs".to_string(), "b".to_string()));
    let diff = differences(&source, &doc);
    assert_eq!(
        diff.len(),
        1,
        "one extra source entry must be one difference: {diff:?}"
    );
    assert!(diff[0].contains("x.rs y"));
}
