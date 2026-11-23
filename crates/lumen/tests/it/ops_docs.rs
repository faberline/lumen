//! The operations documents must cover what the code exposes.
//!
//! Two documents are checked here:
//!
//! - `docs/runbooks/alerts.md` has one section per metric name that the source
//!   registers, plus the two health endpoints. A metric added to the source
//!   with no section fails the first test.
//! - `docs/limits.md` has one row per enforced limit. Each row names the
//!   constant and the file that define it, and the constant must occur in that
//!   file. A limit that moves or is renamed fails the last test.
//!
//! The metric scan reads string literals, so it finds a name only where the
//! source spells it out in full.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    // The lumen crate lives in crates/lumen; the repository root is two up.
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

/// Collect the `.rs` files under `dir`, skipping build output, any directory
/// named `tests`, and any file named `tests.rs`.
fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name == "target" || name == "tests" {
                continue;
            }
            collect_rs(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") && name != "tests.rs" {
            out.push(path);
        }
    }
}

/// Every metric name that a non-test source file registers. A histogram's
/// `_bucket`, `_sum` and `_count` series count as its base name.
fn registered_metric_names(root: &Path) -> BTreeSet<String> {
    let mut files = Vec::new();
    collect_rs(&root.join("crates/lumen-index/src"), &mut files);
    collect_rs(&root.join("crates/lumen/src"), &mut files);

    let mut names = BTreeSet::new();
    for file in files {
        let Ok(text) = fs::read_to_string(&file) else {
            continue;
        };
        // Everything from the first test module on is test code.
        let text = match text.find("#[cfg(test)]") {
            Some(cut) => &text[..cut],
            None => &text,
        };
        let bytes = text.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'"' && bytes[i + 1..].starts_with(b"lumen_") {
                let start = i + 1;
                let mut end = start;
                while end < bytes.len()
                    && (bytes[end].is_ascii_lowercase()
                        || bytes[end].is_ascii_digit()
                        || bytes[end] == b'_')
                {
                    end += 1;
                }
                // A metric name is the whole literal: the closing quote follows it.
                if end < bytes.len() && bytes[end] == b'"' {
                    let mut name = &text[start..end];
                    for suffix in ["_bucket", "_sum", "_count"] {
                        if let Some(base) = name.strip_suffix(suffix) {
                            name = base;
                            break;
                        }
                    }
                    names.insert(name.to_string());
                }
                i = end;
            } else {
                i += 1;
            }
        }
    }
    names
}

/// The names that have no `` ### `name` `` heading in `doc`.
fn missing_sections(names: &BTreeSet<String>, doc: &str) -> Vec<String> {
    names
        .iter()
        .filter(|name| {
            let heading = format!("### `{name}`");
            !doc.lines().any(|line| line.trim_end() == heading)
        })
        .cloned()
        .collect()
}

fn alerts_doc() -> String {
    let path = repo_root().join("docs/runbooks/alerts.md");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

fn limits_doc() -> String {
    let path = repo_root().join("docs/limits.md");
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn every_registered_metric_has_a_section_in_alerts_md() {
    let names = registered_metric_names(&repo_root());
    let missing = missing_sections(&names, &alerts_doc());
    assert!(
        missing.is_empty(),
        "metrics registered in the source with no `### ` section in docs/runbooks/alerts.md: {missing:?}"
    );
}

#[test]
fn the_metric_scan_is_not_vacuous() {
    let names = registered_metric_names(&repo_root());
    assert!(
        names.len() >= 50,
        "the scan found {} metric names; expected at least 50",
        names.len()
    );
    assert!(
        names.contains("lumen_storage_degraded"),
        "scan misses lumen_storage_degraded"
    );
    assert!(
        names.contains("lumen_search_latency_seconds"),
        "scan misses lumen_search_latency_seconds"
    );
}

#[test]
fn every_metric_section_has_meaning_check_and_fix() {
    let doc = alerts_doc();
    let mut incomplete = Vec::new();
    let mut current: Option<(String, [bool; 3])> = None;

    let finish = |section: Option<(String, [bool; 3])>, incomplete: &mut Vec<String>| {
        if let Some((name, [meaning, check, fix])) = section {
            if !(meaning && check && fix) {
                incomplete.push(name);
            }
        }
    };

    for line in doc.lines() {
        if let Some(rest) = line.strip_prefix("### `lumen_") {
            finish(current.take(), &mut incomplete);
            let name = rest.trim_end_matches('`').to_string();
            current = Some((format!("lumen_{name}"), [false; 3]));
        } else if line.starts_with('#') {
            finish(current.take(), &mut incomplete);
        } else if let Some((_, flags)) = current.as_mut() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("- Meaning:") {
                flags[0] = true;
            } else if trimmed.starts_with("- Check:") {
                flags[1] = true;
            } else if trimmed.starts_with("- Fix:") {
                flags[2] = true;
            }
        }
    }
    finish(current.take(), &mut incomplete);

    assert!(
        incomplete.is_empty(),
        "metric sections without all of `- Meaning:`, `- Check:` and `- Fix:`: {incomplete:?}"
    );
}

#[test]
fn health_endpoints_are_documented() {
    let doc = alerts_doc();
    for heading in ["### `/healthz`", "### `/readyz`"] {
        assert!(
            doc.lines().any(|line| line.trim_end() == heading),
            "docs/runbooks/alerts.md has no `{heading}` section"
        );
    }
}

#[test]
fn every_limit_names_its_constant_and_file() {
    let root = repo_root();
    let doc = limits_doc();
    let mut rows = 0;
    let mut problems = Vec::new();

    for line in doc.lines() {
        if !line.starts_with("| ") {
            continue;
        }
        // The header and the separator row are not limits.
        if line.starts_with("| Limit ") || line.starts_with("|---") || line.starts_with("| ---") {
            continue;
        }
        let cells: Vec<&str> = line
            .trim()
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect();
        if cells.len() != 4 {
            problems.push(format!("row does not have 4 columns: {line}"));
            continue;
        }
        rows += 1;
        let (value, constant, file) = (
            cells[1],
            cells[2].trim_matches('`'),
            cells[3].trim_matches('`'),
        );

        if value.starts_with("not enforced") {
            if constant != "-" || file != "-" {
                problems.push(format!(
                    "`not enforced` row must have `-` in Constant and File: {line}"
                ));
            }
            continue;
        }

        let path = root.join(file);
        match fs::read_to_string(&path) {
            Ok(text) if text.contains(constant) => {}
            Ok(_) => problems.push(format!("`{constant}` does not occur in {file}")),
            Err(e) => problems.push(format!("cannot read {file} for `{constant}`: {e}")),
        }
    }

    assert!(rows > 0, "docs/limits.md has no limit rows");
    assert!(
        problems.is_empty(),
        "docs/limits.md problems:\n{}",
        problems.join("\n")
    );
}

#[test]
fn a_missing_metric_section_is_reported() {
    let names: BTreeSet<String> = ["lumen_a", "lumen_b"]
        .into_iter()
        .map(String::from)
        .collect();
    let doc = "### `lumen_a`\n";
    assert_eq!(missing_sections(&names, doc), vec!["lumen_b".to_string()]);
}
