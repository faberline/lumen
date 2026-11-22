//! Checks the Lumen 1.0.0 evidence record and its link from the docs index.

use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

fn record() -> String {
    std::fs::read_to_string(repo_root().join("docs/release/1.0.0-evidence.md"))
        .expect("docs/release/1.0.0-evidence.md must exist")
}

/// The lines after the line that starts with `heading`, up to the next `## ` line.
fn section(text: &str, heading: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for line in text.lines() {
        if line.starts_with(heading) {
            inside = true;
            continue;
        }
        if inside && line.starts_with("## ") {
            break;
        }
        if inside {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

fn line_value<'a>(body: &'a str, prefix: &str) -> &'a str {
    body.lines()
        .find_map(|l| l.strip_prefix(prefix))
        .unwrap_or("")
}

#[test]
fn record_has_one_section_per_point_and_a_gaps_section() {
    let text = record();
    for heading in ["## Point 9", "## Point 6", "## Point 11", "## Gaps"] {
        let count = text.lines().filter(|l| l.starts_with(heading)).count();
        assert_eq!(count, 1, "heading `{heading}` must occur exactly once");
    }
}

#[test]
fn record_has_no_placeholder_text() {
    let text = record();
    let bad: Vec<&str> = text
        .lines()
        .filter(|l| l.contains("TODO") || l.contains("TBD"))
        .collect();
    assert!(bad.is_empty(), "placeholder lines: {bad:?}");
}

#[test]
fn each_point_section_names_command_date_commit_and_result() {
    let text = record();
    for heading in ["## Point 9", "## Point 6", "## Point 11"] {
        let body = section(&text, heading);
        for prefix in ["- Command: ", "- Date: ", "- Commit: ", "- Result: "] {
            assert!(
                body.lines().any(|l| l.starts_with(prefix)),
                "{heading} lacks a line starting with `{prefix}`"
            );
        }
        let date = line_value(&body, "- Date: ");
        let date_ok = date.len() == 10
            && date.bytes().enumerate().all(|(i, b)| match i {
                4 | 7 => b == b'-',
                _ => b.is_ascii_digit(),
            });
        assert!(date_ok, "{heading}: date must be YYYY-MM-DD, got `{date}`");
        let commit = line_value(&body, "- Commit: ");
        let commit_ok =
            (7..=40).contains(&commit.len()) && commit.bytes().all(|b| b.is_ascii_hexdigit());
        assert!(
            commit_ok,
            "{heading}: commit must be 7 to 40 hex digits, got `{commit}`"
        );
    }
}

#[test]
fn gaps_section_proposes_titles() {
    let text = record();
    let gaps = section(&text, "## Gaps");
    let has_title = gaps.lines().any(|l| {
        l.strip_prefix("- Proposed title: ")
            .is_some_and(|t| !t.trim().is_empty())
    });
    assert!(
        has_title,
        "## Gaps needs a `- Proposed title: ` line with text"
    );
}

#[test]
fn docs_index_links_the_record() {
    let index = std::fs::read_to_string(repo_root().join("docs/README.md"))
        .expect("docs/README.md must exist");
    assert!(
        index.contains("release/1.0.0-evidence.md"),
        "docs/README.md must link release/1.0.0-evidence.md"
    );
}
