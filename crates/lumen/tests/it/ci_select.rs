//! Path selection of the CI `test-select` job (faberline/lumen#371).
//!
//! The selection script reads the changed paths of a pull request and the
//! reviewed map `.github/ci-paths.json`. These tests pin its answers and the
//! wiring of `.github/workflows/ci.yml`.

use serde_yaml::Value as Yaml;
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

fn root() -> PathBuf {
    PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

fn parse_bool(value: &str) -> bool {
    match value {
        "true" => true,
        "false" => false,
        other => panic!("not a boolean: {other:?}"),
    }
}

/// Runs `scripts/ci/select_paths.py` on `paths` and returns `(tests, image)`.
fn select(paths: &[&str], all: bool) -> (bool, bool) {
    let mut args = vec![
        "scripts/ci/select_paths.py",
        "--map",
        ".github/ci-paths.json",
    ];
    if all {
        args.push("--all");
    }
    let mut child = Command::new("python3")
        .args(&args)
        .current_dir(root())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("python3 starts");
    child
        .stdin
        .take()
        .expect("stdin is piped")
        .write_all(paths.join("\n").as_bytes())
        .expect("paths are written");
    let out = child.wait_with_output().expect("select_paths.py finishes");
    assert!(
        out.status.success(),
        "select_paths.py failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).expect("stdout is UTF-8");
    let mut tests = None;
    let mut image = None;
    for line in stdout.lines() {
        if let Some(value) = line.strip_prefix("tests=") {
            tests = Some(parse_bool(value));
        } else if let Some(value) = line.strip_prefix("image=") {
            image = Some(parse_bool(value));
        }
    }
    (
        tests.expect("stdout has a tests= line"),
        image.expect("stdout has an image= line"),
    )
}

#[test]
fn select_docs_only_runs_neither_group() {
    for paths in [
        &["docs/glossary.md"][..],
        &["docs/operations/development-pipeline.md"][..],
        &["LICENSE"][..],
    ] {
        assert_eq!(select(paths, false), (false, false), "{paths:?}");
    }
}

#[test]
fn select_files_that_tests_read_run_the_tests() {
    for path in [
        "docs/benchmarks-scale.md",
        "README.md",
        "STATUS.md",
        "llms.txt",
        "clients/README.md",
    ] {
        assert_eq!(select(&[path], false), (true, false), "{path}");
    }
}

#[test]
fn select_unit_paths_run_tests_without_images() {
    assert_eq!(
        select(&["crates/lumen-storage/src/lib.rs"], false),
        (true, false)
    );
    assert_eq!(
        select(&["crates/lumen/tests/it/vector_e2e.rs"], false),
        (true, false)
    );
}

#[test]
fn select_image_paths_run_everything() {
    for path in [
        "crates/lumen-operator/src/lib.rs",
        "k8s/base/pdb.yaml",
        "Dockerfile",
    ] {
        assert_eq!(select(&[path], false), (true, true), "{path}");
    }
}

#[test]
fn select_workflow_and_lock_run_everything() {
    for path in [
        ".github/workflows/ci.yml",
        "Cargo.lock",
        "Cargo.toml",
        "aw.toml",
        "scripts/ci/select_paths.py",
    ] {
        assert_eq!(select(&[path], false), (true, true), "{path}");
    }
}

#[test]
fn select_takes_the_highest_class() {
    assert_eq!(
        select(
            &["docs/glossary.md", "crates/lumen-storage/src/lib.rs"],
            false
        ),
        (true, false)
    );
    assert_eq!(
        select(&["docs/glossary.md", "k8s/base/pdb.yaml"], false),
        (true, true)
    );
}

#[test]
fn select_unknown_path_runs_everything() {
    // Negative control: an unmatched path must not count as `docs`.
    assert_eq!(select(&["somenew/file.txt"], false), (true, true));
}

#[test]
fn select_empty_input_and_all_flag_run_everything() {
    assert_eq!(select(&[], false), (true, true));
    assert_eq!(select(&["docs/glossary.md"], true), (true, true));
}

/// The job ids and their `needs` and `if` values that the selection needs.
/// Returns true only when every one of them is as the spec says.
fn wired(source: &str) -> bool {
    let Ok(workflow) = serde_yaml::from_str::<Yaml>(source) else {
        return false;
    };
    let jobs = &workflow["jobs"];
    let expected: [(&str, &[&str], Option<&str>); 8] = [
        ("test-select", &["lint"], None),
        (
            "test-workspace",
            &["lint", "test-select"],
            Some("needs.test-select.outputs.tests == 'true'"),
        ),
        (
            "test-all-features",
            &["lint", "test-select"],
            Some("needs.test-select.outputs.tests == 'true'"),
        ),
        (
            "test-features",
            &["lint", "test-select"],
            Some("needs.test-select.outputs.tests == 'true'"),
        ),
        (
            "e2e-image",
            &[
                "test-all-features",
                "test-features",
                "test-select",
                "test-workspace",
            ],
            Some("needs.test-select.outputs.image == 'true'"),
        ),
        (
            "e2e-client",
            &[
                "e2e-image",
                "test-all-features",
                "test-features",
                "test-select",
                "test-workspace",
            ],
            Some("needs.test-select.outputs.image == 'true'"),
        ),
        (
            "e2e-gke",
            &["e2e-client"],
            Some("${{ needs.e2e-client.outputs.suites != '[]' }}"),
        ),
        (
            "e2e-native",
            &[
                "e2e-client",
                "test-all-features",
                "test-features",
                "test-workspace",
            ],
            None,
        ),
    ];
    for (id, needs, condition) in expected {
        let Some(job) = jobs.get(id) else {
            return false;
        };
        if sorted_needs(job) != sorted(needs) {
            return false;
        }
        if job.get("if").and_then(Yaml::as_str) != condition {
            return false;
        }
    }
    // `gate` must still need `test-select`.
    let gate_needs = sorted_needs(&jobs["gate"]);
    gate_needs.iter().any(|need| need == "test-select")
}

fn sorted(items: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = items.iter().map(|item| item.to_string()).collect();
    out.sort();
    out
}

fn sorted_needs(job: &Yaml) -> Vec<String> {
    let mut out: Vec<String> = match job.get("needs") {
        Some(Yaml::String(need)) => vec![need.clone()],
        Some(Yaml::Sequence(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .expect("a needs entry is a string")
                    .to_string()
            })
            .collect(),
        _ => Vec::new(),
    };
    out.sort();
    out
}

fn ci_workflow() -> String {
    std::fs::read_to_string(root().join(".github/workflows/ci.yml")).expect("ci.yml is readable")
}

#[test]
fn ci_selection_is_wired() {
    let source = ci_workflow();
    assert!(
        wired(&source),
        "ci.yml does not wire test-select as the spec says"
    );

    // Negative control: without the `if` of `e2e-image` the same check fails.
    let from = "    needs: [test-workspace, test-all-features, test-features, test-select]\n    if: needs.test-select.outputs.image == 'true'\n";
    let to = "    needs: [test-workspace, test-all-features, test-features, test-select]\n";
    assert_eq!(
        source.matches(from).count(),
        1,
        "the e2e-image text is unique"
    );
    let changed = source.replacen(from, to, 1);
    assert!(
        !wired(&changed),
        "a workflow without the e2e-image if passed"
    );
}

#[test]
fn ci_main_run_selects_everything() {
    let workflow: Yaml = serde_yaml::from_str(&ci_workflow()).expect("ci.yml is YAML");
    let steps = workflow["jobs"]["test-select"]["steps"]
        .as_sequence()
        .expect("test-select has steps");
    let script = steps
        .iter()
        .filter_map(|step| step.get("run").and_then(Yaml::as_str))
        .find(|run| run.contains("all=(--all)"))
        .expect("the test-select step has the all=(--all) rule");
    assert!(
        script.contains("EVENT_NAME"),
        "the rule reads the event name"
    );
}
