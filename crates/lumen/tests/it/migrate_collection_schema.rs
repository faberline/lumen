//! `lumen migrate collection-schema` through the real binary: stdin in,
//! stdout and exit status out. The command is offline, so none of these
//! tests starts a server.

use std::io::Write;
use std::process::{Command, Output, Stdio};

use serde_json::Value;

const README_SCHEMA: &str = r#"{
  "fields": {
    "bio": { "type": "text", "analyzer": "whitespace_lower" },
    "email": { "type": "keyword" },
    "tags": { "type": "set" },
    "age": { "type": "number" },
    "embedding": { "type": "vector", "dim": 768, "metric": "cosine", "backend": "hnsw-cpu" },
    "avatar_phash": { "type": "hash" }
  }
}"#;

fn migrate_with_env(stdin: &str, env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lumen"));
    command
        .args(["migrate", "collection-schema"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("spawn lumen");
    child
        .stdin
        .take()
        .expect("piped stdin")
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    child.wait_with_output().expect("wait for lumen")
}

fn migrate(stdin: &str) -> Output {
    migrate_with_env(stdin, &[])
}

fn stdout_json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("stdout is JSON")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn converts_the_readme_schema() {
    let output = migrate(README_SCHEMA);
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(output.stdout.ends_with(b"\n"));
    let json = stdout_json(&output);
    assert_eq!(json["schema"]["fields"]["age"]["type"], "float64");
    assert_eq!(
        json["schema"]["fields"]["tags"],
        serde_json::json!({"type": "keyword", "multi": true})
    );
    assert_eq!(json["schema"]["fields"]["embedding"]["dim"], 768);
    assert_eq!(
        json["report"]["rebuild_required"],
        serde_json::json!(["age", "tags"])
    );
}

#[test]
fn two_runs_are_byte_identical() {
    let first = migrate(README_SCHEMA);
    let second = migrate(README_SCHEMA);
    assert!(first.status.success() && second.status.success());
    assert_eq!(first.stdout, second.stdout);
}

#[test]
fn a_refused_schema_exits_nonzero_and_prints_nothing_to_stdout() {
    let output = migrate(r#"{"fields":{"a":{"type":"int64"}}}"#);
    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "stdout: {:?}", output.stdout);
    let err = stderr(&output);
    assert!(err.contains("refused:"), "stderr: {err}");
    assert!(err.contains(r#"field "a""#), "stderr: {err}");
}

#[test]
fn invalid_json_exits_nonzero() {
    let output = migrate("not json");
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("stdin is not valid JSON"),
        "stderr: {}",
        stderr(&output)
    );
}

#[test]
fn migrate_help_lists_collection_schema() {
    let output = Command::new(env!("CARGO_BIN_EXE_lumen"))
        .args(["migrate", "--help"])
        .output()
        .expect("run lumen migrate --help");
    assert!(output.status.success(), "stderr: {}", stderr(&output));
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("collection-schema"), "help: {help}");
}

#[test]
fn runs_without_a_runtime() {
    // Nothing listens on port 9. The command must succeed without connecting.
    let output = migrate_with_env(
        README_SCHEMA,
        &[
            ("HTTPS_PROXY", "http://127.0.0.1:9"),
            ("HTTP_PROXY", "http://127.0.0.1:9"),
        ],
    );
    assert!(output.status.success(), "stderr: {}", stderr(&output));
}
