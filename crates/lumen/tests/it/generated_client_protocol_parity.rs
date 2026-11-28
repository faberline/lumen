//! Protocol parity of the generated clients (lumen#74): QUERY-first with the
//! POST twin as the fallback, and typed Rust unions and enums.
//!
//! The text tests read the source that `lumen spec gen` writes. The runtime
//! test builds a generated Rust crate and decodes fixed JSON. No network
//! service, credential or environment value is used or printed.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const CARGO_TOML: &str = r#"[package]
name = "generated-lumen-rust-union-parity"
version = "0.0.0"
edition = "2021"

[dependencies]
reqwest = { version = "0.12", default-features = false, features = ["blocking", "json", "rustls-tls-native-roots"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
"#;

const UNION_MAIN_RS: &str = r##"#[allow(dead_code)]
mod client;
#[allow(dead_code)]
mod models;

fn main() {
    // Internally tagged union: a known discriminator decodes into its variant.
    let total: models::TrackTotal =
        serde_json::from_str(r#"{"mode":"up_to","limit":10}"#).expect("up_to decodes");
    assert!(matches!(
        total,
        models::TrackTotal::TrackTotalVariant2(ref v) if v.limit == 10
    ));
    let encoded = serde_json::to_string(&total).expect("TrackTotal encodes");
    assert!(encoded.contains("\"mode\":\"up_to\""), "encoded: {encoded}");

    // Negative control: an unknown discriminator must fail, not fall back.
    assert!(serde_json::from_str::<models::TrackTotal>(r#"{"mode":"bogus"}"#).is_err());
    assert!(serde_json::from_str::<models::BatchSearchResult>(r#"{"status":"bogus"}"#).is_err());
    assert!(serde_json::from_str::<models::ReplaceDocResult>(r#"{"status":"bogus"}"#).is_err());

    // Untagged union of component structs: a term query picks its variant.
    let node: models::QueryNode =
        serde_json::from_str(r#"{"term":{"field":"tags","value":"rust"}}"#)
            .expect("term query decodes");
    assert!(matches!(node, models::QueryNode::QueryNodeTerm(_)));
    assert!(serde_json::from_str::<models::QueryNode>(r#"{"nope":{}}"#).is_err());

    println!("ok");
}
"##;

fn temp_dir(label: &str) -> PathBuf {
    let serial = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "lumen-protocol-parity-{label}-{}-{serial}",
        std::process::id()
    ))
}

fn lumen() -> Command {
    Command::new(env!("CARGO_BIN_EXE_lumen"))
}

fn gen_into(lang: &str, dir: &Path) {
    let out = lumen()
        .args(["spec", "gen", "--lang", lang, "--out"])
        .arg(dir)
        .output()
        .expect("capture lumen spec gen output");
    assert!(
        out.status.success(),
        "spec gen --lang {lang} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Generates `lang` into a fresh temp dir and returns the text of `file`.
fn generated_text(lang: &str, file: &str) -> String {
    let dir = temp_dir(&format!("text-{lang}-{file}"));
    gen_into(lang, &dir);
    let text = fs::read_to_string(dir.join(file))
        .unwrap_or_else(|error| panic!("read generated {file}: {error}"));
    let _ = fs::remove_dir_all(&dir);
    text
}

/// True when the Rust source declares `pub enum <name> {`.
fn models_has_typed_enum(text: &str, name: &str) -> bool {
    text.contains(&format!("pub enum {name} {{"))
}

/// True when `attr` is the line directly above `decl`.
fn attr_then_decl(text: &str, attr: &str, decl: &str) -> bool {
    let lines: Vec<&str> = text.lines().collect();
    lines
        .windows(2)
        .any(|pair| pair[0] == attr && pair[1] == decl)
}

/// The lines of `pub enum <name> {` that start with `prefix`, up to its closing `}`.
fn enum_lines_with_prefix<'a>(text: &'a str, name: &str, prefix: &str) -> Vec<&'a str> {
    let header = format!("pub enum {name} {{");
    let mut inside = false;
    let mut found = Vec::new();
    for line in text.lines() {
        if !inside {
            inside = line == header;
            continue;
        }
        if line == "}" {
            break;
        }
        if line.starts_with(prefix) {
            found.push(line);
        }
    }
    found
}

fn require_cargo() {
    let out = Command::new("cargo")
        .arg("--version")
        .output()
        .unwrap_or_else(|error| {
            panic!("cargo must be installed to build the generated crate: {error}")
        });
    assert!(out.status.success(), "cargo --version failed");
}

#[test]
fn rust_models_type_the_discriminated_unions() {
    let models = generated_text("rust", "models.rs");
    assert!(
        attr_then_decl(
            &models,
            r#"#[serde(tag = "status")]"#,
            "pub enum BatchSearchResult {"
        ),
        "BatchSearchResult must be a tagged enum"
    );
    assert!(
        attr_then_decl(
            &models,
            r#"#[serde(tag = "status")]"#,
            "pub enum ReplaceDocResult {"
        ),
        "ReplaceDocResult must be a tagged enum"
    );
    assert!(
        attr_then_decl(
            &models,
            r#"#[serde(tag = "mode")]"#,
            "pub enum TrackTotal {"
        ),
        "TrackTotal must be a tagged enum"
    );
    assert!(models.contains(r#"#[serde(rename = "up_to")]"#));
    for name in ["BatchSearchResult", "ReplaceDocResult", "TrackTotal"] {
        assert!(
            !models.contains(&format!("pub type {name} =")),
            "{name} is still a serde_json::Value alias"
        );
    }
}

#[test]
fn rust_models_type_query_node_and_range_bound() {
    let models = generated_text("rust", "models.rs");
    assert!(models_has_typed_enum(&models, "QueryNode"));
    assert!(models_has_typed_enum(&models, "RangeBound"));
    assert!(!models.contains("pub type QueryNode ="));
    let variants = enum_lines_with_prefix(&models, "QueryNode", "    QueryNode");
    assert_eq!(variants.len(), 15, "QueryNode variants: {variants:?}");
}

#[test]
fn rust_models_type_string_enums() {
    let models = generated_text("rust", "models.rs");
    assert!(models_has_typed_enum(&models, "FieldType"));
    assert!(models_has_typed_enum(&models, "SortOrder"));
    assert!(models_has_typed_enum(&models, "VectorMetric"));
}

#[test]
fn rust_models_keep_field_value_untyped_by_design() {
    // Documented limit (clients/README.md): FieldValue stays a JSON value.
    // A later core tag that types it makes this fail; update the docs with it.
    let models = generated_text("rust", "models.rs");
    assert!(
        models
            .lines()
            .any(|line| line == "pub type FieldValue = serde_json::Value;"),
        "FieldValue is no longer a serde_json::Value alias; update clients/README.md"
    );
}

#[test]
fn rust_client_chooses_query_first_and_post_when_fallback_is_set() {
    let client = generated_text("rust", "client.rs");
    assert!(client.contains("pub fn with_post_fallback(mut self, use_post_fallback: bool) -> Self"));
    assert!(client.contains(r#"reqwest::Method::from_bytes(b"QUERY")"#));
    assert!(client.contains("self.http.post(twin_url)"));
}

#[test]
fn rust_client_post_fallback_hits_the_documented_twin_path() {
    let client = generated_text("rust", "client.rs");
    assert!(client.contains(r#"format!("{}/collections:search", self.base_url)"#));
}

#[test]
fn typescript_and_python_clients_have_query_and_post_twin() {
    let ts = generated_text("ts", "client.ts");
    assert!(ts.contains(r#"config.usePostFallback ? "POST" : "QUERY""#));
    assert!(ts.contains("/collections:search"));

    let py = generated_text("py", "client.py");
    assert!(py.contains("use_post_fallback: bool"));
    assert!(py.contains(r#"_method = "QUERY""#));
    assert!(py.contains("/collections:search"));
}

#[test]
fn openapi_documents_post_twin_for_every_query_operation() {
    // The OpenAPI document models the twin of `/collections` QUERY as the
    // POST operation on `/collections:search`. The twin of
    // `/collections/{collection_id}` QUERY is not in the document; it only
    // appears in the generated clients (`/collections/{id}/search`).
    let out = lumen().arg("spec").output().expect("run lumen spec");
    assert!(
        out.status.success(),
        "lumen spec failed\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("lumen spec prints the OpenAPI JSON");
    assert!(
        doc["paths"]["/collections"].get("query").is_some(),
        "/collections has a QUERY operation"
    );
    let twin = &doc["paths"]["/collections:search"]["post"];
    assert!(!twin.is_null(), "/collections:search has a POST twin");
    assert_eq!(twin["operationId"], "batch_search");
}

#[test]
fn rust_generated_unions_decode_and_reject_an_unknown_discriminator() {
    require_cargo();
    let dir = temp_dir("union-crate");
    let src = dir.join("src");
    gen_into("rust", &src);
    fs::write(dir.join("Cargo.toml"), CARGO_TOML).expect("write generated union crate Cargo.toml");
    fs::write(src.join("main.rs"), UNION_MAIN_RS).expect("write generated union crate main.rs");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .current_dir(&dir)
        .output()
        .expect("run cargo for the generated union crate");
    assert!(
        output.status.success(),
        "generated Rust union crate failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "ok");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn unions_test_fails_without_the_typed_enum() {
    // Negative control: the checker must reject a model where TrackTotal is an alias.
    let models = generated_text("rust", "models.rs");
    assert!(models_has_typed_enum(&models, "TrackTotal"));
    let broken = models.replace(
        "pub enum TrackTotal {",
        "pub type TrackTotal = serde_json::Value; // {",
    );
    assert!(!models_has_typed_enum(&broken, "TrackTotal"));
}
