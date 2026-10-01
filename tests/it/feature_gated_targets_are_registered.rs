//! Registry and verification of the feature-gated test cases in `lumen`.
//!
//! Integration cases are modules of the one `tests/it` binary, so
//! `tests/it/main.rs` is the case inventory and a case's gate is its inner
//! `#![cfg(...)]`. A few cases are their own binaries under `tests/`; a
//! feature gate there also needs a `[[test]] required-features` stanza.
//!
//! Asserts that every case file is declared exactly once in `main.rs`, that
//! every gated file is recorded in the registry with its exact required
//! features and the registry and tree agree in both directions, and that
//! every command naming a gated case enables the features that case needs. A
//! gated case whose features are off compiles to nothing and the command
//! reports `0 passed`, so a command that drops the feature would still pass.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use toml::Value;

struct GatedTarget {
    path: &'static str,
    gate: &'static str,
    required_features: &'static [&'static str],
}

impl GatedTarget {
    fn name(&self) -> &str {
        Path::new(self.path)
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("file stem")
    }

    fn is_case(&self) -> bool {
        self.path.starts_with("tests/it/")
    }
}

const REGISTRY: &[GatedTarget] = &[
    GatedTarget {
        path: "tests/it/hnsw_shutdown_cache.rs",
        gate: r#"#![cfg(unix)]"#,
        required_features: &[],
    },
    GatedTarget {
        path: "tests/it/inherited_checkpoint_file_sync.rs",
        gate: r#"#![cfg(unix)]"#,
        required_features: &[],
    },
    GatedTarget {
        path: "tests/it/aof_trim_append_progress.rs",
        gate: r#"#![cfg(unix)]"#,
        required_features: &[],
    },
    GatedTarget {
        path: "tests/it/jieba_bigram_fallback_e2e.rs",
        gate: r#"#![cfg(not(feature = "jieba"))]"#,
        required_features: &[],
    },
    GatedTarget {
        path: "tests/it/jieba_fallback_staging.rs",
        gate: r#"#![cfg(not(feature = "jieba"))]"#,
        required_features: &[],
    },
    GatedTarget {
        path: "tests/it/access_render_cli.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/cli_client_ksa_token.rs",
        gate: r#"#![cfg(all(unix, feature = "delegated-auth", feature = "backup"))]"#,
        required_features: &["delegated-auth", "backup"],
    },
    GatedTarget {
        path: "tests/it/operator_backup_kubernetes_wiring.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/operator_render.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/operator_retired_credential_projection.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/reshard_driver_e2e.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/routed_shard_e2e.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/capacity_catalog_contract.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/capacity_retire_hpa.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/capacity_catalog_client.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/report_4018_app.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
    GatedTarget {
        path: "tests/it/standalone_backup_restore_cli.rs",
        gate: r#"#![cfg(unix)]"#,
        required_features: &[],
    },
    GatedTarget {
        path: "tests/it/legacy_3073_app.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_committed_capacity_progress.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_oversized_committed_apply.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_oversized_committed_replace.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_private_layer_during_checkpoint.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_segment_snapshot_archive.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_segment_snapshot_archive_wiring.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/it/raft_shutdown_failover.rs",
        gate: r#"#![cfg(feature = "raft-wal")]"#,
        required_features: &["raft-wal"],
    },
    GatedTarget {
        path: "tests/body_limit_configurable.rs",
        gate: r#"#![cfg(feature = "operator")]"#,
        required_features: &["operator"],
    },
];

const MAINTAINED_CMD: &str = r#"`cargo test -p lumen --features "operator delegated-auth"`"#;
const DIRECT_FEATURES: &[&str] = &["operator", "delegated-auth"];
/// Features the maintained CONTRIBUTING row does not enable. Their cases run
/// only from commands that name them, which the command check holds to the
/// gate.
const NAMED_COMMAND_ONLY_FEATURES: &[&str] = &["raft-wal"];

/// Files that run or document test commands, relative to the repo root.
fn command_files(root: &Path) -> Vec<PathBuf> {
    let mut files = vec![
        root.join("CONTRIBUTING.md"),
        root.join("README.md"),
        root.join("STATUS.md"),
        root.join("llms.txt"),
        root.join("build.sh"),
        root.join("clients/README.md"),
    ];
    for (dir, ext) in [
        (".github/workflows", "yml"),
        ("scripts", "sh"),
        (".", "toml"),
        ("docs", "md"),
    ] {
        let Ok(entries) = fs::read_dir(root.join(dir)) else {
            continue;
        };
        let mut found: Vec<PathBuf> = entries
            .map(|e| e.expect("valid dir entry").path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some(ext))
            .collect();
        found.sort();
        files.extend(found);
    }
    files
}

struct Repo {
    main_rs: String,
    /// Stems of `tests/it/*.rs`, without `main` and `support`.
    case_files: Vec<String>,
    /// Stems of `tests/*.rs`.
    standalone_files: Vec<String>,
    cargo_toml: String,
    contributing: String,
    /// `(file, contents)` for every file in [`command_files`].
    commands: Vec<(String, String)>,
}

fn repo_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.to_path_buf()
}

fn rs_stems(dir: &Path) -> Vec<String> {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|e| panic!("failed to read dir {:?}: {}", dir, e));
    let mut stems: Vec<String> = entries
        .map(|e| e.expect("valid dir entry").path())
        .filter(|p| p.extension().and_then(|ext| ext.to_str()) == Some("rs"))
        .map(|p| p.file_stem().unwrap().to_str().unwrap().to_string())
        .collect();
    stems.sort();
    stems
}

fn load_repo() -> Repo {
    let root = repo_root();
    let read = |p: &Path| {
        fs::read_to_string(p).unwrap_or_else(|e| panic!("failed to read {:?}: {}", p, e))
    };
    let case_files = rs_stems(&root.join("tests/it"))
        .into_iter()
        .filter(|s| s != "main" && s != "support")
        .collect();
    let commands = command_files(&root)
        .into_iter()
        .filter(|p| p.is_file())
        .map(|p| {
            let rel = p.strip_prefix(&root).unwrap().display().to_string();
            (rel, read(&p))
        })
        .collect();
    Repo {
        main_rs: read(&root.join("tests/it/main.rs")),
        case_files,
        standalone_files: rs_stems(&root.join("tests")),
        cargo_toml: read(&root.join("Cargo.toml")),
        contributing: read(&root.join("CONTRIBUTING.md")),
        commands,
    }
}

fn find_cfg_gate(content: &str) -> Option<&str> {
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#![cfg(") {
            return Some(trimmed);
        }
    }
    None
}

fn count_test_rows(content: &str) -> usize {
    content
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            trimmed.starts_with("#[test]")
                || trimmed.starts_with("#[tokio::test]")
                || trimmed.starts_with("#[tokio::test(")
        })
        .count()
}

/// The `mod` declarations of `main.rs`, in order.
fn declared_modules(main_rs: &str) -> Vec<&str> {
    main_rs
        .lines()
        .filter_map(|l| l.trim().strip_prefix("mod "))
        .filter_map(|l| l.strip_suffix(';'))
        .collect()
}

/// Every feature `direct` turns on, following the `[features]` table.
fn feature_closure(manifest: &Value, direct: &[&str]) -> Result<HashSet<String>, String> {
    let features_table = manifest
        .get("features")
        .and_then(Value::as_table)
        .ok_or_else(|| "missing [features] in Cargo.toml".to_string())?;
    let mut active = HashSet::new();
    let mut to_visit: Vec<String> = direct.iter().map(|f| f.to_string()).collect();
    while let Some(feat) = to_visit.pop() {
        let deps = features_table
            .get(&feat)
            .and_then(Value::as_array)
            .ok_or_else(|| format!("feature '{feat}' missing or not an array in [features]"))?;
        for dep in deps.iter().filter_map(Value::as_str) {
            if features_table.contains_key(dep) && !active.contains(dep) {
                to_visit.push(dep.to_string());
            }
        }
        active.insert(feat);
    }
    Ok(active)
}

/// One `cargo test ... --test it ...` command: the features it enables and
/// the case modules its `module::` filters name.
struct CaseCommand {
    features: Vec<String>,
    modules: Vec<String>,
}

/// Finds each `--test it` command in `text`. A trailing `\` joins lines, and
/// a command ends at `&&`, `;`, `|`, a closing backtick, or the line end.
fn case_commands(text: &str) -> Vec<(String, CaseCommand)> {
    let joined = text.replace("\\\n", " ").replace("\\\"", "\"");
    let mut out = Vec::new();
    for line in joined.lines() {
        for segment in line.split(['`', ';', '|']).flat_map(|s| s.split("&&")) {
            let tokens = shell_words(segment);
            let Some(at) = tokens
                .windows(2)
                .position(|w| w[0] == "--test" && w[1] == "it")
            else {
                continue;
            };
            let mut features = Vec::new();
            let mut i = 0;
            while i < tokens.len() {
                if tokens[i] == "--features" && i + 1 < tokens.len() {
                    features.extend(
                        tokens[i + 1]
                            .split([' ', ','])
                            .filter(|f| !f.is_empty())
                            .map(str::to_string),
                    );
                    i += 1;
                } else if let Some(list) = tokens[i].strip_prefix("--features=") {
                    features.extend(list.split([' ', ',']).map(str::to_string));
                }
                i += 1;
            }
            let modules = tokens[at..]
                .iter()
                .skip_while(|t| *t != "--")
                .filter(|t| !t.starts_with('-') && t.contains("::"))
                .map(|t| t.split("::").next().unwrap().to_string())
                .collect();
            out.push((segment.trim().to_string(), CaseCommand { features, modules }));
        }
    }
    out
}

/// Splits on whitespace, keeping a `"..."` or `'...'` run as one word.
fn shell_words(s: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut cur = String::new();
    let mut quote = None;
    let mut in_word = false;
    for c in s.chars() {
        match quote {
            Some(q) if c == q => quote = None,
            Some(_) => cur.push(c),
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                in_word = true;
            }
            None if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            None => {
                cur.push(c);
                in_word = true;
            }
        }
    }
    if in_word {
        words.push(cur);
    }
    words
}

fn validate_layout(repo: &Repo) -> Result<(), String> {
    let manifest: Value =
        toml::from_str(&repo.cargo_toml).map_err(|e| format!("invalid Cargo.toml: {e}"))?;

    if manifest
        .get("package")
        .and_then(|p| p.get("autotests"))
        .and_then(Value::as_bool)
        == Some(false)
    {
        return Err("Cargo.toml sets autotests = false; tests/it would not be built".into());
    }

    // main.rs is the case inventory.
    let declared = declared_modules(&repo.main_rs);
    if declared.first() != Some(&"support") {
        return Err("main.rs must declare `mod support;` first".into());
    }
    let cases = &declared[1..];
    let mut seen = HashSet::new();
    for m in cases {
        if !seen.insert(*m) {
            return Err(format!("main.rs declares `mod {m};` more than once"));
        }
    }
    for f in &repo.case_files {
        if !seen.contains(f.as_str()) {
            return Err(format!("tests/it/{f}.rs is not declared in main.rs"));
        }
    }
    for m in cases {
        if !repo.case_files.iter().any(|f| f == m) {
            return Err(format!("main.rs declares `mod {m};` but tests/it/{m}.rs is missing"));
        }
    }
    // A `name::` filter is a substring match, so it also selects every case
    // whose name ends in `name`.
    for a in cases {
        if let Some(b) = cases.iter().find(|b| *b != a && b.ends_with(a)) {
            return Err(format!("case `{a}` is a suffix of case `{b}`; `{a}::` would run both"));
        }
    }
    if !cases.windows(2).all(|w| w[0] < w[1]) {
        return Err("main.rs case modules are not in sorted order".into());
    }

    // Standalone binaries: a stanza only for a feature requirement.
    let stanzas = manifest
        .get("test")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut stanza_names = HashSet::new();
    for t in &stanzas {
        let name = t
            .get("name")
            .and_then(Value::as_str)
            .ok_or("[[test]] missing name")?;
        if !stanza_names.insert(name) {
            return Err(format!("duplicate [[test]] name in Cargo.toml: {name}"));
        }
        if t.get("path").is_some() {
            return Err(format!("[[test]] {name} sets a path; tests/<name>.rs is found by name"));
        }
        if !repo.standalone_files.iter().any(|f| f == name) {
            return Err(format!("[[test]] {name} has no tests/{name}.rs"));
        }
        let registered = REGISTRY
            .iter()
            .find(|r| !r.is_case() && r.name() == name && !r.required_features.is_empty());
        if registered.is_none() {
            return Err(format!(
                "[[test]] {name} is not a registered feature-gated standalone binary"
            ));
        }
    }
    for entry in REGISTRY
        .iter()
        .filter(|r| !r.is_case() && !r.required_features.is_empty())
    {
        let name = entry.name();
        let target = stanzas
            .iter()
            .find(|t| t.get("name").and_then(Value::as_str) == Some(name))
            .ok_or_else(|| format!("missing [[test]] for registered target {}", entry.path))?;
        let mut actual: Vec<&str> = target
            .get("required-features")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("target {name} missing required-features"))?
            .iter()
            .map(|v| v.as_str().ok_or("non-string feature"))
            .collect::<Result<_, _>>()?;
        actual.sort_unstable();
        let mut expected = entry.required_features.to_vec();
        expected.sort_unstable();
        if actual != expected {
            return Err(format!(
                "target {name} required-features mismatch: got {actual:?}, expected {expected:?}"
            ));
        }
    }
    for entry in REGISTRY.iter().filter(|r| r.is_case()) {
        if !seen.contains(entry.name()) {
            return Err(format!("registered case {} is not declared in main.rs", entry.path));
        }
    }

    // The maintained CONTRIBUTING row enables every gate but the named-only ones.
    let cmd_count = repo.contributing.matches(MAINTAINED_CMD).count();
    if cmd_count != 1 {
        return Err(format!(
            "expected exactly 1 occurrence of {MAINTAINED_CMD} in CONTRIBUTING.md, found {cmd_count}"
        ));
    }
    let active = feature_closure(&manifest, DIRECT_FEATURES)?;
    for entry in REGISTRY {
        for &req in entry.required_features {
            if !active.contains(req) && !NAMED_COMMAND_ONLY_FEATURES.contains(&req) {
                return Err(format!(
                    "target {} required feature '{req}' not satisfied by active features {:?}",
                    entry.name(),
                    active
                ));
            }
        }
    }

    // Every command that names a gated case enables its features.
    for (file, text) in &repo.commands {
        for (cmd, parsed) in case_commands(text) {
            let direct: Vec<&str> = parsed.features.iter().map(String::as_str).collect();
            let enabled = feature_closure(&manifest, &direct)
                .map_err(|e| format!("{file}: `{cmd}`: {e}"))?;
            for module in &parsed.modules {
                if !seen.contains(module.as_str()) {
                    return Err(format!("{file}: `{cmd}` names `{module}::`, not a case"));
                }
                let Some(entry) = REGISTRY
                    .iter()
                    .find(|r| r.is_case() && r.name() == module)
                else {
                    continue;
                };
                let missing: BTreeSet<&str> = entry
                    .required_features
                    .iter()
                    .copied()
                    .filter(|f| !enabled.contains(*f))
                    .collect();
                if !missing.is_empty() {
                    return Err(format!(
                        "{file}: `{cmd}` runs `{module}::` without {missing:?}; it would run 0 tests"
                    ));
                }
            }
        }
    }

    Ok(())
}

#[test]
fn all_gated_files_in_tree_are_registered() {
    let root = repo_root();
    let mut missing_or_mismatched = Vec::new();

    for scan_root in ["tests/it", "tests"] {
        for stem in rs_stems(&root.join(scan_root)) {
            let rel_path = format!("{scan_root}/{stem}.rs");
            let path = root.join(&rel_path);
            let content = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("failed to read {:?}: {}", path, e));
            let Some(gate) = find_cfg_gate(&content) else {
                continue;
            };
            if let Some(registered) = REGISTRY.iter().find(|r| r.path == rel_path) {
                if registered.gate != gate {
                    missing_or_mismatched.push(format!(
                        "{} gate mismatch: tree has {:?}, registry has {:?}",
                        rel_path, gate, registered.gate
                    ));
                }
            } else {
                missing_or_mismatched.push(format!(
                    "{} carries a `#![cfg(` gate that the registry does not list",
                    rel_path
                ));
            }
        }
    }

    assert!(
        missing_or_mismatched.is_empty(),
        "Gated files in tree not registered or mismatched:\n{}",
        missing_or_mismatched.join("\n")
    );
}

#[test]
fn all_registered_targets_exist_and_match_gate_in_tree() {
    let root = repo_root();
    for entry in REGISTRY {
        let file_path = root.join(entry.path);
        assert!(
            file_path.is_file(),
            "Registered target file does not exist: {}",
            entry.path
        );
        let content = fs::read_to_string(&file_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", file_path, e));
        let gate = find_cfg_gate(&content);
        assert_eq!(
            gate,
            Some(entry.gate),
            "Registry claims gate {:?} for {}, but file does not carry it",
            entry.gate,
            entry.path
        );
    }
}

#[test]
fn all_registered_targets_declare_non_zero_test_rows() {
    let root = repo_root();
    for entry in REGISTRY {
        let file_path = root.join(entry.path);
        let content = fs::read_to_string(&file_path)
            .unwrap_or_else(|e| panic!("failed to read {:?}: {}", file_path, e));
        let row_count = count_test_rows(&content);
        assert!(
            row_count > 0,
            "Registered target {} declared 0 test rows",
            entry.path
        );
    }
}

#[test]
fn every_case_file_is_declared_once_in_main_rs() {
    let repo = load_repo();
    let declared = declared_modules(&repo.main_rs);
    let undeclared: Vec<&String> = repo
        .case_files
        .iter()
        .filter(|f| declared.iter().filter(|m| **m == f.as_str()).count() != 1)
        .collect();
    assert!(
        undeclared.is_empty(),
        "tests/it files not declared exactly once in tests/it/main.rs: {:?}",
        undeclared
    );
}

#[test]
fn manifest_and_gate_validation_passes_on_repo() {
    validate_layout(&load_repo()).expect("repository test layout and gates must validate");
}

#[test]
fn negative_fixtures_reject_invalid_manifest_or_gate() {
    enum Mutation {
        MainRs(&'static str, &'static str),
        Cargo(&'static str, &'static str),
        Contributing(&'static str, &'static str),
        AddCaseFile(&'static str),
        AddCommand(&'static str),
    }

    let cases: &[(&str, &[Mutation], &str)] = &[
        (
            "case file without its mod",
            &[Mutation::MainRs("mod access_render_cli;\n", "")],
            "tests/it/access_render_cli.rs is not declared",
        ),
        (
            "case declared twice",
            &[Mutation::MainRs(
                "mod access_render_cli;\n",
                "mod access_render_cli;\nmod access_render_cli;\n",
            )],
            "more than once",
        ),
        (
            "mod without its file",
            &[Mutation::MainRs(
                "mod access_render_cli;\n",
                "mod access_render_cli;\nmod access_render_clj;\n",
            )],
            "tests/it/access_render_clj.rs is missing",
        ),
        (
            "case name that is a suffix of another",
            &[
                Mutation::AddCaseFile("shard_e2e"),
                Mutation::MainRs(
                    "mod shared_stateful_foundations;\n",
                    "mod shard_e2e;\nmod shared_stateful_foundations;\n",
                ),
            ],
            "is a suffix of case",
        ),
        (
            "autotests turned off",
            &[Mutation::Cargo("[package]\n", "[package]\nautotests = false\n")],
            "autotests = false",
        ),
        (
            "standalone stanza without its feature",
            &[Mutation::Cargo("required-features = [\"operator\"]\n", "")],
            "missing required-features",
        ),
        (
            "stanza for a case module",
            &[Mutation::Cargo(
                "[[test]]\n",
                "[[test]]\nname = \"access_render_cli\"\nrequired-features = [\"operator\"]\n\n[[test]]\n",
            )],
            "has no tests/access_render_cli.rs",
        ),
        (
            "maintained command without operator",
            &[Mutation::Contributing(
                r#"`cargo test -p lumen --features "operator delegated-auth"`"#,
                r#"`cargo test -p lumen --features "delegated-auth"`"#,
            )],
            "expected exactly 1 occurrence",
        ),
        (
            "maintained command without delegated-auth",
            &[Mutation::Contributing(
                r#"`cargo test -p lumen --features "operator delegated-auth"`"#,
                r#"`cargo test -p lumen --features "operator"`"#,
            )],
            "expected exactly 1 occurrence",
        ),
        (
            "raft-wal case run without raft-wal",
            &[Mutation::AddCommand(
                "cargo test --locked -p lumen --test it -- raft_shutdown_failover::\n",
            )],
            "it would run 0 tests",
        ),
        (
            "operator case run with a feature list that lacks operator",
            &[Mutation::AddCommand(
                "cargo test -p lumen \\\n  --features \"delegated-auth\" --test it -- operator_render::\n",
            )],
            "it would run 0 tests",
        ),
        (
            "command filter that names no case",
            &[Mutation::AddCommand(
                "cargo test -p lumen --test it -- no_such_case::\n",
            )],
            "not a case",
        ),
    ];

    let base = load_repo();
    validate_layout(&base).expect("unmutated repository must validate");

    for (name, mutations, expected) in cases {
        let mut repo = Repo {
            main_rs: base.main_rs.clone(),
            case_files: base.case_files.clone(),
            standalone_files: base.standalone_files.clone(),
            cargo_toml: base.cargo_toml.clone(),
            contributing: base.contributing.clone(),
            commands: base.commands.clone(),
        };
        for mutation in *mutations {
            let (text, from, to, label) = match mutation {
                Mutation::MainRs(from, to) => (&mut repo.main_rs, *from, *to, "main.rs"),
                Mutation::Cargo(from, to) => (&mut repo.cargo_toml, *from, *to, "Cargo.toml"),
                Mutation::Contributing(from, to) => {
                    (&mut repo.contributing, *from, *to, "CONTRIBUTING.md")
                }
                Mutation::AddCaseFile(stem) => {
                    repo.case_files.push(stem.to_string());
                    continue;
                }
                Mutation::AddCommand(cmd) => {
                    repo.commands
                        .push(("fixture.sh".to_string(), cmd.to_string()));
                    continue;
                }
            };
            let mutated = text.replacen(from, to, 1);
            assert_ne!(
                &mutated, text,
                "negative fixture '{name}' failed to mutate {label}"
            );
            *text = mutated;
        }

        match validate_layout(&repo) {
            Ok(()) => panic!("negative fixture '{name}' was expected to fail validation, but passed"),
            Err(e) => assert!(
                e.contains(expected),
                "negative fixture '{name}' failed for another reason: {e}"
            ),
        }
    }
}

#[test]
fn case_commands_parse_features_and_filters() {
    let text = "run: |\n  cargo test --locked -p lumen --features \"operator delegated-auth\" \\\n    --test it -- operator_render:: access_render_cli::renders --exact\n  cargo test -p lumen --test it -- --list && cargo test -p lumen --features=raft-wal --test it -- raft_shutdown_failover::\n";
    let parsed = case_commands(text);
    assert_eq!(parsed.len(), 3, "{:?}", parsed.iter().map(|p| &p.0).collect::<Vec<_>>());
    assert_eq!(parsed[0].1.features, ["operator", "delegated-auth"]);
    assert_eq!(parsed[0].1.modules, ["operator_render", "access_render_cli"]);
    assert!(parsed[1].1.features.is_empty());
    assert!(parsed[1].1.modules.is_empty());
    assert_eq!(parsed[2].1.features, ["raft-wal"]);
    assert_eq!(parsed[2].1.modules, ["raft_shutdown_failover"]);
}
