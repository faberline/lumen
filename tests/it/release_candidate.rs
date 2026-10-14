//! Static and local-fixture oracle for the run-scoped release candidate.
//!
//! # Facets
//!
//! - Behavior: `tests/it/release_candidate.rs:2573` rejects each broken
//!   coarse-gate workflow shape, and `tests/it/release_candidate.rs:4364`
//!   refuses a 0.6.1 candidate receipt that claims CI performance evidence.
//! - Security: `tests/it/release_candidate.rs:4466` carries missing, corrupt,
//!   and foreign local durable receipt refusal; `tests/it/release_candidate.rs:2638`
//!   requires promotion, not the candidate receipt, to bind that evidence.
//! - Performance: the coarse CI gate is verbatim at
//!   `tests/it/release_candidate.rs:53`; `tests/it/release_candidate.rs:2064`
//!   requires that command and no durable cell in CI. This asserts wiring only,
//!   not that a 30-minute workload has passed.
//!
//! The parent controller owns the workflow and verifier implementation. This
//! file only freezes their externally observable release contract.
use crate::support::perf_cell_receipt;

use serde_json::{json, Value};
use serde_yaml::Value as Yaml;
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

#[derive(Debug, PartialEq, Eq)]
struct Finding(&'static str);

const ACTIONS: &[&str] = &[
    "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
    "dtolnay/rust-toolchain@4360b52568e2003a75bf9bc1d59f33a8e3fc893c",
    "Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6",
    "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
    "actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
    "astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9",
    "ruby/setup-ruby@95ef2b042f9d7a56d8268cba8559e2842e2ad01b",
    "docker/setup-qemu-action@c7c53464625b32c7a7e944ae62b3e17d2b600130",
    "docker/setup-buildx-action@8d2750c68a42422c14e847fe6c8ac0403b4cbd6f",
    "docker/login-action@c94ce9fb468520275223c153574b00df6fe4bcc9",
    "docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a",
    "sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6",
    "actions/attest@1e69f48acb82d1966a394da916b4c1698aa569d6",
    "anchore/sbom-action@e22c389904149dbc22b58101806040fa8d37a610",
];
const WORKFLOW_BYTES_SHA256: &str =
    "123d1ec0f9dae9cee70b7283272abcfdaccfc987ed3ad8a5cb1f196c687029d0";
const KIND_E2E_BYTES_SHA256: &str =
    "e96c2a010d141eb6cfe4c261ad63f7017f4baacdf5509d65606731669beb0044";
const RELEASE_PERF_GATE: &str = "cargo test --release --locked -p lumen --test it -- perf_gate:: --ignored --skip durable_workload --test-threads=1 --nocapture";
const VERIFIER_BYTES_SHA256: &str =
    "21080d327065a21616c6e2fcc9373e9c5366c05b0ff75e908ddae89d9a7254ac";

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut child = Command::new("shasum")
        .args(["-a", "256"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(bytes).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "shasum failed");
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
fn key(name: &str) -> Yaml {
    Yaml::String(name.into())
}
fn field<'a>(value: &'a Yaml, name: &str) -> Option<&'a Yaml> {
    value.as_mapping()?.get(&key(name))
}
fn job<'a>(workflow: &'a Yaml, name: &str) -> Option<&'a Yaml> {
    field(field(workflow, "jobs")?, name)
}
fn strings(value: Option<&Yaml>) -> Vec<&str> {
    match value {
        Some(Yaml::String(value)) => vec![value],
        Some(Yaml::Sequence(values)) => values.iter().filter_map(Yaml::as_str).collect(),
        _ => Vec::new(),
    }
}
fn require(ok: bool, code: &'static str) -> Result<(), Finding> {
    ok.then_some(()).ok_or(Finding(code))
}
fn replace_once(source: &str, from: &str, to: &str) -> String {
    assert_eq!(
        source.matches(from).count(),
        1,
        "mutation target {from:?} is not unique"
    );
    let changed = source.replacen(from, to, 1);
    assert_ne!(changed, source, "mutation did not change bytes");
    changed
}

fn replace_occurrence(source: &str, from: &str, to: &str, occurrence: usize) -> String {
    let offsets = source
        .match_indices(from)
        .map(|(offset, _)| offset)
        .collect::<Vec<_>>();
    assert!(
        occurrence < offsets.len(),
        "mutation target {from:?} occurrence {occurrence} is missing"
    );
    let mut changed = source.to_owned();
    let start = offsets[occurrence];
    changed.replace_range(start..start + from.len(), to);
    assert_ne!(changed, source, "mutation did not change bytes");
    changed
}

fn validate_uv_setup(workflow: &Yaml) -> Result<(), Finding> {
    // The default and operator suites run spec_gen_e2e, which needs uv.
    for (job_name, gate_name) in [
        ("gates-default", "Run default-feature cargo gate"),
        ("gates-operator", "Run operator delegated-auth cargo gate"),
    ] {
        validate_uv_setup_in(workflow, job_name, gate_name)?;
    }
    Ok(())
}

fn validate_uv_setup_in(workflow: &Yaml, job_name: &str, gate_name: &str) -> Result<(), Finding> {
    const UV_SETUP: &str = "astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9";
    let steps = field(
        job(workflow, job_name).ok_or(Finding("UV_SETUP"))?,
        "steps",
    )
    .and_then(Yaml::as_sequence)
    .ok_or(Finding("UV_SETUP"))?;
    let setup_indices: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter_map(|(index, step)| {
            field(step, "uses")
                .and_then(Yaml::as_str)
                .and_then(|uses| uses.strip_prefix("astral-sh/setup-uv@"))
                .map(|_| index)
        })
        .collect();
    require(setup_indices.len() == 1, "UV_SETUP")?;
    let gate_indices: Vec<usize> = steps
        .iter()
        .enumerate()
        .filter_map(|(index, step)| {
            (field(step, "name").and_then(Yaml::as_str) == Some(gate_name)).then_some(index)
        })
        .collect();
    require(gate_indices.len() == 1, "UV_SETUP")?;
    require(setup_indices[0] < gate_indices[0], "UV_SETUP")?;

    let setup = steps[setup_indices[0]]
        .as_mapping()
        .ok_or(Finding("UV_SETUP"))?;
    require(setup.len() == 2, "UV_SETUP")?;
    require(
        setup.get(&key("uses")).and_then(Yaml::as_str) == Some(UV_SETUP),
        "UV_SETUP",
    )?;
    let with = setup
        .get(&key("with"))
        .and_then(Yaml::as_mapping)
        .ok_or(Finding("UV_SETUP"))?;
    require(with.len() == 2, "UV_SETUP")?;
    require(
        with.get(&key("version")).and_then(Yaml::as_str) == Some("0.12.1"),
        "UV_SETUP",
    )?;
    require(
        with.get(&key("enable-cache")).and_then(Yaml::as_bool) == Some(false),
        "UV_SETUP",
    )?;
    Ok(())
}

fn validate_cloud_free_acceptance_gates(workflow: &Yaml) -> Result<(), Finding> {
    const RUBY: &str = "ruby/setup-ruby@95ef2b042f9d7a56d8268cba8559e2842e2ad01b";
    const JOB: &str = "cloud-free-config";
    let steps = field(
        job(workflow, JOB).ok_or(Finding("CLOUD_FREE_GATES"))?,
        "steps",
    )
    .and_then(Yaml::as_sequence)
    .ok_or(Finding("CLOUD_FREE_GATES"))?;
    let index_of = |name: &str| {
        steps
            .iter()
            .enumerate()
            .filter(|(_, step)| field(step, "name").and_then(Yaml::as_str) == Some(name))
            .map(|(index, _)| index)
            .collect::<Vec<_>>()
    };
    // checkout, ruby, terraform, kubectl, then the two gates and nothing else.
    require(steps.len() == 6, "CLOUD_FREE_GATES")?;
    require(
        field(&steps[0], "uses")
            .and_then(Yaml::as_str)
            .is_some_and(|uses| uses.starts_with("actions/checkout@")),
        "CLOUD_FREE_GATES",
    )?;
    let ruby = steps
        .iter()
        .enumerate()
        .filter(|(_, step)| field(step, "uses").and_then(Yaml::as_str) == Some(RUBY))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    require(ruby.len() == 1, "CLOUD_FREE_GATES")?;
    let ruby_step = &steps[ruby[0]];
    require(
        ruby_step.as_mapping().map(|map| map.len()) == Some(2)
            && field(ruby_step, "with")
                .and_then(Yaml::as_mapping)
                .map(|map| map.len())
                == Some(2)
            && field(ruby_step, "with")
                .and_then(|with| field(with, "ruby-version"))
                .and_then(Yaml::as_str)
                == Some("3.3.6")
            && field(ruby_step, "with")
                .and_then(|with| field(with, "bundler"))
                .and_then(Yaml::as_str)
                == Some("none"),
        "CLOUD_FREE_GATES",
    )?;
    validate_exact_run_step(
        workflow,
        JOB,
        "Install verified Terraform 1.9.4",
        &["name", "shell", "run"],
        &[
            "set -euo pipefail",
            "curl -fsSL https://releases.hashicorp.com/terraform/1.9.4/terraform_1.9.4_linux_amd64.zip -o /tmp/terraform.zip",
            "echo '6e9b2cc741875ab906d800af3134b076489f049565e0a1dbdb6deacd91f5054c  /tmp/terraform.zip' | sha256sum -c -",
            "unzip -oq /tmp/terraform.zip -d /tmp/terraform-bin",
            "sudo install -m 0755 /tmp/terraform-bin/terraform /usr/local/bin/terraform",
        ],
    )?;
    validate_exact_run_step(
        workflow,
        JOB,
        "Install verified kubectl v1.37.0",
        &["name", "shell", "run"],
        &[
            "set -euo pipefail",
            "curl -fsSL https://dl.k8s.io/release/v1.37.0/bin/linux/amd64/kubectl -o /tmp/kubectl",
            "echo '6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f  /tmp/kubectl' | sha256sum -c -",
            "chmod +x /tmp/kubectl",
            "sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl",
        ],
    )?;
    validate_exact_run_step(
        workflow,
        JOB,
        "Run cloud-free Terraform acceptance gate",
        &["name", "shell", "run"],
        &["bash terraform/lumen-standalone-gke/scripts/check.sh"],
    )?;
    validate_exact_run_step(
        workflow,
        JOB,
        "Run cloud-free Kustomize acceptance gate",
        &["name", "shell", "run"],
        &["bash kustomize/lumen-standalone-acceptance/tests/contract.sh"],
    )?;
    let names = [
        "Install verified Terraform 1.9.4",
        "Install verified kubectl v1.37.0",
        "Run cloud-free Terraform acceptance gate",
        "Run cloud-free Kustomize acceptance gate",
    ];
    let ordered = names.iter().map(|name| index_of(name)).collect::<Vec<_>>();
    require(
        ordered.iter().all(|indices| indices.len() == 1),
        "CLOUD_FREE_GATES",
    )?;
    require(ruby[0] == 1, "CLOUD_FREE_GATES")?;
    let mut previous = ruby[0];
    for indices in ordered {
        require(previous + 1 == indices[0], "CLOUD_FREE_GATES")?;
        previous = indices[0];
    }
    Ok(())
}

fn validate_libraries_job(workflow: &Yaml) -> Result<(), Finding> {
    let library_job = job(workflow, "verify-libraries").ok_or(Finding("LIBRARIES"))?;
    let library_map = library_job.as_mapping().ok_or(Finding("LIBRARIES"))?;
    require(
        library_map.len() == 5
            && ["name", "needs", "runs-on", "permissions", "steps"]
                .iter()
                .all(|name| library_map.contains_key(&key(name))),
        "LIBRARIES",
    )?;
    require(
        field(library_job, "name").and_then(Yaml::as_str)
            == Some("verify service and Raft library gates"),
        "LIBRARIES",
    )?;
    require(
        field(library_job, "runs-on").and_then(Yaml::as_str) == Some("ubuntu-latest"),
        "LIBRARIES",
    )?;
    let steps = field(library_job, "steps")
        .and_then(Yaml::as_sequence)
        .ok_or(Finding("LIBRARIES"))?;
    require(steps.len() == 2, "LIBRARIES")?;
    require(
        field(&steps[0], "uses").and_then(Yaml::as_str)
            == Some("actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"),
        "LIBRARIES",
    )?;
    let checkout_step = steps[0].as_mapping().ok_or(Finding("LIBRARIES"))?;
    require(
        checkout_step.len() == 2
            && checkout_step.contains_key(&key("uses"))
            && checkout_step.contains_key(&key("with")),
        "LIBRARIES",
    )?;
    let checkout_with = field(&steps[0], "with")
        .and_then(Yaml::as_mapping)
        .ok_or(Finding("LIBRARIES"))?;
    require(
        checkout_with.len() == 2
            && checkout_with.contains_key(&key("ref"))
            && checkout_with.contains_key(&key("fetch-depth")),
        "LIBRARIES",
    )?;
    require(
        checkout_with.get(&key("ref")).and_then(Yaml::as_str)
            == Some("${{ needs.identity.outputs.commit }}"),
        "LIBRARIES",
    )?;
    require(
        checkout_with
            .get(&key("fetch-depth"))
            .and_then(Yaml::as_i64)
            == Some(0),
        "LIBRARIES",
    )?;
    let run_step = steps[1].as_mapping().ok_or(Finding("LIBRARIES"))?;
    require(
        run_step.len() == 3
            && ["name", "shell", "run"]
                .iter()
                .all(|name| run_step.contains_key(&key(name))),
        "LIBRARIES",
    )?;
    require(
        field(&steps[1], "shell").and_then(Yaml::as_str) == Some("bash"),
        "LIBRARIES",
    )?;
    require(
        field(&steps[1], "name").and_then(Yaml::as_str)
            == Some("Run required service and Raft library gates without GKE"),
        "LIBRARIES",
    )?;
    let run = field(&steps[1], "run")
        .and_then(Yaml::as_str)
        .ok_or(Finding("LIBRARIES"))?;
    // The shared library gates run against faberline/core at the exact commit
    // Cargo.lock pins; the implementor build keeps Lumen's own slice.
    require(
        exact_shell_lines(
            run,
            &[
                "set -euo pipefail",
                "core_rev=\"$(sed -n 's|^source = \"git+https://github.com/faberline/core?tag=[^#]*#\\([0-9a-f]\\{40\\}\\)\"$|\\1|p' Cargo.lock | sort -u)\"",
                "[[ \"$core_rev\" =~ ^[0-9a-f]{40}$ ]] || { echo \"Cargo.lock must pin exactly one faberline/core commit\" >&2; exit 1; }",
                "core=\"$RUNNER_TEMP/core\"",
                "git init -q \"$core\"",
                "git -C \"$core\" fetch -q --depth 1 https://github.com/faberline/core \"$core_rev\"",
                "git -C \"$core\" checkout -q --detach FETCH_HEAD",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-k8s --test stateful_instance_render",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-k8s --test stateful_adapter_equivalence",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-k8s",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p storage-durable",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-backup --features http-client",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime --test adversarial_recovery",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core",
                "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime",
                "cargo build --locked -p lumen --features raft-wal",
                "cargo test --locked -p lumen --features raft-wal --lib --no-run",
                "cargo test --locked -p lumen --features raft-wal --test it --no-run",
                "git -c core.fsmonitor=false diff --check",
            ],
        ),
        "LIBRARIES",
    )?;
    Ok(())
}

fn exact_shell_lines(content: &str, expected: &[&str]) -> bool {
    shell_logical_lines(content) == expected
}

fn strip_shell_comment(raw: &str) -> String {
    let mut output = String::with_capacity(raw.len());
    let mut quote = None;
    let mut escaped = false;
    let mut token_start = true;
    for ch in raw.chars() {
        if escaped {
            output.push(ch);
            escaped = false;
            token_start = ch.is_whitespace();
            continue;
        }
        match quote {
            Some('\'') => {
                output.push(ch);
                if ch == '\'' {
                    quote = None;
                }
                token_start = false;
            }
            Some('"') => {
                output.push(ch);
                if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    quote = None;
                }
                token_start = false;
            }
            None if ch == '\\' => {
                output.push(ch);
                escaped = true;
                token_start = false;
            }
            None if ch == '\'' || ch == '"' => {
                output.push(ch);
                quote = Some(ch);
                token_start = false;
            }
            None if ch == '#' && token_start => break,
            None => {
                output.push(ch);
                token_start = ch.is_whitespace();
            }
            Some(other) => unreachable!("unsupported shell quote delimiter: {other}"),
        }
    }
    output
}

fn has_unescaped_continuation(line: &str) -> bool {
    let slash_count = line.chars().rev().take_while(|ch| *ch == '\\').count();
    slash_count % 2 == 1
}

fn shell_logical_lines(content: &str) -> Vec<String> {
    let mut logical = Vec::new();
    let mut pending = String::new();
    for raw in content.lines() {
        let active = strip_shell_comment(raw);
        let line = active.trim();
        if line.is_empty() {
            continue;
        }
        let continued = has_unescaped_continuation(line);
        let part = if continued {
            line.strip_suffix('\\').expect("continuation suffix")
        } else {
            line
        };
        if !pending.is_empty() {
            pending.push(' ');
        }
        pending.push_str(part.trim_end());
        if !continued {
            logical.push(std::mem::take(&mut pending));
        }
    }
    if !pending.is_empty() {
        logical.push(pending);
    }
    logical
}

fn shell_function_body(content: &str, name: &str) -> Option<Vec<String>> {
    let lines = shell_logical_lines(content);
    let header = format!("{name}() {{");
    let starts = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| (line == &header).then_some(index))
        .collect::<Vec<_>>();
    if starts.len() != 1 {
        return None;
    }
    let start = starts[0] + 1;
    let end = lines[start..].iter().position(|line| line == "}")? + start;
    Some(lines[start..end].to_vec())
}

fn validate_kind_e2e_semantics(source: &str) -> Result<(), Finding> {
    const NORMALIZE_API_DEFAULT: &str =
        r#"map(if has("readOnly") then . else . + {"readOnly": false} end)) == ["#;
    const PARENT: &str = r#"{"mountPath":"/var/lib/lumen","name":"raft","readOnly":false}"#;
    const PARENT_COMMA: &str = r#"{"mountPath":"/var/lib/lumen","name":"raft","readOnly":false},"#;
    const CHILD: &str =
        r#"{"mountPath":"/var/lib/lumen/data","name":"raft","readOnly":false,"subPath":"data"}"#;
    let topology = shell_function_body(source, "assert_operator_topology")
        .ok_or(Finding("KIND_STORAGE"))?
        .join("\n");
    let single_replica = [
        r#"([.spec.template.spec.containers[] | select(.name == "server") | .volumeMounts[] | select(.name == "raft")] |"#,
        NORMALIZE_API_DEFAULT,
        PARENT_COMMA,
        CHILD,
        "] and",
    ]
    .join("\n");
    let replicated = [
        r#"([.spec.template.spec.containers[] | select(.name == "server") | .volumeMounts[] | select(.name == "raft")] |"#,
        NORMALIZE_API_DEFAULT,
        PARENT,
        "] and",
    ]
    .join("\n");
    require(
        topology.matches(&single_replica).count() == 1,
        "KIND_STORAGE",
    )?;
    require(topology.matches(&replicated).count() == 1, "KIND_STORAGE")?;

    let live_storage = shell_function_body(source, "assert_operator_storage_live")
        .ok_or(Finding("KIND_STORAGE"))?
        .join("\n");
    let live_mounts = [
        r#"([.spec.containers[] | select(.name == "server") | .volumeMounts[] | select(.name == "raft")] |"#,
        NORMALIZE_API_DEFAULT,
        PARENT_COMMA,
        CHILD,
        "] and",
    ]
    .join("\n");
    require(
        live_storage.matches(&live_mounts).count() == 1,
        "KIND_STORAGE",
    )?;

    let expected = [
        "local pre_id=\"pre-restart-${CLUSTER_NAME}-$$\"",
        "local pre_value=\"${pre_id}@example.invalid\"",
        "local post_id=\"post-restart-${CLUSTER_NAME}-$$\"",
        "local post_value=\"${post_id}@example.invalid\"",
        "local checkpoint old_hits post_hits",
        "api_index_exact \"$pre_id\" \"$pre_value\"",
        "checkpoint=\"$(api_checkpoint)\"",
        "jq -e '.persisted == true' <<<\"$checkpoint\" >/dev/null || die \"/admin/checkpoint did not return persisted=true\"",
        "kubectl -n \"$NAMESPACE\" delete pod -l \"$APP_LABEL\" --wait=true",
        "wait_lumen_ready 240",
        "expose_nodeport",
        "assert_cluster_identity",
        "old_hits=\"$(api_search_exact \"$pre_value\" | jq --arg id \"$pre_id\" '[.hits[] | select(.external_id == $id)] | length')\"",
        "[[ \"$old_hits\" -eq 1 ]] || die \"pre-restart document was not readable before any new write\"",
        "api_index_exact \"$post_id\" \"$post_value\"",
        "post_hits=\"$(api_search_exact \"$post_value\" | jq --arg id \"$post_id\" '[.hits[] | select(.external_id == $id)] | length')\"",
        "[[ \"$post_hits\" -eq 1 ]] || die \"replacement pod did not accept the post-restart write\"",
        "echo \"   durable restart preserved $pre_id and accepted $post_id\"",
    ];
    let body = shell_function_body(source, "durable_restart_oracle")
        .ok_or(Finding("KIND_DURABLE_RESTART"))?;
    require(
        body.iter().map(String::as_str).eq(expected),
        "KIND_DURABLE_RESTART",
    )?;

    let lines = shell_logical_lines(source);
    let collection = "step \"4b. PUT /collections/users\" api_put_collection";
    let oracle =
        "step \"5. checkpoint, replace serving pod, and prove durable recovery\" durable_restart_oracle";
    let positions = |needle: &str| {
        lines
            .iter()
            .enumerate()
            .filter_map(|(index, line)| (line == needle).then_some(index))
            .collect::<Vec<_>>()
    };
    let collections = positions(collection);
    let oracles = positions(oracle);
    require(
        collections.len() == 1 && oracles.len() == 1 && collections[0] < oracles[0],
        "KIND_DURABLE_RESTART",
    )?;
    require(
        !lines[oracles[0] + 1..]
            .iter()
            .any(|line| line.contains("api_put_collection")),
        "KIND_DURABLE_RESTART",
    )
}

fn validate_kind_e2e(source: &str) -> Result<(), Finding> {
    validate_kind_e2e_semantics(source)?;
    require(
        sha256_bytes(source.as_bytes()) == KIND_E2E_BYTES_SHA256,
        "KIND_BYTES",
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ShellToken {
    Word(String, bool),
    Separator,
}

fn shell_tokens(line: &str) -> Vec<ShellToken> {
    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut quoted = false;
    let mut quote = None;
    let mut escaped = false;
    let mut token_start = true;
    let mut chars = line.chars().peekable();
    let flush_word = |tokens: &mut Vec<ShellToken>, word: &mut String, quoted: &mut bool| {
        if !word.is_empty() {
            tokens.push(ShellToken::Word(std::mem::take(word), *quoted));
            *quoted = false;
        }
    };
    while let Some(ch) = chars.next() {
        if escaped {
            word.push(ch);
            escaped = false;
            token_start = false;
            continue;
        }
        match quote {
            Some('\'') => {
                if ch == '\'' {
                    quote = None;
                } else {
                    word.push(ch);
                }
                token_start = false;
            }
            Some('"') => {
                if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    quote = None;
                } else {
                    word.push(ch);
                }
                token_start = false;
            }
            None if ch == '\\' => {
                escaped = true;
                token_start = false;
            }
            None if ch == '\'' || ch == '"' => {
                quote = Some(ch);
                quoted = true;
                token_start = false;
            }
            None if ch == '#' && token_start => break,
            None if ch.is_whitespace() => {
                flush_word(&mut tokens, &mut word, &mut quoted);
                token_start = true;
            }
            None if ch == ';' || ch == '|' || ch == '&' => {
                flush_word(&mut tokens, &mut word, &mut quoted);
                if (ch == '|' || ch == '&') && chars.peek() == Some(&ch) {
                    chars.next();
                }
                tokens.push(ShellToken::Separator);
                token_start = true;
            }
            None => {
                word.push(ch);
                token_start = false;
            }
            Some(other) => unreachable!("unsupported shell quote delimiter: {other}"),
        }
    }
    flush_word(&mut tokens, &mut word, &mut quoted);
    tokens
}

fn is_gcloud_command(word: &str) -> bool {
    word == "gcloud" || word.ends_with("/gcloud")
}

fn is_shell_c_invocation(words: &[String], index: usize) -> bool {
    let word = words[index].as_str();
    let shell = word == "bash" || word == "sh" || word.ends_with("/bash") || word.ends_with("/sh");
    shell && words[index + 1..].iter().any(|word| word == "-c")
}

fn active_shell_c_invocation(tokens: &[ShellToken]) -> bool {
    tokens.iter().enumerate().any(|(index, token)| {
        let ShellToken::Word(shell, false) = token else {
            return false;
        };
        (shell == "bash" || shell == "sh" || shell.ends_with("/bash") || shell.ends_with("/sh"))
            && tokens[index + 1..]
                .iter()
                .any(|token| matches!(token, ShellToken::Word(option, false) if option == "-c"))
    })
}

fn command_segment_executes_gcloud(words: &[String]) -> bool {
    let mut index = 0;
    while index < words.len() {
        let word = words[index].as_str();
        if [
            "if", "then", "do", "done", "else", "elif", "fi", "for", "while", "case", "esac", "{",
            "}", "!",
        ]
        .contains(&word)
            || word.contains('=')
        {
            index += 1;
            continue;
        }
        if word == "sudo" {
            index += 1;
            while index < words.len() {
                let option = words[index].as_str();
                if option == "--" {
                    index += 1;
                    break;
                }
                if !option.starts_with('-') {
                    break;
                }
                index += if ["-u", "-g", "-h", "-p", "-r", "-t", "-C"].contains(&option) {
                    2
                } else {
                    1
                };
            }
            continue;
        }
        if word == "env" {
            index += 1;
            while index < words.len() {
                let option = words[index].as_str();
                if option == "--" {
                    index += 1;
                    break;
                }
                if option.contains('=') {
                    index += 1;
                    continue;
                }
                if option.starts_with('-') {
                    index += if ["-u", "-C"].contains(&option) { 2 } else { 1 };
                    continue;
                }
                break;
            }
            continue;
        }
        if word == "command" {
            index += 1;
            let mut lookup_only = false;
            while index < words.len() && words[index].starts_with('-') {
                lookup_only |= words[index] == "-v" || words[index] == "-V";
                index += 1;
            }
            if lookup_only {
                return false;
            }
            continue;
        }
        if word == "time" {
            index += 1;
            while index < words.len() && words[index].starts_with('-') {
                index += 1;
            }
            continue;
        }
        if (word == "bash" || word == "sh" || word.ends_with("/bash") || word.ends_with("/sh"))
            && is_shell_c_invocation(words, index)
        {
            return true;
        }
        return is_gcloud_command(word);
    }
    false
}

fn logical_line_executes_gcloud(line: &str) -> bool {
    let mut segment = Vec::new();
    for token in shell_tokens(line) {
        match token {
            ShellToken::Word(word, quoted) => segment.push(ShellToken::Word(word, quoted)),
            ShellToken::Separator => {
                if active_shell_c_invocation(&segment)
                    || command_segment_executes_gcloud(
                        &segment
                            .iter()
                            .filter_map(|token| match token {
                                ShellToken::Word(word, _) => Some(word.clone()),
                                ShellToken::Separator => None,
                            })
                            .collect::<Vec<_>>(),
                    )
                {
                    return true;
                }
                segment.clear();
            }
        }
    }
    active_shell_c_invocation(&segment)
        || command_segment_executes_gcloud(
            &segment
                .iter()
                .filter_map(|token| match token {
                    ShellToken::Word(word, _) => Some(word.clone()),
                    ShellToken::Separator => None,
                })
                .collect::<Vec<_>>(),
        )
}

fn validate_no_gcloud_execution(workflow: &Yaml) -> Result<(), Finding> {
    let jobs = field(workflow, "jobs")
        .and_then(Yaml::as_mapping)
        .ok_or(Finding("CANDIDATE_ONLY"))?;
    for job in jobs.values() {
        let Some(steps) = field(job, "steps").and_then(Yaml::as_sequence) else {
            continue;
        };
        for step in steps {
            let Some(run) = field(step, "run").and_then(Yaml::as_str) else {
                continue;
            };
            if shell_logical_lines(run)
                .iter()
                .any(|line| logical_line_executes_gcloud(line))
            {
                return Err(Finding("CANDIDATE_ONLY"));
            }
        }
    }
    Ok(())
}

fn named_step<'a>(workflow: &'a Yaml, job_name: &str, step_name: &str) -> Option<&'a Yaml> {
    let steps = field(job(workflow, job_name)?, "steps")?.as_sequence()?;
    let matches = steps
        .iter()
        .filter(|step| field(step, "name").and_then(Yaml::as_str) == Some(step_name))
        .collect::<Vec<_>>();
    (matches.len() == 1).then(|| matches[0])
}

fn indexed_step_by_id<'a>(
    workflow: &'a Yaml,
    job_name: &str,
    step_id: &str,
) -> Result<(usize, &'a Yaml), Finding> {
    let steps = field(job(workflow, job_name).ok_or(Finding("IMAGE"))?, "steps")
        .and_then(Yaml::as_sequence)
        .ok_or(Finding("IMAGE"))?;
    let matches = steps
        .iter()
        .enumerate()
        .filter(|(_, step)| field(step, "id").and_then(Yaml::as_str) == Some(step_id))
        .collect::<Vec<_>>();
    require(matches.len() == 1, "IMAGE")?;
    Ok(matches[0])
}

fn validate_candidate_image_outputs(workflow: &Yaml) -> Result<(), Finding> {
    const BUILD_PUSH: &str = "docker/build-push-action@53b7df96c91f9c12dcc8a07bcb9ccacbed38856a";
    let image_job = job(workflow, "ghcr-image-and-attest").ok_or(Finding("IMAGE"))?;
    let outputs = field(image_job, "outputs")
        .and_then(Yaml::as_mapping)
        .ok_or(Finding("IMAGE"))?;
    let expected_outputs = [
        ("image_repo", "${{ steps.tags.outputs.image_repo }}"),
        ("candidate_tag", "${{ steps.tags.outputs.candidate_tag }}"),
        ("root_digest", "${{ steps.push.outputs.digest }}"),
        (
            "amd64_digest",
            "${{ steps.platform_digests.outputs.amd64_digest }}",
        ),
        (
            "arm64_digest",
            "${{ steps.platform_digests.outputs.arm64_digest }}",
        ),
    ];
    require(outputs.len() == expected_outputs.len(), "IMAGE")?;
    for (name, expected) in expected_outputs {
        require(
            outputs.get(&key(name)).and_then(Yaml::as_str) == Some(expected),
            "IMAGE",
        )?;
    }

    let (tags_index, tags) = indexed_step_by_id(workflow, "ghcr-image-and-attest", "tags")?;
    let (push_index, push) = indexed_step_by_id(workflow, "ghcr-image-and-attest", "push")?;
    let (platform_index, platform) =
        indexed_step_by_id(workflow, "ghcr-image-and-attest", "platform_digests")?;
    require(
        tags_index < push_index && push_index < platform_index,
        "IMAGE",
    )?;
    require(
        field(tags, "name").and_then(Yaml::as_str)
            == Some("Resolve run-scoped candidate image identity"),
        "IMAGE",
    )?;
    require(
        field(push, "name").and_then(Yaml::as_str)
            == Some("Build and push only the candidate index"),
        "IMAGE",
    )?;
    require(
        field(push, "uses").and_then(Yaml::as_str) == Some(BUILD_PUSH),
        "IMAGE",
    )?;
    require(
        field(platform, "name").and_then(Yaml::as_str)
            == Some("Extract exact two platform child digests"),
        "IMAGE",
    )
}

fn validate_exact_run_step(
    workflow: &Yaml,
    job_name: &str,
    step_name: &str,
    expected_keys: &[&str],
    expected_lines: &[&str],
) -> Result<(), Finding> {
    let step = named_step(workflow, job_name, step_name).ok_or(Finding("GATE_COMMANDS"))?;
    let map = step.as_mapping().ok_or(Finding("GATE_COMMANDS"))?;
    require(
        map.len() == expected_keys.len()
            && expected_keys
                .iter()
                .all(|name| map.contains_key(&key(name))),
        "GATE_COMMANDS",
    )?;
    require(
        field(step, "shell").and_then(Yaml::as_str) == Some("bash"),
        "GATE_COMMANDS",
    )?;
    let run = field(step, "run")
        .and_then(Yaml::as_str)
        .ok_or(Finding("GATE_COMMANDS"))?;
    require(exact_shell_lines(run, expected_lines), "GATE_COMMANDS")
}

fn validate_candidate_and_kind_commands(workflow: &Yaml) -> Result<(), Finding> {
    validate_exact_run_step(
        workflow,
        "verify-candidate",
        "Verify full run-scoped candidate supply chain",
        &["name", "env", "shell", "run"],
        &[
            "set -euo pipefail",
            "scripts/verify-release-candidate.sh --repo faberline/lumen --version \"${{ needs.identity.outputs.version }}\" --commit \"${{ needs.identity.outputs.commit }}\" --run-id \"${{ github.run_id }}\" --run-attempt \"${{ github.run_attempt }}\" --manifest candidate/candidate-manifest.json --manifest-sidecar candidate/candidate-manifest.json.sha256 --artifacts-dir candidate --image \"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" --candidate-tag \"${{ needs.ghcr-image-and-attest.outputs.candidate_tag }}\" --amd64-digest \"${{ needs.ghcr-image-and-attest.outputs.amd64_digest }}\" --arm64-digest \"${{ needs.ghcr-image-and-attest.outputs.arm64_digest }}\" --mode full",
        ],
    )?;
    let supply_chain = named_step(
        workflow,
        "verify-candidate",
        "Verify full run-scoped candidate supply chain",
    )
    .and_then(|step| field(step, "env"))
    .and_then(Yaml::as_mapping)
    .ok_or(Finding("GATE_COMMANDS"))?;
    require(
        supply_chain.len() == 1
            && supply_chain.get(&key("GH_TOKEN")).and_then(Yaml::as_str)
                == Some("${{ github.token }}"),
        "GATE_COMMANDS",
    )?;

    for (job_name, digest) in [
        (
            "kind-amd64",
            "${{ needs.ghcr-image-and-attest.outputs.amd64_digest }}",
        ),
        (
            "kind-arm64",
            "${{ needs.ghcr-image-and-attest.outputs.arm64_digest }}",
        ),
    ] {
        for mode in ["overlay", "operator"] {
        let command = format!(
            "LUMEN_E2E_MODE={mode} LUMEN_E2E_IMAGE_MODE=prebuilt LUMEN_E2E_DOC_COUNT=100000 LUMEN_E2E_IMAGE=\"${{{{ needs.ghcr-image-and-attest.outputs.image_repo }}}}@${{{{ needs.ghcr-image-and-attest.outputs.root_digest }}}}\" LUMEN_E2E_EXPECTED_VERSION=\"${{{{ needs.identity.outputs.version }}}}\" LUMEN_E2E_EXPECTED_GIT_SHA=\"${{short_sha:0:8}}\" LUMEN_E2E_EXPECTED_RUNTIME_DIGEST=\"{digest}\" LUMEN_E2E_REGISTRY_CONFIG=\"$HOME/.docker/config.json\" scripts/kind-e2e.sh"
        );
        validate_exact_run_step(
            workflow,
            job_name,
            &format!("Run prebuilt candidate kind e2e ({mode})"),
            &["name", "shell", "run"],
            &[
                "set -euo pipefail",
                "short_sha=\"${{ needs.identity.outputs.commit }}\"",
                &command,
            ],
        )?;
        }
    }
    Ok(())
}

fn validate_gate_step_inventory(workflow: &Yaml) -> Result<(), Finding> {
    let expected: &[(&str, &[&str])] = &[
        (
            "gates-default",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Reclaim runner disk before the cargo gate",
                "uses:astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9",
                "Run default-feature cargo gate",
            ][..],
        ),
        (
            "gates-operator",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Reclaim runner disk before the cargo gate",
                "uses:astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9",
                "Run operator delegated-auth cargo gate",
            ][..],
        ),
        (
            "gates-raft-wal",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Reclaim runner disk before the cargo gate",
                "Run raft-wal cargo gates",
            ][..],
        ),
        (
            "gates-release-features",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Reclaim runner disk before the cargo gate",
                "Run release feature-set cargo gate",
            ][..],
        ),
        (
            "container-smoke",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Log in to GHCR with read-only job access",
                "Run standalone container smoke gates",
            ][..],
        ),
        (
            "cloud-free-config",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "uses:ruby/setup-ruby@95ef2b042f9d7a56d8268cba8559e2842e2ad01b",
                "Install verified Terraform 1.9.4",
                "Install verified kubectl v1.37.0",
                "Run cloud-free Terraform acceptance gate",
                "Run cloud-free Kustomize acceptance gate",
            ][..],
        ),
        (
            "perf-coarse",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Reclaim runner disk before the cargo gate",
                "Run coarse performance gates",
            ][..],
        ),
        (
            "verify-candidate",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "uses:sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6",
                "uses:docker/setup-buildx-action@8d2750c68a42422c14e847fe6c8ac0403b4cbd6f",
                "Log in to GHCR with read-only job access",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "Verify full run-scoped candidate supply chain",
            ][..],
        ),
        (
            "kind-amd64",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Assert native x86_64 runner architecture",
                "Install verified kind v0.32.0",
                "Log in to GHCR for kind node image pulls",
                "Run prebuilt candidate kind e2e (overlay)",
                "Run prebuilt candidate kind e2e (operator)",
            ][..],
        ),
        (
            "kind-arm64",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "Assert native aarch64 runner architecture",
                "Install verified kind v0.32.0",
                "Log in to GHCR for kind node image pulls",
                "Run prebuilt candidate kind e2e (overlay)",
                "Run prebuilt candidate kind e2e (operator)",
            ][..],
        ),
        (
            "result",
            &[
                "uses:actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "uses:actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093",
                "Verify exact preflight manifest sidecar",
                "Bind all successful job conclusions into final receipt",
                "Verify final receipt as local fixture only",
                "uses:actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
            ][..],
        ),
    ];
    for &(job_name, expected_names) in expected {
        let steps = field(
            job(workflow, job_name).ok_or(Finding("GATE_STEPS"))?,
            "steps",
        )
        .and_then(Yaml::as_sequence)
        .ok_or(Finding("GATE_STEPS"))?;
        let actual = steps
            .iter()
            .map(|step| {
                field(step, "name")
                    .and_then(Yaml::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        field(step, "uses")
                            .and_then(Yaml::as_str)
                            .map(|uses| format!("uses:{uses}"))
                    })
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        require(
            actual
                .iter()
                .map(String::as_str)
                .eq(expected_names.iter().copied()),
            "GATE_STEPS",
        )?;
    }
    Ok(())
}

fn validate_fail_closed_gate_conditions(workflow: &Yaml) -> Result<(), Finding> {
    for name in [
        "gates-default",
        "gates-operator",
        "gates-raft-wal",
        "gates-release-features",
        "container-smoke",
        "cloud-free-config",
        "perf-coarse",
        "verify-candidate",
        "verify-libraries",
        "kind-amd64",
        "kind-arm64",
        "result",
    ] {
        let job = job(workflow, name).ok_or(Finding("CONDITIONS"))?;
        let job_map = job.as_mapping().ok_or(Finding("CONDITIONS"))?;
        for forbidden in ["if", "continue-on-error"] {
            require(!job_map.contains_key(&key(forbidden)), "CONDITIONS")?;
        }
        let steps = field(job, "steps")
            .and_then(Yaml::as_sequence)
            .ok_or(Finding("CONDITIONS"))?;
        for step in steps {
            let step = step.as_mapping().ok_or(Finding("CONDITIONS"))?;
            require(!step.contains_key(&key("continue-on-error")), "CONDITIONS")?;
            require(!step.contains_key(&key("if")), "CONDITIONS")?;
        }
    }
    Ok(())
}

fn validate_product_gate_partition(workflow: &Yaml, _source: &str) -> Result<(), Finding> {
    // Each cargo gate job builds one feature/profile group. Together they
    // cover every product gate that used to run in one serial step.
    let cargo_gates: &[(&str, &str, &[&str])] = &[
        (
            "gates-default",
            "Run default-feature cargo gate",
            &["set -euo pipefail", "cargo test --locked -p lumen"],
        ),
        (
            "gates-operator",
            "Run operator delegated-auth cargo gate",
            &[
                "set -euo pipefail",
                "cargo test --locked -p lumen --features \"operator delegated-auth\"",
            ],
        ),
        (
            "gates-raft-wal",
            "Run raft-wal cargo gates",
            &[
                "set -euo pipefail",
                "cargo test --locked -p lumen --features raft-wal --test it -- legacy_3073_app::",
                "cargo test --locked -p lumen --features raft-wal --bin lumen cluster_state_poller_converges_role_to_live_election_result",
            ],
        ),
        (
            "gates-release-features",
            "Run release feature-set cargo gate",
            &[
                "set -euo pipefail",
                "cargo test --locked -p lumen --features release --test it -- release_feature_set::",
            ],
        ),
    ];
    let mut runs = Vec::new();
    for &(job_name, step_name, lines) in cargo_gates {
        let step = named_step(workflow, job_name, step_name).ok_or(Finding("GATES"))?;
        let env = field(step, "env")
            .and_then(Yaml::as_mapping)
            .ok_or(Finding("GATES"))?;
        require(
            env.len() == 1
                && env.get(&key("CARGO_INCREMENTAL")).and_then(Yaml::as_str) == Some("0"),
            "GATES",
        )?;
        let map = step.as_mapping().ok_or(Finding("GATES"))?;
        require(
            map.len() == 4
                && ["name", "env", "shell", "run"]
                    .iter()
                    .all(|name| map.contains_key(&key(name))),
            "GATES",
        )?;
        require(
            field(step, "shell").and_then(Yaml::as_str) == Some("bash"),
            "GATES",
        )?;
        let run = field(step, "run")
            .and_then(Yaml::as_str)
            .ok_or(Finding("GATES"))?;
        require(exact_shell_lines(run, lines), "GATES")?;
        runs.push(run);
    }
    let smoke = named_step(
        workflow,
        "container-smoke",
        "Run standalone container smoke gates",
    )
    .ok_or(Finding("GATES"))?;
    let smoke_map = smoke.as_mapping().ok_or(Finding("GATES"))?;
    require(
        smoke_map.len() == 3
            && ["name", "shell", "run"]
                .iter()
                .all(|name| smoke_map.contains_key(&key(name))),
        "GATES",
    )?;
    let smoke_run = field(smoke, "run")
        .and_then(Yaml::as_str)
        .ok_or(Finding("GATES"))?;
    require(
        field(smoke, "shell").and_then(Yaml::as_str) == Some("bash")
            && exact_shell_lines(
                smoke_run,
                &[
                    "set -euo pipefail",
                    "bash scripts/standalone-container-smoke.sh bind",
                    "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh durable",
                ],
            ),
        "GATES",
    )?;
    runs.push(smoke_run);
    for run in runs {
        for gate in [
            "https://github.com/faberline/core",
            "--manifest-path",
            "cargo build --locked -p lumen --features raft-wal",
            "cargo test --locked -p lumen --features raft-wal --lib --no-run",
            "cargo test --locked -p lumen --features raft-wal --test it --no-run",
            "git -c core.fsmonitor=false diff --check",
        ] {
            require(!run.contains(gate), "LIBRARIES")?;
        }
    }
    Ok(())
}

fn perf_gate_inventory(source: &str) -> Vec<(String, bool)> {
    let mut attributes = Vec::new();
    let mut inventory = Vec::new();
    let mut in_block_comment = false;
    for raw in source.lines() {
        if in_block_comment {
            if raw.contains("*/") {
                in_block_comment = false;
            }
            continue;
        }
        let line = raw.trim();
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        if let Some(start) = line.find("/*") {
            if !line[start + 2..].contains("*/") {
                in_block_comment = true;
            }
            continue;
        }
        if line.starts_with("#[") {
            attributes.push(line.to_owned());
        } else if let Some(rest) = line.strip_prefix("fn ") {
            if attributes.iter().any(|attr| attr == "#[test]") {
                inventory.push((
                    rest.split('(').next().unwrap_or_default().to_owned(),
                    attributes.iter().any(|attr| attr.starts_with("#[ignore")),
                ));
            }
            attributes.clear();
        } else {
            attributes.clear();
        }
    }
    inventory
}

const DURABLE_WORKLOAD_BEGIN: &str = "// DURABLE-WORKLOAD-BEGIN";
const DURABLE_WORKLOAD_END: &str = "// DURABLE-WORKLOAD-END";

/// Finds a raw-string opener at one UTF-8 boundary and returns its content
/// start plus its closing delimiter.
fn raw_string_start_at(line: &str, start: usize) -> Option<(usize, String)> {
    let bytes = line.as_bytes();
    let raw = match bytes.get(start) {
        Some(b'r') => start,
        Some(b'b') if bytes.get(start + 1) == Some(&b'r') => start + 1,
        _ => return None,
    };
    if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        return None;
    }
    let mut quote = raw + 1;
    while quote < bytes.len() && bytes[quote] == b'#' {
        quote += 1;
    }
    if quote >= bytes.len() || bytes[quote] != b'"' {
        return None;
    }
    Some((quote + 1, format!("\"{}", "#".repeat(quote - raw - 1))))
}

/// Returns the byte after the next unescaped ordinary-string quote.
fn next_unescaped_quote(line: &str, start: usize) -> Option<usize> {
    let mut escaped = false;
    for (relative, character) in line[start..].char_indices() {
        if character == '\\' && !escaped {
            escaped = true;
            continue;
        }
        if character == '"' && !escaped {
            return Some(start + relative + character.len_utf8());
        }
        escaped = false;
    }
    None
}

/// Looks only for one exact executable line. It intentionally does not try to
/// parse Rust. Line comments, block comments, ordinary multiline strings, and
/// raw multiline strings cannot satisfy a durable-workload requirement.
fn has_active_line(source: &str, expected: &str) -> bool {
    let mut in_block_comment = false;
    let mut raw_string_terminator = None::<String>;
    let mut in_quoted_string = false;
    for raw in source.lines() {
        let line = raw.trim();
        let mut active = String::new();
        let mut offset = 0usize;
        loop {
            if let Some(terminator) = raw_string_terminator.as_deref() {
                let Some(relative) = line[offset..].find(terminator) else {
                    break;
                };
                offset += relative + terminator.len();
                raw_string_terminator = None;
                continue;
            }
            if in_quoted_string {
                let Some(end) = next_unescaped_quote(line, offset) else {
                    break;
                };
                offset = end;
                in_quoted_string = false;
                continue;
            }
            if in_block_comment {
                let Some(relative) = line[offset..].find("*/") else {
                    break;
                };
                offset += relative + 2;
                in_block_comment = false;
                continue;
            }
            if offset == line.len() {
                break;
            }
            let rest = &line[offset..];
            if rest.starts_with("//") {
                break;
            }
            if rest.starts_with("/*") {
                let content_start = offset + 2;
                if let Some(relative) = line[content_start..].find("*/") {
                    offset = content_start + relative + 2;
                    continue;
                }
                in_block_comment = true;
                break;
            }
            if let Some((content_start, terminator)) = raw_string_start_at(line, offset) {
                if let Some(relative) = line[content_start..].find(&terminator) {
                    let end = content_start + relative + terminator.len();
                    active.push_str(&line[offset..end]);
                    offset = end;
                    continue;
                }
                raw_string_terminator = Some(terminator);
                break;
            }
            if rest.starts_with('"') {
                let Some(end) = next_unescaped_quote(line, offset + 1) else {
                    in_quoted_string = true;
                    break;
                };
                active.push_str(&line[offset..end]);
                offset = end;
                continue;
            }
            let character = rest
                .chars()
                .next()
                .expect("nonempty remaining source has one character");
            active.push(character);
            offset += character.len_utf8();
        }
        if active.trim() == expected {
            return true;
        }
    }
    false
}

/// Returns one executable source branch bounded by two unique active lines.
/// A comment, quoted string, duplicate decoy, or missing bound refuses the
/// static oracle before its inner requirement can count.
fn active_source_region<'a>(source: &'a str, begin: &str, end: &str) -> Option<&'a str> {
    if !has_active_line(source, begin) || !has_active_line(source, end) {
        return None;
    }
    // Match complete lines. A suffix of a later match arm is a different
    // boundary, and a quoted substring must not determine the byte offsets.
    let mut offset = 0;
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for line in source.split_inclusive('\n') {
        if line.trim() == begin {
            starts.push(offset + line.len());
        }
        if line.trim() == end {
            ends.push(offset);
        }
        offset += line.len();
    }
    match (starts.as_slice(), ends.as_slice()) {
        ([start], [end]) if start <= end => Some(&source[*start..*end]),
        _ => None,
    }
}

#[test]
fn active_line_parser_ignores_comment_markers_inside_literals() {
    const REQUIRED: &str = "const HOT_DOCUMENTS: usize = 500_000;";
    let active = r###"
        let endpoint = "http://127.0.0.1:7373";
        let raw = r#"// const HOT_DOCUMENTS: usize = 500_000;"#;
        const HOT_DOCUMENTS: usize = 500_000;
    "###;
    assert!(
        has_active_line(active, REQUIRED),
        "an executable requirement after a URL literal must stay active"
    );

    let decoys = r###"
        let ordinary = "// const HOT_DOCUMENTS: usize = 500_000;";
        let raw = r#"const HOT_DOCUMENTS: usize = 500_000;"#;
        /* const HOT_DOCUMENTS: usize = 500_000; */
    "###;
    assert!(
        !has_active_line(decoys, REQUIRED),
        "comments and ordinary or raw literals must not satisfy a requirement"
    );
}

#[test]
fn restart_phase_diagnostics_contract_requires_ordered_bounded_terminal_records() {
    validate_restart_phase_diagnostics_source(&perf_gate_source()).expect(
        "restart diagnostics must keep each phase elapsed, readiness attempts, and one bounded terminal record without changing the shared deadline",
    );
}

fn durable_workload_region(source: &str) -> Result<(&str, &str), Finding> {
    require(
        source.matches(DURABLE_WORKLOAD_BEGIN).count() == 1
            && source.matches(DURABLE_WORKLOAD_END).count() == 1,
        "PERF_GATE",
    )?;
    let (coarse, after_begin) = source
        .split_once(DURABLE_WORKLOAD_BEGIN)
        .ok_or(Finding("PERF_GATE"))?;
    let (durable, after_end) = after_begin
        .split_once(DURABLE_WORKLOAD_END)
        .ok_or(Finding("PERF_GATE"))?;
    require(
        !after_end.contains(DURABLE_WORKLOAD_BEGIN) && !after_end.contains(DURABLE_WORKLOAD_END),
        "PERF_GATE",
    )?;
    Ok((coarse, durable))
}

fn validate_perf_workload_ledger_source(source: &str) -> Result<(), Finding> {
    let required = [
        "input_duration: Duration::from_secs(30 * 60),",
        "docops_per_second: 60,",
        "query_qps: 10,",
        "completed_percent: 95,",
        "p99_limit: Duration::from_secs(1),",
        "max_query_limit: Duration::from_secs(5),",
        "drain_limit: Duration::from_secs(60),",
        "rss_limit_bytes: 12 * GIB,",
        "let required_docops = self.limits.docops_per_second * seconds;",
        "if report.docops_offered_in_input < required_docops {",
        "for (second, offered) in self.input_docop_offer_buckets.iter().copied().enumerate() {",
        "let body_started_in_input = body_started_bucket.is_some();",
        "&& operation.submitted.len() == operation.required.len()",
        "&& operation.successful.len() == operation.required.len();",
        "if started_in_input && self.input_bucket(finished_at).is_some() {",
        "if !seen_document_targets.insert(document_target.clone()) {",
        "let required_queries = self.limits.query_qps * seconds;",
        "if report.queries_completed_in_input < required_queries {",
        "for (second, offered) in self.input_query_start_buckets.iter().copied().enumerate() {",
        "if self.input_requests_finished != self.input_requests_submitted {",
        "if report.checkpoints == 0 {",
        "if report.merges == 0 {",
    ];
    require(
        required
            .iter()
            .all(|expected| has_active_line(source, expected)),
        "PERF_GATE",
    )
}

fn validate_perf_gate_source(source: &str) -> Result<(), Finding> {
    let (coarse, durable) = durable_workload_region(source)?;
    let coarse_ok = coarse.contains("const TRUNCATE_LARGE_DOCUMENTS: usize = 100_000;")
        && coarse.contains("const READ_DOCUMENTS: usize = 100_000;")
        && !coarse.contains("500_000")
        && !coarse.contains("500k")
        && coarse.contains("fn number_range_request(start: usize)")
        && coarse.contains("gte: Some(RangeBound::Number(start as f64))")
        && coarse.contains("fn sorted_number_request(cursor: Option<String>, lower_bound: usize)")
        && coarse.contains("gte: Some(RangeBound::Number(lower_bound as f64))")
        && coarse.contains("let mut range_start = READ_RANGE_START;")
        && coarse.contains("let mut sort_lower_bound = READ_SORT_LOWER_BOUND;")
        && coarse.contains("let mut cursor_lower_bound = READ_CURSOR_LOWER_BOUND;");
    require(coarse_ok, "PERF_GATE")?;
    let durable_required = [
        "use crate::support::perf_cell_receipt;",
        "use crate::support::perf_workload_ledger;",
        "mod durable_workload {",
        "const IDLE_COLLECTIONS: usize = 181;",
        "const HOT_DOCUMENTS: usize = 100_000;",
        "const MUTATION_DOCUMENTS_PER_CLASS: usize = (INPUT_SECONDS as usize / 3) * DOCOPS_PER_SECOND;",
        "const VECTOR_READBACK_REFERENCE_NUMBER: usize = 80_000;",
        "const READBACK_MISMATCH_TOKEN: &str = \"readback-mismatch\";",
        "const IDLE_DOCUMENTS_PER_COLLECTION: usize = 100;",
        "const FIELD_COUNT: usize = 14;",
        "const INPUT_SECONDS: u64 = 30 * 60;",
        "const DOCOPS_PER_SECOND: usize = 60;",
        "const QUERY_QPS: usize = 10;",
        "const DOCKER_CPUS: &str = \"2.5\";",
        "const DOCKER_MEMORY_BYTES: u64 = 16 * 1024 * 1024 * 1024;",
        "const DOCKER_MEMORY: &str = \"17179869184\";",
        "const SNAPSHOT_SECONDS: &str = \"15\";",
        "const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);",
        "\"--cpus\",",
        "DOCKER_CPUS,",
        "\"--memory\",",
        "DOCKER_MEMORY,",
        "\"--memory-swap\",",
        "deltas.assert_complete_interval_evidence()?;",
        "Err(failures) => (ledger.report(), failures),",
        "workload_case_outcome(workload_failures, post_input)",
        "Ok(_) if !workload_failures.is_empty() => {",
        "Err(HarnessError::Workload(workload_failures))",
        "counter_delta.checkpoints > 0,",
        "counter_delta.merges > 0,",
        "async fn post_input_restart_step(",
        "let restart_elapsed = server.post_input_restart_step(post_input_deadline).await?;",
        "fn semantic_wrong_field_query(",
        "let mismatch = semantic_wrong_field_query(field, value, fingerprint, document.number)?;",
        "if response_contains_id(&mismatch_response, &document.external_id) {",
        "\"title_ngram\" | \"body_ngram\" | \"summary_ngram\" => {",
        "\"text\": value,",
        "\"{value} {READBACK_MISMATCH_TOKEN} window {number} field {field}\"",
        "fn bounded_vector_query(",
        "{ \"or\": [",
        "\"k\": 1,",
        "fn assert_vector_reference_is_untouched(reference: &SemanticDocument) -> Result<()> {",
        "fn assert_vector_pair_is_distinct_and_non_collinear(",
        "async fn assert_bounded_embedding_readback(",
        "assert_vector_reference_is_untouched(reference)?;",
        "assert_vector_pair_is_distinct_and_non_collinear(target, reference)?;",
        "&vector_readback_reference(),",
        "selection.qualifying.as_deref(),",
        "if diagnostic && qualifying {",
        "(Some(config), false, true) => Ok(Self::Qualifying(config)),",
        "SelectedMode::Diagnostic(config) => {",
        "SelectedMode::Qualifying(config) => {",
        "perf_cell_receipt::write_new(&context.receipt_path, &receipt)",
        "for config in qualifying_matrix() {",
        "run_case(config)",
        "assert_eq!(cases.len(), 16);",
    ];
    let durable_ok = durable_required
        .iter()
        .all(|expected| has_active_line(durable, expected));
    require(durable_ok, "PERF_GATE")?;
    let ngram_positive = active_source_region(
        durable,
        "\"title_ngram\" | \"body_ngram\" | \"summary_ngram\" => {",
        "\"title_text\" | \"body_text\" => {",
    );
    require(
        ngram_positive.is_some_and(|region| has_active_line(region, "\"text\": value,")),
        "PERF_GATE",
    )?;
    let text_mismatch = active_source_region(
        durable,
        "\"title_ngram\" | \"body_ngram\" | \"summary_ngram\" | \"title_text\" | \"body_text\" => {",
        "unexpected => Err(HarnessError::DataInvariant(format!(",
    );
    require(
        text_mismatch.is_some_and(|region| {
            has_active_line(
                region,
                "\"{value} {READBACK_MISMATCH_TOKEN} window {number} field {field}\"",
            )
        }),
        "PERF_GATE",
    )?;
    let expected_inventory = vec![
                ("index_throughput_floor".into(), true),
                ("match_query_latency_floor".into(), true),
                ("term_query_latency_floor".into(), true),
                (
                    "truncate_docs_cost_is_constant_from_10_to_100k_documents".into(),
                    true,
                ),
                ("number_read_costs_on_100k_documents".into(), true),
                ("median_statistic_and_ignored_inventory".into(), false),
                (
                    "docker_run_forwards_recovery_profile_only_when_set".into(),
                    false,
                ),
                (
                    "docker_run_forwards_diagnostic_environment_only_for_diagnostic_mode".into(),
                    false,
                ),
                (
                    "recovery_observation_is_bounded_and_diagnostic_only".into(),
                    false,
                ),
                (
                    "diagnostic_restart_extends_only_its_outer_watchdog".into(),
                    false,
                ),
                (
                    "restart_command_timeout_kills_and_reaps_a_stuck_child".into(),
                    false,
                ),
                ("approved_30_minute_durable_workload".into(), true),
                ("workload_gate_failure_fails_the_cell_after_post_input_gates_pass".into(), false),
                ("post_input_failure_wins_over_a_workload_gate_failure".into(), false),
                ("passing_workload_gates_keep_the_post_input_outcome".into(), false),
                (
                    "fingerprint_only_readback_cannot_pass_an_ignored_field_predicate".into(),
                    false,
                ),
                (
                    "vector_readback_rejects_the_target_for_both_candidate_vectors".into(),
                    false,
                ),
                (
                    "selected_qualifying_cell_is_explicit_and_never_a_diagnostic_false_green"
                        .into(),
                    false,
                ),
                (
                    "qualifying_receipt_path_must_be_absolute_with_an_existing_directory_parent"
                        .into(),
                    false,
                ),
                (
                    "approved_index_operation_requires_the_frozen_fourteen_field_schema".into(),
                    false,
                ),
                (
                    "interval_trace_histograms_require_complete_finite_monotonic_rows".into(),
                    false,
                ),
                (
                    "interval_trace_records_baseline_and_fixed_cadence_deltas".into(),
                    false,
                ),
                (
                    "interval_trace_counts_every_429_after_detailed_journal_cap".into(),
                    false,
                ),
                (
                    "interval_trace_is_bounded_valid_json_without_raw_data".into(),
                    false,
                ),
                (
                    "interval_trace_assigns_bucket_zero_failures_once".into(),
                    false,
                ),
                (
                    "interval_trace_preserves_error_totals_when_detail_is_capped".into(),
                    false,
                ),
                (
                    "interval_trace_separates_timeouts_from_other_transport_after_detailed_journal_cap"
                        .into(),
                    false,
                ),
                (
                    "runtime_sample_keeps_pending_occupancy_and_durable_progress_without_metric_comments".into(),
                    false,
                ),
                (
                    "runtime_metrics_reject_a_missing_required_row".into(),
                    false,
                ),
                (
                    "runtime_metrics_reject_a_duplicate_required_row".into(),
                    false,
                ),
                (
                    "runtime_metrics_reject_an_invalid_required_row".into(),
                    false,
                ),
                (
                    "runtime_metrics_reject_an_unavailable_vmhwm_probe".into(),
                    false,
                ),
                (
                    "duration_counters_reject_backward_and_nonfinite_values".into(),
                    false,
                ),
                (
                    "warmup_retry_policy_accepts_only_the_one_second_backpressure_hint".into(),
                    false,
                ),
                (
                    "seed_backpressure_retry_honors_absolute_setup_deadline".into(),
                    false,
                ),
                (
                    "hnsw_cache_seal_requires_a_strict_hnsw_receipt_and_sends_no_body"
                        .into(),
                    false,
                ),
                (
                    "seed_checkpoint_requires_one_persisted_drained_publication".into(),
                    false,
                ),
                (
                    "seed_checkpoint_honors_setup_and_body_deadlines".into(),
                    false,
                ),
                (
                    "request_deadline_is_the_approved_five_seconds".into(),
                    false,
                ),
                (
                    "post_input_workload_drain_deadline_returns_promptly".into(),
                    false,
                ),
                (
                    "hnsw_cache_seal_uses_remaining_post_input_deadline_for_delayed_response"
                        .into(),
                    false,
                ),
                (
                    "hnsw_cache_seal_never_response_fails_at_remaining_post_input_deadline"
                        .into(),
                    false,
                ),
                (
                    "input_window_deadline_returns_promptly_with_the_timed_out_stage".into(),
                    false,
                ),
                (
                    "input_window_timeout_aborts_sampler_before_drive_finalization".into(),
                    false,
                ),
                (
                    "request_pump_capacity_wait_honors_input_window_deadline".into(),
                    false,
                ),
                (
                    "request_pump_records_body_start_and_completion_latency".into(),
                    false,
                ),
                (
                    "post_restart_backend_residency_rejects_flat_and_hnsw_coercion".into(),
                    false,
                ),
                (
                    "post_restart_backend_residency_rejects_missing_or_unknown_stats".into(),
                    false,
                ),
                (
                    "post_restart_backend_residency_covers_every_fixed_collection_and_vector_field"
                        .into(),
                    false,
                ),
                (
                    "bounded_failure_streams_keep_success_stderr_and_http_bodies_small".into(),
                    false,
                ),
                (
                    "docker_log_tail_keeps_final_stdout_and_stderr_within_artifact_cap".into(),
                    false,
                ),
                (
                    "failure_evidence_keeps_request_chain_and_collects_before_cleanup".into(),
                    false,
                ),
                (
                    "restart_total_deadline_covers_restart_port_lookup_and_readiness".into(),
                    false,
                ),
                (
                    "restart_diagnostics_emit_complete_record_after_not_ready_polls".into(),
                    false,
                ),
                (
                    "recovery_observation_can_find_late_ready_but_restart_still_fails".into(),
                    false,
                ),
                (
                    "restart_diagnostics_keep_partial_records_for_terminal_phase_failures"
                        .into(),
                    false,
                ),
                (
                    "restart_failure_trace_writes_timing_and_cold_readback_unavailable".into(),
                    false,
                ),
                (
                    "journal_records_a_real_reqwest_connect_error_with_full_chain".into(),
                    false,
                ),
                (
                    "request_error_journal_lands_in_the_failure_evidence_bundle".into(),
                    false,
                ),
                (
                    "request_error_journal_caps_entries_and_counts_the_overflow".into(),
                    false,
                ),
                (
                    "non_success_status_record_carries_status_and_a_truncated_body".into(),
                    false,
                ),
                (
                    "a_failed_metrics_scrape_costs_every_second_it_spent".into(),
                    false,
                ),
                (
                    "only_the_typed_capacity_refusal_with_retry_after_is_resendable".into(),
                    false,
                ),
                (
                    "capacity_refusal_is_resent_after_retry_after_and_counted_once".into(),
                    false,
                ),
                (
                    "untyped_429_and_exhausted_budget_are_request_errors_without_resend".into(),
                    false,
                ),
                (
                    "unindex_resends_capacity_refusal_and_requires_no_content".into(),
                    false,
                ),
                (
                    "qualifying_matrix_keeps_every_mutation_endpoint_and_backend".into(),
                    false,
                ),
                (
                    "restart_phase_diagnostics_success_reports_monotonic_phase_elapsed_and_first_ready"
                        .into(),
                    false,
                ),
                (
                    "restart_phase_diagnostics_keeps_one_total_deadline_and_counts_readiness_attempts"
                        .into(),
                    false,
                ),
                (
                    "restart_phase_diagnostics_retains_bounded_terminal_records_for_every_phase"
                        .into(),
                    false,
                ),
                (
                    "restart_readiness_diagnostics_keep_bounded_http_transport_process_and_deadline_evidence"
                        .into(),
                    false,
                ),
                (
                    "restart_readiness_diagnostic_record_names_each_failure_kind_and_caps_text"
                        .into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_late_binds_and_distinguishes_repeated_nonready"
                        .into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_caps_rows_and_counts_omissions".into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_does_not_retain_raw_errors_or_bodies".into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_success_does_not_create_trace_or_change_receipt"
                        .into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_renders_ordered_safe_listener_bound_terminal_failure"
                        .into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_rejects_non_monotonic_poll_timing".into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_caps_rows_and_reports_all_omissions".into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_rendering_excludes_body_error_and_url_data".into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_is_written_before_cleanup_on_failure".into(),
                    false,
                ),
                (
                    "readyz_readiness_trace_pre_poll_restart_failure_creates_no_trace_artifact"
                        .into(),
                    false,
                ),
                (
                    "workload_slots_are_absolute_for_independent_queries_and_paced_mutations"
                        .into(),
                    false,
                ),
                (
                    "diagnostic_seed_traceparent_is_valid_and_fresh_per_actual_send".into(),
                    false,
                ),
                (
                    "diagnostic_seed_failure_record_matches_the_failed_send_and_is_bounded".into(),
                    false,
                ),
                (
                    "diagnostic_seed_setup_timeout_attributes_only_an_active_send".into(),
                    false,
                ),
                (
                    "ordinary_seed_does_no_identity_header_or_private_evidence_work".into(),
                    false,
                ),
            ];
    let actual_inventory = perf_gate_inventory(source);
    require(actual_inventory == expected_inventory, "PERF_GATE")
}

/// The restart budget is one public 30-second deadline. These diagnostics are
/// private, bounded stderr records only. They must expose where that fixed
/// budget went without changing a receipt, a timeout, or a public error.
fn validate_restart_phase_diagnostics_source(source: &str) -> Result<(), Finding> {
    let start = source
        .find("const RESTART_DIAGNOSTIC_PREFIX: &str = \"PERF_RESTART_DIAGNOSTIC\";")
        .ok_or(Finding("RESTART_DIAGNOSTICS"))?;
    let end = source
        .find("fn published_loopback_base")
        .ok_or(Finding("RESTART_DIAGNOSTICS"))?;
    require(start < end, "RESTART_DIAGNOSTICS")?;
    let region = source
        .get(start..end)
        .ok_or(Finding("RESTART_DIAGNOSTICS"))?;
    let has = |expected: &str| {
        region.lines().map(str::trim).any(|line| {
            !line.starts_with("//") && !line.starts_with("/*") && line.contains(expected)
        })
    };
    let required = [
        "struct RestartPhaseDiagnostics {",
        "restart_elapsed: Option<Duration>,",
        "port_lookup_elapsed: Option<Duration>,",
        "readyz_wait_elapsed: Option<Duration>,",
        "readiness_attempts: usize,",
        "first_ready_elapsed: Option<Duration>,",
        "None => \"null\".to_owned(),",
        "fn emit_restart_phase_diagnostics(",
        "eprintln!(\"{record}\");",
        "Observe: FnMut(&str, RestartPhaseDiagnostics),",
        "Instant::now() + STARTUP_TIMEOUT,",
        "deadline.saturating_duration_since(Instant::now())",
        "let restart_started = Instant::now();",
        "diagnostics.restart_elapsed = Some(restart_started.elapsed());",
        "let port_lookup_started = Instant::now();",
        "diagnostics.port_lookup_elapsed = Some(port_lookup_started.elapsed());",
        "let readyz_wait_started = Instant::now();",
        "diagnostics.readyz_wait_elapsed = Some(readyz_wait_started.elapsed());",
        "diagnostics.readiness_attempts += 1;",
        "diagnostics.first_ready_elapsed.get_or_insert_with(|| started.elapsed());",
        "let record = emit_restart_phase_diagnostics(diagnostics, phase, outcome);",
        "observe(&record, *diagnostics);",
        "\"complete\", \"success\"",
        "\"docker-restart\", \"error\"",
        "\"docker-restart\", \"timeout\"",
        "\"published-port\", \"error\"",
        "\"published-port\", \"timeout\"",
        "\"readyz\", \"timeout\"",
        "enum RestartFailureTracePhase {",
        "enum RestartFailureTraceOutcome {",
        "enum RestartFailureTraceColdReadback {",
        "struct RestartFailureTrace {",
        "RestartFailureTraceColdReadback::Unavailable",
        "restart-failure-trace.json",
        "STARTUP_TIMEOUT",
    ];
    require(
        required.iter().all(|expected| has(expected)),
        "RESTART_DIAGNOSTICS",
    )
}

fn validate_scale_matrix_sources(
    scale_source: &str,
    scale_script: &str,
    benchmark_doc: &str,
) -> Result<(), Finding> {
    require(
        scale_source.contains("const DEFAULT_SCALE_ROWS: &[usize] = &[1_000, 10_000, 100_000];")
            && scale_source.contains("const DEFAULT_SCALE_MAX_ROWS: usize = 100_000;")
            && scale_source.contains(
                "const SCALE_READ_CELLS: &[&str] = &[\"range\", \"filter_sort\", \"keyword_sort\", \"sorted_page_deep\"];",
            )
            && scale_source
                .contains("`range` is deliberately a compound range + boolean filter cell")
            && scale_source.contains("`filter_sort` is the required numeric-sort cell")
            && scale_source
                .contains("`keyword_sort` is a deterministic high-cardinality keyword sort")
            && scale_source
                .contains("`sorted_page_deep` is cursor pagination from a precomputed deep cursor")
            && scale_source.contains("fn scale_precompute_mid_collection_cursor(")
            && scale_source.contains("fn scale_preflight_fixture(")
            && scale_source
                .contains("fixture count differs from N={documents} before measurements")
            && scale_source.contains(
                "range filter result set/order changed from deterministic fixture document order",
            )
            && scale_source
                .contains("age ascending with deterministic fixture document-order tie-break")
            && scale_source.contains("sparse high-cardinality missing:last keyword sort changed")
            && scale_source.contains("single-engine preflight requires HTTP client")
            && scale_source.contains("precomputed mid-collection cursor")
            && scale_source.contains("scale cursor preflight pages overlap")
            && scale_source.contains("(cell == \"sorted_page_deep\")")
            && scale_source.contains(".error_for_status()")
            && scale_source.contains("read qps matrix had {request_errors} request errors")
            && scale_source.contains("batch_ids: Vec<String> = (0..documents.min(1_000))")
            && scale_source.contains("docs:unindex")
            && scale_source.contains("docs:truncate")
            && scale_source.contains("const SCALE_RECLAIMER_DRAIN_TIMEOUT: Duration")
            && scale_source.contains("fn wait_for_scale_reclaimer_drain(")
            && scale_source.contains("lumen::storage::collection_reclaimer_snapshot()")
            && scale_source
                .contains("snapshot.pending_generations == baseline.pending_generations")
            && scale_source.contains("reclaimer_queue_high_water=")
            && scale_source.contains("reclaimer_drain_from_truncate_start=")
            && !scale_source.contains("reclaimer_backlog=unavailable"),
        "SCALE_MATRIX",
    )?;
    require(
        scale_script.contains("ROWS=\"${1:-1000,10000,100000}\"")
            && scale_script.contains("LUMEN_SCALE_MAX_ROWS=\"${LUMEN_SCALE_MAX_ROWS:-100000}\"")
            && scale_script
                .contains("LUMEN_SCALE_QPS_TARGETS=\"${LUMEN_SCALE_QPS_TARGETS:-10,100,1000}\"")
            && scale_script
                .contains("cargo test --release --locked -p lumen --test perf_gate_vs_db"),
        "SCALE_MATRIX",
    )?;
    require(
        benchmark_doc.contains("LUMEN_SCALE_DISK=1 LUMEN_SCALE_QPS=1 LUMEN_SCALE_CELLS=range,filter_sort,keyword_sort,sorted_page_deep LUMEN_SCALE_QPS_TARGETS=10 scripts/lumen_scale.sh 1000")
            && benchmark_doc.contains("LUMEN_SCALE_DISK=1 LUMEN_SCALE_QPS=1 LUMEN_SCALE_CELLS=range,filter_sort,keyword_sort,sorted_page_deep LUMEN_SCALE_QPS_TARGETS=10,100,1000 scripts/lumen_scale.sh 1000,10000,100000")
            && benchmark_doc.contains("standard 100K cap")
            && benchmark_doc.contains("QPS applies only to reads")
            && benchmark_doc.contains("actual shared-queue high-water mark")
            && benchmark_doc.contains("30 seconds for this truncate generation to complete")
            && benchmark_doc.contains("`/stats.documents_indexed == N`")
            && benchmark_doc.contains("fixture-document order for `range`")
            && benchmark_doc.contains("age-then-fixture-document order for `filter_sort`")
            && benchmark_doc.contains("`d0` to `d99` for the first `sorted_page_deep`"),
        "SCALE_MATRIX",
    )
}

const PERF_WORKFLOW: &str = "PERF_WORKFLOW";
const PERF_FINAL_RECEIPT: &str = "PERF_FINAL_RECEIPT";

fn exact_mapping_keys(value: &Yaml, expected: &[&str]) -> bool {
    value.as_mapping().is_some_and(|map| {
        map.len() == expected.len() && expected.iter().all(|name| map.contains_key(&key(name)))
    })
}

fn step_named<'a>(steps: &'a [Yaml], name: &str) -> Result<&'a Yaml, Finding> {
    let matches = steps
        .iter()
        .filter(|step| field(step, "name").and_then(Yaml::as_str) == Some(name))
        .collect::<Vec<_>>();
    require(matches.len() == 1, PERF_WORKFLOW)?;
    Ok(matches[0])
}

/// lumen@0.6.1 keeps only the coarse in-process perf gates in CI. The
/// sixteen 30-minute durable cells run on a local host against the candidate
/// digest, and promotion verifies their receipts (`--durable-perf-dir`).
fn validate_durable_perf_workflow(source: &str) -> Result<(), Finding> {
    let workflow: Yaml = serde_yaml::from_str(source).map_err(|_| Finding(PERF_WORKFLOW))?;
    let candidate = job(&workflow, "perf-coarse").ok_or(Finding(PERF_WORKFLOW))?;
    require(
        field(candidate, "runs-on").and_then(Yaml::as_str) == Some("ubuntu-latest")
            && field(candidate, "strategy").is_none(),
        PERF_WORKFLOW,
    )?;
    for forbidden in [
        "LUMEN_PERF_DIAGNOSTIC",
        "LUMEN_PERF_QUALIFYING_CELL",
        "LUMEN_PERF_RECEIPT_PATH",
        "verify-durable-perf",
        "durable-perf",
        "matrix.cell_id",
    ] {
        require(!source.contains(forbidden), PERF_WORKFLOW)?;
    }
    require(
        source.matches(RELEASE_PERF_GATE).count() == 1,
        PERF_WORKFLOW,
    )?;
    let steps = field(candidate, "steps")
        .and_then(Yaml::as_sequence)
        .ok_or(Finding(PERF_WORKFLOW))?;
    let step = step_named(steps, "Run coarse performance gates")?;
    require(
        exact_mapping_keys(step, &["name", "env", "shell", "run"])
            && field(step, "shell").and_then(Yaml::as_str) == Some("bash"),
        PERF_WORKFLOW,
    )?;
    let environment = field(step, "env")
        .and_then(Yaml::as_mapping)
        .ok_or(Finding(PERF_WORKFLOW))?;
    require(
        environment.len() == 1
            && environment
                .get(&key("CARGO_INCREMENTAL"))
                .and_then(Yaml::as_str)
                == Some("0"),
        PERF_WORKFLOW,
    )?;
    let run = field(step, "run")
        .and_then(Yaml::as_str)
        .ok_or(Finding(PERF_WORKFLOW))?;
    require(
        exact_shell_lines(run, &["set -euo pipefail", RELEASE_PERF_GATE]),
        PERF_WORKFLOW,
    )
}

/// From lumen@0.6.1 the candidate receipt carries no performance block; the
/// promotion verifier binds the local durable evidence to the candidate.
fn validate_durable_final_verifier(candidate: &str, promotion: &str) -> Result<(), Finding> {
    for forbidden in ["verify-durable-perf", "performance", "durable-perf-summary"] {
        require(!candidate.contains(forbidden), PERF_FINAL_RECEIPT)?;
    }
    for needle in [
        "verify-durable-perf.py",
        "--verify-existing",
        "--summary-sha256",
        "--durable-perf-dir",
        "--durable-perf-summary-sha256",
        "durable-perf-summary.json",
        "tag_requires_durable_perf_evidence() { tag_at_least 6 1; }",
        "- Standalone GKE: not claimed",
    ] {
        require(promotion.contains(needle), PERF_FINAL_RECEIPT)?;
    }
    Ok(())
}

fn validate_final_jobs_manifest_binding(workflow: &Yaml) -> Result<(), Finding> {
    const JOBS: &str = "jobs:{identity:\"${{ needs.identity.result }}\",build:\"${{ needs.build.result }}\",manifest:\"${{ needs.manifest.result }}\",\"ghcr-image-and-attest\":\"${{ needs.ghcr-image-and-attest.result }}\",\"gates-default\":\"${{ needs.gates-default.result }}\",\"gates-operator\":\"${{ needs.gates-operator.result }}\",\"gates-raft-wal\":\"${{ needs.gates-raft-wal.result }}\",\"gates-release-features\":\"${{ needs.gates-release-features.result }}\",\"container-smoke\":\"${{ needs.container-smoke.result }}\",\"cloud-free-config\":\"${{ needs.cloud-free-config.result }}\",\"perf-coarse\":\"${{ needs.perf-coarse.result }}\",\"verify-candidate\":\"${{ needs.verify-candidate.result }}\",\"verify-libraries\":\"${{ needs.verify-libraries.result }}\",\"kind-amd64\":\"${{ needs.kind-amd64.result }}\",\"kind-arm64\":\"${{ needs.kind-arm64.result }}\",result:\"success\"}";
    let step = named_step(
        workflow,
        "result",
        "Bind all successful job conclusions into final receipt",
    )
    .ok_or(Finding("MANIFEST"))?;
    let run = field(step, "run")
        .and_then(Yaml::as_str)
        .ok_or(Finding("MANIFEST"))?;
    let jq_lines = shell_logical_lines(run)
        .into_iter()
        .filter(|line| line.starts_with("jq -c "))
        .collect::<Vec<_>>();
    require(
        jq_lines.len() == 1
            && jq_lines[0].contains(". + {")
            && jq_lines[0].contains(JOBS)
            && jq_lines[0].contains("> final-candidate-manifest.json"),
        "MANIFEST",
    )
}

fn validate_workflow_semantics(source: &str, dockerfile: &str) -> Result<(), Finding> {
    let workflow: Yaml = serde_yaml::from_str(source).map_err(|_| Finding("YAML"))?;
    let events = field(&workflow, "on")
        .and_then(Yaml::as_mapping)
        .ok_or(Finding("TRIGGER"))?;
    require(events.len() == 1, "TRIGGER")?;
    let dispatch = events
        .get(&key("workflow_dispatch"))
        .ok_or(Finding("TRIGGER"))?;
    let inputs = field(dispatch, "inputs").ok_or(Finding("INPUTS"))?;
    for name in ["version", "commit"] {
        let input = field(inputs, name).ok_or(Finding("INPUTS"))?;
        require(
            field(input, "required").and_then(Yaml::as_bool) == Some(true),
            "INPUTS",
        )?;
        require(
            field(input, "type").and_then(Yaml::as_str) == Some("string"),
            "INPUTS",
        )?;
    }
    let concurrency = field(&workflow, "concurrency").ok_or(Finding("CONCURRENCY"))?;
    require(
        field(concurrency, "group").and_then(Yaml::as_str)
            == Some("lumen-release-candidate-${{ inputs.version }}"),
        "CONCURRENCY",
    )?;
    require(
        field(concurrency, "cancel-in-progress").and_then(Yaml::as_bool) == Some(false),
        "CONCURRENCY",
    )?;
    let names = [
        "identity",
        "build",
        "ghcr-image-and-attest",
        "manifest",
        "gates-default",
        "gates-operator",
        "gates-raft-wal",
        "gates-release-features",
        "container-smoke",
        "cloud-free-config",
        "perf-coarse",
        "verify-candidate",
        "verify-libraries",
        "kind-amd64",
        "kind-arm64",
        "result",
    ];
    let jobs = field(&workflow, "jobs")
        .and_then(Yaml::as_mapping)
        .ok_or(Finding("JOBS"))?;
    require(
        jobs.len() == names.len() && names.iter().all(|name| job(&workflow, name).is_some()),
        "JOBS",
    )?;
    validate_candidate_image_outputs(&workflow)?;
    validate_no_gcloud_execution(&workflow)?;
    validate_uv_setup(&workflow)?;
    validate_cloud_free_acceptance_gates(&workflow)?;
    validate_libraries_job(&workflow)?;
    validate_product_gate_partition(&workflow, source)?;
    validate_perf_gate_source(&perf_gate_source())?;
    validate_perf_workload_ledger_source(&perf_workload_ledger_source())?;
    validate_scale_matrix_sources(
        &perf_gate_vs_db_source(),
        &lumen_scale_script(),
        &benchmark_scale_doc(),
    )?;
    validate_fail_closed_gate_conditions(&workflow)?;
    validate_gate_step_inventory(&workflow)?;
    validate_candidate_and_kind_commands(&workflow)?;
    validate_kind_e2e(&kind_e2e_source())?;
    let graph = [
        ("identity", &[][..]),
        ("build", &["identity"][..]),
        ("ghcr-image-and-attest", &["identity", "build"][..]),
        (
            "manifest",
            &["identity", "build", "ghcr-image-and-attest"][..],
        ),
        ("gates-default", &["identity"][..]),
        ("gates-operator", &["identity"][..]),
        ("gates-raft-wal", &["identity"][..]),
        ("gates-release-features", &["identity"][..]),
        ("container-smoke", &["identity", "ghcr-image-and-attest"][..]),
        ("cloud-free-config", &["identity"][..]),
        ("perf-coarse", &["identity"][..]),
        (
            "verify-candidate",
            &["identity", "manifest", "ghcr-image-and-attest"][..],
        ),
        ("verify-libraries", &["identity"][..]),
        (
            "kind-amd64",
            &["identity", "verify-libraries", "ghcr-image-and-attest"][..],
        ),
        (
            "kind-arm64",
            &["identity", "verify-libraries", "ghcr-image-and-attest"][..],
        ),
        (
            "result",
            &[
                "identity",
                "build",
                "manifest",
                "ghcr-image-and-attest",
                "gates-default",
                "gates-operator",
                "gates-raft-wal",
                "gates-release-features",
                "container-smoke",
                "cloud-free-config",
                "perf-coarse",
                "verify-candidate",
                "verify-libraries",
                "kind-amd64",
                "kind-arm64",
            ][..],
        ),
    ];
    for (name, expected) in graph {
        require(
            strings(field(job(&workflow, name).unwrap(), "needs")) == expected,
            "GRAPH",
        )?;
    }
    let permissions = [
        (
            "identity",
            "actions: read\ncontents: read\npull-requests: read",
        ),
        ("build", "contents: read"),
        (
            "ghcr-image-and-attest",
            "attestations: write\ncontents: read\nid-token: write\npackages: write",
        ),
        ("manifest", "contents: read"),
        ("gates-default", "contents: read"),
        ("gates-operator", "contents: read"),
        ("gates-raft-wal", "contents: read"),
        ("gates-release-features", "contents: read"),
        ("container-smoke", "contents: read\npackages: read"),
        ("cloud-free-config", "contents: read"),
        ("perf-coarse", "contents: read"),
        (
            "verify-candidate",
            "attestations: read\ncontents: read\npackages: read",
        ),
        ("verify-libraries", "contents: read"),
        ("kind-amd64", "contents: read\npackages: read"),
        ("kind-arm64", "contents: read\npackages: read"),
        ("result", "contents: read"),
    ];
    for (name, expected) in permissions {
        let actual = field(job(&workflow, name).unwrap(), "permissions")
            .and_then(Yaml::as_mapping)
            .map(|map| {
                map.iter()
                    .filter_map(|(k, v)| Some(format!("{}: {}", k.as_str()?, v.as_str()?)))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut actual = actual;
        actual.sort();
        let mut expected = expected.split('\n').map(str::to_owned).collect::<Vec<_>>();
        expected.sort();
        require(actual == expected, "PERMISSIONS")?;
    }
    for line in source.lines().filter_map(|line| {
        let line = line.trim();
        line.strip_prefix("- uses: ")
            .or_else(|| line.strip_prefix("uses: "))
    }) {
        let action = line.split_whitespace().next().unwrap_or("");
        require(ACTIONS.contains(&action), "ACTION_PIN")?;
        let (_, sha) = action.rsplit_once('@').ok_or(Finding("ACTION_PIN"))?;
        require(
            sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()),
            "ACTION_PIN",
        )?;
    }
    for needle in [
        "candidate must dispatch main",
        "REQUESTED_COMMIT",
        "REQUESTED_VERSION",
        "git merge-base --is-ancestor",
        "expected one merged main PR",
        "git ls-remote --exit-code --tags origin",
        "the exact release tag already exists",
        "release_query=\"query",
        "the exact GitHub Release already exists",
        "cargo test --locked -p lumen --features raft-wal --test it -- legacy_3073_app::",
        "cargo test --locked -p lumen --features raft-wal --bin lumen cluster_state_poller_converges_role_to_live_election_result",
        "cargo test --locked -p lumen",
        "cargo test --locked -p lumen --features \"operator delegated-auth\"",
        "cargo test --locked -p lumen --features release --test it -- release_feature_set::",
        "bash scripts/standalone-container-smoke.sh bind",
        "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh durable",
        RELEASE_PERF_GATE,
        "git -C \"$core\" fetch -q --depth 1 https://github.com/faberline/core \"$core_rev\"",
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-k8s --test stateful_instance_render",
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-k8s --test stateful_adapter_equivalence",
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p service-k8s",
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime --test adversarial_recovery",
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core",
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime",
        "cargo build --locked -p lumen --features raft-wal",
        "cargo test --locked -p lumen --features raft-wal --lib --no-run",
        "cargo test --locked -p lumen --features raft-wal --test it --no-run",
        "git -c core.fsmonitor=false diff --check",
        "--image \"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\"",
        "LUMEN_E2E_EXPECTED_RUNTIME_DIGEST",
        "LUMEN_E2E_MODE=overlay LUMEN_E2E_IMAGE_MODE=prebuilt LUMEN_E2E_DOC_COUNT=100000",
        "LUMEN_E2E_MODE=operator LUMEN_E2E_IMAGE_MODE=prebuilt LUMEN_E2E_DOC_COUNT=100000",
        "final-candidate-manifest.json",
        "Verify final receipt as local fixture only",
        "--mode local",
        "ruby/setup-ruby@95ef2b042f9d7a56d8268cba8559e2842e2ad01b",
        "ruby-version: '3.3.6'",
        "terraform/1.9.4/terraform_1.9.4_linux_amd64.zip",
        "6e9b2cc741875ab906d800af3134b076489f049565e0a1dbdb6deacd91f5054c",
        "v1.37.0/bin/linux/amd64/kubectl",
        "6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f",
        "bash terraform/lumen-standalone-gke/scripts/check.sh",
        "bash kustomize/lumen-standalone-acceptance/tests/contract.sh",
    ] {
        require(source.contains(needle), "GATES")?;
    }
    for forbidden in [
        "gh release create",
        "gh release publish",
        "gh release upload",
        "imagetools create",
        ":latest",
        "gke-gcloud-auth-plugin",
        "git tag ",
    ] {
        require(!source.contains(forbidden), "CANDIDATE_ONLY")?;
    }
    require(
        source.contains(
            "candidate_tag=release-candidate-${{ github.run_id }}-${{ github.run_attempt }}",
        ),
        "IMAGE",
    )?;
    require(source.contains("org.opencontainers.image.url=https://github.com/${{ github.repository }}/actions/runs/${{ github.run_id }}/attempts/${{ github.run_attempt }}"), "IMAGE")?;
    require(
        source.contains(".manifests | type == \"array\" and length == 2"),
        "DIGESTS",
    )?;
    require(
        source.contains("[.manifests[].digest] | unique | length == 2"),
        "DIGESTS",
    )?;
    require(source.contains("sort == [\"amd64\",\"arm64\"]"), "DIGESTS")?;
    require(
        source.contains(
            "\"$root\" != \"$amd64\" && \"$root\" != \"$arm64\" && \"$amd64\" != \"$arm64\"",
        ),
        "DIGESTS",
    )?;
    require(source.matches("cosign sign --yes").count() == 1, "ATTEST")?;
    require(
        source.matches("uses: actions/attest@").count() == 3,
        "ATTEST",
    )?;
    require(
        source.matches("uses: anchore/sbom-action@").count() == 2,
        "ATTEST",
    )?;
    require(source.contains("name: lumen-candidate-${{ matrix.target }}-${{ github.run_id }}-${{ github.run_attempt }}"), "ARTIFACTS")?;
    for target in [
        "aarch64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-musl",
    ] {
        require(source.contains(&format!("target: {target}")), "ARTIFACTS")?;
    }
    require(
        source.contains("schema:\"cclab.lumen.candidate-manifest.v3\""),
        "MANIFEST",
    )?;
    for binding in [
        "--arg run_id",
        "--arg run_attempt",
        "--arg run_url",
        "--arg source_ref",
        "--arg workflow_ref",
        "--argjson pr_number",
        "--arg pr_url",
    ] {
        require(source.contains(binding), "MANIFEST")?;
    }
    validate_final_jobs_manifest_binding(&workflow)?;
    require(source.contains("LUMEN_E2E_EXPECTED_RUNTIME_DIGEST=\"${{ needs.ghcr-image-and-attest.outputs.amd64_digest }}\""), "KIND")?;
    require(source.contains("LUMEN_E2E_EXPECTED_RUNTIME_DIGEST=\"${{ needs.ghcr-image-and-attest.outputs.arm64_digest }}\""), "KIND")?;
    require(
        source.contains("--manifest-sidecar candidate/final-candidate-manifest.json.sha256"),
        "MANIFEST",
    )?;
    validate_dockerfile(dockerfile)?;
    Ok(())
}

fn validate_workflow(source: &str, dockerfile: &str) -> Result<(), Finding> {
    validate_workflow_semantics(source, dockerfile)?;
    validate_durable_perf_workflow(source)?;
    require(
        sha256_bytes(source.as_bytes()) == WORKFLOW_BYTES_SHA256,
        "WORKFLOW_BYTES",
    )
}

fn validate_dockerfile(source: &str) -> Result<(), Finding> {
    const DEBIAN: &str = "debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171";
    const DISTROLESS: &str = "gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab";
    let froms = source
        .lines()
        .filter_map(|line| line.trim().strip_prefix("FROM "))
        .map(|line| {
            line.split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>();
    require(
        froms
            == vec![
                format!("{DEBIAN} AS seed"),
                format!("{DEBIAN} AS binary-source-fetch"),
                format!("{DEBIAN} AS binary-source-staged"),
                "binary-source-${SOURCE} AS binary-source".to_string(),
                DISTROLESS.to_string(),
            ],
        "BASE_IMAGE",
    )
}

fn validate_verifier(source: &str, mode: u32) -> Result<(), Finding> {
    require(mode & 0o777 == 0o755, "MODE")?;
    for needle in [
        "`local` validates synthetic files only. It is not candidate acceptance.",
        "`full` also validates the run-scoped GHCR image",
        "cclab.lumen.candidate-manifest.v3",
        "run_url == (\"https://github.com/\" + $repo + \"/actions/runs/\" + $run_id + \"/attempts/\" + $attempt)",
        ".source_ref == \"refs/heads/main\"",
        ".workflow_ref == $workflow_ref",
        ".jobs == {identity:\"success\",build:\"success\",manifest:\"success\",\"ghcr-image-and-attest\":\"success\",\"gates-default\":\"success\",\"gates-operator\":\"success\",\"gates-raft-wal\":\"success\",\"gates-release-features\":\"success\",\"container-smoke\":\"success\",\"cloud-free-config\":\"success\",\"perf-coarse\":\"success\",\"verify-candidate\":\"success\",\"verify-libraries\":\"success\",\"kind-amd64\":\"success\",\"kind-arm64\":\"success\",result:\"success\"}",
        "archive members changed",
        "archive binary is not executable",
        "invalid SPDX 2.3 SBOM",
        "predicate == $sbom[0]",
        "--certificate-identity \"$EXPECTED_CERT_ID\"",
        "--cert-oidc-issuer https://token.actions.githubusercontent.com",
        "expected_run_url=\"https://github.com/${REPO}/actions/runs/${RUN_ID}/attempts/${RUN_ATTEMPT}\"",
        "org.opencontainers.image.url",
        "archive_sha256",
        "sidecar_sha256",
        "spdx-${sbom}.json",
        "candidate tag is not scoped to this run attempt",
        "LOCAL FIXTURE ONLY: artifacts verified; this is not candidate acceptance.",
    ] {
        require(source.contains(needle), "VERIFIER")?;
    }
    for forbidden in [
        "gh release create",
        "gh release publish",
        "cosign sign",
        "imagetools create",
    ] {
        require(!source.contains(forbidden), "VERIFIER_SIDE_EFFECT")?;
    }
    require(
        sha256_bytes(source.as_bytes()) == VERIFIER_BYTES_SHA256,
        "VERIFIER_BYTES",
    )
}

fn expect_workflow(source: &str, from: &str, to: &str, code: &'static str) {
    let changed = replace_once(source, from, to);
    assert_eq!(
        validate_workflow(&changed, &dockerfile()).unwrap_err(),
        Finding(code)
    );
}
fn expect_verifier(source: &str, from: &str, to: &str, code: &'static str) {
    let changed = replace_once(source, from, to);
    assert_eq!(
        validate_verifier(&changed, 0o755).unwrap_err(),
        Finding(code)
    );
}
fn workflow() -> String {
    fs::read_to_string(root().join(".github/workflows/lumen-release-candidate.yml")).unwrap()
}
fn kind_e2e_source() -> String {
    fs::read_to_string(root().join("scripts/kind-e2e.sh")).unwrap()
}
fn perf_gate_source() -> String {
    fs::read_to_string(root().join("tests/it/perf_gate.rs")).unwrap()
}
fn perf_workload_ledger_source() -> String {
    fs::read_to_string(root().join("tests/it/support/perf_workload_ledger.rs")).unwrap()
}
fn perf_gate_vs_db_source() -> String {
    fs::read_to_string(root().join("tests/perf_gate_vs_db.rs")).unwrap()
}
fn lumen_scale_script() -> String {
    fs::read_to_string(root().join("scripts/lumen_scale.sh")).unwrap()
}
fn benchmark_scale_doc() -> String {
    fs::read_to_string(root().join("docs/benchmarks-scale.md")).unwrap()
}
fn verifier() -> (String, u32) {
    let path = root().join("scripts/verify-release-candidate.sh");
    (
        fs::read_to_string(&path).unwrap(),
        fs::metadata(path).unwrap().permissions().mode(),
    )
}
fn dockerfile() -> String {
    fs::read_to_string(root().join("Dockerfile.release")).unwrap()
}

fn promotion_verifier() -> String {
    fs::read_to_string(root().join("scripts/verify-release-artifacts.sh")).unwrap()
}

#[test]
fn coarse_performance_release_workflow_is_fail_closed() {
    let source = workflow();
    validate_durable_perf_workflow(&source)
        .expect("the checked-in workflow runs only the coarse perf gates");

    const STEP: &str = "      - name: Run coarse performance gates\n        env:\n          CARGO_INCREMENTAL: '0'\n";
    for (name, changed) in [
        (
            "durable matrix restored",
            replace_once(
                &source,
                "    name: coarse performance gates\n",
                "    name: coarse performance gates\n    strategy:\n      fail-fast: false\n",
            ),
        ),
        (
            "qualifying cell in CI",
            replace_once(
                &source,
                STEP,
                &format!("{STEP}          LUMEN_PERF_QUALIFYING_CELL: '1'\n"),
            ),
        ),
        (
            "extra env",
            replace_once(&source, STEP, &format!("{STEP}          RUST_LOG: debug\n")),
        ),
        (
            "conditional step",
            replace_once(
                &source,
                "      - name: Run coarse performance gates\n",
                "      - name: Run coarse performance gates\n        if: ${{ always() }}\n",
            ),
        ),
        (
            "durable workload not skipped",
            replace_once(&source, " --skip durable_workload", ""),
        ),
        (
            "duplicate gate",
            replace_once(
                &source,
                RELEASE_PERF_GATE,
                &format!("{RELEASE_PERF_GATE}\n          {RELEASE_PERF_GATE}"),
            ),
        ),
        (
            "durable summary uploaded",
            replace_once(
                &source,
                "            candidate/final-candidate-manifest.json\n",
                "            candidate/final-candidate-manifest.json\n            candidate/durable-perf-summary.json\n",
            ),
        ),
    ] {
        assert_eq!(
            validate_durable_perf_workflow(&changed),
            Err(Finding(PERF_WORKFLOW)),
            "{name} mutation passed"
        );
    }
}

#[test]
fn durable_final_receipt_verifier_is_fail_closed() {
    let (candidate, _) = verifier();
    let promotion = promotion_verifier();
    validate_durable_final_verifier(&candidate, &promotion)
        .expect("durable evidence must move from the candidate receipt to promotion");

    for needle in [
        "--verify-existing",
        "--summary-sha256",
        "--durable-perf-dir",
        "tag_requires_durable_perf_evidence() { tag_at_least 6 1; }",
        "- Standalone GKE: not claimed",
    ] {
        assert_eq!(
            validate_durable_final_verifier(&candidate, &promotion.replace(needle, "removed")),
            Err(Finding(PERF_FINAL_RECEIPT)),
            "promotion verifier accepted missing {needle}"
        );
    }
    assert_eq!(
        validate_durable_final_verifier(
            &format!("{candidate}\nverify_performance_evidence() {{ :; }}\n"),
            &promotion
        ),
        Err(Finding(PERF_FINAL_RECEIPT)),
        "candidate verifier accepted a performance block"
    );
}

#[test]
fn cloud_free_gate_mutations_fail_without_hash_oracle() {
    let source = workflow();
    let assert_finding = |from: &str, to: &str, finding| {
        let changed = replace_once(&source, from, to);
        assert_eq!(
            validate_workflow_semantics(&changed, &dockerfile()).unwrap_err(),
            Finding(finding),
        );
    };
    assert_finding(
        "ruby/setup-ruby@95ef2b042f9d7a56d8268cba8559e2842e2ad01b",
        "ruby/setup-ruby@0000000000000000000000000000000000000000",
        "CLOUD_FREE_GATES",
    );
    assert_finding(
        "ruby-version: '3.3.6'",
        "ruby-version: '3.3.7'",
        "CLOUD_FREE_GATES",
    );
    assert_finding(
        "terraform/1.9.4/terraform_1.9.4_linux_amd64.zip",
        "terraform/1.9.5/terraform_1.9.5_linux_amd64.zip",
        "GATE_COMMANDS",
    );
    assert_finding(
        "6e9b2cc741875ab906d800af3134b076489f049565e0a1dbdb6deacd91f5054c",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "GATE_COMMANDS",
    );
    assert_finding(
        "v1.37.0/bin/linux/amd64/kubectl",
        "v1.37.1/bin/linux/amd64/kubectl",
        "GATE_COMMANDS",
    );
    assert_finding(
        "6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "GATE_COMMANDS",
    );
    assert_finding(
        "bash terraform/lumen-standalone-gke/scripts/check.sh",
        "bash terraform/lumen-standalone-gke/scripts/other.sh",
        "GATE_COMMANDS",
    );
    assert_finding(
        "bash kustomize/lumen-standalone-acceptance/tests/contract.sh",
        "bash kustomize/lumen-standalone-acceptance/tests/other.sh",
        "GATE_COMMANDS",
    );
    let terraform_step = "      - name: Install verified Terraform 1.9.4\n        shell: bash\n        run: |\n          set -euo pipefail\n          curl -fsSL https://releases.hashicorp.com/terraform/1.9.4/terraform_1.9.4_linux_amd64.zip -o /tmp/terraform.zip\n          echo '6e9b2cc741875ab906d800af3134b076489f049565e0a1dbdb6deacd91f5054c  /tmp/terraform.zip' | sha256sum -c -\n          unzip -oq /tmp/terraform.zip -d /tmp/terraform-bin\n          sudo install -m 0755 /tmp/terraform-bin/terraform /usr/local/bin/terraform\n";
    let without_terraform = replace_once(&source, terraform_step, "");
    assert_eq!(
        validate_workflow_semantics(&without_terraform, &dockerfile()).unwrap_err(),
        Finding("CLOUD_FREE_GATES"),
    );
    let terraform_start = source
        .find("      - name: Run cloud-free Terraform acceptance gate\n")
        .unwrap();
    let kustomize_start = source
        .find("      - name: Run cloud-free Kustomize acceptance gate\n")
        .unwrap();
    assert!(terraform_start < kustomize_start);
    let kustomize_end = kustomize_start
        + source[kustomize_start..]
            .find("\n\n  perf-coarse:\n")
            .unwrap()
        + 1;
    let terraform_block = &source[terraform_start..kustomize_start];
    let kustomize_block = &source[kustomize_start..kustomize_end];
    let moved = format!(
        "{}{}{}{}",
        &source[..terraform_start],
        kustomize_block,
        terraform_block,
        &source[kustomize_end..]
    );
    assert_eq!(
        validate_workflow_semantics(&moved, &dockerfile()).unwrap_err(),
        Finding("CLOUD_FREE_GATES"),
    );
    assert_finding(
        "        run: bash terraform/lumen-standalone-gke/scripts/check.sh",
        "        run: '# bash terraform/lumen-standalone-gke/scripts/check.sh'",
        "GATE_COMMANDS",
    );
    assert_finding(
        "        run: bash kustomize/lumen-standalone-acceptance/tests/contract.sh",
        "        run: echo 'bash kustomize/lumen-standalone-acceptance/tests/contract.sh'",
        "GATE_COMMANDS",
    );
    assert_finding(
        "        run: bash terraform/lumen-standalone-gke/scripts/check.sh",
        "        run: if false; then bash terraform/lumen-standalone-gke/scripts/check.sh; fi",
        "GATE_COMMANDS",
    );
}

// QA-FROZEN-CLIENT-SEED-INVENTORY-BEGIN
fn client_seed_inventory_replace_actual_anchor(
    source: &str,
    anchor: &str,
    replacement: &str,
) -> String {
    assert_ne!(anchor, replacement);
    assert_eq!(source.match_indices(anchor).count(), 1, "actual source anchor");
    let start = source.find(anchor).unwrap();
    let end = start + anchor.len();
    let mut changed = source.to_owned();
    changed.replace_range(start..end, replacement);
    assert_ne!(changed, source);
    assert_eq!(&changed[..start], &source[..start]);
    assert_eq!(
        &changed[start..start + replacement.len()],
        replacement,
    );
    assert_eq!(&changed[start + replacement.len()..], &source[end..]);
    changed
}

#[test]
fn client_seed_inventory_requires_each_approved_case_and_rejects_unapproved_cases() {
    let source = perf_gate_source();
    let inventory = perf_gate_inventory(&source);
    let positive = validate_perf_gate_source(&source);
    let mut negative_outcomes = Vec::new();
    for identity in [
        "diagnostic_seed_traceparent_is_valid_and_fresh_per_actual_send",
        "diagnostic_seed_failure_record_matches_the_failed_send_and_is_bounded",
        "diagnostic_seed_setup_timeout_attributes_only_an_active_send",
        "ordinary_seed_does_no_identity_header_or_private_evidence_work",
    ] {
        let anchor = format!("    #[test]\n    fn {identity}() {{");
        let function_anchor = format!("    fn {identity}() {{");
        assert_eq!(source.match_indices(&function_anchor).count(), 1);
        let positions: Vec<_> = inventory
            .iter()
            .enumerate()
            .filter_map(|(index, (name, _))| (name == identity).then_some(index))
            .collect();
        assert_eq!(positions.len(), 1, "one actual approved identity");
        let position = positions[0];
        assert!(!inventory[position].1, "approved case is nonignored");

        let missing = client_seed_inventory_replace_actual_anchor(
            &source,
            &anchor,
            &function_anchor,
        );
        let mut missing_inventory = inventory.clone();
        missing_inventory.remove(position);

        let renamed_identity = format!("renamed_{identity}");
        let renamed_function_anchor = format!("    fn {renamed_identity}() {{");
        assert_eq!(source.match_indices(&renamed_function_anchor).count(), 0);
        let renamed = client_seed_inventory_replace_actual_anchor(
            &source,
            &anchor,
            &format!("    #[test]\n{renamed_function_anchor}"),
        );
        let mut renamed_inventory = inventory.clone();
        renamed_inventory[position].0 = renamed_identity;

        let ignored = client_seed_inventory_replace_actual_anchor(
            &source,
            &anchor,
            &format!("    #[test]\n    #[ignore]\n{function_anchor}"),
        );
        let mut ignored_inventory = inventory.clone();
        ignored_inventory[position].1 = true;

        for (mutation, fixture, expected_inventory) in [
            ("missing_test_attribute", missing, missing_inventory),
            ("renamed_function_identity", renamed, renamed_inventory),
            ("changed_ignore_flag", ignored, ignored_inventory),
        ] {
            assert_eq!(
                perf_gate_inventory(&fixture),
                expected_inventory,
                "exactly one intended inventory change: {identity}::{mutation}",
            );
            negative_outcomes.push((identity, mutation, validate_perf_gate_source(&fixture)));
        }
    }

    let extra_identity = "unapproved_client_seed_inventory_case";
    let extra_anchor = format!("    #[test]\n    fn {extra_identity}() {{");
    assert_eq!(source.match_indices(&extra_anchor).count(), 0);
    let module_end = "}\n// DURABLE-WORKLOAD-END\n// CODEGEN-END\n";
    let extra = client_seed_inventory_replace_actual_anchor(
        &source,
        module_end,
        &format!(
            "\n{extra_anchor}\n        assert_eq!(REQUEST_TIMEOUT, Duration::from_secs(5));\n    }}\n{module_end}"
        ),
    );
    let mut extra_inventory = inventory.clone();
    extra_inventory.push((extra_identity.into(), false));
    assert_eq!(
        perf_gate_inventory(&extra),
        extra_inventory,
        "exactly one additional real synchronous test",
    );
    negative_outcomes.push((
        extra_identity,
        "one_additional_unapproved_synchronous_test",
        validate_perf_gate_source(&extra),
    ));

    assert_eq!(negative_outcomes.len(), 13);
    for (identity, mutation, outcome) in negative_outcomes {
        assert_eq!(
            outcome,
            Err(Finding("PERF_GATE")),
            "{identity}::{mutation} must fail closed",
        );
        println!("client seed inventory negative asserted: {identity}::{mutation} => {outcome:?}");
    }
    println!("client seed inventory: all 13 negative outcomes asserted; real source => {positive:?}");
    assert_eq!(positive, Ok(()), "real frozen perf source must pass");
}
// QA-FROZEN-CLIENT-SEED-INVENTORY-END

#[test]
fn live_candidate_contract_is_fail_closed() {
    let source = workflow();
    const DURABLE_GATE: &str = "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh durable";
    for (replacement, finding) in [
        (
            "# LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh durable",
            "GATES",
        ),
        (
            "echo 'LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh durable'",
            "GATES",
        ),
        (
            "if false; then LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh durable; fi",
            "GATES",
        ),
        (
            "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.amd64_digest }}\" bash scripts/standalone-container-smoke.sh durable",
            "GATES",
        ),
        (
            "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.arm64_digest }}\" bash scripts/standalone-container-smoke.sh durable",
            "GATES",
        ),
        (
            "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}:latest\" bash scripts/standalone-container-smoke.sh durable",
            "GATES",
        ),
        (
            "LUMEN_STANDALONE_DURABLE_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\" bash scripts/standalone-container-smoke.sh bind",
            "GATES",
        ),
    ] {
        let changed = source.replace(DURABLE_GATE, replacement);
        assert_eq!(
            validate_workflow_semantics(&changed, &dockerfile()).unwrap_err(),
            Finding(finding)
        );
    }
    validate_workflow(&source, &dockerfile()).expect("candidate workflow contract");
    let (script, mode) = verifier();
    validate_verifier(&script, mode).expect("candidate verifier contract");
    assert_eq!(validate_verifier(&script, 0o644), Err(Finding("MODE")));
}

#[test]
fn kind_storage_mutations_fail_without_hash_oracle() {
    let source = kind_e2e_source();
    validate_kind_e2e_semantics(&source).expect("kind storage semantics");
    let normalize = r#"map(if has("readOnly") then . else . + {"readOnly": false} end)"#;
    for (label, occurrence, replacement) in [
        ("normalization removed", 0, "map(.)"),
        (
            "explicit true overwritten",
            2,
            r#"map(. + {"readOnly": false})"#,
        ),
    ] {
        let changed = replace_occurrence(&source, normalize, replacement, occurrence);
        assert_eq!(
            validate_kind_e2e_semantics(&changed).unwrap_err(),
            Finding("KIND_STORAGE"),
            "{label} mutation passed",
        );
    }

    let child =
        r#"{"mountPath":"/var/lib/lumen/data","name":"raft","readOnly":false,"subPath":"data"}"#;
    for (label, occurrence, replacement) in [
        (
            "read-only child accepted",
            0,
            r#"{"mountPath":"/var/lib/lumen/data","name":"raft","readOnly":true,"subPath":"data"}"#,
        ),
        (
            "child subPath removed",
            1,
            r#"{"mountPath":"/var/lib/lumen/data","name":"raft","readOnly":false}"#,
        ),
    ] {
        let changed = replace_occurrence(&source, child, replacement, occurrence);
        assert_eq!(
            validate_kind_e2e_semantics(&changed).unwrap_err(),
            Finding("KIND_STORAGE"),
            "{label} mutation passed",
        );
    }
}

#[test]
fn kind_durable_restart_mutations_fail_without_hash_oracle() {
    let source = kind_e2e_source();
    validate_kind_e2e_semantics(&source).expect("kind durable restart semantics");
    for (label, line) in [
        ("checkpoint", "checkpoint=\"$(api_checkpoint)\""),
        (
            "restart",
            "kubectl -n \"$NAMESPACE\" delete pod -l \"$APP_LABEL\" --wait=true",
        ),
        (
            "old document search",
            "old_hits=\"$(api_search_exact \"$pre_value\" | jq --arg id \"$pre_id\" '[.hits[] | select(.external_id == $id)] | length')\"",
        ),
    ] {
        let quoted = line.replace('\'', "'\"'\"'");
        for (mutation, replacement) in [
            ("comment", format!("# {line}")),
            ("quoted prose", format!("printf '%s\\n' '{quoted}'")),
            ("dead branch", format!("if false; then {line}; fi")),
            ("ignored failure", format!("{line} || true")),
        ] {
            let changed = replace_once(&source, &format!("  {line}"), &format!("  {replacement}"));
            assert_eq!(
                validate_kind_e2e_semantics(&changed).unwrap_err(),
                Finding("KIND_DURABLE_RESTART"),
                "{label} {mutation} mutation passed",
            );
        }
    }

    let old_read_then_new_write = concat!(
        "  old_hits=\"$(api_search_exact \"$pre_value\" | jq --arg id \"$pre_id\" '[.hits[] | select(.external_id == $id)] | length')\"\n",
        "  [[ \"$old_hits\" -eq 1 ]] || die \"pre-restart document was not readable before any new write\"\n",
        "  api_index_exact \"$post_id\" \"$post_value\""
    );
    let new_write_then_old_read = concat!(
        "  api_index_exact \"$post_id\" \"$post_value\"\n",
        "  old_hits=\"$(api_search_exact \"$pre_value\" | jq --arg id \"$pre_id\" '[.hits[] | select(.external_id == $id)] | length')\"\n",
        "  [[ \"$old_hits\" -eq 1 ]] || die \"pre-restart document was not readable before any new write\""
    );
    let reordered = replace_once(&source, old_read_then_new_write, new_write_then_old_read);
    assert_eq!(
        validate_kind_e2e_semantics(&reordered).unwrap_err(),
        Finding("KIND_DURABLE_RESTART"),
        "new write before old-document read passed",
    );
}

#[test]
fn candidate_source_mutations_fail_with_stable_categories() {
    let source = workflow();
    let root_digest_metadata = replace_once(
        &source,
        "root_digest: ${{ steps.push.outputs.digest }}",
        "root_digest: ${{ steps.push.outputs.metadata }}",
    );
    assert_eq!(
        validate_workflow_semantics(&root_digest_metadata, &dockerfile()).unwrap_err(),
        Finding("IMAGE")
    );
    let gcloud_anchor = "          echo \"workflow_ref=$WORKFLOW_REF\"\n";
    for (label, injection) in [
        ("gcloud tab", "          gcloud\tcontainer clusters list\n"),
        (
            "gcloud continuation",
            "          gcloud \\\n          container clusters list\n",
        ),
        (
            "gcloud executable path",
            "          /usr/local/bin/gcloud container clusters list\n",
        ),
        (
            "gcloud sudo wrapper",
            "          sudo -u root gcloud container clusters list\n",
        ),
        (
            "gcloud env wrapper",
            "          env gcloud container clusters list\n",
        ),
        (
            "gcloud command wrapper",
            "          command gcloud container clusters list\n",
        ),
        (
            "gcloud bash c wrapper",
            "          bash -c 'gcloud container clusters list'\n",
        ),
        (
            "gcloud sh c wrapper",
            "          sh -c 'gcloud container clusters list'\n",
        ),
        (
            "gcloud env path bash c wrapper",
            "          /usr/bin/env bash -c 'gcloud container clusters list'\n",
        ),
    ] {
        let changed = replace_once(
            &source,
            gcloud_anchor,
            &format!("{gcloud_anchor}{injection}"),
        );
        assert_eq!(
            validate_workflow_semantics(&changed, &dockerfile()).unwrap_err(),
            Finding("CANDIDATE_ONLY"),
            "{label} mutation passed"
        );
    }
    for occurrence in 0..2 {
        let same_name_step = replace_occurrence(
            &source,
            "      - name: Log in to GHCR with read-only job access\n        uses: docker/login-action@c94ce9fb468520275223c153574b00df6fe4bcc9 # v3.7.0\n        with:\n          registry: ghcr.io\n          username: ${{ github.actor }}\n          password: ${{ github.token }}\n",
            "      - name: Log in to GHCR with read-only job access\n        shell: bash\n        run: |\n          # docker/login-action@c94ce9fb468520275223c153574b00df6fe4bcc9\n          printf '#!/usr/bin/env bash\\nexit 0\\n' > scripts/verify-release-candidate.sh\n",
            occurrence,
        );
        assert_ne!(same_name_step, source);
        assert_eq!(
            validate_workflow(&same_name_step, &dockerfile()).unwrap_err(),
            Finding("WORKFLOW_BYTES"),
            "same-name GHCR login occurrence {occurrence} passed"
        );
    }
    for (from, to, code) in [
        (
            "description: Exact Lumen semver without the lumen@ prefix.\n        required: true",
            "description: Exact Lumen semver without the lumen@ prefix.\n        required: false",
            "INPUTS",
        ),
        (
            "needs: [identity, manifest, ghcr-image-and-attest]",
            "needs: [identity, ghcr-image-and-attest]",
            "GRAPH",
        ),
        (
            "attestations: read\n      contents: read\n      packages: read",
            "attestations: read\n      contents: read",
            "PERMISSIONS",
        ),
        (
            "candidate_tag=release-candidate-${{ github.run_id }}-${{ github.run_attempt }}",
            "candidate_tag=lumen@${{ needs.identity.outputs.version }}",
            "IMAGE",
        ),
        ("--mode local", "--mode full", "GATES"),
        (
            "cargo test --locked -p lumen --features release --test it -- release_feature_set::",
            "true",
            "GATES",
        ),
        (
            "cargo test --locked -p lumen --features raft-wal --test it -- legacy_3073_app::",
            "true",
            "GATES",
        ),
        (
            "dtolnay/rust-toolchain@4360b52568e2003a75bf9bc1d59f33a8e3fc893c",
            "dtolnay/rust-toolchain@stable",
            "ACTION_PIN",
        ),
        (
            "imagetools inspect --raw",
            "imagetools create",
            "CANDIDATE_ONLY",
        ),
    ] {
        expect_workflow(&source, from, to, code);
    }
    for (occurrence, finding) in [(0, "UV_SETUP")] {
        let changed = replace_occurrence(&source, "version: 0.12.1", "version: 0.12.2", occurrence);
        assert_eq!(
            validate_workflow(&changed, &dockerfile()).unwrap_err(),
            Finding(finding),
            "uv setup version occurrence {occurrence} passed",
        );
    }
    for (name, replacement, finding) in [
        (
            "missing release",
            RELEASE_PERF_GATE.replace("--release ", ""),
            "GATES",
        ),
        (
            "missing locked",
            RELEASE_PERF_GATE.replace("--locked ", ""),
            "GATES",
        ),
        (
            "missing ignored",
            RELEASE_PERF_GATE.replace("--ignored ", ""),
            "GATES",
        ),
        (
            "missing test threads",
            RELEASE_PERF_GATE.replace("--test-threads=1 ", ""),
            "GATES",
        ),
        (
            "missing nocapture",
            RELEASE_PERF_GATE.replace("--nocapture", ""),
            "GATES",
        ),
        (
            "missing durable skip",
            RELEASE_PERF_GATE.replace("--skip durable_workload ", ""),
            "GATES",
        ),
        ("comment", format!("# {RELEASE_PERF_GATE}"), PERF_WORKFLOW),
        (
            "quoted prose",
            format!("echo '{RELEASE_PERF_GATE}'"),
            PERF_WORKFLOW,
        ),
        (
            "reordered command",
            RELEASE_PERF_GATE.replace("--release --locked", "--locked --release"),
            "GATES",
        ),
        (
            "semicolon split",
            format!("{RELEASE_PERF_GATE}; true"),
            PERF_WORKFLOW,
        ),
        (
            "and split",
            format!("true && {RELEASE_PERF_GATE}"),
            PERF_WORKFLOW,
        ),
        (
            "if split",
            format!("if true; then {RELEASE_PERF_GATE}; fi"),
            PERF_WORKFLOW,
        ),
        (
            "eval split",
            format!("eval '{RELEASE_PERF_GATE}'"),
            PERF_WORKFLOW,
        ),
    ] {
        let changed = replace_once(&source, RELEASE_PERF_GATE, &replacement);
        assert_eq!(
            validate_workflow(&changed, &dockerfile()).unwrap_err(),
            Finding(finding),
            "{name} mutation passed",
        );
    }
    let perf_source = perf_gate_source();
    for occurrence in 0..5 {
        let changed = replace_occurrence(
            &perf_source,
            "#[ignore = \"coarse performance gate runs in the release candidate workflow\"]\n",
            "",
            occurrence,
        );
        assert_eq!(
            validate_perf_gate_source(&changed).unwrap_err(),
            Finding("PERF_GATE"),
            "missing ignore occurrence {occurrence} passed",
        );
    }
    let without_ignores = perf_source.replace(
        "#[ignore = \"coarse performance gate runs in the release candidate workflow\"]\n",
        "",
    );
    assert_eq!(
        validate_perf_gate_source(&without_ignores).unwrap_err(),
        Finding("PERF_GATE")
    );
    let statistic_ignored = replace_once(
        &perf_source,
        "#[test]\nfn median_statistic_and_ignored_inventory",
        "#[test]\n#[ignore]\nfn median_statistic_and_ignored_inventory",
    );
    assert_eq!(
        validate_perf_gate_source(&statistic_ignored).unwrap_err(),
        Finding("PERF_GATE")
    );
    let extra_ignored = replace_once(
        &perf_source,
        "// CODEGEN-END",
        "#[test]\n#[ignore]\nfn extra_perf_row() {}\n// CODEGEN-END",
    );
    assert_eq!(
        validate_perf_gate_source(&extra_ignored).unwrap_err(),
        Finding("PERF_GATE")
    );
    let renamed_truncate = replace_once(
        &perf_source,
        "fn truncate_docs_cost_is_constant_from_10_to_100k_documents",
        "fn renamed_truncate_docs_cost_is_constant_from_10_to_100k_documents",
    );
    assert_eq!(
        validate_perf_gate_source(&renamed_truncate).unwrap_err(),
        Finding("PERF_GATE")
    );
    let removed_read_test = replace_once(
        &perf_source,
        "#[test]\n#[ignore = \"coarse performance gate runs in the release candidate workflow\"]\nfn number_read_costs_on_100k_documents",
        "fn number_read_costs_on_100k_documents",
    );
    assert_eq!(
        validate_perf_gate_source(&removed_read_test).unwrap_err(),
        Finding("PERF_GATE")
    );
    let unignored_read_test = replace_once(
        &perf_source,
        "#[ignore = \"coarse performance gate runs in the release candidate workflow\"]\nfn number_read_costs_on_100k_documents",
        "fn number_read_costs_on_100k_documents",
    );
    assert_eq!(
        validate_perf_gate_source(&unignored_read_test).unwrap_err(),
        Finding("PERF_GATE")
    );
    let renamed_read_test = replace_once(
        &perf_source,
        "fn number_read_costs_on_100k_documents",
        "fn renamed_number_read_costs_on_100k_documents",
    );
    assert_eq!(
        validate_perf_gate_source(&renamed_read_test).unwrap_err(),
        Finding("PERF_GATE")
    );
    let fixed_range_request = replace_once(
        &perf_source,
        "gte: Some(RangeBound::Number(start as f64))",
        "gte: Some(RangeBound::Number(READ_RANGE_START as f64))",
    );
    assert_eq!(
        validate_perf_gate_source(&fixed_range_request).unwrap_err(),
        Finding("PERF_GATE")
    );
    let fixed_sorted_request = replace_once(
        &perf_source,
        "gte: Some(RangeBound::Number(lower_bound as f64))",
        "gte: Some(RangeBound::Number(READ_SORT_LOWER_BOUND as f64))",
    );
    assert_eq!(
        validate_perf_gate_source(&fixed_sorted_request).unwrap_err(),
        Finding("PERF_GATE")
    );
    let comment_decoy = replace_occurrence(
        &perf_source,
        "#[ignore = \"coarse performance gate runs in the release candidate workflow\"]\n",
        "// #[ignore]\n",
        0,
    );
    assert_eq!(
        validate_perf_gate_source(&comment_decoy).unwrap_err(),
        Finding("PERF_GATE")
    );
    let block_comment_decoy = replace_occurrence(
        &perf_source,
        "#[ignore = \"coarse performance gate runs in the release candidate workflow\"]\n",
        "/*\n#[ignore]\nfn comment_decoy() {}\n*/\n",
        0,
    );
    assert_eq!(
        validate_perf_gate_source(&block_comment_decoy).unwrap_err(),
        Finding("PERF_GATE")
    );
    let standard_500k = replace_once(
        &perf_source,
        "const READ_DOCUMENTS: usize = 100_000;",
        "const READ_DOCUMENTS: usize = 500_000;",
    );
    assert_eq!(
        validate_perf_gate_source(&standard_500k).unwrap_err(),
        Finding("PERF_GATE")
    );

    for (name, changed) in [
        (
            "durable workload module vanished",
            replace_once(
                &perf_source,
                "mod durable_workload {",
                "mod removed_durable_workload {",
            ),
        ),
        (
            "durable workload lost ignore",
            replace_once(
                &perf_source,
                "#[ignore = \"30-minute Docker release workload; default execution runs all sixteen matrix cells serially\"]\n",
                "",
            ),
        ),
        (
            "durable document count",
            replace_once(
                &perf_source,
                "const HOT_DOCUMENTS: usize = 100_000;",
                "const HOT_DOCUMENTS: usize = 99_999;",
            ),
        ),
        (
            "durable input duration",
            replace_once(
                &perf_source,
                "const INPUT_SECONDS: u64 = 30 * 60;",
                "const INPUT_SECONDS: u64 = 29 * 60;",
            ),
        ),
        (
            "durable document offer rate",
            replace_once(
                &perf_source,
                "const DOCOPS_PER_SECOND: usize = 60;",
                "const DOCOPS_PER_SECOND: usize = 59;",
            ),
        ),
        (
            "durable query offer rate",
            replace_once(
                &perf_source,
                "const QUERY_QPS: usize = 10;",
                "const QUERY_QPS: usize = 9;",
            ),
        ),
        (
            "durable CPU limit",
            replace_once(
                &perf_source,
                "const DOCKER_CPUS: &str = \"2.5\";",
                "const DOCKER_CPUS: &str = \"3.0\";",
            ),
        ),
        (
            "durable memory limit",
            replace_once(
                &perf_source,
                "const DOCKER_MEMORY: &str = \"17179869184\";",
                "const DOCKER_MEMORY: &str = \"17179869183\";",
            ),
        ),
        (
            "interval metric evidence",
            replace_once(
                &perf_source,
                "deltas.assert_complete_interval_evidence()?;",
                "// deltas.assert_complete_interval_evidence()?;",
            ),
        ),
        (
            "diagnostic and qualifying mode are no longer exclusive",
            replace_once(
                &perf_source,
                "if diagnostic && qualifying {",
                "if false {",
            ),
        ),
        (
            "selected qualifying mode becomes diagnostic",
            replace_once(
                &perf_source,
                "(Some(config), false, true) => Ok(Self::Qualifying(config)),",
                "(Some(config), false, true) => Ok(Self::Diagnostic(config)),",
            ),
        ),
        (
            "qualifying receipt write is hidden in a comment",
            replace_once(
                &perf_source,
                "perf_cell_receipt::write_new(&context.receipt_path, &receipt)",
                "// perf_cell_receipt::write_new(&context.receipt_path, &receipt)",
            ),
        ),
        (
            "paired scalar mismatch builder is hidden in a comment",
            replace_once(
                &perf_source,
                "let mismatch = semantic_wrong_field_query(field, value, fingerprint, document.number)?;",
                "// semantic_wrong_field_query(field, value, fingerprint, document.number)?;",
            ),
        ),
        (
            "paired scalar mismatch exclusion is hidden in a comment",
            replace_once(
                &perf_source,
                "if response_contains_id(&mismatch_response, &document.external_id) {",
                "// if response_contains_id(&mismatch_response, &document.external_id) {",
            ),
        ),
        (
            "long ngram readback is reduced to a prefix",
            replace_occurrence(
                &perf_source,
                "\"text\": value,",
                "\"text\": \"ngram document\",",
                0,
            ),
        ),
        (
            "ngram mismatch loses its known absent token and window",
            replace_once(
                &perf_source,
                "{value} {READBACK_MISMATCH_TOKEN} window {number} field {field}",
                "{value} window {number} field {field}",
            ),
        ),
        (
            "bounded vector candidate OR filter becomes AND",
            replace_once(&perf_source, "{ \"or\": [", "{ \"and\": ["),
        ),
        (
            "bounded vector k becomes two",
            replace_once(&perf_source, "\"k\": 1,", "\"k\": 2,"),
        ),
        (
            "bounded vector reference liveness proof is hidden in a comment",
            replace_once(
                &perf_source,
                "assert_vector_reference_is_untouched(reference)?;",
                "// assert_vector_reference_is_untouched(reference)?;",
            ),
        ),
        (
            "bounded vector noncollinearity proof is hidden in a comment",
            replace_once(
                &perf_source,
                "assert_vector_pair_is_distinct_and_non_collinear(target, reference)?;",
                "// assert_vector_pair_is_distinct_and_non_collinear(target, reference)?;",
            ),
        ),
        (
            "fingerprint-only readback oracle is renamed",
            replace_once(
                &perf_source,
                "fn fingerprint_only_readback_cannot_pass_an_ignored_field_predicate",
                "fn renamed_fingerprint_only_readback_oracle",
            ),
        ),
        (
            "bounded-vector ordering oracle is renamed",
            replace_once(
                &perf_source,
                "fn vector_readback_rejects_the_target_for_both_candidate_vectors",
                "fn renamed_vector_readback_oracle",
            ),
        ),
        (
            "default matrix hidden in a comment",
            replace_once(
                &perf_source,
                "for config in qualifying_matrix() {",
                "// for config in qualifying_matrix() {",
            ),
        ),
        (
            "default matrix hidden in quoted prose",
            replace_once(
                &perf_source,
                "for config in qualifying_matrix() {",
                "let _ = \"for config in qualifying_matrix() {\";",
            ),
        ),
        (
            "default matrix hidden in a multiline raw string",
            replace_once(
                &perf_source,
                "for config in qualifying_matrix() {",
                "let _ = r#\"\nfor config in qualifying_matrix() {\n\"#;",
            ),
        ),
    ] {
        assert_eq!(
            validate_perf_gate_source(&changed).unwrap_err(),
            Finding("PERF_GATE"),
            "durable workload mutation {name} passed",
        );
    }
    let ledger_source = perf_workload_ledger_source();
    for (name, changed) in [
        (
            "ledger input duration",
            replace_once(
                &ledger_source,
                "input_duration: Duration::from_secs(30 * 60),",
                "input_duration: Duration::from_secs(29 * 60),",
            ),
        ),
        (
            "ledger document offer rate",
            replace_once(
                &ledger_source,
                "docops_per_second: 60,",
                "docops_per_second: 59,",
            ),
        ),
        (
            "ledger query offer rate",
            replace_once(&ledger_source, "query_qps: 10,", "query_qps: 9,"),
        ),
        (
            "ledger checkpoint evidence",
            replace_once(
                &ledger_source,
                "if report.checkpoints == 0 {",
                "// if report.checkpoints == 0 {",
            ),
        ),
        (
            "ledger merge evidence",
            replace_once(
                &ledger_source,
                "if report.merges == 0 {",
                "// if report.merges == 0 {",
            ),
        ),
        (
            "ledger input request drain evidence",
            replace_once(
                &ledger_source,
                "if self.input_requests_finished != self.input_requests_submitted {",
                "// if self.input_requests_finished != self.input_requests_submitted {",
            ),
        ),
        (
            "ledger counts scheduled work instead of actual HTTP body starts",
            replace_once(
                &ledger_source,
                "let body_started_in_input = body_started_bucket.is_some();",
                "let body_started_in_input = scheduled_in_input;",
            ),
        ),
        (
            "ledger counts a partial Index document as complete",
            replace_once(
                &ledger_source,
                "&& operation.successful.len() == operation.required.len();",
                "&& true;",
            ),
        ),
        (
            "ledger counts a late completion as in-window evidence",
            replace_once(
                &ledger_source,
                "if started_in_input && self.input_bucket(finished_at).is_some() {",
                "if started_in_input {",
            ),
        ),
        (
            "ledger permits duplicate logical targets in one request",
            replace_once(
                &ledger_source,
                "if !seen_document_targets.insert(document_target.clone()) {",
                "if false {",
            ),
        ),
    ] {
        assert_eq!(
            validate_perf_workload_ledger_source(&changed).unwrap_err(),
            Finding("PERF_GATE"),
            "durable ledger mutation {name} passed",
        );
    }

    let scale_source = perf_gate_vs_db_source();
    let scale_script = lumen_scale_script();
    let benchmark_doc = benchmark_scale_doc();
    for (name, changed) in [
        (
            "500k standard cap",
            replace_once(
                &scale_source,
                "const DEFAULT_SCALE_MAX_ROWS: usize = 100_000;",
                "const DEFAULT_SCALE_MAX_ROWS: usize = 500_000;",
            ),
        ),
        (
            "cursor preflight",
            replace_once(
                &scale_source,
                "fn scale_precompute_mid_collection_cursor(",
                "fn removed_scale_precompute_mid_collection_cursor(",
            ),
        ),
        (
            "fixture/order preflight",
            replace_once(
                &scale_source,
                "fn scale_preflight_fixture(",
                "fn removed_scale_preflight_fixture(",
            ),
        ),
        (
            "range fixture-order preflight",
            replace_once(
                &scale_source,
                "range filter result set/order changed from deterministic fixture document order",
                "range filter result set checked without order",
            ),
        ),
        (
            "numeric-sort tie-break preflight",
            replace_once(
                &scale_source,
                "age ascending with deterministic fixture document-order tie-break",
                "age ascending without a deterministic tie-break",
            ),
        ),
        (
            "request error failure",
            replace_once(
                &scale_source,
                "read qps matrix had {request_errors} request errors",
                "read qps matrix status omitted",
            ),
        ),
        (
            "reclaimer drain wait",
            replace_once(
                &scale_source,
                "fn wait_for_scale_reclaimer_drain(",
                "fn removed_scale_reclaimer_drain_wait(",
            ),
        ),
    ] {
        assert_eq!(
            validate_scale_matrix_sources(&changed, &scale_script, &benchmark_doc).unwrap_err(),
            Finding("SCALE_MATRIX"),
            "scale source mutation {name} passed",
        );
    }
    let unlocked_scale_script = replace_once(
        &scale_script,
        "cargo test --release --locked -p lumen --test perf_gate_vs_db",
        "cargo test --release -p lumen --test perf_gate_vs_db",
    );
    assert_eq!(
        validate_scale_matrix_sources(&scale_source, &unlocked_scale_script, &benchmark_doc)
            .unwrap_err(),
        Finding("SCALE_MATRIX")
    );
    let wrong_dev_matrix = replace_once(
        &benchmark_doc,
        "LUMEN_SCALE_DISK=1 LUMEN_SCALE_QPS=1 LUMEN_SCALE_CELLS=range,filter_sort,keyword_sort,sorted_page_deep LUMEN_SCALE_QPS_TARGETS=10 scripts/lumen_scale.sh 1000",
        "LUMEN_SCALE_DISK=1 LUMEN_SCALE_QPS=1 LUMEN_SCALE_CELLS=range,filter_sort,keyword_sort,sorted_page_deep LUMEN_SCALE_QPS_TARGETS=100 scripts/lumen_scale.sh 1000",
    );
    assert_eq!(
        validate_scale_matrix_sources(&scale_source, &scale_script, &wrong_dev_matrix).unwrap_err(),
        Finding("SCALE_MATRIX")
    );
    let wrong_sort_tie_break = replace_once(
        &benchmark_doc,
        "age-then-fixture-document order for `filter_sort`",
        "age-then-external-ID order for `filter_sort`",
    );
    assert_eq!(
        validate_scale_matrix_sources(&scale_source, &scale_script, &wrong_sort_tie_break)
            .unwrap_err(),
        Finding("SCALE_MATRIX")
    );
    let missing_range_order = replace_once(
        &benchmark_doc,
        "fixture-document order for `range`",
        "result set for `range`",
    );
    assert_eq!(
        validate_scale_matrix_sources(&scale_source, &scale_script, &missing_range_order)
            .unwrap_err(),
        Finding("SCALE_MATRIX")
    );
    let changed = replace_occurrence(
        &source,
        "LUMEN_E2E_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.root_digest }}\"",
        "LUMEN_E2E_IMAGE=\"${{ needs.ghcr-image-and-attest.outputs.image_repo }}@${{ needs.ghcr-image-and-attest.outputs.amd64_digest }}\"",
        0,
    );
    assert_eq!(
        validate_workflow(&changed, &dockerfile()).unwrap_err(),
        Finding("GATE_COMMANDS")
    );
    // Each arch runs overlay then operator, so every digest appears twice.
    for (from, to) in [("amd64", "arm64"), ("arm64", "amd64")] {
        for occurrence in 0..2 {
            let changed = replace_occurrence(
                &source,
                &format!("LUMEN_E2E_EXPECTED_RUNTIME_DIGEST=\"${{{{ needs.ghcr-image-and-attest.outputs.{from}_digest }}}}\""),
                &format!("LUMEN_E2E_EXPECTED_RUNTIME_DIGEST=\"${{{{ needs.ghcr-image-and-attest.outputs.{to}_digest }}}}\""),
                occurrence,
            );
            assert_eq!(
                validate_workflow(&changed, &dockerfile()).unwrap_err(),
                Finding("GATE_COMMANDS"),
                "{from} runtime digest occurrence {occurrence} accepted {to}"
            );
        }
    }
    expect_workflow(
        &source,
        "schema:\"cclab.lumen.candidate-manifest.v3\"",
        "schema:\"cclab.lumen.candidate-manifest.v2\"",
        "MANIFEST",
    );
    expect_workflow(
        &source,
        "  verify-libraries:\n",
        "  verify-libraries-missing:\n",
        "JOBS",
    );
    expect_workflow(
        &source,
        "\n  kind-amd64:\n",
        "\n  extra-job:\n    name: extra\n    needs: [identity]\n    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n    steps: []\n\n  kind-amd64:\n",
        "JOBS",
    );
    expect_workflow(
        &source,
        "\"verify-libraries\":\"${{ needs.verify-libraries.result }}\"",
        "\"verify-libraries\":\"failure\"",
        "MANIFEST",
    );
    expect_workflow(
        &source,
        "  kind-amd64:\n    name: kind e2e (amd64)\n    needs: [identity, verify-libraries, ghcr-image-and-attest]",
        "  kind-amd64:\n    name: kind e2e (amd64)\n    needs: [identity, ghcr-image-and-attest]",
        "GRAPH",
    );
    expect_workflow(
        &source,
        "cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core",
        "true",
        "LIBRARIES",
    );
    expect_workflow(
        &source,
        "git -C \"$core\" fetch -q --depth 1 https://github.com/faberline/core \"$core_rev\"",
        "git -C \"$core\" fetch -q --depth 1 https://github.com/faberline/core main",
        "LIBRARIES",
    );
    expect_workflow(
        &source,
        "          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime --test adversarial_recovery
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime
          cargo build --locked -p lumen --features raft-wal",
        "          cargo build --locked -p lumen --features raft-wal
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime --test adversarial_recovery
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime",
        "LIBRARIES",
    );
    expect_workflow(
        &source,
        "          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime --test adversarial_recovery
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime
          cargo build --locked -p lumen --features raft-wal",
        "          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime --test adversarial_recovery
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-runtime
          cargo test --locked --manifest-path \"$core/Cargo.toml\" -p raft-core
          cargo build --locked -p lumen --features raft-wal",
        "LIBRARIES",
    );
    // Split gate steps carry an exact key set, so a step-level condition is
    // rejected by the gate partition before the condition scan runs.
    for (from, to) in [
        (
            "      - name: Run raft-wal cargo gates\n        env:",
            "      - name: Run raft-wal cargo gates\n        if: false\n        env:",
        ),
        (
            "      - name: Run standalone container smoke gates\n        shell: bash",
            "      - name: Run standalone container smoke gates\n        continue-on-error: true\n        shell: bash",
        ),
    ] {
        expect_workflow(&source, from, to, "GATES");
    }
    for (from, to) in [
        (
            "  perf-coarse:\n    name: coarse performance gates",
            "  perf-coarse:\n    name: coarse performance gates\n    if: always()",
        ),
        (
            "  cloud-free-config:\n    name: cloud-free config gates",
            "  cloud-free-config:\n    name: cloud-free config gates\n    continue-on-error: true",
        ),
        (
            "      - name: Verify full run-scoped candidate supply chain\n        env:",
            "      - name: Verify full run-scoped candidate supply chain\n        continue-on-error: true\n        env:",
        ),
        (
            "  kind-amd64:\n    name: kind e2e (amd64)",
            "  kind-amd64:\n    name: kind e2e (amd64)\n    if: always()",
        ),
        (
            "  kind-arm64:\n    name: kind e2e (arm64)",
            "  kind-arm64:\n    name: kind e2e (arm64)\n    continue-on-error: true",
        ),
        (
            "  result:\n    name: final candidate receipt",
            "  result:\n    name: final candidate receipt\n    if: always()",
        ),
        (
            "      - name: Bind all successful job conclusions into final receipt\n        shell: bash",
            "      - name: Bind all successful job conclusions into final receipt\n        if: false\n        shell: bash",
        ),
    ] {
        expect_workflow(&source, from, to, "CONDITIONS");
    }
    for mode in ["overlay", "operator"] {
        for occurrence in 0..2 {
            let changed = replace_occurrence(
                &source,
                &format!("      - name: Run prebuilt candidate kind e2e ({mode})\n        shell: bash"),
                &format!(
                    "      - name: Run prebuilt candidate kind e2e ({mode})\n        if: false\n        shell: bash"
                ),
                occurrence,
            );
            assert_eq!(
                validate_workflow(&changed, &dockerfile()).unwrap_err(),
                Finding("CONDITIONS"),
                "kind {mode} YAML condition occurrence {occurrence} passed",
            );
        }
    }
    for (job, gate, marker, occurrence) in [
        (
            "verify-candidate",
            "Verify full run-scoped candidate supply chain",
            "scripts/verify-release-candidate.sh \\",
            0,
        ),
        (
            "gates-default",
            "Run default-feature cargo gate",
            "Cargo.toml",
            0,
        ),
        (
            "gates-raft-wal",
            "Run raft-wal cargo gates",
            "Cargo.toml",
            0,
        ),
        (
            "container-smoke",
            "Run standalone container smoke gates",
            "scripts/standalone-container-smoke.sh",
            0,
        ),
        (
            "perf-coarse",
            "Run coarse performance gates",
            "Cargo.toml",
            0,
        ),
        (
            "kind-amd64",
            "Run prebuilt candidate kind e2e (overlay)",
            "scripts/kind-e2e.sh",
            0,
        ),
        (
            "kind-amd64",
            "Run prebuilt candidate kind e2e (operator)",
            "scripts/kind-e2e.sh",
            0,
        ),
        (
            "kind-arm64",
            "Run prebuilt candidate kind e2e (overlay)",
            "scripts/kind-e2e.sh",
            1,
        ),
        (
            "kind-arm64",
            "Run prebuilt candidate kind e2e (operator)",
            "scripts/kind-e2e.sh",
            1,
        ),
    ] {
        let insertion = format!(
            "      - name: Unrecognized overwrite step\n        shell: bash\n        run: |\n          printf '#!/usr/bin/env bash\\nexit 0\\n' > {marker}\n"
        );
        let anchor = format!("      - name: {gate}\n");
        let changed = replace_occurrence(
            &source,
            &anchor,
            &format!("{insertion}{anchor}"),
            occurrence,
        );
        assert_eq!(
            validate_workflow(&changed, &dockerfile()).unwrap_err(),
            Finding("GATE_STEPS"),
            "unrecognized step before {job} gate passed",
        );
    }
    expect_workflow(
        &source,
        "  verify-libraries:\n    name: verify service and Raft library gates\n    needs: [identity]\n    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          ref: ${{ needs.identity.outputs.commit }}",
        "  verify-libraries:\n    name: verify service and Raft library gates\n    needs: [identity]\n    runs-on: ubuntu-latest\n    permissions:\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          ref: ${{ github.sha }}",
        "LIBRARIES",
    );
    for occurrence in 0..4 {
        let changed = replace_occurrence(
            &source,
            "          scripts/kind-e2e.sh",
            "          if false; then scripts/kind-e2e.sh; fi",
            occurrence,
        );
        assert_eq!(
            validate_workflow(&changed, &dockerfile()).unwrap_err(),
            Finding("GATE_COMMANDS"),
            "kind dead-branch occurrence {occurrence} passed",
        );
    }
    let supply_chain_dead = replace_occurrence(
        &source,
        "          scripts/verify-release-candidate.sh \\",
        "          if false; then\n          scripts/verify-release-candidate.sh \\",
        0,
    );
    let supply_chain_dead = replace_once(
        &supply_chain_dead,
        "            --mode full",
        "            --mode full\n          fi",
    );
    assert_eq!(
        validate_workflow(&supply_chain_dead, &dockerfile()).unwrap_err(),
        Finding("GATE_COMMANDS"),
        "candidate supply-chain dead branch passed",
    );
    let uv_setup = "      - uses: astral-sh/setup-uv@c771a70e6277c0a99b617c7a806ffedaca235ff9 # v9.0.0\n        with:\n          version: 0.12.1\n          enable-cache: false\n";
    let without_uv = replace_occurrence(&source, uv_setup, "", 0);
    let next_job = "\n\n  gates-operator:\n";
    let moved_uv = format!("\n{uv_setup}{}", &next_job[1..]);
    let setup_after_gate = replace_once(&without_uv, next_job, &moved_uv);
    assert_ne!(setup_after_gate, source);
    assert_eq!(
        validate_workflow(&setup_after_gate, &dockerfile()).unwrap_err(),
        Finding("UV_SETUP")
    );
    let (script, _) = verifier();
    let bypassed_supply_chain = replace_once(
        &script,
        "\n  verify_full_supply_chain\n",
        "\n  true # verify_full_supply_chain\n",
    );
    assert_eq!(
        validate_verifier(&bypassed_supply_chain, 0o755).unwrap_err(),
        Finding("VERIFIER_BYTES")
    );
    for (from, to, code) in [
        (
            "LOCAL FIXTURE ONLY: artifacts verified; this is not candidate acceptance.",
            "CANDIDATE ACCEPTED",
            "VERIFIER",
        ),
        (
            ".source_ref == \"refs/heads/main\"",
            ".source_ref == \"refs/heads/dev\"",
            "VERIFIER",
        ),
        (
            "candidate tag is not scoped to this run attempt",
            "candidate tag accepted",
            "VERIFIER",
        ),
        ("cosign verify", "cosign sign", "VERIFIER_SIDE_EFFECT"),
    ] {
        expect_verifier(&script, from, to, code);
    }
    let docker = dockerfile();
    let changed = docker.replacen("FROM debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171", "FROM debian:bookworm-slim@sha256:0000000000000000000000000000000000000000000000000000000000000000", 1);
    assert_ne!(changed, docker);
    assert_eq!(validate_dockerfile(&changed), Err(Finding("BASE_IMAGE")));
    assert!(validate_dockerfile(&docker).is_ok());
}

fn sha(path: &Path) -> String {
    let output = Command::new("shasum")
        .args(["-a", "256"])
        .arg(path)
        .output()
        .unwrap();
    String::from_utf8(output.stdout)
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .into()
}
fn write_manifest(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    let digest = sha(path);
    fs::write(
        path.with_extension("json.sha256"),
        format!(
            "{digest}  {}\n",
            path.file_name().unwrap().to_string_lossy()
        ),
    )
    .unwrap();
}
fn local_fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let artifacts = dir.path();
    let stage = artifacts.join("stage");
    for target in [
        "aarch64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-musl",
    ] {
        let package = stage.join(format!("lumen-{target}"));
        fs::create_dir_all(&package).unwrap();
        let binary = package.join("lumen");
        fs::write(&binary, "#!/bin/sh\nprintf 'lumen 0.4.27\\n'\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(package.join("README.md"), "fixture\n").unwrap();
        let archive = artifacts.join(format!("lumen-{target}.tar.gz"));
        assert!(Command::new("tar")
            .args([
                "-C",
                stage.to_str().unwrap(),
                "-czf",
                archive.to_str().unwrap(),
                &format!("lumen-{target}")
            ])
            .status()
            .unwrap()
            .success());
        let archive_sha = sha(&archive);
        fs::write(
            artifacts.join(format!("lumen-{target}.tar.gz.sha256")),
            format!("{archive_sha}  lumen-{target}.tar.gz\n"),
        )
        .unwrap();
    }
    for arch in ["amd64", "arm64"] {
        fs::write(
            artifacts.join(format!("spdx-{arch}.json")),
            r#"{"spdxVersion":"SPDX-2.3"}"#,
        )
        .unwrap();
    }
    let targets = [
        "aarch64-apple-darwin",
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-unknown-linux-musl",
        "aarch64-unknown-linux-musl",
    ];
    let artifacts_json: Vec<_> = targets.iter().map(|target| json!({"target":target,"archive":format!("lumen-{target}.tar.gz"),"archive_sha256":sha(&artifacts.join(format!("lumen-{target}.tar.gz"))),"sidecar":format!("lumen-{target}.tar.gz.sha256"),"sidecar_sha256":sha(&artifacts.join(format!("lumen-{target}.tar.gz.sha256")))})).collect();
    let manifest = json!({"schema":"cclab.lumen.candidate-manifest.v3","repository":"faberline/lumen","workflow_path":".github/workflows/lumen-release-candidate.yml","workflow_id":42,"run_id":"7","run_attempt":"2","run_url":"https://github.com/faberline/lumen/actions/runs/7/attempts/2","source_ref":"refs/heads/main","workflow_ref":"faberline/lumen/.github/workflows/lumen-release-candidate.yml@refs/heads/main","commit":"0123456789012345678901234567890123456789","version":"0.4.27","tag":"lumen@0.4.27","candidate_tag":"release-candidate-7-2","pr":{"number":42,"url":"https://github.com/faberline/lumen/pull/42"},"image":{"repository":"ghcr.io/faberline/lumen","root_digest":format!("sha256:{}", "1".repeat(64)),"amd64_digest":format!("sha256:{}", "2".repeat(64)),"arm64_digest":format!("sha256:{}", "3".repeat(64))},"artifacts":artifacts_json,"sboms":{"amd64":{"file":"spdx-amd64.json","sha256":sha(&artifacts.join("spdx-amd64.json"))},"arm64":{"file":"spdx-arm64.json","sha256":sha(&artifacts.join("spdx-arm64.json"))}},"jobs":{"identity":"success","build":"success","manifest":"success","ghcr-image-and-attest":"success","gates-default":"success","gates-operator":"success","gates-raft-wal":"success","gates-release-features":"success","container-smoke":"success","cloud-free-config":"success","perf-coarse":"success","verify-candidate":"success","verify-libraries":"success","kind-amd64":"success","kind-arm64":"success","result":"success"}});
    write_manifest(&artifacts.join("final-candidate-manifest.json"), &manifest);
    dir
}

fn replace_host_archive(dir: &Path, stage_name: &str, readme: bool, mode: u32, version: &str) {
    let target = match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("linux", "x86_64") => "x86_64-unknown-linux-gnu",
        ("linux", "aarch64") => "aarch64-unknown-linux-gnu",
        _ => panic!("unsupported verifier host"),
    };
    let stage_root = dir.join(stage_name);
    let package = stage_root.join(format!("lumen-{target}"));
    fs::create_dir_all(&package).unwrap();
    let binary = package.join("lumen");
    fs::write(&binary, format!("#!/bin/sh\nprintf 'lumen {version}\\n'\n")).unwrap();
    fs::set_permissions(&binary, fs::Permissions::from_mode(mode)).unwrap();
    if readme {
        fs::write(package.join("README.md"), "fixture\n").unwrap();
    }
    let archive = dir.join(format!("lumen-{target}.tar.gz"));
    assert!(Command::new("tar")
        .args([
            "-C",
            stage_root.to_str().unwrap(),
            "-czf",
            archive.to_str().unwrap(),
            &format!("lumen-{target}")
        ])
        .status()
        .unwrap()
        .success());
    let archive_sidecar = dir.join(format!("lumen-{target}.tar.gz.sha256"));
    let archive_digest = sha(&archive);
    fs::write(
        &archive_sidecar,
        format!("{archive_digest}  lumen-{target}.tar.gz\n"),
    )
    .unwrap();
    let path = dir.join("final-candidate-manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let index = manifest["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .position(|item| item["target"] == target)
        .unwrap();
    manifest["artifacts"][index]["archive_sha256"] = json!(archive_digest);
    manifest["artifacts"][index]["sidecar_sha256"] = json!(sha(&archive_sidecar));
    write_manifest(&path, &manifest);
}

fn run_local_version(dir: &Path, version: &str) -> Output {
    Command::new("bash")
        .arg(root().join("scripts/verify-release-candidate.sh"))
        .args([
            "--repo",
            "faberline/lumen",
            "--version",
            version,
            "--commit",
            "0123456789012345678901234567890123456789",
            "--run-id",
            "7",
            "--run-attempt",
            "2",
            "--manifest",
        ])
        .arg(dir.join("final-candidate-manifest.json"))
        .args(["--manifest-sidecar"])
        .arg(dir.join("final-candidate-manifest.json.sha256"))
        .args(["--artifacts-dir"])
        .arg(dir)
        .args(["--mode", "local"])
        .output()
        .unwrap()
}

fn run_local(dir: &Path) -> Output {
    run_local_version(dir, "0.4.27")
}

fn set_fixture_version(dir: &Path, version: &str) {
    replace_host_archive(dir, "version-stage", true, 0o755, version);
    let path = dir.join("final-candidate-manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest["version"] = json!(version);
    manifest["tag"] = json!(format!("lumen@{version}"));
    write_manifest(&path, &manifest);
}

fn durable_perf_fixture_binding() -> perf_cell_receipt::Binding {
    perf_cell_receipt::Binding {
        repository: "faberline/lumen".to_owned(),
        run_id: "7".to_owned(),
        run_attempt: "2".to_owned(),
        commit: "0123456789012345678901234567890123456789".to_owned(),
        image_reference: format!("ghcr.io/faberline/lumen@sha256:{}", "1".repeat(64)),
        actual_image_id: format!("sha256:{}", "4".repeat(64)),
    }
}

pub(crate) fn durable_perf_fixture_measurement() -> perf_cell_receipt::Measurement {
    perf_cell_receipt::Measurement {
        input_duration_ms: 1_800_000,
        observed_input_elapsed_ms: 1_800_000,
        requests_offered: 18_000,
        requests_finished: 18_000,
        requests_completed: 18_000,
        requests_started_in_input: 18_000,
        requests_finished_in_input: 18_000,
        requests_completed_in_input: 18_000,
        requests_started_per_second_milli: 10_000,
        requests_completed_per_second_milli: 10_000,
        request_errors: 0,
        client_cancellations: 0,
        capacity_refusals: 0,
        capacity_refused_requests: 0,
        metric_sample_seconds: 1_800,
        metric_sample_missed_seconds: 0,
        items_offered: 2_520_000,
        items_completed: 2_520_000,
        items_failed: 0,
        items_started_in_input: 2_520_000,
        items_completed_in_input: 2_520_000,
        items_started_per_second_milli: 1_400_000,
        items_completed_per_second_milli: 1_400_000,
        docops_offered: 180_000,
        docops_completed: 180_000,
        docops_offered_in_input: 180_000,
        docops_completed_in_input: 180_000,
        index_requests_started_in_input: 6_000,
        replace_requests_started_in_input: 6_000,
        unindex_requests_started_in_input: 6_000,
        docops_offered_per_second_milli: 100_000,
        docops_completed_per_second_milli: 100_000,
        docops_completion_percent_milli: 100_000,
        request_latency_p99_ms: 800,
        request_latency_max_ms: 1_200,
        queries_offered: 18_000,
        queries_completed: 18_000,
        queries_started_in_input: 18_000,
        queries_completed_in_input: 18_000,
        hot_queries_started_in_input: 14_400,
        idle_queries_started_in_input: 3_600,
        queries_started_per_second_milli: 10_000,
        queries_completed_per_second_milli: 10_000,
        query_errors_or_timeouts: 0,
        query_latency_p99_ms: 900,
        query_latency_max_ms: 1_100,
        request_drain_ms: 45,
        query_drain_ms: 50,
        checkpoint_delta: 2,
        merge_delta: 1,
        checkpoint_bytes: 9,
        merge_read_bytes: 10,
        merge_write_bytes: 11,
        capture_hold_ns_total: 12,
        pending_delta_bytes: 13,
        pending_delta_layers: 14,
        backpressure_events: 0,
        segment_disk_bytes: 15,
        peak_rss_bytes: perf_cell_receipt::RSS_LIMIT_BYTES - 1,
        restart_duration_ms: 16,
        restart_recovered: true,
        live_mutation_readback: true,
        cold_mutation_readback: true,
    }
}

fn run_durable_perf_verifier(dir: &Path) -> Output {
    Command::new("python3")
        .arg(root().join("scripts/verify-durable-perf.py"))
        .args([
            "--receipts-dir",
            dir.join("perf").to_str().unwrap(),
            "--repo",
            "faberline/lumen",
            "--run-id",
            "7",
            "--run-attempt",
            "2",
            "--commit",
            "0123456789012345678901234567890123456789",
            "--image",
            &durable_perf_fixture_binding().image_reference,
            "--output",
            dir.join("durable-perf-summary.json").to_str().unwrap(),
        ])
        .output()
        .expect("run local durable receipt verifier")
}

/// The exact check promotion runs on the local durable evidence directory.
fn run_durable_perf_verify_existing(dir: &Path, summary_sha256: &str) -> Output {
    Command::new("python3")
        .arg("-I")
        .arg(root().join("scripts/verify-durable-perf.py"))
        .args([
            "--receipts-dir",
            dir.join("perf").to_str().unwrap(),
            "--repo",
            "faberline/lumen",
            "--run-id",
            "7",
            "--run-attempt",
            "2",
            "--commit",
            "0123456789012345678901234567890123456789",
            "--image",
            &durable_perf_fixture_binding().image_reference,
            "--output",
            dir.join("durable-perf-summary.json").to_str().unwrap(),
            "--verify-existing",
            "--summary-sha256",
            summary_sha256,
        ])
        .output()
        .expect("run local durable receipt verifier")
}

fn durable_perf_summary_sha256(dir: &Path) -> String {
    sha(&dir.join("durable-perf-summary.json"))
}

fn set_durable_perf_restart_duration(dir: &Path, duration_ms: u64) {
    let receipt_path = dir.join("perf/index-1-flat-cpu.json");
    let mut receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    receipt["measurement"]["restart_duration_ms"] = json!(duration_ms);
    fs::write(&receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
}

fn remove_durable_perf_aggregate(dir: &Path) {
    fs::remove_file(dir.join("durable-perf-summary.json")).unwrap();
    fs::remove_file(dir.join("durable-perf-summary.json.sha256")).unwrap();
}

fn forge_durable_perf_aggregate(dir: &Path) {
    let summary_path = dir.join("durable-perf-summary.json");
    let mut summary: Value = serde_json::from_slice(&fs::read(&summary_path).unwrap()).unwrap();
    let record = summary["receipts"]
        .as_array_mut()
        .expect("aggregate receipt records")
        .iter_mut()
        .find(|record| record["file"] == "index-1-flat-cpu.json")
        .expect("aggregate record for the forged receipt");
    record["sha256"] = json!(sha(&dir.join("perf/index-1-flat-cpu.json")));
    fs::write(&summary_path, serde_json::to_vec(&summary).unwrap()).unwrap();
    fs::write(
        dir.join("durable-perf-summary.json.sha256"),
        format!("{}\n", sha(&summary_path)),
    )
    .unwrap();
}

fn add_valid_durable_perf_evidence(dir: &Path) {
    let receipts = dir.join("perf");
    fs::create_dir_all(&receipts).unwrap();
    for cell in perf_cell_receipt::expected_cells() {
        let receipt = perf_cell_receipt::Receipt {
            schema_version: perf_cell_receipt::SCHEMA_VERSION,
            kind: perf_cell_receipt::RECEIPT_KIND.to_owned(),
            binding: durable_perf_fixture_binding(),
            cell: cell.clone(),
            limits: perf_cell_receipt::Limits::approved(),
            measurement: durable_perf_fixture_measurement(),
            outcome: perf_cell_receipt::Outcome {
                qualifying: true,
                diagnostic: false,
                succeeded: true,
            },
        };
        perf_cell_receipt::write_new(&receipts.join(format!("{}.json", cell.id)), &receipt)
            .expect("write a unique valid receipt");
    }
    let output = run_durable_perf_verifier(dir);
    assert!(
        output.status.success(),
        "build a valid local durable summary: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn final_v061_with_durable_perf_fixture() -> tempfile::TempDir {
    let fixture = local_fixture();
    set_fixture_version(fixture.path(), "0.6.1");
    add_valid_durable_perf_evidence(fixture.path());
    fixture
}

fn durable_perf_evidence_is_valid(dir: &Path) -> bool {
    run_durable_perf_verify_existing(dir, &durable_perf_summary_sha256(dir))
        .status
        .success()
}

#[test]
fn final_v061_candidate_receipt_carries_no_durable_performance_block() {
    let fixture = local_fixture();
    set_fixture_version(fixture.path(), "0.6.1");
    let output = run_local_version(fixture.path(), "0.6.1");
    assert!(
        output.status.success(),
        "a final 0.6.1 candidate receipt must verify without CI performance evidence: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let path = fixture.path().join("final-candidate-manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest["performance"] = json!({
        "schema_version": 1,
        "summary_file": "durable-perf-summary.json",
        "summary_sha256": "0".repeat(64),
        "receipts_directory": "perf",
    });
    write_manifest(&path, &manifest);
    let output = run_local_version(fixture.path(), "0.6.1");
    assert!(
        !output.status.success(),
        "a candidate receipt claiming CI performance evidence passed"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("candidate manifest keys changed"),
        "performance block refusal: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn preflight_v061_receipt_without_jobs_or_performance_remains_valid() {
    let fixture = local_fixture();
    set_fixture_version(fixture.path(), "0.6.1");
    let path = fixture.path().join("final-candidate-manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest
        .as_object_mut()
        .expect("fixture manifest object")
        .remove("jobs");
    manifest
        .as_object_mut()
        .expect("fixture manifest object")
        .remove("performance");
    write_manifest(&path, &manifest);

    let output = run_local_version(fixture.path(), "0.6.1");
    assert!(
        output.status.success(),
        "a preflight 0.6.1 manifest must not require final-only performance evidence: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn durable_perf_restart_duration_accepts_exactly_thirty_seconds() {
    let fixture = final_v061_with_durable_perf_fixture();
    set_durable_perf_restart_duration(fixture.path(), 30_000);
    remove_durable_perf_aggregate(fixture.path());
    let output = run_durable_perf_verifier(fixture.path());
    assert!(
        output.status.success(),
        "restart_duration_ms=30000 must remain a qualifying receipt: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        durable_perf_evidence_is_valid(fixture.path()),
        "a locally verified receipt with restart_duration_ms=30000 must remain valid"
    );
}

#[test]
fn durable_perf_restart_duration_above_thirty_seconds_is_rejected_by_python_verifier() {
    let fixture = final_v061_with_durable_perf_fixture();
    set_durable_perf_restart_duration(fixture.path(), 30_001);
    remove_durable_perf_aggregate(fixture.path());
    let output = run_durable_perf_verifier(fixture.path());
    assert!(
        !output.status.success(),
        "Python verifier accepted restart_duration_ms=30001: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn final_v061_receipt_refuses_forged_aggregate_with_one_overdue_restart() {
    let fixture = final_v061_with_durable_perf_fixture();
    set_durable_perf_restart_duration(fixture.path(), 30_001);
    forge_durable_perf_aggregate(fixture.path());
    let output = run_durable_perf_verify_existing(
        fixture.path(),
        &durable_perf_summary_sha256(fixture.path()),
    );
    assert!(
        !output.status.success(),
        "promotion verifier accepted a forged sixteen-cell aggregate with one restart_duration_ms=30001: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn local_v061_durable_evidence_binds_all_raw_receipts() {
    let fixture = final_v061_with_durable_perf_fixture();
    assert!(
        durable_perf_evidence_is_valid(fixture.path()),
        "a complete sixteen-cell local evidence directory must verify"
    );

    fn negative(name: &str, mutate: fn(&Path), needle: &str) {
        let fixture = final_v061_with_durable_perf_fixture();
        let summary_sha256 = durable_perf_summary_sha256(fixture.path());
        mutate(fixture.path());
        let summary_sha256 = if name == "promotion summary hash" {
            "0".repeat(64)
        } else {
            summary_sha256
        };
        let output = run_durable_perf_verify_existing(fixture.path(), &summary_sha256);
        assert!(!output.status.success(), "{name} mutation passed");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(needle),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    negative(
        "missing receipt",
        |dir| fs::remove_file(dir.join("perf/index-1-flat-cpu.json")).unwrap(),
        "expected exactly 16 receipt files, got 15",
    );
    negative(
        "corrupt summary",
        |dir| fs::write(dir.join("durable-perf-summary.json"), "{}\n").unwrap(),
        "existing aggregate SHA-256 does not match --summary-sha256",
    );
    negative(
        "corrupt summary sidecar",
        |dir| {
            fs::write(
                dir.join("durable-perf-summary.json.sha256"),
                format!("{}\n", "0".repeat(64)),
            )
            .unwrap()
        },
        "existing aggregate sidecar does not bind its exact bytes",
    );
    negative(
        "promotion summary hash",
        |_| {},
        "existing aggregate SHA-256 does not match --summary-sha256",
    );
    negative(
        "foreign receipt binding",
        |dir| {
            let path = dir.join("perf/index-1-flat-cpu.json");
            let mut receipt: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            receipt["binding"]["run_id"] = json!("foreign-run");
            fs::write(&path, serde_json::to_vec(&receipt).unwrap()).unwrap();
        },
        "receipt binding does not match command-line binding",
    );
}

#[test]
fn local_final_receipt_and_negative_fixtures_are_executable() {
    assert!(run_local(local_fixture().path()).status.success());
    fn negative(name: &str, mutate: fn(&Path), needle: &str) {
        let dir = local_fixture();
        mutate(dir.path());
        let output = run_local(dir.path());
        assert!(!output.status.success(), "{name} mutation passed");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(needle),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    negative(
        "checksum",
        |dir| {
            let path = dir.join("final-candidate-manifest.json");
            let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            value["artifacts"][0]["archive_sha256"] = json!("0".repeat(64));
            write_manifest(&path, &value);
        },
        "archive checksum mismatch",
    );
    negative(
        "manifest sidecar",
        |dir| {
            fs::write(
                dir.join("final-candidate-manifest.json.sha256"),
                "0  final-candidate-manifest.json\n",
            )
            .unwrap();
        },
        "manifest sidecar does not bind",
    );
    negative(
        "SBOM",
        |dir| {
            fs::write(dir.join("spdx-amd64.json"), "{}\n").unwrap();
        },
        "SBOM checksum mismatch",
    );
    negative(
        "job result",
        |dir| {
            let path = dir.join("final-candidate-manifest.json");
            let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            value["jobs"]["kind-amd64"] = json!("failure");
            write_manifest(&path, &value);
        },
        "final candidate manifest does not bind",
    );
    negative(
        "run/ref",
        |dir| {
            let path = dir.join("final-candidate-manifest.json");
            let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            value["source_ref"] = json!("refs/heads/dev");
            write_manifest(&path, &value);
        },
        "candidate manifest bindings changed",
    );
    negative(
        "archive member",
        |dir| replace_host_archive(dir, "bad-stage", false, 0o755, "0.4.27"),
        "archive members changed",
    );
    negative(
        "executable mode",
        |dir| replace_host_archive(dir, "mode-stage", true, 0o644, "0.4.27"),
        "archive binary is not executable",
    );
    negative(
        "wrong version",
        |dir| replace_host_archive(dir, "version-stage", true, 0o755, "0.4.26"),
        "candidate binary version mismatch",
    );
}
