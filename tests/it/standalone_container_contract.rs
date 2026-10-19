//! Deterministic contract test for Lumen standalone container and binary defaults.

use std::{fs, path::PathBuf};

const EXPECTED_DOCKERFILES: &[&str] = &["Dockerfile", "Dockerfile.release", "Dockerfile.test"];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn discover_dockerfiles() -> Vec<String> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(repo_root()) {
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("Dockerfile") {
                files.push(name);
            }
        }
    }
    files.sort();
    files
}

fn replace_exact(source: &str, target: &str, replacement: &str) -> String {
    let count = source.matches(target).count();
    assert_eq!(
        count, 1,
        "expected target {target:?} to occur exactly once, found {count}"
    );
    source.replacen(target, replacement, 1)
}

fn insert_after_first_from(source: &str, instruction: &str) -> String {
    let mut output = String::with_capacity(source.len() + instruction.len() + 1);
    let mut inserted = false;
    for line in source.split_inclusive('\n') {
        output.push_str(line);
        if !inserted && line.trim_start().to_ascii_uppercase().starts_with("FROM ") {
            output.push_str(instruction);
            output.push('\n');
            inserted = true;
        }
    }
    assert!(inserted, "fixture has no FROM instruction");
    output
}

const DURABLE_SUITE: &str = "tests/it/e2e/standalone_container.rs";
/// Pins the bytes of the suite file. A comment-only edit keeps the durable
/// semantics below but still has to update this pin, so every change to the
/// suite is reviewed against this contract.
const DURABLE_SUITE_SHA256: &str =
    "2c1345bae15e03884799d6670535452b75e7d3b897c99668eaacf963688beae7";
const DURABLE_HEADER: &str =
    "\n\n#[tokio::test]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn durable() {\n";
const BIND_HEADER: &str =
    "\n\n#[tokio::test]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn bind() {\n";

/// Every active line of `durable()`, trimmed, in order. Blank lines and
/// full-line `//` comments are not steps; anything else that is added,
/// removed, reordered, or changed fails the contract. The steps are: an
/// exact GHCR root digest; the 0428 image writes a legacy `gen-N` layout on a
/// named volume at /var/lib/lumen/data; the candidate adopts it
/// (`adopted_legacy_0428`, CURRENT `generation:gen-<n>`), keeps the first
/// record, takes a second, and checkpoints to a CURRENT revision; a
/// replacement restores that revision (`restored_current_generation`) with
/// both records; a volume with an unknown root fails startup, never serves
/// /readyz, logs the refusal, and keeps its sentinel without gaining CURRENT.
const DURABLE_STEPS: &[&str] = &[
    "let image = required(\"E2E_STANDALONE_IMAGE\");",
    "assert!(",
    "image",
    ".strip_prefix(REPO_DIGEST_PREFIX)",
    ".is_some_and(|hex| super::support::is_lower_hex(hex, 64)),",
    "\"E2E_STANDALONE_IMAGE must be an exact GHCR root digest ({REPO_DIGEST_PREFIX}<64 hex>) for durable, got {image}\"",
    ");",
    "let id = suffix();",
    "let volume = format!(\"lumen-smoke-durable-{id}\");",
    "let reject_volume = format!(\"lumen-smoke-durable-reject-{id}\");",
    "let old = format!(\"lumen-smoke-durable-old-{id}\");",
    "let candidate = format!(\"lumen-smoke-durable-candidate-{id}\");",
    "let replacement = format!(\"lumen-smoke-durable-replacement-{id}\");",
    "let seed = format!(\"lumen-smoke-durable-seed-{id}\");",
    "let rejected = format!(\"lumen-smoke-durable-rejected-{id}\");",
    "let mut scratch = Scratch::new();",
    "let tmp = scratch.dir(&format!(\"durable-{id}\"));",
    "scratch.volume(&volume);",
    "scratch.volume(&reject_volume);",
    "let mount = format!(\"type=volume,src={volume},dst={DATA}\");",
    "let reject_mount = format!(\"type=volume,src={reject_volume},dst={DATA}\");",
    "say(format!(\">> [{SUITE}] durable: the 0428 image writes a legacy layout\"));",
    "let api = serve(",
    "&mut scratch,",
    "&old,",
    "&[",
    "\"--mount\", &mount,",
    "\"-e\", \"LUMEN_AUTH=off\",",
    "\"-e\", &format!(\"LUMEN_DATA_DIR={DATA}\"),",
    "\"-e\", \"LUMEN_PERSISTENCE=segment\",",
    "\"-e\", \"LUMEN_SNAPSHOT_SECS=1\",",
    "\"-e\", \"LUMEN_GRACE_SECS=1\",",
    "],",
    "OLD_IMAGE,",
    ")",
    ".await;",
    "api.put(\"/collections/durable\", &json!({\"fields\": {\"tag\": {\"type\": \"keyword\"}}}), TIMEOUT).await;",
    "index_tag(&api, \"first\").await;",
    "assert_search(&api, \"first\").await;",
    "let old_data = tmp.join(\"old-data\");",
    "let old_data_arg = old_data.to_str().expect(\"utf-8 path\");",
    "for _ in 0..60 {",
    "let _ = docker(&[\"cp\", &format!(\"{old}:{DATA}\"), old_data_arg]);",
    "if has_legacy_generation(&old_data) {",
    "break;",
    "}",
    "let _ = std::fs::remove_dir_all(&old_data);",
    "tokio::time::sleep(Duration::from_millis(500)).await;",
    "}",
    "assert!(",
    "old_data.is_dir() && !old_data.join(\"CURRENT\").exists(),",
    "\"the 0428 image left no legacy checkpoint\\n{}\",",
    "logs(&old)",
    ");",
    "scratch.remove(&old);",
    "say(format!(\">> [{SUITE}] durable: the candidate adopts the legacy layout\"));",
    "let json_logs = [\"--mount\", mount.as_str(), \"-e\", \"LUMEN_AUTH=off\", \"-e\", \"LUMEN_LOG_FORMAT=json\"];",
    "let api = serve(&mut scratch, &candidate, &json_logs, &image).await;",
    "assert_startup_decision(&candidate, \"adopted_legacy_0428\");",
    "let current = read_current(&scratch, &candidate, \"current-first\");",
    "assert!(is_current(&current, false), \"adopted CURRENT is not generation:gen-<n>: {:?}\", String::from_utf8_lossy(&current));",
    "assert_search(&api, \"first\").await;",
    "index_tag(&api, \"second\").await;",
    "assert_search(&api, \"second\").await;",
    "let checkpoint = api.post(\"/admin/checkpoint\", &json!({}), TIMEOUT).await;",
    "assert!(checkpoint[\"persisted\"] == true, \"checkpoint did not persist: {checkpoint}\");",
    "let current = read_current(&scratch, &candidate, \"current-rev\");",
    "assert!(is_current(&current, true), \"checkpointed CURRENT is not a revision: {:?}\", String::from_utf8_lossy(&current));",
    "must(&[\"kill\", &candidate]);",
    "scratch.remove(&candidate);",
    "say(format!(\">> [{SUITE}] durable: a replacement restores the CURRENT revision\"));",
    "let api = serve(&mut scratch, &replacement, &json_logs, &image).await;",
    "assert_startup_decision(&replacement, \"restored_current_generation\");",
    "let current = read_current(&scratch, &replacement, \"current-replacement\");",
    "assert!(is_current(&current, true), \"restored CURRENT is not a revision: {:?}\", String::from_utf8_lossy(&current));",
    "assert_search(&api, \"first\").await;",
    "assert_search(&api, \"second\").await;",
    "say(format!(\">> [{SUITE}] durable: an unknown root is refused and left as it was\"));",
    "let foreign = tmp.join(\"foreign-layout\");",
    "std::fs::create_dir(&foreign).expect(\"create foreign-layout\");",
    "std::fs::write(foreign.join(\"sentinel\"), b\"fail-closed unknown root\\n\").expect(\"write sentinel\");",
    "scratch.containers.push(seed.clone());",
    "must(&[\"create\", \"--name\", &seed, \"--mount\", &reject_mount, &image]);",
    "must(&[\"cp\", foreign.to_str().expect(\"utf-8 path\"), &format!(\"{seed}:{DATA}/foreign-layout\")]);",
    "scratch.remove(&seed);",
    "scratch.run(&rejected, &[\"--mount\", &reject_mount, \"-e\", \"LUMEN_AUTH=off\"], &image);",
    "wait_failed_exit(&rejected).await;",
    "let port = published_port(&rejected).unwrap_or_else(|_| panic!(\"{}\", port_diagnostics(&rejected)));",
    "if !port.is_empty() {",
    "assert!(is_port(&port), \"rejected container has an invalid published port {port}\\n{}\", port_diagnostics(&rejected));",
    "let url = format!(\"http://127.0.0.1:{port}/readyz\");",
    "assert!(!Api::new(&format!(\"http://127.0.0.1:{port}\")).answers(\"/readyz\").await, \"rejected container served {url}\");",
    "}",
    "let log = logs(&rejected);",
    "assert!(refused_root(&log), \"rejected container did not refuse the unknown root\\n{log}\");",
    "let rejected_data = tmp.join(\"rejected-data\");",
    "must(&[\"cp\", &format!(\"{rejected}:{DATA}\"), rejected_data.to_str().expect(\"utf-8 path\")]);",
    "assert!(!rejected_data.join(\"CURRENT\").exists(), \"the refused volume gained a CURRENT\");",
    "assert_eq!(",
    "std::fs::read(rejected_data.join(\"foreign-layout/sentinel\")).ok(),",
    "Some(b\"fail-closed unknown root\\n\".to_vec()),",
    "\"the refused volume's sentinel changed\"",
    ");",
    "evidence(SUITE, \"durable\", json!({\"image\": image, \"old_image\": OLD_IMAGE, \"result\": \"pass\"}));",
    "say(format!(\">> [{SUITE}] durable PASS\"));",
];

/// Lines the durable steps lean on outside `durable()`. Each must occur
/// exactly once in the suite.
const DURABLE_HELPER_LINES: &[(&str, &str)] = &[
    ("data path", "const DATA: &str = \"/var/lib/lumen/data\";"),
    (
        "candidate digest prefix",
        "const REPO_DIGEST_PREFIX: &str = \"ghcr.io/faberline/lumen@sha256:\";",
    ),
    (
        "0428 image digest",
        "\"ghcr.io/chrischeng-c4/lumen@sha256:59a85c96d807428c424ec8889ac830b14e02869da49c4b44ae12dcce3786d03d\";",
    ),
    ("docker exit status", "if !out.status.success() {"),
    ("docker failure panics", "docker(args).unwrap_or_else(|e| panic!(\"{e}\"))"),
    (
        "host-loopback published port",
        "all.extend_from_slice(&[\"-p\", \"127.0.0.1::7373\", image]);",
    ),
    ("cleanup removes containers", "if let Err(e) = docker(&[\"rm\", \"-f\", c]) {"),
    ("cleanup removes volumes", "if let Err(e) = docker(&[\"volume\", \"rm\", v]) {"),
    ("serve runs the container", "scratch.run(name, args, image);"),
    ("serve waits for the port", "let port = wait_published_port(name).await;"),
    ("serve waits for readiness", "wait_answers(&api, name, \"/readyz\").await;"),
    (
        "search finds exactly one record",
        "found[\"total\"] == 1 && found[\"hits\"].as_array().map(Vec::len) == Some(1) && found[\"hits\"][0][\"external_id\"] == id.as_str(),",
    ),
    (
        "index writes the tagged record",
        "&json!({\"items\": [{\"external_id\": format!(\"durable-{value}\"), \"field\": \"tag\", \"value\": value}]}),",
    ),
    ("legacy layout has no CURRENT", "&& !dir.join(\"CURRENT\").exists()"),
    ("legacy layout has gen-N", ".and_then(|n| n.strip_prefix(\"gen-\"))"),
    (
        "CURRENT read from the data path",
        "must(&[\"cp\", &format!(\"{container}:{DATA}/CURRENT\"), dest.to_str().expect(\"utf-8 path\")]);",
    ),
    (
        "every log line is JSON",
        "serde_json::from_str(line).unwrap_or_else(|_| panic!(\"invalid structured startup log line: {line}\\n{log}\"));",
    ),
    ("decision log schema", "if entry[\"schema\"] == \"axiom.service.log.v1\""),
    ("decision log service", "&& entry[\"service\"][\"name\"] == \"lumen\""),
    (
        "decision log message",
        "&& entry[\"message\"] == \"segment checkpoint startup decision\"",
    ),
    ("decision log value", "&& entry[\"attributes\"][\"decision\"] == expected"),
    ("one decision", "matches == 1,"),
    (
        "rejected container must fail",
        "assert!(code != \"0\", \"rejected container {container} exited successfully\\n{}\", logs(container));",
    ),
    (
        "refusal log line",
        "line.split_once(\"invalid segment checkpoint root inventory \")",
    ),
    (
        "refusal reason",
        ".is_some_and(|(_, rest)| rest.contains(\"refusing to initialize CURRENT\"))",
    ),
    ("CURRENT marker unit test", "fn current_markers() {"),
    ("refusal unit test", "fn refusal_line() {"),
    (
        "revision 0 is not a revision",
        "assert!(!is_current(b\"generation:gen-12-rev-0\\n\", true));",
    ),
];

/// Ways around the contract: a skipped test, a compiled-out step, an exit
/// that bypasses the panic, docker housekeeping that touches what the run
/// did not create, or a durability override the shipped image does not use.
const DURABLE_FORBIDDEN: &[&str] = &[
    "should_panic",
    "#[cfg",
    "cfg_attr",
    "process::exit",
    "catch_unwind",
    "unsafe",
    "/*",
    "\"prune\"",
    "\"rmi\"",
    "\"system\"",
    "\"build\"",
    "LUMEN_WAL",
    "LUMEN_FSYNC",
];

fn sha256_text(content: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(content.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn durable_body(source: &str) -> Result<&str, String> {
    if source.matches(DURABLE_HEADER).count() != 1 {
        return Err("durable() must be one #[tokio::test] ignored for E2E_STANDALONE_IMAGE".into());
    }
    let start = source.find(DURABLE_HEADER).unwrap() + DURABLE_HEADER.len();
    let end = source[start..]
        .find("\n}\n")
        .ok_or("durable() has no closing brace")?;
    Ok(&source[start..start + end])
}

fn active_lines(body: &str) -> Vec<&str> {
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//"))
        .collect()
}

fn validate_durable_suite_semantics(source: &str) -> Result<(), String> {
    if source.matches(BIND_HEADER).count() != 1 {
        return Err("bind() must be one #[tokio::test] ignored for E2E_STANDALONE_IMAGE".into());
    }
    let steps = active_lines(durable_body(source)?);
    if steps != DURABLE_STEPS {
        let at = steps
            .iter()
            .zip(DURABLE_STEPS)
            .position(|(got, want)| got != want)
            .unwrap_or(steps.len().min(DURABLE_STEPS.len()));
        return Err(format!(
            "durable() steps differ at step {at}: got {:?}, want {:?}",
            steps.get(at),
            DURABLE_STEPS.get(at)
        ));
    }
    let lines: Vec<&str> = source.lines().map(str::trim).collect();
    for (label, expected) in DURABLE_HELPER_LINES {
        let count = lines.iter().filter(|line| *line == expected).count();
        if count != 1 {
            return Err(format!(
                "{label}: expected {expected:?} once, found {count}"
            ));
        }
    }
    for token in DURABLE_FORBIDDEN {
        if source.contains(token) {
            return Err(format!("forbidden token {token:?} in the suite"));
        }
    }
    Ok(())
}

fn validate_durable_suite_bytes(source: &str) -> Result<(), String> {
    let digest = sha256_text(source);
    if digest != DURABLE_SUITE_SHA256 {
        return Err(format!(
            "{DURABLE_SUITE} sha256 {digest} != {DURABLE_SUITE_SHA256}; review the change against this contract"
        ));
    }
    Ok(())
}

fn assert_durable_rejected(label: &str, source: String) {
    assert!(
        validate_durable_suite_semantics(&source).is_err(),
        "mutation must fail: {label}"
    );
}

/// The durable container smoke is the Rust suite
/// `e2e::standalone_container::durable`; the candidate's `container-smoke`
/// job runs it against the candidate root (tests/it/release_candidate.rs
/// pins that command). This pins what the suite checks.
#[test]
fn test_durable_suite_contract_and_negative_mutations() {
    let source = fs::read_to_string(repo_root().join(DURABLE_SUITE)).expect("read durable suite");
    validate_durable_suite_semantics(&source).expect("durable suite contract");
    validate_durable_suite_bytes(&source).expect("durable suite bytes");

    let byte_drift = replace_exact(
        &source,
        "    scratch.remove(&old);\n",
        "    scratch.remove(&old);\n    // reviewed byte drift\n",
    );
    validate_durable_suite_semantics(&byte_drift)
        .expect("a comment-only byte drift keeps the durable semantics");
    assert!(
        validate_durable_suite_bytes(&byte_drift).is_err(),
        "the suite digest must reject a generic byte drift"
    );

    // Deleting any one step fails.
    let body = durable_body(&source).expect("durable body");
    let body_start = source.find(body).expect("body offset");
    let mut offset = 0;
    for line in body.split_inclusive('\n') {
        let trimmed = line.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("//") {
            let at = body_start + offset;
            let mutated = format!("{}{}", &source[..at], &source[at + line.len()..]);
            assert_durable_rejected(&format!("delete step {trimmed:?}"), mutated);
        }
        offset += line.len();
    }

    let candidate_serve =
        "    let api = serve(&mut scratch, &candidate, &json_logs, &image).await;\n";
    let adopted = "    assert_startup_decision(&candidate, \"adopted_legacy_0428\");\n";
    for (label, target, replacement) in [
        (
            "commented-out step",
            adopted,
            "    // assert_startup_decision(&candidate, \"adopted_legacy_0428\");\n",
        ),
        (
            "dead branch",
            adopted,
            "    if false {\n        assert_startup_decision(&candidate, \"adopted_legacy_0428\");\n    }\n",
        ),
        (
            "reordered steps",
            "    let api = serve(&mut scratch, &replacement, &json_logs, &image).await;\n    assert_startup_decision(&replacement, \"restored_current_generation\");\n",
            "    assert_startup_decision(&replacement, \"restored_current_generation\");\n    let api = serve(&mut scratch, &replacement, &json_logs, &image).await;\n",
        ),
        (
            "candidate image by tag",
            candidate_serve,
            "    let api = serve(&mut scratch, &candidate, &json_logs, \"ghcr.io/faberline/lumen:latest\").await;\n",
        ),
        (
            "candidate digest not checked",
            "            .is_some_and(|hex| super::support::is_lower_hex(hex, 64)),\n",
            "            .is_some(),\n",
        ),
        (
            "plain-text logs",
            "\"-e\", \"LUMEN_LOG_FORMAT=json\"];",
            "\"-e\", \"LUMEN_LOG_FORMAT=text\"];",
        ),
        (
            "candidate durability override",
            "\"-e\", \"LUMEN_LOG_FORMAT=json\"];",
            "\"-e\", \"LUMEN_LOG_FORMAT=json\", \"-e\", \"LUMEN_SNAPSHOT_SECS=1\"];",
        ),
        (
            "0428 persistence flag dropped",
            "            \"-e\", \"LUMEN_PERSISTENCE=segment\",\n",
            "",
        ),
        (
            "0428 data dir flag dropped",
            "            \"-e\", &format!(\"LUMEN_DATA_DIR={DATA}\"),\n",
            "",
        ),
        (
            "bind mount",
            "    let mount = format!(\"type=volume,src={volume},dst={DATA}\");\n",
            "    let mount = format!(\"type=bind,src={volume},dst={DATA}\");\n",
        ),
        (
            "wrong data path",
            "const DATA: &str = \"/var/lib/lumen/data\";",
            "const DATA: &str = \"/data\";",
        ),
        (
            "0428 digest drift",
            "59a85c96d807428c424ec8889ac830b14e02869da49c4b44ae12dcce3786d03d",
            "0000000000000000000000000000000000000000000000000000000000000000",
        ),
        (
            "candidate repository drift",
            "const REPO_DIGEST_PREFIX: &str = \"ghcr.io/faberline/lumen@sha256:\";",
            "const REPO_DIGEST_PREFIX: &str = \"ghcr.io/\";",
        ),
        (
            "wrong adopted decision",
            adopted,
            "    assert_startup_decision(&candidate, \"restored_current_generation\");\n",
        ),
        ("decision count", "matches == 1,", "matches >= 1,"),
        (
            "decision schema",
            "\"axiom.service.log.v1\"",
            "\"axiom.service.log.v2\"",
        ),
        (
            "decision message",
            "&& entry[\"message\"] == \"segment checkpoint startup decision\"",
            "",
        ),
        (
            "non-JSON log lines tolerated",
            "serde_json::from_str(line).unwrap_or_else(|_| panic!(\"invalid structured startup log line: {line}\\n{log}\"));",
            "match serde_json::from_str(line) { Ok(entry) => entry, Err(_) => continue };",
        ),
        (
            "all-interface published port",
            "\"127.0.0.1::7373\"",
            "\"7373:7373\"",
        ),
        (
            "search accepts more hits",
            "found[\"total\"] == 1 && ",
            "found[\"total\"] != 0 && ",
        ),
        (
            "refusal reason not checked",
            ".is_some_and(|(_, rest)| rest.contains(\"refusing to initialize CURRENT\"))",
            ".is_some()",
        ),
        (
            "rejected exit not checked",
            "assert!(code != \"0\", ",
            "assert!(code == code, ",
        ),
        (
            "cleanup keeps volumes",
            "if let Err(e) = docker(&[\"volume\", \"rm\", v]) {",
            "if let Err(e) = docker(&[\"volume\", \"inspect\", v]) {",
        ),
        (
            "skipped test",
            "#[tokio::test]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn durable() {\n",
            "#[tokio::test]\n#[should_panic]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn durable() {\n",
        ),
        (
            "compiled out",
            "\n\n#[tokio::test]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn durable() {\n",
            "\n\n#[cfg(any())]\n#[tokio::test]\n#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn durable() {\n",
        ),
        (
            "ignore reason drift",
            "#[ignore = \"e2e: needs E2E_STANDALONE_IMAGE\"]\nasync fn durable() {\n",
            "#[ignore = \"e2e: needs docker\"]\nasync fn durable() {\n",
        ),
        (
            "early return",
            "    let id = suffix();\n    let volume",
            "    return;\n    let id = suffix();\n    let volume",
        ),
        (
            "docker prune",
            "    scratch.remove(&old);\n",
            "    scratch.remove(&old);\n    must(&[\"system\", \"prune\", \"-af\"]);\n",
        ),
        (
            "local build",
            "    must(&all);\n",
            "    must(&[\"build\", \"-t\", image, \".\"]);\n    must(&all);\n",
        ),
        (
            "WAL override",
            "\"-e\", \"LUMEN_AUTH=off\", \"-e\", \"LUMEN_LOG_FORMAT=json\"];",
            "\"-e\", \"LUMEN_AUTH=off\", \"-e\", \"LUMEN_WAL=off\", \"-e\", \"LUMEN_LOG_FORMAT=json\"];",
        ),
    ] {
        assert_durable_rejected(label, replace_exact(&source, target, replacement));
    }
}

fn validate_compose_contract(source: &str) -> Result<(), String> {
    let document: serde_yaml::Value = serde_yaml::from_str(source).map_err(|e| e.to_string())?;
    let services = document
        .get("services")
        .and_then(serde_yaml::Value::as_mapping)
        .ok_or("services must be a mapping")?;
    let expected_services = ["otel-collector", "lumen", "prometheus", "jaeger", "grafana"];
    if services.len() != expected_services.len()
        || expected_services
            .iter()
            .any(|name| !services.contains_key(serde_yaml::Value::String((*name).into())))
    {
        return Err("Compose service inventory changed".into());
    }
    for (name, image) in [
        (
            "otel-collector",
            "otel/opentelemetry-collector-contrib:0.119.0",
        ),
        ("prometheus", "prom/prometheus:v3.1.0"),
        ("jaeger", "jaegertracing/all-in-one:1.65.0"),
        ("grafana", "grafana/grafana:11.5.1"),
    ] {
        if services
            .get(serde_yaml::Value::String(name.into()))
            .and_then(|service| service.get("image"))
            .and_then(serde_yaml::Value::as_str)
            != Some(image)
        {
            return Err(format!("{name} image changed"));
        }
    }
    let lumen = document
        .get("services")
        .and_then(|v| v.get("lumen"))
        .ok_or("services.lumen is missing")?;
    let mounts = lumen
        .get("volumes")
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or("services.lumen.volumes must be a sequence")?;
    if mounts.len() != 1 || mounts[0].as_str() != Some("lumen-data:/var/lib/lumen/data") {
        return Err("services.lumen must mount exactly lumen-data:/var/lib/lumen/data".into());
    }
    let ports = lumen
        .get("ports")
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or("services.lumen.ports must be a sequence")?;
    if ports.len() != 1 || ports[0].as_str() != Some("7373:7373") {
        return Err("services.lumen must expose exactly 7373:7373".into());
    }
    let named_volumes = document
        .get("volumes")
        .and_then(serde_yaml::Value::as_mapping)
        .ok_or("top-level volumes must be a mapping")?;
    if !named_volumes
        .get(serde_yaml::Value::String("lumen-data".into()))
        .is_some_and(serde_yaml::Value::is_mapping)
    {
        return Err("top-level lumen-data volume is missing".into());
    }
    if document
        .get("networks")
        .and_then(|v| v.get("default"))
        .and_then(|v| v.get("name"))
        .and_then(serde_yaml::Value::as_str)
        != Some("lumen-otlp")
    {
        return Err("default network name changed".into());
    }
    let environment = lumen
        .get("environment")
        .and_then(serde_yaml::Value::as_mapping)
        .ok_or("services.lumen.environment must be a mapping")?;
    if environment.get(serde_yaml::Value::String("LUMEN_AUTH".into()))
        != Some(&serde_yaml::Value::String("off".into()))
    {
        return Err("LUMEN_AUTH must remain off".into());
    }
    for forbidden in [
        "LUMEN_DATA_DIR",
        "LUMEN_PERSISTENCE",
        "LUMEN_WAL",
        "LUMEN_FSYNC",
    ] {
        if environment.contains_key(serde_yaml::Value::String(forbidden.into())) {
            return Err(format!("forbidden persistence override: {forbidden}"));
        }
    }
    Ok(())
}

#[test]
fn test_checked_in_compose_satisfies_durability_contract() {
    let source =
        std::fs::read_to_string(repo_root().join("compose.yaml")).expect("read compose file");
    validate_compose_contract(&source).expect("compose durability contract must hold");
}

#[test]
fn test_negative_compose_durability_mutations() {
    let source =
        std::fs::read_to_string(repo_root().join("compose.yaml")).expect("read compose file");
    let mount = "      - lumen-data:/var/lib/lumen/data\n";
    for replacement in [
        "      - /var/lib/lumen/data\n",
        "      - ./lumen-data:/var/lib/lumen/data\n",
        "      - lumen-data:/data\n",
        "      - other-data:/var/lib/lumen/data\n",
    ] {
        assert!(validate_compose_contract(&replace_exact(&source, mount, replacement)).is_err());
    }
    for replacement in [
        "volumes:\n",
        "volumes:\n  lumen-data: scalar\n",
        "volumes:\n  lumen-data: []\n",
    ] {
        assert!(validate_compose_contract(&replace_exact(
            &source,
            "volumes:\n  lumen-data: {}\n",
            replacement,
        ))
        .is_err());
    }
    assert!(validate_compose_contract(&replace_exact(
        &source,
        "LUMEN_AUTH: \"off\"",
        "LUMEN_AUTH: \"on\""
    ))
    .is_err());
    for forbidden in [
        "LUMEN_DATA_DIR",
        "LUMEN_PERSISTENCE",
        "LUMEN_WAL",
        "LUMEN_FSYNC",
    ] {
        let mutated = source.replace(
            "LUMEN_OTLP_ENDPOINT: http://otel-collector:4317",
            &format!(
                "LUMEN_OTLP_ENDPOINT: http://otel-collector:4317\n      {forbidden}: forbidden"
            ),
        );
        assert!(
            validate_compose_contract(&mutated).is_err(),
            "{forbidden} must be rejected"
        );
    }
    assert!(validate_compose_contract(&replace_exact(
        &source,
        "    ports: [\"7373:7373\"]",
        "    ports: [\"7374:7373\"]",
    ))
    .is_err());
    assert!(validate_compose_contract(&source.replace(
        "\nnetworks:\n",
        "\n  extra:\n    image: example/extra:1\n\nnetworks:\n",
    ))
    .is_err());
    assert!(validate_compose_contract(&replace_exact(
        &source,
        "  jaeger:\n    image: jaegertracing/all-in-one:1.65.0\n    environment:\n      COLLECTOR_OTLP_ENABLED: \"true\"\n    ports: [\"16686:16686\"] # Jaeger UI\n",
        "",
    ))
    .is_err());
    assert!(validate_compose_contract(&replace_exact(
        &source,
        "otel/opentelemetry-collector-contrib:0.119.0",
        "otel/opentelemetry-collector-contrib:changed",
    ))
    .is_err());
    assert!(validate_compose_contract(&replace_exact(
        &source,
        "name: lumen-otlp",
        "name: changed-network",
    ))
    .is_err());
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DockerfileValidationError {
    InventoryMismatch(Vec<String>, Vec<String>),
    NoStagesFound(String),
    MissingFinalStageInstruction(String, String),
    BuilderStageOnly(String, String),
    DuplicateInstruction(String, String, usize),
    MissingEntrypoint(String),
    PlacedAfterEntrypoint(String, String),
    ForbiddenHostFlagInEntrypoint(String),
    ForbiddenHostFlagInCmd(String),
    InvalidInstruction(String, String, String),
    ForbiddenFsyncKnob(String),
}

pub fn validate_inventory(discovered: &[String]) -> Result<(), DockerfileValidationError> {
    let expected: Vec<String> = EXPECTED_DOCKERFILES.iter().map(|s| s.to_string()).collect();
    if discovered != expected.as_slice() {
        return Err(DockerfileValidationError::InventoryMismatch(
            expected,
            discovered.to_vec(),
        ));
    }
    Ok(())
}

pub fn validate_dockerfile_content(
    path: &str,
    content: &str,
) -> Result<(), DockerfileValidationError> {
    let error_path = path.to_owned();
    let mut stages: Vec<Vec<(usize, &str)>> = Vec::new();
    let mut cur: Vec<(usize, &str)> = Vec::new();
    let mut in_stage = false;

    for (idx, line) in content.lines().enumerate() {
        let (num, trimmed) = (idx + 1, line.trim());
        let is_from =
            trimmed.starts_with("FROM ") || trimmed.to_ascii_uppercase().starts_with("FROM ");
        if is_from {
            if in_stage {
                stages.push(std::mem::take(&mut cur));
            }
            in_stage = true;
        }
        if in_stage {
            cur.push((num, line));
        }
    }
    if in_stage && !cur.is_empty() {
        stages.push(cur);
    }
    if stages.is_empty() {
        return Err(DockerfileValidationError::NoStagesFound(error_path));
    }

    let last_s = stages.len() - 1;
    let entrypoints: Vec<(usize, &str)> = stages[last_s]
        .iter()
        .filter(|(_, l)| l.trim().starts_with("ENTRYPOINT"))
        .copied()
        .collect();

    if entrypoints.is_empty() {
        return Err(DockerfileValidationError::MissingEntrypoint(error_path));
    }

    for (_, ep_line) in &entrypoints {
        if ep_line.contains("--host") {
            return Err(DockerfileValidationError::ForbiddenHostFlagInEntrypoint(
                error_path.clone(),
            ));
        }
    }

    let (last_ep_num, _) = entrypoints.last().unwrap();
    if stages.iter().flatten().any(|(_, line)| {
        line.trim()
            .strip_prefix("ENV ")
            .is_some_and(|value| value.starts_with("LUMEN_FSYNC"))
    }) {
        return Err(DockerfileValidationError::ForbiddenFsyncKnob(
            error_path.clone(),
        ));
    }

    const REQUIRED: [(&str, &str); 5] = [
        ("ENV LUMEN_HOST", "ENV LUMEN_HOST=0.0.0.0"),
        (
            "ENV LUMEN_DATA_DIR",
            "ENV LUMEN_DATA_DIR=/var/lib/lumen/data",
        ),
        ("ENV LUMEN_PERSISTENCE", "ENV LUMEN_PERSISTENCE=segment"),
        ("ENV LUMEN_WAL", "ENV LUMEN_WAL=embedded"),
        ("VOLUME", "VOLUME [\"/var/lib/lumen/data\"]"),
    ];
    for (prefix, expected) in REQUIRED {
        let matches: Vec<_> = stages
            .iter()
            .enumerate()
            .flat_map(|(stage, lines)| {
                lines.iter().filter_map(move |(line_number, line)| {
                    let line = line.trim();
                    (line == prefix
                        || line.starts_with(&format!("{prefix} "))
                        || line.starts_with(&format!("{prefix}=")))
                    .then_some((stage, *line_number, line))
                })
            })
            .collect();
        if matches.is_empty() {
            return Err(DockerfileValidationError::MissingFinalStageInstruction(
                error_path.clone(),
                expected.into(),
            ));
        }
        if matches.len() > 1 {
            return Err(DockerfileValidationError::DuplicateInstruction(
                error_path.clone(),
                expected.into(),
                matches.len(),
            ));
        }
        let (stage, line_number, actual) = matches[0];
        if stage != last_s {
            return Err(DockerfileValidationError::BuilderStageOnly(
                error_path.clone(),
                expected.into(),
            ));
        }
        if actual != expected {
            return Err(DockerfileValidationError::InvalidInstruction(
                error_path.clone(),
                prefix.into(),
                actual.into(),
            ));
        }
        if line_number >= *last_ep_num {
            return Err(DockerfileValidationError::PlacedAfterEntrypoint(
                error_path.clone(),
                expected.into(),
            ));
        }
    }

    if stages[last_s]
        .iter()
        .any(|(_, l)| l.trim().starts_with("CMD") && l.contains("--host"))
    {
        return Err(DockerfileValidationError::ForbiddenHostFlagInCmd(
            error_path,
        ));
    }
    Ok(())
}

fn for_each_dockerfile(f: impl Fn(&str, &str)) {
    let root = repo_root();
    for &rel_path in EXPECTED_DOCKERFILES {
        let content = std::fs::read_to_string(root.join(rel_path))
            .unwrap_or_else(|err| panic!("read {rel_path}: {err}"));
        f(rel_path, &content);
    }
}

#[test]
fn test_checked_in_dockerfiles_satisfy_contract() {
    let discovered = discover_dockerfiles();
    validate_inventory(&discovered).expect("inventory must match expected");
    for_each_dockerfile(|path, content| {
        validate_dockerfile_content(path, content)
            .unwrap_or_else(|err| panic!("validation failed for {path}: {err:?}"));
    });
}

#[test]
fn test_negative_fixture_inventory_mismatch() {
    let mut missing: Vec<String> = EXPECTED_DOCKERFILES.iter().map(|s| s.to_string()).collect();
    missing.pop();
    assert!(matches!(
        validate_inventory(&missing),
        Err(DockerfileValidationError::InventoryMismatch(_, _))
    ));

    let mut extra = missing;
    extra.extend(["Dockerfile.test".into(), "Dockerfile.extra".into()]);
    assert!(matches!(
        validate_inventory(&extra),
        Err(DockerfileValidationError::InventoryMismatch(_, _))
    ));
}

#[test]
fn test_negative_fixture_no_stages_found() {
    let no_from = "# syntax=docker/dockerfile:1\nENV LUMEN_HOST=0.0.0.0\n\
        ENTRYPOINT [\"/usr/local/bin/lumen\"]\nCMD [\"serve\"]";
    assert_eq!(
        validate_dockerfile_content("Dockerfile", no_from),
        Err(DockerfileValidationError::NoStagesFound(
            "Dockerfile".to_string()
        ))
    );
}

#[test]
fn test_negative_fixtures_per_file() {
    for_each_dockerfile(|path, content| {
        let p = path.to_string();
        let check = |mutated: String, err: DockerfileValidationError| {
            assert_eq!(validate_dockerfile_content(path, &mutated), Err(err));
        };

        check(
            replace_exact(content, "ENTRYPOINT [\"/usr/local/bin/lumen\"]\n", ""),
            DockerfileValidationError::MissingEntrypoint(p.clone()),
        );
        check(
            replace_exact(
                content,
                "ENTRYPOINT [\"/usr/local/bin/lumen\"]\n",
                "ENTRYPOINT [\"/usr/local/bin/lumen\"]\nENTRYPOINT [\"/usr/local/bin/lumen\", \"--host\", \"0.0.0.0\"]\n",
            ),
            DockerfileValidationError::ForbiddenHostFlagInEntrypoint(p.clone()),
        );
        check(
            replace_exact(
                content,
                "CMD [\"serve\"]",
                "CMD [\"serve\", \"--host\", \"0.0.0.0\"]",
            ),
            DockerfileValidationError::ForbiddenHostFlagInCmd(p.clone()),
        );
        check(
            replace_exact(
                content,
                "ENTRYPOINT [\"/usr/local/bin/lumen\"]\n",
                "ENV LUMEN_FSYNC=always\nENTRYPOINT [\"/usr/local/bin/lumen\"]\n",
            ),
            DockerfileValidationError::ForbiddenFsyncKnob(p.clone()),
        );

        let required = [
            (
                "ENV LUMEN_HOST",
                "ENV LUMEN_HOST=0.0.0.0",
                "ENV LUMEN_HOST=127.0.0.1",
            ),
            (
                "ENV LUMEN_DATA_DIR",
                "ENV LUMEN_DATA_DIR=/var/lib/lumen/data",
                "ENV LUMEN_DATA_DIR=/data",
            ),
            (
                "ENV LUMEN_PERSISTENCE",
                "ENV LUMEN_PERSISTENCE=segment",
                "ENV LUMEN_PERSISTENCE=cbor",
            ),
            (
                "ENV LUMEN_WAL",
                "ENV LUMEN_WAL=embedded",
                "ENV LUMEN_WAL=auto",
            ),
            (
                "VOLUME",
                "VOLUME [\"/var/lib/lumen/data\"]",
                "VOLUME [\"/data\"]",
            ),
        ];
        for (prefix, exact, wrong) in required {
            let exact_line = format!("{exact}\n");
            let missing = replace_exact(content, &exact_line, "");
            check(
                missing.clone(),
                DockerfileValidationError::MissingFinalStageInstruction(p.clone(), exact.into()),
            );
            check(
                replace_exact(content, &exact_line, &format!("{exact}\n{exact}\n")),
                DockerfileValidationError::DuplicateInstruction(p.clone(), exact.into(), 2),
            );
            check(
                replace_exact(content, exact, wrong),
                DockerfileValidationError::InvalidInstruction(
                    p.clone(),
                    prefix.into(),
                    wrong.into(),
                ),
            );
            check(
                replace_exact(
                    &missing,
                    "ENTRYPOINT [\"/usr/local/bin/lumen\"]\n",
                    &format!("ENTRYPOINT [\"/usr/local/bin/lumen\"]\n{exact}\n"),
                ),
                DockerfileValidationError::PlacedAfterEntrypoint(p.clone(), exact.into()),
            );
            check(
                insert_after_first_from(&missing, exact),
                DockerfileValidationError::BuilderStageOnly(p.clone(), exact.into()),
            );
        }
    });
}

#[tokio::test]
async fn test_bare_process_defaults_to_localhost() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
        listener.local_addr().expect("local addr").port()
    };

    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_lumen"));
    cmd.args(["serve", "--port", &port.to_string()])
        .env(
            "TOKIO_WORKER_THREADS",
            crate::support::test_load::SERVE_WORKER_THREADS,
        )
        .env_remove("LUMEN_HOST")
        .env_remove("RUST_LOG")
        .env("LUMEN_AUTH", "off")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    struct ChildGuard(Option<std::process::Child>);
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    let child = cmd.spawn().expect("failed to spawn lumen serve");
    let mut guard = ChildGuard(Some(child));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(500))
        .no_proxy()
        .build()
        .expect("build reqwest client");

    let url = format!("http://127.0.0.1:{port}/healthz");
    let (start, timeout) = (
        std::time::Instant::now(),
        std::time::Duration::from_secs(15),
    );
    let mut healthy = false;

    while start.elapsed() < timeout {
        if client
            .get(&url)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            healthy = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let mut child = guard.0.take().expect("child exists");
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait for child output");
    let logs = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        healthy,
        "lumen serve did not answer /healthz within deadline\nlogs:\n{logs}"
    );

    let expected_addr = format!("127.0.0.1:{port}");
    let record_matches = logs
        .lines()
        .any(|l| l.contains("lumen serve listening") && l.contains(&expected_addr));
    assert!(
        record_matches,
        "single log line must contain 'lumen serve listening' and '{expected_addr}', logs:\n{logs}"
    );
}
