//! Suite `standalone_container`: the image itself under `docker` on the
//! runner, reached through a host-loopback published port. The image comes
//! from `E2E_STANDALONE_IMAGE` (rust-tests.md rule 6, image-targeted suites);
//! a missing one fails the test.
//!
//!   bind     without LUMEN_HOST the container listens on 0.0.0.0 and answers
//!            through 127.0.0.1::7373; with LUMEN_HOST=127.0.0.1 it starts and
//!            logs its 127.0.0.1:7373 listener while the published port stays
//!            unreachable.
//!   durable  a data volume written by the 0428 image is adopted, moves to a
//!            CURRENT revision on checkpoint, is restored by a replacement
//!            container, and a volume with an unknown root is refused without
//!            being touched. Needs an exact GHCR root digest.
//!
//! The candidate's `container-smoke` job runs both against the candidate
//! root. tests/it/standalone_container_contract.rs pins the durable steps,
//! their helpers, and this file's bytes; a change here updates that pin.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use super::support::{evidence, required, say, Api};

const SUITE: &str = "standalone_container";
const DATA: &str = "/var/lib/lumen/data";
const REPO_DIGEST_PREFIX: &str = "ghcr.io/faberline/lumen@sha256:";
/// The 0428 image whose legacy `gen-N` layout the candidate must adopt.
const OLD_IMAGE: &str =
    "ghcr.io/chrischeng-c4/lumen@sha256:59a85c96d807428c424ec8889ac830b14e02869da49c4b44ae12dcce3786d03d";

fn docker(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .map_err(|e| format!("docker {args:?}: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "docker {args:?} failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn must(args: &[&str]) -> String {
    docker(args).unwrap_or_else(|e| panic!("{e}"))
}

/// stdout and stderr of the container, as `docker logs c 2>&1` gives them.
fn logs(container: &str) -> String {
    match Command::new("docker").args(["logs", container]).output() {
        Ok(out) => format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(e) => format!("docker logs {container}: {e}"),
    }
}

fn running(container: &str) -> Option<bool> {
    docker(&["inspect", "--format", "{{.State.Running}}", container])
        .ok()
        .map(|s| s == "true")
}

/// Unique per run, so parallel runs on one host never share a name.
fn suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("{nanos}-{}", std::process::id())
}

/// Removes what the test created, newest first, whether it passes or not.
/// A failed removal fails a passing test.
struct Scratch {
    containers: Vec<String>,
    volumes: Vec<String>,
    dir: Option<PathBuf>,
}

impl Scratch {
    fn new() -> Self {
        Self {
            containers: Vec::new(),
            volumes: Vec::new(),
            dir: None,
        }
    }

    fn volume(&mut self, name: &str) {
        assert!(
            docker(&["volume", "inspect", name]).is_err(),
            "volume {name} unexpectedly preexists"
        );
        must(&["volume", "create", name]);
        self.volumes.push(name.to_string());
    }

    /// `docker run -d` with a host-loopback published 7373.
    fn run(&mut self, name: &str, args: &[&str], image: &str) {
        self.containers.push(name.to_string());
        let mut all = vec!["run", "-d", "--name", name];
        all.extend_from_slice(args);
        all.extend_from_slice(&["-p", "127.0.0.1::7373", image]);
        must(&all);
    }

    fn remove(&mut self, name: &str) {
        must(&["rm", "-f", name]);
        self.containers.retain(|c| c != name);
    }

    fn dir(&mut self, name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lumen-{name}"));
        std::fs::create_dir(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
        self.dir = Some(dir.clone());
        dir
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let mut errors = Vec::new();
        for c in self.containers.iter().rev() {
            if let Err(e) = docker(&["rm", "-f", c]) {
                errors.push(e);
            }
        }
        for v in self.volumes.iter().rev() {
            if let Err(e) = docker(&["volume", "rm", v]) {
                errors.push(e);
            }
        }
        if let Some(dir) = &self.dir {
            if let Err(e) = std::fs::remove_dir_all(dir) {
                errors.push(format!("remove {}: {e}", dir.display()));
            }
        }
        if errors.is_empty() {
            return;
        }
        let message = format!("cleanup failed: {}", errors.join("; "));
        if std::thread::panicking() {
            say(format!("   {message}"));
        } else {
            panic!("{message}");
        }
    }
}

fn port_diagnostics(container: &str) -> String {
    let state = docker(&[
        "inspect",
        "--format",
        "state={{.State.Status}} running={{.State.Running}} exit_code={{.State.ExitCode}} error={{json .State.Error}} ports={{json .NetworkSettings.Ports}}",
        container,
    ])
    .unwrap_or_else(|e| e);
    format!(
        "published 7373/tcp port unavailable for {container}\n{state}\n{}",
        logs(container)
    )
}

/// The host port docker published for 7373/tcp, or "" when there is none.
fn published_port(container: &str) -> Result<String, String> {
    docker(&[
        "inspect",
        "--format",
        r#"{{with .NetworkSettings.Ports}}{{with index . "7373/tcp"}}{{with index . 0}}{{.HostPort}}{{end}}{{end}}{{end}}"#,
        container,
    ])
}

fn is_port(s: &str) -> bool {
    !s.is_empty() && !s.starts_with('0') && s.bytes().all(|c| c.is_ascii_digit())
}

async fn wait_published_port(container: &str) -> String {
    for _ in 0..60 {
        let port =
            published_port(container).unwrap_or_else(|_| panic!("{}", port_diagnostics(container)));
        if is_port(&port) {
            return port;
        }
        if running(container) != Some(true) {
            panic!("{}", port_diagnostics(container));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("{}", port_diagnostics(container));
}

async fn wait_answers(api: &Api, container: &str, path: &str) {
    for _ in 0..60 {
        if api.answers(path).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("container {container} failed {path}\n{}", logs(container));
}

/// Runs `name` and waits until it is ready on its published port.
async fn serve(scratch: &mut Scratch, name: &str, args: &[&str], image: &str) -> Api {
    scratch.run(name, args, image);
    let port = wait_published_port(name).await;
    let api = Api::new(&format!("http://127.0.0.1:{port}"));
    wait_answers(&api, name, "/readyz").await;
    api
}

/// Whether anything answers HTTP at `url` within 2s, success or not.
async fn reachable(url: &str) -> bool {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(2))
        .no_proxy()
        .build()
        .expect("HTTP client");
    client.get(url).send().await.is_ok()
}

const TIMEOUT: Duration = Duration::from_secs(5);

async fn assert_search(api: &Api, value: &str) {
    let found = api
        .post(
            "/collections/durable/search",
            &json!({"query": {"term": {"field": "tag", "value": value}}, "limit": 10}),
            TIMEOUT,
        )
        .await;
    let id = format!("durable-{value}");
    assert!(
        found["total"] == 1
            && found["hits"].as_array().map(Vec::len) == Some(1)
            && found["hits"][0]["external_id"] == id.as_str(),
        "{} must find exactly {id}, got {found}",
        api.base
    );
}

async fn index_tag(api: &Api, value: &str) {
    api.post(
        "/collections/durable/index",
        &json!({"items": [{"external_id": format!("durable-{value}"), "field": "tag", "value": value}]}),
        TIMEOUT,
    )
    .await;
}

/// A data directory the 0428 image left: no CURRENT, at least one `gen-N`.
fn has_legacy_generation(dir: &Path) -> bool {
    dir.is_dir()
        && !dir.join("CURRENT").exists()
        && std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .any(|entry| {
                entry.path().is_dir()
                    && entry
                        .file_name()
                        .to_str()
                        .and_then(|n| n.strip_prefix("gen-"))
                        .is_some_and(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()))
            })
}

/// `generation:gen-<n>\n`, or with `rev`, `generation:gen-<n>-rev-<m>\n` (m ≥ 1).
fn is_current(bytes: &[u8], rev: bool) -> bool {
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit());
    let Some(body) = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.strip_prefix("generation:gen-"))
        .and_then(|s| s.strip_suffix('\n'))
    else {
        return false;
    };
    if !rev {
        return digits(body);
    }
    match body.split_once("-rev-") {
        Some((generation, revision)) => {
            digits(generation) && digits(revision) && !revision.starts_with('0')
        }
        None => false,
    }
}

fn read_current(scratch: &Scratch, container: &str, name: &str) -> Vec<u8> {
    let dest = scratch.dir.as_ref().expect("scratch dir").join(name);
    must(&[
        "cp",
        &format!("{container}:{DATA}/CURRENT"),
        dest.to_str().expect("utf-8 path"),
    ]);
    std::fs::read(&dest).unwrap_or_else(|e| panic!("read {}: {e}", dest.display()))
}

/// The JSON log holds exactly one startup decision, and it is `expected`.
/// Every log line must be JSON.
fn assert_startup_decision(container: &str, expected: &str) {
    let log = logs(container);
    let mut matches = 0;
    for line in log.lines().filter(|l| !l.trim().is_empty()) {
        let entry: Value = serde_json::from_str(line)
            .unwrap_or_else(|_| panic!("invalid structured startup log line: {line}\n{log}"));
        if entry["schema"] == "axiom.service.log.v1"
            && entry["service"]["name"] == "lumen"
            && entry["message"] == "segment checkpoint startup decision"
            && entry["attributes"]["decision"] == expected
        {
            matches += 1;
        }
    }
    assert!(
        matches == 1,
        "expected one segment checkpoint startup decision '{expected}' from {container}; found {matches}\n{log}"
    );
    evidence(
        SUITE,
        "startup-decision",
        json!({"container": container, "decision": expected}),
    );
}

/// Waits for the container to stop, and it must have failed.
async fn wait_failed_exit(container: &str) {
    for _ in 0..60 {
        if running(container) == Some(false) {
            let code = must(&["inspect", "--format", "{{.State.ExitCode}}", container]);
            assert!(
                code != "0",
                "rejected container {container} exited successfully\n{}",
                logs(container)
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!(
        "rejected container {container} did not exit\n{}",
        logs(container)
    );
}

fn refused_root(log: &str) -> bool {
    log.lines().any(|line| {
        line.split_once("invalid segment checkpoint root inventory ")
            .is_some_and(|(_, rest)| rest.contains("refusing to initialize CURRENT"))
    })
}

#[tokio::test]
#[ignore = "e2e: needs E2E_STANDALONE_IMAGE"]
async fn bind() {
    let image = required("E2E_STANDALONE_IMAGE");
    let id = suffix();
    let positive = format!("lumen-smoke-pos-{id}");
    let negative = format!("lumen-smoke-neg-{id}");
    let mut scratch = Scratch::new();
    let host_ip = r#"{{(index (index .NetworkSettings.Ports "7373/tcp") 0).HostIp}}"#;
    let host_port = r#"{{(index (index .NetworkSettings.Ports "7373/tcp") 0).HostPort}}"#;

    say(format!(
        ">> [{SUITE}] bind: image default listens on 0.0.0.0 ({positive})"
    ));
    scratch.run(&positive, &["-e", "LUMEN_AUTH=off"], &image);
    let ip = must(&["inspect", "--format", host_ip, &positive]);
    assert!(
        ip == "127.0.0.1",
        "expected published HostIp 127.0.0.1, got '{ip}'\n{}",
        logs(&positive)
    );
    let port = must(&["inspect", "--format", host_port, &positive]);
    assert!(
        !port.is_empty(),
        "no published HostPort for the positive container\n{}",
        logs(&positive)
    );
    let api = Api::new(&format!("http://127.0.0.1:{port}"));
    wait_answers(&api, &positive, "/healthz").await;
    api.put(
        "/collections/containerbind",
        &json!({"fields": {"tag": {"type": "keyword"}}}),
        TIMEOUT,
    )
    .await;
    api.post(
        "/collections/containerbind/index",
        &json!({"items": [{"external_id": "containerbind1", "field": "tag", "value": "ready"}]}),
        TIMEOUT,
    )
    .await;
    let found = api
        .post(
            "/collections/containerbind/search",
            &json!({"query": {"term": {"field": "tag", "value": "ready"}}}),
            TIMEOUT,
        )
        .await;
    assert!(
        super::support::hits_for(&found, "containerbind1") >= 1,
        "search did not return containerbind1: {found}\n{}",
        logs(&positive)
    );
    say("   positive container answered through 127.0.0.1 and found containerbind1");

    say(format!(
        ">> [{SUITE}] bind: LUMEN_HOST=127.0.0.1 is unreachable from the host ({negative})"
    ));
    scratch.run(
        &negative,
        &["-e", "LUMEN_HOST=127.0.0.1", "-e", "LUMEN_AUTH=off"],
        &image,
    );
    let ip = must(&["inspect", "--format", host_ip, &negative]);
    assert!(
        ip == "127.0.0.1",
        "negative container HostIp expected 127.0.0.1, got '{ip}'\n{}",
        logs(&negative)
    );
    let mut listening = false;
    for _ in 0..60 {
        let log = logs(&negative);
        assert!(
            running(&negative) == Some(true),
            "negative container exited before proving startup\n{log}"
        );
        if log
            .lines()
            .any(|l| l.contains("lumen serve listening") && l.contains("127.0.0.1:7373"))
        {
            listening = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        listening,
        "negative container logs did not confirm 'lumen serve listening' on 127.0.0.1:7373\n{}",
        logs(&negative)
    );
    let port = must(&["inspect", "--format", host_port, &negative]);
    let url = format!("http://127.0.0.1:{port}/healthz");
    for _ in 0..10 {
        assert!(
            !reachable(&url).await,
            "negative container host port {url} was reachable\n{}",
            logs(&negative)
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        running(&negative) == Some(true),
        "negative container is not running after the host probe\n{}",
        logs(&negative)
    );
    evidence(SUITE, "bind", json!({"image": image, "result": "pass"}));
    say(format!(">> [{SUITE}] bind PASS"));
}

#[tokio::test]
#[ignore = "e2e: needs E2E_STANDALONE_IMAGE"]
async fn durable() {
    let image = required("E2E_STANDALONE_IMAGE");
    assert!(
        image
            .strip_prefix(REPO_DIGEST_PREFIX)
            .is_some_and(|hex| super::support::is_lower_hex(hex, 64)),
        "E2E_STANDALONE_IMAGE must be an exact GHCR root digest ({REPO_DIGEST_PREFIX}<64 hex>) for durable, got {image}"
    );
    let id = suffix();
    let volume = format!("lumen-smoke-durable-{id}");
    let reject_volume = format!("lumen-smoke-durable-reject-{id}");
    let old = format!("lumen-smoke-durable-old-{id}");
    let candidate = format!("lumen-smoke-durable-candidate-{id}");
    let replacement = format!("lumen-smoke-durable-replacement-{id}");
    let seed = format!("lumen-smoke-durable-seed-{id}");
    let rejected = format!("lumen-smoke-durable-rejected-{id}");
    let mut scratch = Scratch::new();
    let tmp = scratch.dir(&format!("durable-{id}"));
    scratch.volume(&volume);
    scratch.volume(&reject_volume);
    let mount = format!("type=volume,src={volume},dst={DATA}");
    let reject_mount = format!("type=volume,src={reject_volume},dst={DATA}");

    say(format!(
        ">> [{SUITE}] durable: the 0428 image writes a legacy layout"
    ));
    let api = serve(
        &mut scratch,
        &old,
        &[
            "--mount",
            &mount,
            "-e",
            "LUMEN_AUTH=off",
            "-e",
            &format!("LUMEN_DATA_DIR={DATA}"),
            "-e",
            "LUMEN_PERSISTENCE=segment",
            "-e",
            "LUMEN_SNAPSHOT_SECS=1",
            "-e",
            "LUMEN_GRACE_SECS=1",
        ],
        OLD_IMAGE,
    )
    .await;
    api.put(
        "/collections/durable",
        &json!({"fields": {"tag": {"type": "keyword"}}}),
        TIMEOUT,
    )
    .await;
    index_tag(&api, "first").await;
    assert_search(&api, "first").await;
    let old_data = tmp.join("old-data");
    let old_data_arg = old_data.to_str().expect("utf-8 path");
    for _ in 0..60 {
        let _ = docker(&["cp", &format!("{old}:{DATA}"), old_data_arg]);
        if has_legacy_generation(&old_data) {
            break;
        }
        let _ = std::fs::remove_dir_all(&old_data);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        old_data.is_dir() && !old_data.join("CURRENT").exists(),
        "the 0428 image left no legacy checkpoint\n{}",
        logs(&old)
    );
    scratch.remove(&old);

    say(format!(
        ">> [{SUITE}] durable: the candidate adopts the legacy layout"
    ));
    let json_logs = [
        "--mount",
        mount.as_str(),
        "-e",
        "LUMEN_AUTH=off",
        "-e",
        "LUMEN_LOG_FORMAT=json",
    ];
    let api = serve(&mut scratch, &candidate, &json_logs, &image).await;
    assert_startup_decision(&candidate, "adopted_legacy_0428");
    let current = read_current(&scratch, &candidate, "current-first");
    assert!(
        is_current(&current, false),
        "adopted CURRENT is not generation:gen-<n>: {:?}",
        String::from_utf8_lossy(&current)
    );
    assert_search(&api, "first").await;
    index_tag(&api, "second").await;
    assert_search(&api, "second").await;
    let checkpoint = api.post("/admin/checkpoint", &json!({}), TIMEOUT).await;
    assert!(
        checkpoint["persisted"] == true,
        "checkpoint did not persist: {checkpoint}"
    );
    let current = read_current(&scratch, &candidate, "current-rev");
    assert!(
        is_current(&current, true),
        "checkpointed CURRENT is not a revision: {:?}",
        String::from_utf8_lossy(&current)
    );
    must(&["kill", &candidate]);
    scratch.remove(&candidate);

    say(format!(
        ">> [{SUITE}] durable: a replacement restores the CURRENT revision"
    ));
    let api = serve(&mut scratch, &replacement, &json_logs, &image).await;
    assert_startup_decision(&replacement, "restored_current_generation");
    let current = read_current(&scratch, &replacement, "current-replacement");
    assert!(
        is_current(&current, true),
        "restored CURRENT is not a revision: {:?}",
        String::from_utf8_lossy(&current)
    );
    assert_search(&api, "first").await;
    assert_search(&api, "second").await;

    say(format!(
        ">> [{SUITE}] durable: an unknown root is refused and left as it was"
    ));
    let foreign = tmp.join("foreign-layout");
    std::fs::create_dir(&foreign).expect("create foreign-layout");
    std::fs::write(foreign.join("sentinel"), b"fail-closed unknown root\n")
        .expect("write sentinel");
    scratch.containers.push(seed.clone());
    must(&["create", "--name", &seed, "--mount", &reject_mount, &image]);
    must(&[
        "cp",
        foreign.to_str().expect("utf-8 path"),
        &format!("{seed}:{DATA}/foreign-layout"),
    ]);
    scratch.remove(&seed);
    scratch.run(
        &rejected,
        &["--mount", &reject_mount, "-e", "LUMEN_AUTH=off"],
        &image,
    );
    wait_failed_exit(&rejected).await;
    let port =
        published_port(&rejected).unwrap_or_else(|_| panic!("{}", port_diagnostics(&rejected)));
    if !port.is_empty() {
        assert!(
            is_port(&port),
            "rejected container has an invalid published port {port}\n{}",
            port_diagnostics(&rejected)
        );
        let url = format!("http://127.0.0.1:{port}/readyz");
        assert!(
            !Api::new(&format!("http://127.0.0.1:{port}"))
                .answers("/readyz")
                .await,
            "rejected container served {url}"
        );
    }
    let log = logs(&rejected);
    assert!(
        refused_root(&log),
        "rejected container did not refuse the unknown root\n{log}"
    );
    let rejected_data = tmp.join("rejected-data");
    must(&[
        "cp",
        &format!("{rejected}:{DATA}"),
        rejected_data.to_str().expect("utf-8 path"),
    ]);
    assert!(
        !rejected_data.join("CURRENT").exists(),
        "the refused volume gained a CURRENT"
    );
    assert_eq!(
        std::fs::read(rejected_data.join("foreign-layout/sentinel")).ok(),
        Some(b"fail-closed unknown root\n".to_vec()),
        "the refused volume's sentinel changed"
    );
    evidence(
        SUITE,
        "durable",
        json!({"image": image, "old_image": OLD_IMAGE, "result": "pass"}),
    );
    say(format!(">> [{SUITE}] durable PASS"));
}

mod tests {
    use super::*;

    #[test]
    fn current_markers() {
        assert!(is_current(b"generation:gen-12\n", false));
        assert!(!is_current(b"generation:gen-12", false));
        assert!(!is_current(b"generation:gen-\n", false));
        assert!(!is_current(b"generation:gen-12-rev-1\n", false));
        assert!(is_current(b"generation:gen-12-rev-3\n", true));
        assert!(!is_current(b"generation:gen-12-rev-0\n", true));
        assert!(!is_current(b"generation:gen-12-rev-03\n", true));
        assert!(!is_current(b"generation:gen-12\n", true));
    }

    #[test]
    fn refusal_line() {
        assert!(refused_root(
            "x invalid segment checkpoint root inventory [foo]: refusing to initialize CURRENT"
        ));
        assert!(!refused_root(
            "invalid segment checkpoint root inventory\nrefusing to initialize CURRENT"
        ));
        assert!(is_port("49153") && !is_port("") && !is_port("0") && !is_port("12a"));
    }
}
