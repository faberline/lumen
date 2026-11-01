//! Current execution owners for the old EC contracts. Each owner runs the
//! original commands or scenario assertions against an owned live process.

use super::serve_budget_support::{LumenProcess, ServeMode};
use rig::scenario::Step;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn evidence(kind: &str, name: &str, values: serde_json::Value) {
    if let Some(path) = std::env::var_os("LUMEN_NATIVE_CONTRACT_RECEIPT") {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open isolated native contract receipt");
        writeln!(
            file,
            "{}",
            serde_json::json!({"kind":kind,"name":name,"values":values})
        )
        .expect("write native contract measurement");
    }
}

pub fn cli_help() {
    for group in ["connect", "query"] {
        let out = Command::new(env!("CARGO_BIN_EXE_lumen"))
            .args([group, "--help"])
            .output()
            .expect("run installed CLI help");
        assert!(
            out.status.success(),
            "{group}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).expect("UTF-8 help");
        assert!(
            text.contains("Usage:") && text.contains(group),
            "missing {group} command help: {text}"
        );
        evidence(
            "cli",
            group,
            serde_json::json!({"status":out.status.code(),"help":text}),
        );
    }
}

pub fn kustomize(paths: &[&str], observability: bool) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for path in paths {
        let out = Command::new("kustomize")
            .arg("build")
            .arg(root.join(path))
            .output()
            .expect("run pinned kustomize");
        assert!(
            out.status.success(),
            "{path}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout).expect("UTF-8 Kubernetes manifests");
        let documents: Vec<serde_yaml::Value> = serde_yaml::Deserializer::from_str(&text)
            .map(|doc| serde::Deserialize::deserialize(doc).expect("valid rendered YAML"))
            .collect();
        assert!(!documents.is_empty(), "empty {path} render");
        for doc in &documents {
            assert!(
                doc["apiVersion"].as_str().is_some()
                    && doc["kind"].as_str().is_some()
                    && doc["metadata"]["name"].as_str().is_some(),
                "invalid resource in {path}: {doc:?}"
            );
        }
        if observability {
            for kind in ["ServiceMonitor", "PrometheusRule"] {
                assert!(
                    documents.iter().any(|d| d["kind"] == kind),
                    "{path} must render {kind}"
                );
            }
        }
        evidence(
            "kustomize",
            path,
            serde_json::json!({"documents":documents.len(),"rendered":text}),
        );
    }
}

/// Run an independently built, exact test. A missing test never becomes a
/// successful zero-test cargo subprocess as it did in the old shell wrapper.
pub fn test_binary(variable: &str, exact_name: &str) {
    let path = std::env::var_os(variable).unwrap_or_else(|| panic!("missing {variable}"));
    let listed = Command::new(&path)
        .args([exact_name, "--exact", "--list", "--format", "pretty"])
        .output()
        .expect("discover exact external test");
    assert!(listed.status.success(), "external test discovery failed");
    let text = String::from_utf8(listed.stdout).expect("UTF-8 discovery");
    let expected = format!("{exact_name}: test");
    assert_eq!(
        text.lines()
            .filter(|l| l.ends_with(": test"))
            .collect::<Vec<_>>(),
        [expected.as_str()]
    );
    let out = Command::new(&path)
        .args([
            exact_name,
            "--exact",
            "--include-ignored",
            "--test-threads",
            "1",
            "--show-output",
            "--format",
            "pretty",
            "--color",
            "never",
        ])
        .output()
        .expect("run exact external test");
    let stdout = String::from_utf8(out.stdout).expect("UTF-8 test output");
    let stderr = String::from_utf8_lossy(&out.stderr);
    eprintln!("external test {exact_name}:\n{stdout}\n{stderr}");
    assert!(out.status.success(), "external {exact_name} failed");
    assert!(stdout.lines().any(|l| l == "running 1 test"));
    assert!(stdout.contains(&format!("test {exact_name} ... ok")));
    assert!(stdout.contains("test result: ok. 1 passed; 0 failed; 0 ignored;"));
    assert!(
        !stdout.contains("SKIP") && !stderr.contains("SKIP"),
        "external test skipped actual work"
    );
    evidence(
        "exact_test_binary",
        exact_name,
        serde_json::json!({"stdout":stdout,"stderr":stderr,"executed":1}),
    );
}

struct Proxy {
    child: Child,
    api: String,
    listener: String,
    _logs: tempfile::NamedTempFile,
}

impl Proxy {
    fn spawn() -> Self {
        let api = TcpListener::bind(("127.0.0.1", 0)).expect("reserve proxy API");
        let listener =
            TcpListener::bind(("127.0.0.1", 0)).expect("reserve isolated proxy listener");
        let api_port = api.local_addr().unwrap().port();
        let listen_addr = listener.local_addr().unwrap().to_string();
        let logs = tempfile::NamedTempFile::new().expect("capture proxy logs");
        let binary = std::env::var_os("LUMEN_TOXIPROXY_SERVER")
            .expect("pinned proxy executable is required");
        drop(api);
        let child = Command::new(binary)
            .args(["-host", "127.0.0.1", "-port", &api_port.to_string()])
            .env_remove("TOXIPROXY_AUTH_TOKEN")
            .stdout(Stdio::from(logs.reopen().unwrap()))
            .stderr(Stdio::from(logs.reopen().unwrap()))
            .spawn()
            .expect("spawn owned fault proxy");
        let mut proxy = Self {
            child,
            api: format!("127.0.0.1:{api_port}"),
            listener: listen_addr,
            _logs: logs,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(&proxy.api).is_err() {
            assert!(
                proxy.child.try_wait().expect("poll owned proxy").is_none(),
                "proxy exited before ready"
            );
            assert!(Instant::now() < deadline, "proxy startup timed out");
            std::thread::sleep(Duration::from_millis(25));
        }
        // Keep the proxy listener reserved until its API is ready.
        drop(listener);
        proxy
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn scenarios(cases: &[&str]) {
    assert!(!cases.is_empty(), "empty scenario owner");
    for case in cases {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = root.join("tests/rig/cases").join(format!("{case}.toml"));
        let text = std::fs::read_to_string(&path).expect("read original scenario");
        let mut scenario =
            rig::scenario::parse_scenario(&path, &text).expect("original scenario contract");
        let mut server = LumenProcess::spawn(None, ServeMode::MemoryOnly, 0);
        server.wait_until_ready(0);
        scenario
            .env
            .insert("upstream".into(), format!("127.0.0.1:{}", server.port));
        let proxy = case.starts_with("resilience/").then(Proxy::spawn);
        if let Some(proxy) = &proxy {
            scenario.env.insert("toxiproxy".into(), proxy.api.clone());
            scenario
                .env
                .insert("proxy_listen".into(), proxy.listener.clone());
        }
        for step in &mut scenario.steps {
            if let Step::Exec(exec) = step {
                for arg in &mut exec.cmd {
                    if arg.starts_with("tests/rig/helpers/") {
                        *arg = root
                            .join(&*arg)
                            .to_str()
                            .expect("UTF-8 helper path")
                            .to_owned();
                    }
                }
            }
        }
        let run = rig::engine::run_scenario(&scenario);
        eprintln!(
            "native scenario {case}: steps={} findings={:?}",
            run.steps_run, run.findings
        );
        assert!(
            run.raw_passed && run.findings.is_empty(),
            "original {case} scenario failed"
        );
        assert_eq!(
            run.steps_run,
            scenario.steps.len(),
            "scenario did not execute its complete step roster"
        );
        let steps = serde_json::to_value(&scenario.steps).expect("serialize original step roster");
        let mut captures = serde_json::Map::new();
        for step in steps.as_array().expect("step array") {
            if let Some(capture) = step.get("capture").and_then(serde_json::Value::as_object) {
                for key in capture.keys() {
                    captures.insert(
                        key.clone(),
                        run.vars
                            .get(key)
                            .unwrap_or_else(|| panic!("missing capture {key}"))
                            .clone(),
                    );
                }
            }
            if step["type"] == "wait_until" {
                let key = format!(
                    "{}_recovered_secs",
                    step["name"].as_str().expect("wait identity")
                );
                captures.insert(
                    key.clone(),
                    run.vars
                        .get(&key)
                        .unwrap_or_else(|| panic!("missing recovery measurement {key}"))
                        .clone(),
                );
            }
        }
        evidence(
            "scenario",
            case,
            serde_json::json!({"steps":run.steps_run,"expected_steps":scenario.steps.len(),"captures":captures}),
        );
        if let Some(profile) = &scenario.load {
            let stats = rig::engine::loadgen::run(profile, &run.vars);
            eprintln!("native load {case}: {stats:?}");
            assert!(
                stats.abort.is_none() && stats.total > 0,
                "load did no actual work"
            );
            let offered =
                u64::from(profile.target_qps) * (profile.duration_secs - profile.warmup_secs);
            assert_eq!(stats.total, offered, "incomplete offered request schedule");
            assert!(
                stats.achieved_qps >= f64::from(profile.target_qps) * 0.95,
                "original 95% honesty floor failed"
            );
            let pins = rig::pins::load_pins(&root.join("tests/rig/config/pins"))
                .expect("original load pins");
            let mut checked = 0;
            for pin in pins.iter().filter(|p| p.matches(&run.scenario_id)) {
                let value = stats.get(&pin.metric).expect("pin metric must be measured");
                assert!(value.is_finite(), "non-finite load metric");
                // This owner is the original overload contract. A future
                // ratcheted workload must supply actual baseline evidence.
                assert!(
                    pin.ratchet.is_none(),
                    "ratchet requires measured base evidence"
                );
                let floor = pin.floor.expect("missing original load floor");
                assert!(value <= floor, "{}={value} exceeds {floor}", pin.metric);
                checked += 1;
            }
            assert!(checked > 0, "no matching load pins");
            evidence(
                "load",
                case,
                serde_json::json!({"p99_ms":stats.p99_ms,"error_rate":stats.error_rate,"achieved_qps":stats.achieved_qps,"total":stats.total,"failed":stats.failed,"pins_checked":checked,"profile":profile}),
            );
        }
        server.get_text("/readyz");
    }
}
