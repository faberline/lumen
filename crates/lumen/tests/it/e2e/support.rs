//! What the e2e suites share: their inputs, the evidence lines, `kubectl`,
//! the HTTP client, and the 100k-doc fixture.

use std::io::Write as _;
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// An input the suite cannot run without. A missing one fails the test
/// (rust-tests.md rule 4); it never skips.
pub fn required(name: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => panic!("{name} is not set; this e2e test needs it (see tests/it/e2e/mod.rs)"),
    }
}

/// A Kubernetes object name or namespace: an RFC 1123 label.
pub fn is_dns_label(s: &str) -> bool {
    let b = s.as_bytes();
    let ok = |c: &u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    !b.is_empty()
        && b.len() <= 63
        && ok(&b[0])
        && ok(&b[b.len() - 1])
        && b.iter().all(|c| ok(c) || *c == b'-')
}

pub fn is_lower_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

/// One line on the process's stdout. libtest captures `println!` and drops it
/// when the test passes; the pod log is the evidence, so write past it.
pub fn say(line: impl AsRef<str>) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{}", line.as_ref());
    let _ = out.flush();
}

/// `E2E_EVIDENCE {"suite":…,"step":…,"data":…}` (rust-tests.md rule 9).
pub fn evidence(suite: &str, step: &str, data: Value) {
    say(format!(
        "E2E_EVIDENCE {}",
        json!({"suite": suite, "step": step, "data": data})
    ));
}

/// A numbered step of a journey: says where the run is, and records how long
/// the step took as `timing` evidence when it ends.
pub struct Step {
    suite: &'static str,
    name: String,
    start: Instant,
}

impl Step {
    pub fn begin(suite: &'static str, name: impl Into<String>) -> Self {
        let name = name.into();
        say(format!(">> [{suite}] {name}"));
        Self {
            suite,
            name,
            start: Instant::now(),
        }
    }

    pub fn end(self) {
        let seconds = self.start.elapsed().as_secs();
        say(format!("   ({} finished in {seconds}s)", self.name));
        evidence(
            self.suite,
            "timing",
            json!({"step": self.name, "seconds": seconds}),
        );
    }
}

// ---------------------------------------------------------------------------
// kubectl
// ---------------------------------------------------------------------------

/// `kubectl` in one namespace, with the Job's ServiceAccount.
pub struct Kube {
    pub namespace: String,
}

impl Kube {
    pub fn from_env() -> Self {
        let namespace = required("E2E_NAMESPACE");
        assert!(
            is_dns_label(&namespace),
            "invalid E2E_NAMESPACE: {namespace}"
        );
        Self { namespace }
    }

    /// `kubectl -n <namespace> <args>`: stdout, or the failure with stderr.
    pub fn try_run(&self, args: &[&str]) -> Result<String, String> {
        let out = Command::new("kubectl")
            .arg("-n")
            .arg(&self.namespace)
            .args(args)
            .output()
            .map_err(|e| format!("kubectl {args:?}: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "kubectl {args:?} failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    pub fn run(&self, args: &[&str]) -> String {
        self.try_run(args).unwrap_or_else(|e| panic!("{e}"))
    }

    /// `kubectl get <args> -o json`.
    pub fn get(&self, args: &[&str]) -> Value {
        let mut all = vec!["get"];
        all.extend_from_slice(args);
        all.extend_from_slice(&["-o", "json"]);
        let text = self.run(&all);
        serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("kubectl {all:?}: not JSON ({e}): {text}"))
    }

    /// Whether `kind/name` exists. NotFound is no; any other failure fails
    /// the test, so a missing permission never reads as "absent".
    pub fn exists(&self, what: &str) -> bool {
        match self.try_run(&["get", what, "-o", "name"]) {
            Ok(_) => true,
            Err(e) if e.contains("(NotFound)") => false,
            Err(e) => panic!("{e}"),
        }
    }

    /// Prints the command's output, or its failure, without failing.
    pub fn show(&self, args: &[&str]) {
        match self.try_run(args) {
            Ok(out) => say(out.trim_end()),
            Err(e) => say(format!("   {e}")),
        }
    }

    pub fn pods(&self, label: &str) -> Value {
        self.get(&["pods", "-l", label])
    }
}

/// Prints what the runner would otherwise have to go and look at when a
/// journey fails. aw keeps this pod's log, every pod's log, and `describe
/// pods` as evidence.
pub struct DumpOnFailure<'a> {
    pub suite: &'static str,
    pub kube: &'a Kube,
}

impl Drop for DumpOnFailure<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        say(format!("!! [{}] FAIL; cluster state follows", self.suite));
        self.kube.show(&["get", "lumens.lumen.dev", "-o", "yaml"]);
        self.kube.show(&[
            "get",
            "statefulsets,deployments,pods,pvc,endpoints",
            "-o",
            "wide",
        ]);
        self.kube
            .show(&["logs", "deploy/lumen-operator", "--tail=400"]);
    }
}

/// Polls until at least `expected` pods match `label`, so `kubectl wait`
/// does not fail with "no matching resources" before the controller has
/// made them.
pub fn wait_pods_exist(kube: &Kube, label: &str, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(out) = kube.try_run(&["get", "pod", "-l", label, "-o", "name"]) {
            if out.lines().filter(|l| !l.trim().is_empty()).count() >= expected {
                return;
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    kube.show(&["get", "pod", "-l", label]);
    panic!(
        "expected at least {expected} pod(s) for {label} within {}s",
        timeout.as_secs()
    );
}

pub fn wait_ready(kube: &Kube, label: &str, expected: usize, timeout: Duration) {
    say(format!(
        "   waiting up to {}s for serving pods ({label}) Ready",
        timeout.as_secs()
    ));
    wait_pods_exist(kube, label, expected, timeout);
    let timeout = format!("--timeout={}s", timeout.as_secs());
    kube.run(&[
        "wait",
        "--for=condition=Ready",
        "pod",
        "-l",
        label,
        &timeout,
    ]);
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// The server's HTTP API at a base URL.
pub struct Api {
    pub base: String,
    client: reqwest::Client,
}

impl Api {
    pub fn new(base: &str) -> Self {
        let base = base.trim_end_matches('/').to_string();
        assert!(
            base.starts_with("http://") || base.starts_with("https://"),
            "endpoint must be a base URL, got {base}"
        );
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .no_proxy()
            .build()
            .expect("HTTP client");
        Self { base, client }
    }

    pub fn from_env() -> Self {
        Self::new(&required("E2E_ENDPOINT"))
    }

    /// One request. A transport failure (refused or reset connection, empty
    /// reply, timeout) waits and repeats it, up to 5 attempts; an HTTP error
    /// status fails at once. Every request made through here is safe to
    /// repeat: index items carry external ids.
    pub async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&Value>,
        timeout: Duration,
    ) -> Value {
        let url = format!("{}{path}", self.base);
        let mut last = String::new();
        for attempt in 1..=5u64 {
            let mut req = self.client.request(method.clone(), &url).timeout(timeout);
            if let Some(body) = body {
                req = req.json(body);
            }
            let sent = match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    resp.bytes().await.map(|bytes| (status, bytes))
                }
                Err(e) => Err(e),
            };
            match sent {
                Ok((status, bytes)) => {
                    let text = String::from_utf8_lossy(&bytes);
                    assert!(
                        !status.is_client_error() && !status.is_server_error(),
                        "HTTP {status} from {method} {url}: {text}"
                    );
                    if text.trim().is_empty() {
                        return Value::Null;
                    }
                    return serde_json::from_str(&text)
                        .unwrap_or_else(|_| Value::String(text.into_owned()));
                }
                Err(e) => {
                    last = format!("{e:?}");
                    say(format!(
                        "   transport failure ({method} {url}, attempt {attempt}): {e}; retrying"
                    ));
                    tokio::time::sleep(Duration::from_secs(attempt * 3)).await;
                }
            }
        }
        panic!("request still failing after 5 attempts: {method} {url}: {last}");
    }

    pub async fn get(&self, path: &str, timeout: Duration) -> Value {
        self.call(reqwest::Method::GET, path, None, timeout).await
    }

    pub async fn post(&self, path: &str, body: &Value, timeout: Duration) -> Value {
        self.call(reqwest::Method::POST, path, Some(body), timeout)
            .await
    }

    pub async fn put(&self, path: &str, body: &Value, timeout: Duration) -> Value {
        self.call(reqwest::Method::PUT, path, Some(body), timeout)
            .await
    }

    /// Whether `path` answers with a success status within 5s, once.
    pub async fn answers(&self, path: &str) -> bool {
        let url = format!("{}{path}", self.base);
        match self
            .client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) => resp.status().is_success(),
            Err(_) => false,
        }
    }

    /// Polls `path` every `every` until it answers or `timeout` passes.
    pub async fn wait_for(&self, path: &str, timeout: Duration, every: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.answers(path).await {
                return true;
            }
            tokio::time::sleep(every).await;
        }
        false
    }
}

/// How many search hits carry `external_id`.
pub fn hits_for(resp: &Value, external_id: &str) -> usize {
    resp["hits"]
        .as_array()
        .unwrap_or_else(|| panic!("search response has no hits array: {resp}"))
        .iter()
        .filter(|hit| hit["external_id"] == external_id)
        .count()
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Each doc is 2 index items (bio + email); the public HTTP cap is
/// MAX_INDEX_BATCH_SIZE=1000 items per request.
pub const DOCS_PER_BATCH: u64 = 500;

const ADJ: &[&str] = &[
    "fast", "quiet", "fierce", "kind", "bold", "calm", "eager", "proud", "happy", "curious",
    "brave", "gentle", "lucky", "wise", "young", "old",
];
const NOUN: &[&str] = &[
    "engineer",
    "designer",
    "writer",
    "doctor",
    "teacher",
    "musician",
    "scientist",
    "explorer",
    "chef",
    "pilot",
    "farmer",
    "architect",
];
const LANG: &[&str] = &[
    "rust",
    "python",
    "go",
    "typescript",
    "kotlin",
    "swift",
    "java",
    "c++",
    "haskell",
    "ocaml",
    "elixir",
    "ruby",
];
const HOBBY: &[&str] = &[
    "hiking", "cycling", "running", "reading", "gaming", "cooking", "painting", "writing",
    "climbing", "skiing", "surfing",
];

fn h(i: u64, k: u64) -> u64 {
    ((i + 1) * k) % 1_000_003
}

fn pick(words: &[&'static str], i: u64, k: u64) -> &'static str {
    words[(h(i, k) % words.len() as u64) as usize]
}

/// The two index items of doc `i`, the same shape as scripts/load-fixture.py:
/// one `bio` from four word lists, and one `email`, about 5% of them from a
/// pool of 50 shared addresses so the duplicates endpoint has groups to
/// return. A fixed hash of the doc number replaces the seeded RNG, so every
/// run indexes the same bytes.
fn doc(i: u64) -> [Value; 2] {
    let id = format!("u{i:07}");
    let bio = format!(
        "a {} {} who codes in {} and loves {}",
        pick(ADJ, i, 7919),
        pick(NOUN, i, 104_729),
        pick(LANG, i, 1_299_709),
        pick(HOBBY, i, 15_485_863)
    );
    let email = if h(i, 32_452_843).is_multiple_of(20) {
        format!("shared{}@example.com", h(i, 49_979_687) % 50)
    } else {
        format!("user{i}@example.com")
    };
    [
        json!({"external_id": id, "field": "bio", "value": bio}),
        json!({"external_id": id, "field": "email", "value": email}),
    ]
}

pub fn batch_count(docs: u64) -> u64 {
    docs.div_ceil(DOCS_PER_BATCH)
}

/// Request body `b` of a `docs`-doc fixture: at most DOCS_PER_BATCH docs, with
/// a request_id unique per batch (the server dedups a repeated id as an
/// idempotent retry).
pub fn batch(docs: u64, b: u64) -> Value {
    let items: Vec<Value> = (b * DOCS_PER_BATCH..((b + 1) * DOCS_PER_BATCH).min(docs))
        .flat_map(doc)
        .collect();
    json!({"items": items, "request_id": format!("e2e-gke-{docs}-{b}")})
}

mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    /// The bytes k8s/gke-test-job/base/e2e.sh's jq generator wrote for the
    /// 100k fixture (one compact body per line), so the GKE suites index
    /// exactly what they indexed before the move to Rust.
    #[test]
    fn fixture_is_the_same_bytes_as_the_retired_jq_generator() {
        let docs = 100_000;
        assert_eq!(batch_count(docs), 200);
        let mut sha = Sha256::new();
        let mut items = 0;
        for b in 0..batch_count(docs) {
            let body = batch(docs, b);
            items += body["items"].as_array().unwrap().len() as u64;
            sha.update(serde_json::to_string(&body).unwrap());
            sha.update(b"\n");
        }
        assert_eq!(items, 2 * docs);
        assert_eq!(
            format!("{:x}", sha.finalize()),
            "56052727b45081159f3d8675862aac5aff613014b94f7f63c331aaffc338a808"
        );
        assert_eq!(
            doc(0)[0]["value"],
            "a old musician who codes in java and loves reading"
        );
        assert_eq!(doc(6)[1]["value"], "shared12@example.com");
        assert_eq!(batch_count(1201), 3);
        assert_eq!(batch(1201, 2)["items"].as_array().unwrap().len(), 402);
    }

    #[test]
    fn names_and_hex() {
        assert!(is_dns_label("lumen-gha-1-1"));
        assert!(!is_dns_label("Lumen"));
        assert!(!is_dns_label("-lumen"));
        assert!(!is_dns_label(""));
        assert!(is_lower_hex("0123abcd", 8));
        assert!(!is_lower_hex("0123ABCD", 8));
        assert!(!is_lower_hex("0123abc", 8));
    }
}
