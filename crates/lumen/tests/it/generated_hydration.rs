//! Generated ordered hydration helpers (issue #80).
//!
//! `lumen spec gen` writes `hydrate.ts`, `hydrate.py` and `hydrate.rs` next to
//! each generated client. Each helper takes a Lumen search response and one
//! bulk-fetch callback, calls it once with the unique hit IDs, restores hit
//! order, reports missing IDs, and keeps score, cursor and total.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_temp_dir(label: &str) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "lumen-hydration-{label}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create hydration temp dir");
    dir
}

fn require_command(binary: &str) {
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .unwrap_or_else(|error| panic!("required hydration tool {binary}: {error}"));
    assert!(
        output.status.success(),
        "required hydration tool {binary} failed"
    );
}

fn spec_gen(lang: &str, out: &Path) -> Output {
    let output = Command::new(env!("CARGO_BIN_EXE_lumen"))
        .args(["spec", "gen", "--lang", lang, "--out"])
        .arg(out)
        .output()
        .expect("run lumen spec gen");
    assert!(
        output.status.success(),
        "lumen spec gen --lang {lang} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

#[test]
fn hydrate_files_are_written_and_exported() {
    let ts = unique_temp_dir("files-ts");
    spec_gen("ts", &ts);
    assert!(ts.join("hydrate.ts").is_file(), "hydrate.ts is written");
    let ts_index = fs::read_to_string(ts.join("index.ts")).expect("read index.ts");
    assert!(
        ts_index.contains(r#"export * from "./hydrate";"#),
        "index.ts exports hydrate"
    );
    assert!(
        ts_index.contains(r#"export * from "./client";"#),
        "negative control: original client export is kept"
    );

    let py = unique_temp_dir("files-py");
    spec_gen("py", &py);
    assert!(py.join("hydrate.py").is_file(), "hydrate.py is written");
    let py_init = fs::read_to_string(py.join("__init__.py")).expect("read __init__.py");
    assert!(
        py_init.contains("from .hydrate import"),
        "__init__.py exports hydrate"
    );

    let rs = unique_temp_dir("files-rs");
    spec_gen("rust", &rs);
    assert!(rs.join("hydrate.rs").is_file(), "hydrate.rs is written");
    let rs_mod = fs::read_to_string(rs.join("mod.rs")).expect("read mod.rs");
    assert!(
        rs_mod.contains("pub mod hydrate;"),
        "mod.rs declares hydrate"
    );
}

#[test]
fn python_hydration_orders_and_reports_missing() {
    require_command("python3");
    let dir = unique_temp_dir("py-run");
    spec_gen("py", &dir);
    let script = dir.join("smoke.py");
    fs::write(
        &script,
        r#"import asyncio
import importlib.util
import sys
import types

spec = importlib.util.spec_from_file_location("hydrate", sys.argv[1])
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)

def hit(external_id, score):
    return types.SimpleNamespace(external_id=external_id, score=score)

hits = [hit("b", 0.9), hit("a", 0.8), hit("b", 0.7), hit("z", 0.6)]
response = types.SimpleNamespace(hits=hits, cursor="c1", total=7)
calls = []

def fetch(ids):
    calls.append(list(ids))
    return {"a": "A", "b": "B"}

r = h.hydrate_hits(response, fetch)
assert calls == [["b", "a", "z"]], calls
assert [i.record for i in r.items] == ["B", "A", "B"], r.items
assert [i.hit.score for i in r.items] == [0.9, 0.8, 0.7], r.items
assert [m.external_id for m in r.missing] == ["z"], r.missing
assert r.cursor == "c1" and r.total == 7, r

calls.clear()
empty = h.hydrate_hits(types.SimpleNamespace(hits=[], cursor=None, total=0), fetch)
assert calls == [], calls
assert empty.items == [] and empty.missing == [], empty

def boom(ids):
    raise RuntimeError("boom")

try:
    h.hydrate_hits(response, boom)
    raise SystemExit("callback error was swallowed")
except RuntimeError as error:
    assert str(error) == "boom", error

async def slow(ids):
    await asyncio.sleep(10)
    return {}

async def cancelled():
    try:
        await asyncio.wait_for(h.hydrate_hits_async(response, slow), 0.05)
        raise SystemExit("cancellation was swallowed")
    except TimeoutError:
        pass

asyncio.run(cancelled())
print("PASS")
"#,
    )
    .expect("write python hydration smoke");
    let output = Command::new("python3")
        .arg("-I")
        .arg(&script)
        .arg(dir.join("hydrate.py"))
        .output()
        .expect("run python hydration smoke");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("PASS"),
        "python hydration smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn typescript_hydration_orders_and_reports_missing() {
    require_command("tsc");
    require_command("node");
    let dir = unique_temp_dir("ts-run");
    spec_gen("ts", &dir);
    fs::write(
        dir.join("smoke.ts"),
        r#"import { hydrateHits, HydrationResponse } from "./hydrate";

declare const process: { exit(code?: number): never };

type Hit = { external_id: string; score: number };

const hits: Hit[] = [
  { external_id: "b", score: 0.9 },
  { external_id: "a", score: 0.8 },
  { external_id: "b", score: 0.7 },
  { external_id: "z", score: 0.6 },
];
const response: HydrationResponse<Hit> = { hits, cursor: "c1", total: 7 };

function check(condition: boolean, message: string): void {
  if (!condition) {
    console.error(message);
    process.exit(1);
  }
}

async function main(): Promise<void> {
  const calls: string[][] = [];
  const result = await hydrateHits(response, async (ids) => {
    calls.push([...ids]);
    return new Map([["a", "A"], ["b", "B"]]);
  });
  check(JSON.stringify(calls) === JSON.stringify([["b", "a", "z"]]), "one call with unique ids");
  check(
    JSON.stringify(result.items.map((i) => i.record)) === JSON.stringify(["B", "A", "B"]),
    "hit order restored",
  );
  check(
    JSON.stringify(result.items.map((i) => i.hit.score)) === JSON.stringify([0.9, 0.8, 0.7]),
    "score kept",
  );
  check(
    JSON.stringify(result.missing.map((m) => m.external_id)) === JSON.stringify(["z"]),
    "missing id reported",
  );
  check(result.cursor === "c1" && result.total === 7, "cursor and total kept");

  calls.length = 0;
  await hydrateHits({ hits: [], cursor: null, total: 0 }, async (ids) => {
    calls.push([...ids]);
    return new Map<string, string>();
  });
  check(calls.length === 0, "empty hits do not call the callback");

  let threw = false;
  try {
    await hydrateHits(response, async () => {
      throw new Error("boom");
    });
  } catch (error) {
    threw = (error as Error).message === "boom";
  }
  check(threw, "callback error propagates");

  const controller = new AbortController();
  controller.abort();
  let rejected = false;
  let called = false;
  try {
    await hydrateHits(
      response,
      async () => {
        called = true;
        return new Map<string, string>();
      },
      { signal: controller.signal },
    );
  } catch {
    rejected = true;
  }
  check(rejected && !called, "aborted signal rejects before the callback runs");

  console.log("PASS");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
"#,
    )
    .expect("write typescript hydration smoke");
    let dist = dir.join("dist");
    let tsc = Command::new("tsc")
        .current_dir(&dir)
        .args([
            "--target",
            "ES2022",
            "--module",
            "commonjs",
            "--lib",
            "ES2022,DOM",
            "--skipLibCheck",
            "--outDir",
            dist.to_str().unwrap(),
            "hydrate.ts",
            "smoke.ts",
        ])
        .output()
        .expect("run tsc for hydration smoke");
    assert!(
        tsc.status.success(),
        "tsc failed for hydration smoke\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&tsc.stdout),
        String::from_utf8_lossy(&tsc.stderr)
    );
    let node = Command::new("node")
        .arg(dist.join("smoke.js"))
        .output()
        .expect("run node hydration smoke");
    assert!(
        node.status.success() && String::from_utf8_lossy(&node.stdout).contains("PASS"),
        "node hydration smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&node.stdout),
        String::from_utf8_lossy(&node.stderr)
    );
}

#[test]
fn rust_hydration_orders_and_reports_missing() {
    require_command("cargo");
    let dir = unique_temp_dir("rs-run");
    // The generated client uses `crate::models`, so its files form the root of a
    // library crate: `mod.rs` becomes `lib.rs`.
    let src = dir.join("src");
    spec_gen("rust", &src);
    fs::rename(src.join("mod.rs"), src.join("lib.rs")).expect("generated mod.rs becomes lib.rs");
    fs::write(
        dir.join("Cargo.toml"),
        r#"[package]
name = "generated-lumen-hydration-smoke"
version = "0.0.0"
edition = "2021"

[dependencies]
reqwest = { version = "0.12", default-features = false, features = ["blocking", "json", "rustls-tls-native-roots"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
"#,
    )
    .expect("write hydration smoke Cargo.toml");
    fs::write(
        dir.join("src/main.rs"),
        r#"use generated_lumen_hydration_smoke::hydrate;
use generated_lumen_hydration_smoke::models::{SearchHit, SearchResponse};
use std::collections::HashMap;

type Fetched = Result<HashMap<String, String>, String>;

fn hit(external_id: &str, score: f64) -> SearchHit {
    SearchHit {
        external_id: external_id.to_string(),
        score,
    }
}

fn response(hits: Vec<SearchHit>, cursor: Option<String>, total: i64) -> SearchResponse {
    SearchResponse {
        cursor,
        hits,
        took_ms: 0,
        took_us: None,
        total,
    }
}

fn check(condition: bool, message: &str) {
    if !condition {
        eprintln!("{message}");
        std::process::exit(1);
    }
}

fn main() {
    let mut calls: Vec<Vec<String>> = Vec::new();
    let hits = vec![hit("b", 0.9), hit("a", 0.8), hit("b", 0.7), hit("z", 0.6)];
    let result = hydrate::hydrate_hits(
        response(hits, Some("c1".to_string()), 7),
        |ids: &[String]| -> Fetched {
            calls.push(ids.to_vec());
            Ok(HashMap::from([
                ("a".to_string(), "A".to_string()),
                ("b".to_string(), "B".to_string()),
            ]))
        },
    )
    .unwrap_or_else(|error| panic!("hydrate failed: {error}"));

    let want_ids: Vec<String> = ["b", "a", "z"].iter().map(|s| s.to_string()).collect();
    check(calls == vec![want_ids], "one call with unique ids");
    let records: Vec<&str> = result.items.iter().map(|i| i.record.as_str()).collect();
    check(records == ["B", "A", "B"], "hit order restored");
    let scores: Vec<f64> = result.items.iter().map(|i| i.hit.score).collect();
    check(scores == [0.9, 0.8, 0.7], "score kept");
    let missing: Vec<&str> = result.missing.iter().map(|h| h.external_id.as_str()).collect();
    check(missing == ["z"], "missing id reported");
    check(result.cursor.as_deref() == Some("c1"), "cursor kept");
    check(result.total == 7, "total kept");

    let mut empty_calls = 0;
    let empty = hydrate::hydrate_hits(
        response(Vec::new(), None, 0),
        |_: &[String]| -> Fetched {
            empty_calls += 1;
            Ok(HashMap::new())
        },
    );
    check(empty.is_ok(), "empty hits succeed");
    check(empty_calls == 0, "empty hits do not call the callback");

    let failed = hydrate::hydrate_hits(
        response(vec![hit("a", 0.1)], None, 1),
        |_: &[String]| -> Fetched { Err("boom".to_string()) },
    );
    match failed {
        Err(error) => check(error == "boom", "callback error propagates"),
        Ok(_) => check(false, "callback error was swallowed"),
    }

    println!("PASS");
}
"#,
    )
    .expect("write hydration smoke main.rs");
    let output = Command::new("cargo")
        .current_dir(&dir)
        .args(["run", "--quiet"])
        .env("CARGO_TARGET_DIR", dir.join("target"))
        .output()
        .expect("run cargo for hydration smoke");
    assert!(
        output.status.success() && String::from_utf8_lossy(&output.stdout).contains("PASS"),
        "rust hydration smoke failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
