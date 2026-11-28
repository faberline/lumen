//! Idle resident memory per indexed value, measured after a segment reopen.
//!
//! Its own test binary, like `disk_scale_proof.rs`, because it measures
//! whole-process RSS, which other cases in the same process would inflate.
//!
//! Reproduces the measurement of faberline/lumen#577: after a restart and with
//! no requests, how many bytes of anonymous memory stay resident per indexed
//! value. The data shape has 10 values per document (see `schema`).
//!
//!   * `values_per_doc_matches_schema` — fast, runs in the default `cargo test`.
//!   * `bytes_per_value_report` — heavy, `#[ignore]`; run with `--ignored`.

use std::collections::BTreeMap;

use lumen::storage::Engine;
use lumen::types::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType, FieldValue, IndexItem, IndexRequest,
};

mod procmem {
    /// Discard unused allocator pages, so they do not count as idle state.
    pub fn release_unused() {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        // SAFETY: malloc_trim only releases unused pages owned by the allocator.
        unsafe {
            libc::malloc_trim(0);
        }
        #[cfg(target_os = "macos")]
        unsafe {
            unsafe extern "C" {
                fn malloc_zone_pressure_relief(zone: *mut libc::c_void, goal: usize) -> usize;
            }
            // SAFETY: a null zone requests all zones; no live allocation is freed.
            malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
        }
    }

    /// Physical resident set size in bytes, or `None` when the platform has no backend.
    pub fn rss_bytes() -> Option<u64> {
        memory_stats::memory_stats().map(|m| m.physical_mem as u64)
    }
}

const VALUES_PER_DOC: u64 = 10;
const TEXT_VOCAB: u64 = 400;

/// Splitmix64 — a pure mixing function over `i`. Each call site folds in a salt.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Text body for doc `i`: six tokens from a small vocabulary.
fn body_of(i: u64) -> String {
    let mut s = String::new();
    for t in 0..6u64 {
        if t > 0 {
            s.push(' ');
        }
        let tok = mix(i.wrapping_mul(7).wrapping_add(t) ^ 0xC3) % TEXT_VOCAB;
        s.push_str(&format!("t{tok}"));
    }
    s
}

fn num_spec() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Number,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
        facetable: false,
    }
}

fn kw_spec() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Keyword,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
        facetable: false,
    }
}

fn body_spec() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Text,
        analyzer: Some(Analyzer::WhitespaceLower),
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
        facetable: false,
    }
}

fn eid_of(i: u64) -> String {
    format!("d{i}")
}

/// Ten indexed values per document: 3 Number, 4 Keyword, 3 Text.
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    for name in ["n1", "n2", "n3"] {
        fields.insert(name.to_string(), num_spec());
    }
    for name in ["k1", "k2", "k3", "k4"] {
        fields.insert(name.to_string(), kw_spec());
    }
    for name in ["t1", "t2", "t3"] {
        fields.insert(name.to_string(), body_spec());
    }
    CreateCollectionRequest { fields }
}

/// Index docs `[0, hi)` into `coll`, one batch per doc (mirrors the live write
/// path). Every 4096 documents and at the last one, the engine is snapshotted to
/// a file and restored, so pending changes stay under the 256 MiB budget. Do not
/// remove this: it is the same setup as `disk_scale_proof.rs`.
fn index_range(e: &Engine, coll: &str, hi: u64) {
    let setup = tempfile::tempdir().expect("owned setup checkpoint directory");
    for i in 0..hi {
        let eid = eid_of(i);
        let k3 = format!("{:08}", mix(i ^ 0xB3) % 100_000_000);
        let k4 = format!("cat-{:038}", mix(i ^ 0xB4) % 274);
        let values: [(&str, FieldValue); 10] = [
            ("n1", FieldValue::Number((mix(i ^ 0xA1) % 855) as f64)),
            ("n2", FieldValue::Number((1_700_000_000_000 + i) as f64)),
            ("n3", FieldValue::Number((mix(i ^ 0xA3) % 100_000) as f64)),
            (
                "k1",
                FieldValue::String(format!("user-{}", mix(i ^ 0xB1) % 10)),
            ),
            ("k2", FieldValue::String("false".to_string())),
            ("k3", FieldValue::String(k3.clone())),
            ("k4", FieldValue::String(k4.clone())),
            ("t1", FieldValue::String(k3)),
            ("t2", FieldValue::String(k4)),
            ("t3", FieldValue::String(body_of(i))),
        ];
        let items: Vec<IndexItem> = values
            .into_iter()
            .map(|(field, value)| IndexItem {
                external_id: eid.clone(),
                field: field.to_string(),
                value,
                version: None,
            })
            .collect();
        e.index(
            coll,
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        if (i + 1).is_multiple_of(4096) || i + 1 == hi {
            let path = setup.path().join("setup.json");
            let mut file = std::fs::File::create(&path).expect("create setup snapshot");
            {
                use std::io::Write;
                let snapshot = e.snapshot().expect("capture complete setup snapshot");
                let mut writer = std::io::BufWriter::new(&mut file);
                serde_json::to_writer(&mut writer, &snapshot).expect("persist setup snapshot");
                writer.flush().expect("flush complete setup snapshot");
            }
            file.sync_all().expect("sync setup snapshot");
            std::fs::File::open(setup.path())
                .and_then(|directory| directory.sync_all())
                .expect("sync setup snapshot directory");
            drop(file);
            let snapshot = serde_json::from_reader(std::io::BufReader::new(
                std::fs::File::open(&path).expect("read persisted setup snapshot"),
            ))
            .expect("decode persisted setup snapshot");
            e.restore(snapshot)
                .expect("restore complete RAM setup state");
            assert_eq!(
                e.stats(coll).unwrap().documents_indexed,
                i + 1,
                "setup publication must retain every indexed document"
            );
        }
    }
    // All snapshot DTOs, buffers and files are gone before either RSS sample.
    drop(setup);
}

/// Total size of the `.lseg` segment files under `dir`.
fn segment_bytes(dir: &std::path::Path) -> u64 {
    fn walk(p: &std::path::Path, acc: &mut u64) {
        let Ok(rd) = std::fs::read_dir(p) else { return };
        for ent in rd.flatten() {
            let path = ent.path();
            if path.is_dir() {
                walk(&path, acc);
            } else if path.extension().and_then(|e| e.to_str()) == Some("lseg") {
                if let Ok(md) = std::fs::metadata(&path) {
                    *acc += md.len();
                }
            }
        }
    }
    let mut acc = 0u64;
    walk(dir, &mut acc);
    acc
}

/// Index `n` docs, seal them, drop the engine, reopen from the segments and
/// return `(idle RSS growth in bytes, on-disk segment bytes)`. No query runs
/// after the reopen: the report measures idle RSS only.
fn idle_rss_after_reopen(n: u64) -> Option<(u64, u64)> {
    procmem::rss_bytes()?;
    let dir = tempfile::tempdir().unwrap();
    let on_disk;
    {
        let live = Engine::new();
        live.create_collection("c", schema()).unwrap();
        index_range(&live, "c", n);
        procmem::release_unused();
        live.flush_to_segments(dir.path(), 1).unwrap();
        on_disk = segment_bytes(dir.path());
        assert!(on_disk > 0, "no segment bytes were written");
        // `live` is dropped here; only the segment files remain.
    }
    procmem::release_unused();
    let baseline = procmem::rss_bytes()?;

    let reopened = Engine::new();
    reopened.reopen_from_segment_dir(dir.path()).unwrap();
    assert_eq!(
        reopened.stats("c").unwrap().documents_indexed,
        n,
        "reopened document count must equal the indexed count"
    );
    procmem::release_unused();
    let after = procmem::rss_bytes()?;
    Some((after.saturating_sub(baseline), on_disk))
}

#[test]
fn values_per_doc_matches_schema() {
    let sch = schema();
    assert_eq!(sch.fields.len() as u64, VALUES_PER_DOC);
    let names: Vec<&str> = sch.fields.keys().map(String::as_str).collect();
    assert_eq!(
        names,
        ["k1", "k2", "k3", "k4", "n1", "n2", "n3", "t1", "t2", "t3"]
    );

    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    index_range(&e, "c", 5);
    assert_eq!(e.stats("c").unwrap().documents_indexed, 5);
}

#[test]
#[ignore = "heavy: measures process RSS; run with --ignored"]
fn bytes_per_value_report() {
    let n: u64 = std::env::var("LUMEN_IDLE_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);
    let mib = |b: u64| b as f64 / (1024.0 * 1024.0);

    eprintln!(
        "\n{:>10} {:>12} {:>14} {:>16} {:>14} {:>14} {:>10}",
        "docs",
        "values",
        "idle_delta_MiB",
        "bytes_per_value",
        "bytes_per_doc",
        "on_disk_MiB",
        "delta/disk"
    );
    let mut per_value = Vec::new();
    for docs in [n, 2 * n] {
        let Some((delta, on_disk)) = idle_rss_after_reopen(docs) else {
            eprintln!("NO SIGNAL: process RSS is not available on this platform");
            return;
        };
        if delta == 0 {
            eprintln!("NO SIGNAL: idle_delta is 0, the platform gave no RSS signal");
            return;
        }
        let values = docs * VALUES_PER_DOC;
        let bpv = delta as f64 / values as f64;
        let bpd = delta as f64 / docs as f64;
        eprintln!(
            "{docs:>10} {values:>12} {:>14.1} {bpv:>16.1} {bpd:>14.1} {:>14.1} {:>10.2}",
            mib(delta),
            mib(on_disk),
            delta as f64 / on_disk as f64,
        );
        per_value.push(bpv);
    }

    assert!(
        per_value.iter().all(|v| *v > 0.0),
        "bytes_per_value must be positive at both sizes"
    );
    let lo = per_value.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = per_value.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    assert!(
        hi < 2.0 * lo,
        "bytes_per_value differs by a factor of 2 or more between n and 2n: {per_value:?}"
    );
}
