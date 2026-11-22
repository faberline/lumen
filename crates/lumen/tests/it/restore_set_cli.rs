//! `lumen restore-set`: restore a backup set, restart, and print the verification
//! report. Runs the CLI binary offline; no server is started.

#![cfg(feature = "backup")]
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use lumen::backup::{build_backup_manifest, ShardArtifact, BACKUP_MANIFEST_KEY};
use lumen::routing::VirtualBucketShardMap;
use lumen::storage::Engine;
use lumen::types::{
    CreateCollectionRequest, FieldSpec, FieldType, FieldValue, IndexItem, IndexRequest,
};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lumen"))
        .args(args)
        .output()
        .expect("run lumen restore-set")
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("utf-8 test path")
}

/// Writes a 3-shard backup of 24 `users` documents into `dir`.
fn write_backup_set(dir: &Path) {
    let engine = Engine::new();
    engine
        .create_collection(
            "users",
            CreateCollectionRequest {
                fields: BTreeMap::from([(
                    "email".to_string(),
                    FieldSpec {
                        field_type: FieldType::Keyword,
                        analyzer: None,
                        multi: None,
                        dim: None,
                        metric: None,
                        backend: None,
                        quantize: None,
                        facetable: false,
                    },
                )]),
            },
        )
        .unwrap();
    for i in 0..24 {
        engine
            .index(
                "users",
                IndexRequest {
                    request_id: None,
                    items: vec![IndexItem {
                        external_id: format!("doc-{i:02}"),
                        field: "email".into(),
                        value: FieldValue::String(format!("doc-{i:02}@example.com")),
                        version: None,
                    }],
                },
            )
            .unwrap();
    }
    let snapshot = engine.snapshot().unwrap();
    let map = VirtualBucketShardMap::balanced(7, 12, 3).unwrap();
    let mut artifacts = Vec::new();
    for shard in 0..3u32 {
        let buckets: BTreeSet<u32> = (0..12)
            .filter(|bucket| map.assignment_for_bucket(*bucket) == Some(shard))
            .collect();
        let subset = lumen::reshard::snapshot_bucket_subset(&snapshot, 12, &buckets).unwrap();
        let bytes = serde_json::to_vec(&subset).unwrap();
        let key = format!("shard-{shard}.json");
        std::fs::write(dir.join(&key), &bytes).unwrap();
        artifacts.push(ShardArtifact { shard, key, bytes });
    }
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    std::fs::write(dir.join(BACKUP_MANIFEST_KEY), manifest.to_bytes()).unwrap();
}

fn write_queries(dir: &Path) -> PathBuf {
    let path = dir.join("queries.json");
    std::fs::write(
            &path,
            r#"{"users":[{"query":{"term":{"field":"email","value":"doc-05@example.com"}},"limit":10}]}"#,
        )
        .unwrap();
    path
}

#[test]
fn cli_restores_restarts_and_reports() {
    let tmp = tempfile::tempdir().unwrap();
    let backup = tmp.path().join("backup");
    std::fs::create_dir_all(&backup).unwrap();
    write_backup_set(&backup);
    let queries = write_queries(tmp.path());
    let data = tmp.path().join("data");

    let out = run(&[
        "restore-set",
        "--from",
        path_str(&backup),
        "--data-root",
        path_str(&data),
        "--queries",
        path_str(&queries),
    ]);

    assert!(
        out.status.success(),
        "restore-set failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["status"], "passed");
    assert_eq!(report["shards"].as_array().unwrap().len(), 3);
    assert_eq!(report["documents_total"], 24);
    assert_eq!(report["queries_compared"], 1);
    for shard in 0..3 {
        assert!(
            data.join(format!("shard-{shard}"))
                .join("CURRENT")
                .is_file(),
            "shard {shard} has no CURRENT file after restart"
        );
    }
}

#[test]
fn cli_corrupt_object_prints_no_report_and_writes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let backup = tmp.path().join("backup");
    std::fs::create_dir_all(&backup).unwrap();
    write_backup_set(&backup);
    // A trailing space keeps the file valid JSON but changes its digest.
    let mut bytes = std::fs::read(backup.join("shard-1.json")).unwrap();
    bytes.push(b' ');
    std::fs::write(backup.join("shard-1.json"), bytes).unwrap();
    let data = tmp.path().join("data");

    let out = run(&[
        "restore-set",
        "--from",
        path_str(&backup),
        "--data-root",
        path_str(&data),
    ]);

    assert!(!out.status.success(), "a corrupt backup must fail");
    assert!(
        out.stdout.is_empty(),
        "no report may print for a refused restore"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains(
            "restore refused: shard 1 artifact shard-1.json does not match the manifest digest"
        ),
        "stderr: {stderr}"
    );
    assert!(!data.join("shard-0").join("CURRENT").exists());
}

#[test]
fn cli_refuses_a_non_empty_data_root() {
    let tmp = tempfile::tempdir().unwrap();
    let backup = tmp.path().join("backup");
    std::fs::create_dir_all(&backup).unwrap();
    write_backup_set(&backup);
    let data = tmp.path().join("data");
    std::fs::create_dir_all(data.join("shard-1")).unwrap();
    std::fs::write(data.join("shard-1").join("x"), b"x").unwrap();

    let out = run(&[
        "restore-set",
        "--from",
        path_str(&backup),
        "--data-root",
        path_str(&data),
    ]);

    assert!(
        !out.status.success(),
        "a non-empty data root must be refused"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("is not empty"), "stderr: {stderr}");
    assert!(!data.join("shard-0").join("CURRENT").exists());
}

#[test]
fn cli_restart_returns_the_same_ordered_results() {
    let tmp = tempfile::tempdir().unwrap();
    let backup = tmp.path().join("backup");
    std::fs::create_dir_all(&backup).unwrap();
    write_backup_set(&backup);
    let queries = write_queries(tmp.path());

    let answers = |data: PathBuf| -> serde_json::Value {
        let out = run(&[
            "restore-set",
            "--from",
            path_str(&backup),
            "--data-root",
            path_str(&data),
            "--queries",
            path_str(&queries),
        ]);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        report["answers"].clone()
    };

    let first = answers(tmp.path().join("a"));
    let second = answers(tmp.path().join("b"));
    assert_eq!(first, second);
    assert_eq!(first.as_array().unwrap().len(), 3);
}
