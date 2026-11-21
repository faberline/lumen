use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use super::*;
use crate::index::application::engine::Engine;
use crate::persistence::application::backup_set_restore::{
    restore_backup_set, DirSource, EngineSet, RestoreOptions,
};
use crate::persistence::domain::backup_manifest::tests::three_shard_set;
use crate::persistence::domain::backup_manifest::{build_backup_manifest, BACKUP_MANIFEST_KEY};
use crate::sharding::domain::tests::field;
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldType};

/// Writes the 3-shard fixture set and its manifest into `dir`.
fn store_set(dir: &Path) {
    let (map, artifacts) = three_shard_set();
    for artifact in &artifacts {
        std::fs::write(dir.join(&artifact.key), &artifact.bytes).unwrap();
    }
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    std::fs::write(dir.join(BACKUP_MANIFEST_KEY), manifest.to_bytes()).unwrap();
}

fn fresh_targets() -> EngineSet {
    EngineSet((0..3).map(|_| Arc::new(Engine::new())).collect())
}

/// The fixed query: `doc-05@example.com` in the `users` collection.
fn queries() -> BTreeMap<String, Vec<SearchRequest>> {
    BTreeMap::from([(
        "users".to_string(),
        vec![serde_json::from_value::<SearchRequest>(serde_json::json!({
            "query": {"term": {"field": "email", "value": "doc-05@example.com"}},
            "limit": 10
        }))
        .unwrap()],
    )])
}

/// Restores the fixture set into `targets` and returns the answers before a restart.
fn restored(dir: &Path, targets: &EngineSet) -> Vec<QueryAnswer> {
    restore_backup_set(
        &DirSource(dir.to_path_buf()),
        targets,
        &RestoreOptions { replace: false },
    )
    .unwrap();
    run_queries(targets, &queries())
}

fn check<'a>(report: &'a VerificationReport, name: &str) -> &'a Check {
    report
        .checks
        .iter()
        .find(|check| check.name == name)
        .unwrap()
}

#[test]
fn report_passes_for_a_clean_restore() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    let before = restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();

    let report = verify_restored_set(&manifest, &artifacts, &targets, &queries(), &before);

    assert_eq!(report.status, "passed");
    assert_eq!(report.failed_check, None);
    let names: Vec<&str> = report.checks.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        ["shard_digests", "shard_documents", "collections", "queries"]
    );
    assert!(report.checks.iter().all(|c| c.ok));
    assert_eq!(report.shards.len(), 3);
    assert_eq!(report.documents_total, 24);
    assert_eq!(report.collections, ["users"]);
    assert_eq!(report.queries_compared, 1);
    assert_eq!(report.answers.len(), 3);
    let hits: Vec<&QueryAnswer> = report.answers.iter().filter(|a| a.total == 1).collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].ids, ["doc-05"]);
}

#[test]
fn report_bytes_are_stable() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    let before = restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();

    let first = verify_restored_set(&manifest, &artifacts, &targets, &queries(), &before);
    let second = verify_restored_set(&manifest, &artifacts, &targets, &queries(), &before);
    let first_json = serde_json::to_string_pretty(&first).unwrap();
    assert_eq!(first_json, serde_json::to_string_pretty(&second).unwrap());

    let compact = serde_json::to_string(&first).unwrap();
    assert!(compact.starts_with("{\"report_version\":1,\"backup_id\":\""));
    let keys = [
        "report_version",
        "backup_id",
        "status",
        "shards",
        "collections",
        "documents_total",
        "queries_compared",
        "answers",
        "checks",
        "failed_check",
    ];
    let mut last = 0;
    for key in keys {
        let at = compact.find(&format!("\"{key}\":")).unwrap();
        assert!(at >= last, "key {key} is out of order");
        last = at;
    }
}

#[test]
fn corrupt_artifact_names_the_digest_check() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    let before = restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    let mut corrupt = artifacts.clone();
    corrupt[1].bytes[0] ^= 0x01;

    let report = verify_restored_set(&manifest, &corrupt, &targets, &queries(), &before);

    assert_eq!(report.status, "failed");
    assert_eq!(report.failed_check, Some("shard_digests".to_string()));
    assert!(!report.shards[1].digest_ok);
    assert!(report.shards[0].digest_ok);
    assert!(report.shards[2].digest_ok);
    assert_eq!(
        check(&report, "shard_digests").detail,
        "shard 1 artifact shard-1.json does not match the manifest digest"
    );
}

#[test]
fn lost_documents_name_the_documents_check() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let mut targets = fresh_targets();
    let before = restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    targets.0[2] = Arc::new(Engine::new());

    let report = verify_restored_set(&manifest, &artifacts, &targets, &queries(), &before);

    assert_eq!(report.failed_check, Some("shard_documents".to_string()));
    assert!(!report.shards[2].documents_ok);
    assert_eq!(report.shards[2].restored_documents, 0);
    assert!(!check(&report, "collections").ok);
    assert_eq!(report.checks.len(), 4);
}

#[test]
fn changed_query_answers_name_the_queries_check() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let mut targets = fresh_targets();
    let before = restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    let shard = before
        .iter()
        .find(|answer| answer.total == 1)
        .unwrap()
        .shard;
    let empty = Engine::new();
    empty
        .create_collection(
            "users",
            CreateCollectionRequest {
                fields: BTreeMap::from([("email".to_string(), field(FieldType::Keyword))]),
            },
        )
        .unwrap();
    targets.0[shard as usize] = Arc::new(empty);

    let report = verify_restored_set(&manifest, &artifacts, &targets, &queries(), &before);

    let queries_check = check(&report, "queries");
    assert!(!queries_check.ok);
    assert_eq!(
        queries_check.detail,
        format!("collection users query 0 on shard {shard} answered differently after restart")
    );
}

#[test]
fn queries_check_passes_with_no_queries() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    let none = BTreeMap::new();

    let report = verify_restored_set(&manifest, &artifacts, &targets, &none, &[]);

    let queries_check = check(&report, "queries");
    assert!(queries_check.ok);
    assert_eq!(queries_check.detail, "no queries given");
    assert_eq!(report.queries_compared, 0);
}

#[test]
fn missing_artifact_is_a_failed_digest_check() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    let before = restored(dir.path(), &targets);
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();

    let report = verify_restored_set(&manifest, &artifacts[..2], &targets, &queries(), &before);

    assert_eq!(report.failed_check, Some("shard_digests".to_string()));
    assert_eq!(
        check(&report, "shard_digests").detail,
        "shard 2 artifact is missing"
    );
}
