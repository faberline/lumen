use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use sha2::{Digest, Sha256};

use super::*;
use crate::index::application::engine::Engine;
use crate::persistence::domain::backup_manifest::tests::{original_engine, three_shard_set};
use crate::persistence::domain::backup_manifest::{
    build_backup_manifest, BackupManifest, BACKUP_MANIFEST_KEY,
};
use crate::sharding::domain::tests::field;
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

/// Writes the 3-shard fixture set and its manifest into `dir`.
fn store_set(dir: &Path) -> BackupManifest {
    let (map, artifacts) = three_shard_set();
    for artifact in &artifacts {
        std::fs::write(dir.join(&artifact.key), &artifact.bytes).unwrap();
    }
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    std::fs::write(dir.join(BACKUP_MANIFEST_KEY), manifest.to_bytes()).unwrap();
    manifest
}

fn fresh_targets() -> EngineSet {
    EngineSet((0..3).map(|_| Arc::new(Engine::new())).collect())
}

fn all_empty(targets: &EngineSet) -> bool {
    targets
        .0
        .iter()
        .all(|engine| engine.list_collections().unwrap().is_empty())
}

fn restore(
    dir: &Path,
    targets: &dyn ShardRestoreTarget,
    replace: bool,
) -> Result<RestoreReport, RestoreError> {
    restore_backup_set(
        &DirSource(dir.to_path_buf()),
        targets,
        &RestoreOptions { replace },
    )
}

/// The external ids that `id@example.com` finds in `engine`, with the total.
fn search_email(engine: &Engine, id: &str) -> (u64, Vec<String>) {
    let request: SearchRequest = serde_json::from_value(serde_json::json!({
        "query": {"term": {"field": "email", "value": format!("{id}@example.com")}},
        "limit": 10
    }))
    .unwrap();
    let response = engine.search("users", request).unwrap();
    (
        response.total,
        response.hits.into_iter().map(|h| h.external_id).collect(),
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Gives the target shard 2 a collection the backup does not have.
fn prefill_shard_two(targets: &EngineSet) {
    targets.0[2]
        .create_collection(
            "old",
            CreateCollectionRequest {
                fields: BTreeMap::from([("k".to_string(), field(FieldType::Keyword))]),
            },
        )
        .unwrap();
}

#[test]
fn round_trip_restores_search_results_and_source_ids() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = store_set(dir.path());
    let targets = fresh_targets();

    let report = restore(dir.path(), &targets, false).expect("restore");
    assert_eq!(report.shards_restored, 3);
    assert_eq!(report.documents, 24);
    assert_eq!(report.backup_id, manifest.backup_id);

    let original = original_engine();
    let mut all_ids = Vec::new();
    for i in 0..24 {
        let id = format!("doc-{i:02}");
        let (original_total, original_hits) = search_email(&original, &id);
        assert_eq!(original_total, 1, "original finds {id}");
        assert_eq!(original_hits, vec![id.clone()]);

        let found: Vec<(u64, Vec<String>)> = targets
            .0
            .iter()
            .map(|engine| search_email(engine, &id))
            .filter(|(total, _)| *total > 0)
            .collect();
        assert_eq!(found.len(), 1, "exactly one shard holds {id}");
        assert_eq!(found[0].0, 1);
        assert_eq!(found[0].1, original_hits);
    }
    for engine in &targets.0 {
        let snapshot = engine.snapshot().unwrap();
        all_ids.extend(snapshot.collections["users"].eid_fields.keys().cloned());
    }
    all_ids.sort();
    let expected: Vec<String> = (0..24).map(|i| format!("doc-{i:02}")).collect();
    assert_eq!(all_ids, expected, "each source id is in exactly one engine");
}

#[test]
fn missing_shard_artifact_fails_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    std::fs::remove_file(dir.path().join("shard-1.json")).unwrap();
    let targets = fresh_targets();

    let err = restore(dir.path(), &targets, false).expect_err("missing shard");
    assert_eq!(
        err.to_string(),
        "shard 1 artifact shard-1.json is missing from the backup set"
    );
    assert!(all_empty(&targets), "no shard was written");
}

#[test]
fn corrupt_artifact_fails_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let path = dir.path().join("shard-2.json");
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    let targets = fresh_targets();

    let err = restore(dir.path(), &targets, false).expect_err("corrupt object");
    assert_eq!(
        err.to_string(),
        "shard 2 artifact shard-2.json does not match the manifest digest"
    );
    assert!(all_empty(&targets), "no shard was written");
}

#[test]
fn identity_mismatch_fails_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut manifest = store_set(dir.path());
    // A forged schema with a recomputed identity passes the manifest's own check; only
    // rebuilding the manifest from the objects can catch it.
    manifest
        .collections
        .insert("users".into(), serde_json::json!({}));
    manifest.backup_id = String::new();
    manifest.backup_id = sha256_hex(&manifest.to_bytes());
    std::fs::write(dir.path().join(BACKUP_MANIFEST_KEY), manifest.to_bytes()).unwrap();
    let targets = fresh_targets();

    let err = restore(dir.path(), &targets, false).expect_err("identity mismatch");
    assert!(matches!(err, RestoreError::IdentityMismatch), "{err:?}");
    assert_eq!(
        err.to_string(),
        "backup manifest does not describe the stored objects; refusing to restore"
    );
    assert!(all_empty(&targets), "no shard was written");
}

#[test]
fn manifest_with_duplicate_shard_entry_fails_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut manifest = store_set(dir.path());
    manifest.shards[2].shard = 1;
    std::fs::write(dir.path().join(BACKUP_MANIFEST_KEY), manifest.to_bytes()).unwrap();
    let targets = fresh_targets();

    let err = restore(dir.path(), &targets, false).expect_err("duplicate shard");
    assert!(
        matches!(
            err,
            RestoreError::Manifest(ManifestError::DuplicateShard(1))
        ),
        "{err:?}"
    );
    assert!(all_empty(&targets), "no shard was written");
}

#[test]
fn missing_manifest_fails_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    std::fs::remove_file(dir.path().join(BACKUP_MANIFEST_KEY)).unwrap();
    let targets = fresh_targets();

    let err = restore(dir.path(), &targets, false).expect_err("missing manifest");
    assert!(matches!(err, RestoreError::ManifestMissing), "{err:?}");
    assert!(all_empty(&targets), "no shard was written");
}

#[test]
fn topology_mismatch_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = EngineSet((0..2).map(|_| Arc::new(Engine::new())).collect());

    let err = restore(dir.path(), &targets, false).expect_err("topology");
    assert_eq!(
        err.to_string(),
        "backup was taken from 3 shards but the target has 2"
    );
    assert!(all_empty(&targets), "no shard was written");
}

#[test]
fn non_empty_target_without_replace_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    prefill_shard_two(&targets);

    let err = restore(dir.path(), &targets, false).expect_err("refused");
    assert_eq!(
        err.to_string(),
        "shard 2 already holds data; restore without --replace is refused"
    );
    assert!(targets.0[0].list_collections().unwrap().is_empty());
    assert!(targets.0[1].list_collections().unwrap().is_empty());
    assert_eq!(
        targets.0[2].list_collections().unwrap(),
        vec!["old".to_string()]
    );
}

#[test]
fn replace_overwrites_a_non_empty_target() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = fresh_targets();
    prefill_shard_two(&targets);

    let report = restore(dir.path(), &targets, true).expect("replace restores");
    assert_eq!(report.documents, 24);
    assert_eq!(
        targets.0[2].list_collections().unwrap(),
        vec!["users".to_string()],
        "the old collection is gone"
    );
}

/// Wraps a target and fails the write of one shard.
struct FailingTarget {
    inner: EngineSet,
    fail_shard: u32,
}

impl ShardRestoreTarget for FailingTarget {
    fn shard_count(&self) -> u32 {
        self.inner.shard_count()
    }

    fn shard_is_empty(&self, shard: u32) -> anyhow::Result<bool> {
        self.inner.shard_is_empty(shard)
    }

    fn restore_shard(&self, shard: u32, snapshot: SnapshotV1) -> anyhow::Result<()> {
        if shard == self.fail_shard {
            return Err(anyhow!("disk full"));
        }
        self.inner.restore_shard(shard, snapshot)
    }
}

#[test]
fn write_failure_reports_progress() {
    let dir = tempfile::tempdir().unwrap();
    store_set(dir.path());
    let targets = FailingTarget {
        inner: fresh_targets(),
        fail_shard: 1,
    };

    let err = restore(dir.path(), &targets, false).expect_err("write fails");
    assert_eq!(
        err.to_string(),
        "restoring shard 1 failed after 1 shard(s) were written: disk full"
    );
}

#[test]
fn dir_source_reports_absent_and_present() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("present.bin"), b"x").unwrap();
    let source = DirSource(dir.path().to_path_buf());

    assert_eq!(source.read("absent.bin").unwrap(), None);
    assert_eq!(source.read("present.bin").unwrap(), Some(b"x".to_vec()));
}
