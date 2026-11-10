use std::collections::{BTreeMap, BTreeSet};

use super::{
    build_backup_manifest, read_backup_manifest, verify_artifact, ManifestError, ShardArtifact,
};
use crate::index::application::engine::Engine;
use crate::sharding::domain::snapshot_subset::snapshot_bucket_subset;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
};

fn field(field_type: FieldType) -> FieldSpec {
    FieldSpec {
        field_type,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn item(external_id: &str, field: &str, value: FieldValue) -> IndexItem {
    IndexItem {
        external_id: external_id.into(),
        field: field.into(),
        value,
        version: None,
    }
}

/// Engine with one collection `users`; `extra_field` adds a second schema field.
fn engine_with(docs: usize, extra_field: bool) -> Engine {
    let engine = Engine::new();
    let mut fields = BTreeMap::from([("email".into(), field(FieldType::Keyword))]);
    if extra_field {
        fields.insert("plan".into(), field(FieldType::Keyword));
    }
    engine
        .create_collection("users", CreateCollectionRequest { fields })
        .unwrap();
    for i in 0..docs {
        let external_id = format!("doc-{i:02}");
        engine
            .index(
                "users",
                IndexRequest {
                    request_id: None,
                    items: vec![item(
                        &external_id,
                        "email",
                        FieldValue::String(format!("{external_id}@example.com")),
                    )],
                },
            )
            .unwrap();
    }
    engine
}

/// Three shards over 12 virtual buckets, 24 documents, one artifact per shard.
fn three_shard_set() -> (VirtualBucketShardMap, Vec<ShardArtifact>) {
    let snapshot = engine_with(24, false).snapshot().unwrap();
    let map = VirtualBucketShardMap::balanced(7, 12, 3).unwrap();
    let artifacts = (0..3u32)
        .map(|s| {
            let buckets = (0..12u32)
                .filter(|b| map.assignment_for_bucket(*b) == Some(s))
                .collect::<BTreeSet<u32>>();
            let subset = snapshot_bucket_subset(&snapshot, 12, &buckets).unwrap();
            ShardArtifact {
                shard: s,
                key: format!("shard-{s}.json"),
                bytes: serde_json::to_vec(&subset).unwrap(),
            }
        })
        .collect();
    (map, artifacts)
}

#[test]
fn manifest_lists_every_shard_once_with_matching_digests() {
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();

    assert_eq!(manifest.shards.len(), 3);
    let shard_order: Vec<u32> = manifest.shards.iter().map(|e| e.shard).collect();
    assert_eq!(shard_order, vec![0, 1, 2]);

    let dir = tempfile::tempdir().unwrap();
    for (entry, artifact) in manifest.shards.iter().zip(&artifacts) {
        let path = dir.path().join(&artifact.key);
        std::fs::write(&path, &artifact.bytes).unwrap();
        let read_back = std::fs::read(&path).unwrap();
        assert_eq!(verify_artifact(entry, &read_back), Ok(()));
    }

    let documents: u64 = manifest.shards.iter().map(|e| e.documents).sum();
    assert_eq!(documents, 24);
    let collection_ids: Vec<&String> = manifest.collections.keys().collect();
    assert_eq!(collection_ids, vec!["users"]);
    assert_eq!(manifest.virtual_bucket_count, 12);
    assert_eq!(manifest.physical_shard_count, 3);
    assert_eq!(manifest.shard_map_version, 7);
    assert_eq!(manifest.assignments.len(), 12);
}

#[test]
fn manifest_round_trips_through_bytes() {
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    assert_eq!(read_backup_manifest(&manifest.to_bytes()), Ok(manifest));
}

#[test]
fn backup_id_is_stable() {
    let (map, artifacts) = three_shard_set();
    let first = build_backup_manifest(&map, &artifacts).unwrap();
    let second = build_backup_manifest(&map, &artifacts).unwrap();
    assert_eq!(first.backup_id, second.backup_id);
    assert_eq!(first.backup_id.len(), 64);
    assert!(first
        .backup_id
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
}

#[test]
fn backup_id_changes_when_any_shard_artifact_changes() {
    let (map, artifacts) = three_shard_set();
    let original = build_backup_manifest(&map, &artifacts).unwrap().backup_id;

    let mut changed_ids = Vec::new();
    for changed in 0..3 {
        let mut edited: Vec<ShardArtifact> = artifacts
            .iter()
            .map(|a| ShardArtifact {
                shard: a.shard,
                key: a.key.clone(),
                bytes: a.bytes.clone(),
            })
            .collect();
        // Appending a space keeps the JSON valid but changes the stored bytes.
        edited[changed].bytes.push(b' ');
        let id = build_backup_manifest(&map, &edited).unwrap().backup_id;
        assert_ne!(id, original, "shard {changed} change must move backup_id");
        changed_ids.push(id);
    }
    assert_ne!(changed_ids[0], changed_ids[1]);
    assert_ne!(changed_ids[0], changed_ids[2]);
    assert_ne!(changed_ids[1], changed_ids[2]);
}

#[test]
fn verify_artifact_rejects_a_changed_object() {
    let (map, artifacts) = three_shard_set();
    let manifest = build_backup_manifest(&map, &artifacts).unwrap();
    let entry = &manifest.shards[1];

    let mut stored = artifacts[1].bytes.clone();
    stored[0] ^= 0x01;
    let err = verify_artifact(entry, &stored).unwrap_err();
    assert_eq!(
        err,
        ManifestError::DigestMismatch {
            key: "shard-1.json".into()
        }
    );
    assert_eq!(
        err.to_string(),
        "artifact shard-1.json does not match the manifest digest"
    );
}

#[test]
fn reader_rejects_a_duplicate_shard_entry() {
    let (map, artifacts) = three_shard_set();
    let mut manifest = build_backup_manifest(&map, &artifacts).unwrap();
    manifest.shards[2].shard = 1;
    let err = read_backup_manifest(&manifest.to_bytes()).unwrap_err();
    assert_eq!(err, ManifestError::DuplicateShard(1));
    assert_eq!(err.to_string(), "shard 1 appears more than once");
}

#[test]
fn reader_rejects_a_missing_shard_entry() {
    let (map, artifacts) = three_shard_set();
    let mut manifest = build_backup_manifest(&map, &artifacts).unwrap();
    manifest.shards.remove(1);
    let err = read_backup_manifest(&manifest.to_bytes()).unwrap_err();
    assert_eq!(err, ManifestError::MissingShard(1));
    assert_eq!(err.to_string(), "shard 1 is missing");
}

#[test]
fn reader_rejects_a_changed_identity() {
    let (map, artifacts) = three_shard_set();
    let mut manifest = build_backup_manifest(&map, &artifacts).unwrap();
    manifest.shards[0].bytes += 1;
    assert_eq!(
        read_backup_manifest(&manifest.to_bytes()),
        Err(ManifestError::IdentityMismatch)
    );
}

#[test]
fn reader_rejects_unknown_version() {
    let (map, artifacts) = three_shard_set();
    let mut manifest = build_backup_manifest(&map, &artifacts).unwrap();
    manifest.version = 2;
    assert_eq!(
        read_backup_manifest(&manifest.to_bytes()),
        Err(ManifestError::UnsupportedVersion(2))
    );
}

#[test]
fn reader_rejects_bad_topology() {
    let (map, artifacts) = three_shard_set();
    let mut manifest = build_backup_manifest(&map, &artifacts).unwrap();
    manifest.assignments.pop();
    let err = read_backup_manifest(&manifest.to_bytes()).unwrap_err();
    assert!(matches!(err, ManifestError::InvalidTopology(_)));
    assert!(err
        .to_string()
        .starts_with("invalid backup manifest topology:"));
}

#[test]
fn reader_rejects_garbage() {
    let err = read_backup_manifest(b"{").unwrap_err();
    assert!(matches!(err, ManifestError::Malformed(_)));
}

#[test]
fn builder_rejects_duplicate_missing_and_out_of_range() {
    let (map, artifacts) = three_shard_set();
    let pick = |shards: &[u32]| -> Vec<ShardArtifact> {
        shards
            .iter()
            .map(|s| {
                let original = &artifacts[*s as usize];
                ShardArtifact {
                    shard: *s,
                    key: original.key.clone(),
                    bytes: original.bytes.clone(),
                }
            })
            .collect()
    };

    assert_eq!(
        build_backup_manifest(&map, &pick(&[0, 1, 1])).unwrap_err(),
        ManifestError::DuplicateShard(1)
    );
    assert_eq!(
        build_backup_manifest(&map, &pick(&[0, 2])).unwrap_err(),
        ManifestError::MissingShard(1)
    );
    let mut out_of_range = pick(&[0, 1]);
    out_of_range.push(ShardArtifact {
        shard: 5,
        key: "shard-5.json".into(),
        bytes: artifacts[0].bytes.clone(),
    });
    assert_eq!(
        build_backup_manifest(&map, &out_of_range).unwrap_err(),
        ManifestError::ShardOutOfRange { shard: 5, count: 3 }
    );
    assert_eq!(
        build_backup_manifest(&map, &[]).unwrap_err(),
        ManifestError::NoShards
    );
}

#[test]
fn builder_rejects_a_non_snapshot_artifact() {
    let (map, artifacts) = three_shard_set();
    let mut broken: Vec<ShardArtifact> = artifacts
        .iter()
        .map(|a| ShardArtifact {
            shard: a.shard,
            key: a.key.clone(),
            bytes: a.bytes.clone(),
        })
        .collect();
    broken[1].bytes = b"not json".to_vec();
    let err = build_backup_manifest(&map, &broken).unwrap_err();
    assert!(matches!(
        err,
        ManifestError::UnreadableArtifact { shard: 1, .. }
    ));
}

#[test]
fn builder_rejects_schema_drift_between_shards() {
    let (map, artifacts) = three_shard_set();
    // Shard 2 comes from an engine whose `users` schema has an extra field.
    let drifted_snapshot = engine_with(24, true).snapshot().unwrap();
    let buckets = (0..12u32)
        .filter(|b| map.assignment_for_bucket(*b) == Some(2))
        .collect::<BTreeSet<u32>>();
    let drifted = snapshot_bucket_subset(&drifted_snapshot, 12, &buckets).unwrap();
    let mut set: Vec<ShardArtifact> = artifacts
        .iter()
        .map(|a| ShardArtifact {
            shard: a.shard,
            key: a.key.clone(),
            bytes: a.bytes.clone(),
        })
        .collect();
    set[2].bytes = serde_json::to_vec(&drifted).unwrap();
    assert_eq!(
        build_backup_manifest(&map, &set).unwrap_err(),
        ManifestError::SchemaMismatch(2)
    );
}
