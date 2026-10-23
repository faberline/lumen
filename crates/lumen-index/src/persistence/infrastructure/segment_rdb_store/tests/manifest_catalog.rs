use crate::index::application::engine::Engine;
use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, LocalRowsReference, SegmentFormat, SegmentGenerationManifest, SegmentKind,
    SegmentReference, SegmentRole,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::catalog::validate_catalog_references;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, write_generation_manifest,
};
#[cfg(unix)]
use crate::persistence::infrastructure::segment_rdb_store::tests::first_segment_file;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    current_generation, index_kw, kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    SegmentRdbStore, GENERATION_MANIFEST_FILE,
};
use std::collections::BTreeSet;
use std::sync::Arc;

#[test]
fn catalog_rejects_noncanonical_paths_before_filesystem_normalization() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    store.save(&engine, 7).unwrap();
    let path = store
        .generations
        .generation_path(&current_generation(&store));
    let manifest = read_generation_manifest(&path).unwrap();
    for separator in ["/./", "//"] {
        let mut mutated = manifest.clone();
        let reference = &mut mutated.collections[0].segments[0];
        reference.path = reference.path.replacen('/', separator, 1);
        assert!(
            validate_catalog_references(&path, &mutated).is_err(),
            "a raw {separator:?} path component must not be normalized into an accepted path"
        );
    }
}

#[test]
fn v2_manifest_reads_the_approved_collection_and_segment_matrix() {
    let dir = tempfile::tempdir().unwrap();
    let mut collections = Vec::new();
    for collection in 0..182 {
        let mut segments = Vec::new();
        let mut schema = serde_json::Map::new();
        for field in 0..14 {
            let field_name = format!("field{field:02}");
            schema.insert(field_name.clone(), serde_json::json!({"type": "keyword"}));
            for ordinal in 0..=16 {
                let path = format!("collection{collection:03}/{field_name}/segment{ordinal:02}");
                segments.push(SegmentReference {
                    role: SegmentRole::Field,
                    field: Some(field_name.clone()),
                    ordinal,
                    kind: if ordinal == 0 {
                        SegmentKind::Base
                    } else {
                        SegmentKind::Delta
                    },
                    format: SegmentFormat::LsegV1,
                    path: format!("{path}.lseg"),
                    local_rows: (ordinal != 0).then(|| LocalRowsReference {
                        format: "lumen-local-eids-cbor-v1".into(),
                        path: format!("{path}.rows.cbor"),
                        count: 1,
                    }),
                    applied_seq: None,
                    payload_sha256: None,
                });
            }
        }
        collections.push(CollectionCatalog {
            collection_id: format!("collection{collection:03}"),
            collection_generation: collection + 1,
            schema_version: 1,
            data_version: 17,
            schema: serde_json::Value::Object(schema),
            segments,
        });
    }
    let manifest = SegmentGenerationManifest {
        schema_version: 2,
        checkpoint_sequence: 17,
        revision: 1,
        previous: None,
        next_collection_generation: 183,
        collections,
    };
    write_generation_manifest(dir.path(), &manifest).unwrap();
    assert!(
        std::fs::metadata(dir.path().join(GENERATION_MANIFEST_FILE))
            .unwrap()
            .len()
            > 4 * 1024 * 1024
    );
    let loaded = read_generation_manifest(dir.path())
        .expect("the approved 182 collection / 14 field / 16 delta catalog must be readable");
    assert_eq!(loaded.collections.len(), 182);
    assert!(loaded
        .collections
        .iter()
        .all(|collection| collection.segments.len() == 14 * 17));
}

#[test]
fn checkpoint_manifest_starts_v2_complete_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    store.save(&engine, 7).unwrap();

    let generation = current_generation(&store);
    let manifest =
        read_generation_manifest(&store.generations.generation_path(&generation)).unwrap();
    assert_eq!(manifest.schema_version, 2);
}
#[test]
fn unchanged_checkpoint_preserves_data_version_across_sequence_change() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    let first = store.save_required(&engine, 31).unwrap();
    let before = read_generation_manifest(&dir.path().join(first.as_str())).unwrap();
    let second = store.save_required(&engine, 32).unwrap();
    let after = read_generation_manifest(&dir.path().join(second.as_str())).unwrap();
    assert_eq!(
        before.collections[0].data_version, after.collections[0].data_version,
        "checkpoint sequence is not a collection data version"
    );
}

#[test]
fn checkpoint_epoch_advances_after_restart_and_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    let first = store.save_required(&engine, 41).unwrap();
    let before = read_generation_manifest(&dir.path().join(first.as_str())).unwrap();
    let (loaded, _) = store.load_latest().unwrap().unwrap();
    loaded.truncate_docs("u").unwrap();
    let second = store.save_required(&loaded, 42).unwrap();
    let after = read_generation_manifest(&dir.path().join(second.as_str())).unwrap();
    assert!(
        after.collections[0].collection_generation > before.collections[0].collection_generation,
        "truncate must allocate beyond the restored epoch"
    );
}
#[test]
fn catalog_rejects_missing_required_base_before_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    let name = store.save_required(&engine, 12).unwrap();
    let path = dir.path().join(name.as_str());
    let mut manifest = read_generation_manifest(&path).unwrap();
    manifest.collections[0]
        .segments
        .retain(|s| !matches!(s.role, SegmentRole::CollectionEids));
    assert!(
        validate_catalog_references(&path, &manifest).is_err(),
        "required base reference must be validated before reopen"
    );
}

#[cfg(unix)]
#[test]
fn v1_upgrade_reuses_loaded_immutable_base() {
    use std::os::unix::fs::MetadataExt;
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("gen-42-rev-1");
    let source = Arc::new(Engine::new());
    source.create_collection("u", kw_schema()).unwrap();
    index_kw(&source, "u1", "a@x.com");
    source.flush_to_segments(&legacy, 42).unwrap();
    std::fs::write(
        legacy.join(GENERATION_MANIFEST_FILE),
        br#"{"schema_version":1,"sequence":42,"revision":1,"previous":null}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("CURRENT"), b"generation:gen-42-rev-1\n").unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let (loaded, _) = store.load_latest().unwrap().unwrap();
    let before = first_segment_file(&legacy);
    let next = store.save_required(&loaded, 43).unwrap();
    let after = dir
        .path()
        .join(next.as_str())
        .join(before.strip_prefix(&legacy).unwrap());
    assert_eq!(
        std::fs::metadata(before).unwrap().ino(),
        std::fs::metadata(after).unwrap().ino(),
        "upgrading a loaded v1 base must link unchanged bytes"
    );
}
#[test]
fn new_vector_checkpoints_use_the_checkpoint_watermark() {
    for backend in ["flat-cpu", "hnsw-cpu"] {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new();
        engine.create_collection("u", serde_json::from_value(serde_json::json!({"fields":{"v":{"type":"vector","dim":3,"metric":"cosine","backend":backend}}})).unwrap()).unwrap();
        engine.flush_to_segments(dir.path(), 17).unwrap();
        let reader = SegmentReader::open(&dir.path().join("75/v.lseg")).unwrap();
        assert_eq!(
            reader.applied_seq(),
            17,
            "vector header must carry the checkpoint cut, not its row count"
        );
    }
}
#[test]
fn base_checkpoint_field_names_are_paths_only_after_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine
        .create_collection(
            "names",
            serde_json::from_value(serde_json::json!({
                "fields": {
                    "../outside": {"type":"keyword"},
                    "_collection.lmeta": {"type":"keyword"},
                    "tag.eids": {"type":"keyword"},
                    "tag": {"type":"vector","dim":2,"metric":"l2","backend":"flat-cpu"}
                }
            }))
            .unwrap(),
        )
        .unwrap();
    let generation = store
        .save_required(&engine, 17)
        .expect("accepted field names must produce distinct confined checkpoint files");
    let root = dir.path().join(generation.as_str());
    assert!(
        !root.join("outside.lseg").exists(),
        "field name escaped its collection"
    );
    let manifest = read_generation_manifest(&root).unwrap();
    let paths: BTreeSet<_> = manifest.collections[0]
        .segments
        .iter()
        .map(|s| &s.path)
        .collect();
    assert_eq!(
        paths.len(),
        6,
        "collection, four fields and vector IDs must be distinct"
    );
    let (cold, cut) = store.load_latest().unwrap().unwrap();
    assert_eq!(cut, 17);
    assert_eq!(cold.list_collections().unwrap(), vec!["names"]);
}
