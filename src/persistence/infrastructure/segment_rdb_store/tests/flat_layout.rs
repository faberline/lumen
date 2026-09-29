use crate::persistence::domain::generation_manifest::SegmentKind;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::catalog::validate_catalog_references;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    current_generation, index_kw, index_kw_in, kw_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use std::path::Path;
use std::sync::Arc;
use storage_durable::CurrentTarget;

#[test]
fn first_ordinary_save_publishes_from_an_empty_current() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "first", "value");

    store.save(&engine, 1).unwrap();

    assert!(matches!(
        store.generations.read_current().unwrap(),
        CurrentTarget::Generation(_)
    ));
}

#[test]
fn flat_layout_writes_and_reopens_multiple_collections() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    engine.create_collection("v", kw_schema()).unwrap();
    index_kw_in(&engine, "u", "u1", "one");
    index_kw_in(&engine, "v", "v1", "two");
    store.save(&engine, 1).unwrap();
    let (cold, _) = store.load_latest().unwrap().unwrap();
    assert_eq!(cold.list_collections().unwrap(), vec!["u", "v"]);
}

#[test]
fn flat_layout_second_checkpoint_and_cold_restart_completes() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    engine.create_collection("v", kw_schema()).unwrap();
    index_kw_in(&engine, "u", "u1", "one");
    index_kw_in(&engine, "v", "v1", "two");
    store.save(&engine, 1).unwrap();
    index_kw_in(&engine, "u", "u2", "three");
    store.save(&engine, 2).unwrap();
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 2);
    assert_eq!(cold.list_collections().unwrap(), vec!["u", "v"]);
}

#[test]
fn flat_layout_rejects_more_than_sixteen_deltas_per_field() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw_in(&engine, "u", "u1", "one");
    store.save(&engine, 1).unwrap();
    index_kw_in(&engine, "u", "u2", "two");
    store.save(&engine, 2).unwrap();

    let generation = current_generation(&store);
    let generation_path = dir.path().join(generation.as_str());
    let mut manifest = read_generation_manifest(&generation_path).unwrap();
    let collection = manifest
        .collections
        .iter_mut()
        .find(|collection| collection.collection_id == "u")
        .unwrap();
    let template = collection
        .segments
        .iter()
        .find(|segment| matches!(segment.kind, SegmentKind::Delta))
        .cloned()
        .expect("second save must retain a delta for the cap fixture");
    let template_rows = template.local_rows.clone().unwrap();
    let template_reader = SegmentReader::open(&generation_path.join(&template.path)).unwrap();
    assert_eq!(template_reader.n_docs(), template_rows.count);
    assert_eq!(template_reader.applied_seq(), template.applied_seq.unwrap());
    let template_applied_seq = template.applied_seq.unwrap();
    let template_row_count = template_rows.count;
    assert!(template_applied_seq <= manifest.checkpoint_sequence);
    let existing_delta_count = collection
        .segments
        .iter()
        .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
        .count();
    let max_ordinal = collection
        .segments
        .iter()
        .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
        .map(|segment| segment.ordinal)
        .max()
        .unwrap();
    let template_segment_parent = Path::new(&template.path)
        .parent()
        .expect("v2 delta must have a directory parent")
        .to_owned();
    let template_rows_parent = Path::new(&template_rows.path)
        .parent()
        .expect("v2 local rows must have a directory parent")
        .to_owned();
    for ordinal in (max_ordinal + 1)..=(max_ordinal + (17 - existing_delta_count) as u32) {
        let mut delta = template.clone();
        delta.ordinal = ordinal;
        delta.path = template_segment_parent
            .join(format!("{ordinal}.lseg"))
            .to_string_lossy()
            .into_owned();
        let mut rows = template_rows.clone();
        rows.path = template_rows_parent
            .join(format!("{ordinal}.rows.cbor"))
            .to_string_lossy()
            .into_owned();
        delta.applied_seq = Some(template_applied_seq);
        rows.count = template_row_count;
        delta.local_rows = Some(rows.clone());
        std::fs::create_dir_all(generation_path.join(&delta.path).parent().unwrap()).unwrap();
        std::fs::create_dir_all(generation_path.join(&rows.path).parent().unwrap()).unwrap();
        let delta_path = generation_path.join(&delta.path);
        if !delta_path.exists() {
            std::fs::hard_link(generation_path.join(&template.path), &delta_path).unwrap();
        }
        let rows_path = generation_path.join(&rows.path);
        if !rows_path.exists() {
            std::fs::hard_link(generation_path.join(&template_rows.path), &rows_path).unwrap();
        }
        collection.segments.push(delta);
    }
    assert_eq!(
        collection
            .segments
            .iter()
            .filter(|segment| matches!(segment.kind, SegmentKind::Delta))
            .count(),
        17
    );
    let error = validate_catalog_references(&generation_path, &manifest).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("field exceeds sixteen delta segments"),
        "unexpected v3 delta-cap error: {error:#}"
    );
}
