use crate::index::application::engine::Engine;
use crate::persistence::domain::generation_manifest::SegmentKind;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    committed_text, has_keyword, index_kw, kw_schema, text_schema,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::FieldType;
use std::sync::Arc;

#[test]
fn large_keyword_survives_each_checkpoint_and_merge() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    let mut values = Vec::new();
    for ordinal in 0..6u64 {
        let mut state = (ordinal + 1).wrapping_mul(0x9E37_79B9);
        let mut bytes = vec![0; 6 * 1024 * 1024 - 32 * 1024];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = b'a' + (state % 26) as u8;
        }
        let marker = format!("capacity-row-{ordinal:02}-");
        bytes[..marker.len()].copy_from_slice(marker.as_bytes());
        let value = String::from_utf8(bytes).unwrap();
        index_kw(&engine, &format!("row-{ordinal}"), &value);
        assert!(has_keyword(&engine, &value), "live row {ordinal}");
        values.push(value);
        store.save_required(&engine, ordinal + 1).unwrap();
        for (row, value) in values.iter().enumerate() {
            assert!(
                has_keyword(&engine, value),
                "row {row} after checkpoint {ordinal}"
            );
        }
        store
            .wait_for_merges(std::time::Duration::from_secs(30))
            .unwrap();
        for (row, value) in values.iter().enumerate() {
            assert!(
                has_keyword(&engine, value),
                "row {row} after merge {ordinal}"
            );
        }
    }
    assert!(engine.metrics().segment_merge_completed_total.get() > 0);
    let (cold, sequence) = store.load_latest().unwrap().unwrap();
    assert_eq!(sequence, 6);
    for (row, value) in values.iter().enumerate() {
        assert!(has_keyword(&cold, value), "row {row} after cold reopen");
    }
}

#[test]
fn keyword_checkpoint_writes_only_changed_local_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "u1", "a@x.com");
    index_kw(&engine, "u2", "b@x.com");
    store.save_required(&engine, 61).unwrap();
    index_kw(&engine, "u1", "new@x.com");
    let next = store.save_required(&engine, 62).unwrap();
    let manifest = read_generation_manifest(&dir.path().join(next.as_str())).unwrap();
    let deltas: Vec<_> = manifest.collections[0]
        .segments
        .iter()
        .filter(|s| matches!(s.kind, SegmentKind::Delta) && s.applied_seq == Some(62))
        .collect();
    assert_eq!(deltas.len(), 1, "keyword update must write a delta");
    assert_eq!(deltas[0].local_rows.as_ref().unwrap().count, 1);
}

/// #4246 drain guard: a published checkpoint generation must release the
/// staged Text rows it captured, so the staged set stays bounded by one
/// checkpoint interval of writes instead of growing for the life of the
/// process.
#[test]
fn published_checkpoint_releases_the_staged_text_rows_it_captured() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(dir.path()).unwrap());
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", text_schema()).unwrap();
    for n in 0..200u64 {
        committed_text(&engine, &format!("u{n}"), &format!("alpha term{n}"), n + 1);
    }
    assert_eq!(
        engine.staged_text_row_count("u", "email"),
        200,
        "committed Text applies through staged rows"
    );

    store.save_required(&engine, 100_000).unwrap();
    assert_eq!(
        engine.staged_text_row_count("u", "email"),
        0,
        "the first published generation must absorb every captured row"
    );

    // Rows written while the checkpoint runs belong to the NEXT generation.
    let writer_engine = engine.clone();
    let writer = std::thread::spawn(move || {
        let mut seq = 200_000u64;
        for n in 200..600u64 {
            seq += 1;
            committed_text(
                &writer_engine,
                &format!("u{n}"),
                &format!("alpha term{n}"),
                seq,
            );
        }
    });
    store.save_required(&engine, 150_000).unwrap();
    writer.join().unwrap();

    store.save_required(&engine, 300_000).unwrap();
    assert_eq!(
        engine.staged_text_row_count("u", "email"),
        0,
        "a quiescent generation must leave no staged row behind"
    );
    assert_eq!(
        engine.stats("u").unwrap().fields["email"].unique_terms,
        601,
        "alpha plus one private term per document"
    );
}

#[test]
fn text_checkpoint_writes_changed_row_with_coherent_local_statistics() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    let mut schema = kw_schema();
    schema.fields.get_mut("email").unwrap().field_type = FieldType::Text;
    engine.create_collection("u", schema).unwrap();
    index_kw(&engine, "u1", "alpha beta");
    index_kw(&engine, "u2", "alpha gamma");
    store.save_required(&engine, 71).unwrap();
    index_kw(&engine, "u1", "alpha alpha delta");
    let next = store.save_required(&engine, 72).unwrap();
    let root = dir.path().join(next.as_str());
    let manifest = read_generation_manifest(&root).unwrap();
    let deltas: Vec<_> = manifest.collections[0]
        .segments
        .iter()
        .filter(|s| matches!(s.kind, SegmentKind::Delta) && s.applied_seq == Some(72))
        .collect();
    assert_eq!(deltas.len(), 1, "text update must write a delta");
    assert_eq!(deltas[0].local_rows.as_ref().unwrap().count, 1);
    let reader = SegmentReader::open(&root.join(&deltas[0].path)).unwrap();
    assert_eq!(reader.text_doc_count(), 1);
    assert_eq!(reader.text_total_doc_len(), 3);
    assert_eq!(reader.text_doc_len(0), 3);
    assert_eq!(reader.text_postings("alpha"), Some((vec![0], vec![2])));
}

#[test]
fn vector_checkpoint_delta_does_not_enumerate_hnsw_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    let mut schema = kw_schema();
    let field = schema.fields.get_mut("email").unwrap();
    field.field_type = FieldType::Vector;
    field.dim = Some(2);
    field.metric = Some(crate::shared_kernel::types::schema::VectorMetric::L2);
    field.backend = Some(crate::shared_kernel::types::schema::VectorBackend::HnswCpu);
    engine.create_collection("u", schema).unwrap();
    let put = |id: &str, value: Vec<f32>| {
        engine
            .index(
                "u",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: id.into(),
                        field: "email".into(),
                        value: FieldValue::Vector(value),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
    };
    put("u1", vec![1.0, 0.0]);
    put("u2", vec![0.0, 1.0]);
    store.save_required(&engine, 81).unwrap();
    let scans = crate::index::domain::vector::HNSW_CHECKPOINT_FULL_SCANS.with(|count| count.get());
    put("u1", vec![2.0, 0.0]);
    let next = store.save_required(&engine, 82).unwrap();
    assert_eq!(
        crate::index::domain::vector::HNSW_CHECKPOINT_FULL_SCANS.with(|count| count.get()),
        scans,
        "incremental checkpoint must not enumerate the HNSW corpus"
    );
    let manifest = read_generation_manifest(&dir.path().join(next.as_str())).unwrap();
    let deltas: Vec<_> = manifest.collections[0]
        .segments
        .iter()
        .filter(|s| matches!(s.kind, SegmentKind::Delta) && s.applied_seq == Some(82))
        .collect();
    assert_eq!(deltas.len(), 1, "vector update must write a delta");
    assert_eq!(deltas[0].local_rows.as_ref().unwrap().count, 1);
}

#[test]
fn text_delta_adds_a_previously_absent_base_field_without_subtracting_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(dir.path()).unwrap();
    let engine = Arc::new(Engine::new());
    let mut schema = kw_schema();
    let mut text = schema.fields["email"].clone();
    text.field_type = FieldType::Text;
    text.analyzer = Some(crate::shared_kernel::types::schema::Analyzer::WhitespaceLower);
    schema.fields.insert("body".into(), text);
    engine.create_collection("u", schema).unwrap();
    index_kw(&engine, "u1", "first");
    index_kw(&engine, "u2", "second");
    let put = |id: &str, value: &str| {
        engine
            .index(
                "u",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: id.into(),
                        field: "body".into(),
                        value: FieldValue::String(value.into()),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap();
    };
    put("u1", "alpha beta");
    store.save_required(&engine, 91).unwrap();
    put("u2", "alpha");
    assert_eq!(
        engine.stats("u").unwrap().fields["body"].avg_doc_len,
        Some(1.5)
    );
    store.save_required(&engine, 92).unwrap();
    let (cold, _) = store.load_latest().unwrap().unwrap();
    assert_eq!(
        cold.stats("u").unwrap().fields["body"].avg_doc_len,
        Some(1.5),
        "adding Text to an absent base field must increase the corpus exactly once"
    );
}
