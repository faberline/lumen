use crate::index::domain::postings::Postings;
use crate::persistence::domain::generation_manifest::{
    CollectionCatalog, LocalRowsReference, SegmentFormat, SegmentKind, SegmentReference,
    SegmentRole,
};
use crate::persistence::infrastructure::segment::eid_writer::write_eid_segment;
use crate::persistence::infrastructure::segment::sparse_rows::encode_sparse_local_rows;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment::{
    keyword_writer::write_keyword_segment, number_writer::write_number_segment,
    text_writer::write_text_segment, vector_writer::write_vector_segment,
};
use crate::persistence::infrastructure::segment_rdb_store::compaction::{
    validate_compaction_inputs, write_compacted_field,
};
use crate::persistence::infrastructure::segment_rdb_store::field_deltas::delta_path_prefix;
use std::collections::BTreeMap;

fn reference(
    role: SegmentRole,
    field: Option<&str>,
    kind: SegmentKind,
    ordinal: u32,
    path: &str,
    rows: Option<(&str, u32)>,
) -> SegmentReference {
    SegmentReference {
        role,
        field: field.map(str::to_owned),
        ordinal,
        kind,
        format: SegmentFormat::LsegV1,
        path: path.to_owned(),
        local_rows: rows.map(|(path, count)| LocalRowsReference {
            format: "lumen-local-eids-cbor-v1".to_owned(),
            path: path.to_owned(),
            count,
        }),
        applied_seq: Some(1),
        payload_sha256: None,
    }
}

fn fixture() -> (tempfile::TempDir, CollectionCatalog) {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("75");
    std::fs::create_dir_all(dir.join("__delta/k")).unwrap();
    std::fs::create_dir_all(dir.join("__delta/n")).unwrap();
    std::fs::create_dir_all(dir.join("__delta/v")).unwrap();
    std::fs::create_dir_all(dir.join("__delta/t")).unwrap();
    let fields = serde_json::json!({
        "k": {"type":"keyword"}, "n": {"type":"number"},
        "v": {"type":"vector", "dim":2, "metric":"l2", "backend":"flat-cpu"},
        "t": {"type":"text", "analyzer":"whitespace_lower"}
    });
    std::fs::write(
        dir.join("_schema.json"),
        serde_json::to_vec(&serde_json::json!({"version":1,"fields":fields})).unwrap(),
    )
    .unwrap();
    write_eid_segment(&dir.join("_collection.lmeta.lseg"), 1, &["b"]).unwrap();
    write_eid_segment(&dir.join("v.eids.lseg"), 1, &["b"]).unwrap();
    let mut postings = BTreeMap::new();
    postings.insert("old".to_owned(), roaring::RoaringBitmap::from_iter([0]));
    write_keyword_segment(&dir.join("k.lseg"), 1, &[Some("old")], &postings).unwrap();
    write_keyword_segment(
        &dir.join("__delta/k/1.lseg"),
        1,
        &[Some("new")],
        &BTreeMap::from([("new".to_owned(), roaring::RoaringBitmap::from_iter([0]))]),
    )
    .unwrap();
    write_number_segment(&dir.join("n.lseg"), 1, &[Some(1.0)]).unwrap();
    write_number_segment(&dir.join("__delta/n/1.lseg"), 1, &[Some(2.0)]).unwrap();
    write_vector_segment(&dir.join("v.lseg"), 1, 2, &[Some(&[1.0, 1.0])]).unwrap();
    write_vector_segment(&dir.join("__delta/v/1.lseg"), 1, 2, &[Some(&[2.0, 2.0])]).unwrap();
    let mut base_tokens = BTreeMap::new();
    base_tokens
        .entry("old".to_owned())
        .or_insert_with(Postings::default)
        .upsert(0, 1);
    write_text_segment(&dir.join("t.lseg"), 1, &base_tokens, &[1], &[true], 1, 1).unwrap();
    let mut delta_tokens = BTreeMap::new();
    delta_tokens
        .entry("new".to_owned())
        .or_insert_with(Postings::default)
        .upsert(0, 1);
    write_text_segment(
        &dir.join("__delta/t/1.lseg"),
        1,
        &delta_tokens,
        &[1],
        &[true],
        1,
        1,
    )
    .unwrap();
    for field in ["k", "n", "v", "t"] {
        encode_sparse_local_rows(
            &dir.join(format!("__delta/{field}/1.rows.cbor")),
            &["a".to_owned()],
        )
        .unwrap();
    }
    let mut segments = vec![reference(
        SegmentRole::CollectionEids,
        None,
        SegmentKind::Base,
        0,
        "75/_collection.lmeta.lseg",
        None,
    )];
    for field in ["k", "n", "v", "t"] {
        segments.push(reference(
            SegmentRole::Field,
            Some(field),
            SegmentKind::Base,
            0,
            &format!("75/{field}.lseg"),
            None,
        ));
        segments.push(reference(
            SegmentRole::Field,
            Some(field),
            SegmentKind::Delta,
            1,
            &format!("75/__delta/{field}/1.lseg"),
            Some((&format!("75/__delta/{field}/1.rows.cbor"), 1)),
        ));
    }
    segments.push(reference(
        SegmentRole::VectorEids,
        Some("v"),
        SegmentKind::Base,
        0,
        "75/v.eids.lseg",
        None,
    ));
    (
        root,
        CollectionCatalog {
            collection_id: "u".to_owned(),
            collection_generation: 1,
            schema_version: 1,
            data_version: 1,
            schema: serde_json::json!({"k":{"type":"keyword"},"n":{"type":"number"},"v":{"type":"vector","dim":2,"metric":"l2","backend":"flat-cpu"},"t":{"type":"text","analyzer":"whitespace_lower"}}),
            segments,
        },
    )
}

fn inputs(collection: &CollectionCatalog, field: &str) -> Vec<SegmentReference> {
    collection
        .segments
        .iter()
        .filter(|reference| {
            matches!(reference.role, SegmentRole::Field)
                && reference.field.as_deref() == Some(field)
        })
        .cloned()
        .collect()
}

#[test]
fn partial_window_uses_catalog_order_not_ordinal_arithmetic() {
    let (_root, mut collection) = fixture();
    collection.segments.retain(|reference| {
        !(matches!(reference.role, SegmentRole::Field)
            && reference.field.as_deref() == Some("k")
            && matches!(reference.kind, SegmentKind::Delta))
    });
    let mut deltas = Vec::new();
    for ordinal in [2, 4, 6] {
        let prefix = delta_path_prefix(&collection.collection_id, "k", ordinal);
        let segment = format!("{prefix}.lseg");
        let rows = format!("{prefix}.rows.cbor");
        let reference = reference(
            SegmentRole::Field,
            Some("k"),
            SegmentKind::Delta,
            ordinal,
            &segment,
            Some((&rows, 1)),
        );
        collection.segments.push(reference.clone());
        deltas.push(reference);
    }
    validate_compaction_inputs(&collection, "k", &deltas[..2], false)
        .expect("ordinal-2 then ordinal-4 is a contiguous catalog window");
    assert!(
        validate_compaction_inputs(
            &collection,
            "k",
            &[deltas[0].clone(), deltas[2].clone()],
            false
        )
        .is_err(),
        "selecting ordinal-2 then ordinal-6 skips the catalogued ordinal-4 delta"
    );
}

#[test]
fn compacts_keyword_number_vector_and_text_with_real_readers() {
    let (root, collection) = fixture();
    let keyword = write_compacted_field(
        root.path(),
        9,
        &collection,
        "k",
        &inputs(&collection, "k"),
        true,
    )
    .unwrap();
    let number = write_compacted_field(
        root.path(),
        9,
        &collection,
        "n",
        &inputs(&collection, "n"),
        true,
    )
    .unwrap();
    let vector = write_compacted_field(
        root.path(),
        9,
        &collection,
        "v",
        &inputs(&collection, "v"),
        true,
    )
    .unwrap();
    let text = write_compacted_field(
        root.path(),
        9,
        &collection,
        "t",
        &inputs(&collection, "t"),
        true,
    )
    .unwrap();
    assert_eq!(
        SegmentReader::open(&root.path().join(&keyword.output.path))
            .unwrap()
            .keyword_at(0),
        Some("new".to_owned())
    );
    assert_eq!(
        SegmentReader::open(&root.path().join(&number.output.path))
            .unwrap()
            .number_at(0),
        Some(2.0)
    );
    assert_eq!(
        SegmentReader::open(&root.path().join(&vector.output.path))
            .unwrap()
            .vector_at(0, 2),
        Some(&[2.0, 2.0][..])
    );
    assert_eq!(
        SegmentReader::open(&root.path().join(&text.output.path))
            .unwrap()
            .text_postings("new"),
        Some((vec![0], vec![1]))
    );
    assert!(vector.vector_eids.is_some());
    assert!(keyword.logical_read_bytes > 0 && keyword.logical_write_bytes > 0);
}

mod scalar_statistics;
