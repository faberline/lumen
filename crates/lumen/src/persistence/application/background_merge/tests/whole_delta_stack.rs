use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::tests::{
    capacity_schema, current_manifest, field_delta_count,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

fn vector_capacity_schema(
    backend: Option<crate::shared_kernel::types::schema::VectorBackend>,
) -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "v_capacity".to_owned(),
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(4),
            metric: Some(crate::shared_kernel::types::schema::VectorMetric::L2),
            backend,
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

fn put_vector(engine: &Engine, id: &str, value: Vec<f32>) {
    engine
        .index(
            "u",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: id.into(),
                    field: "v_capacity".into(),
                    value: FieldValue::Vector(value),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
}

fn put_keyword_row(engine: &Engine, field: &str, id: &str, value: &str) {
    engine
        .index(
            "u",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: id.into(),
                    field: field.into(),
                    value: FieldValue::String(value.into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
}

/// Build one field a two-phase way: phase A folds a wide row set (plus a
/// few tiny spacer rows) into a real, sizable base via one background
/// merge; phase B then writes exactly four small deltas on top of that
/// base (nothing else touches the store while they land) and drains
/// exactly one background merge job. `wide_rows` and `spacer` build phase
/// A's base; `small_delta` writes phase B's small per-sequence change.
fn assert_whole_delta_stack_compacts_in_one_job(
    schema: CreateCollectionRequest,
    field: &str,
    wide_rows: impl Fn(&Engine),
    spacer: impl Fn(&Engine, u64),
    small_delta: impl Fn(&Engine, u64),
) {
    let directory = tempfile::tempdir().unwrap();
    let store = SegmentRdbStore::new(directory.path()).unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", schema).unwrap();
    wide_rows(&engine);
    store.save_required(&engine, 1).unwrap();
    for sequence in 2..=4 {
        spacer(&engine, sequence);
        store.save_required(&engine, sequence).unwrap();
    }
    store
        .wait_for_merges(Duration::from_secs(10))
        .expect("phase A background merge worker must drain before phase B");
    let based = current_manifest(&store);
    assert_eq!(
        field_delta_count(&based, field),
        0,
        "phase A must fold the wide row set into the base before phase B"
    );

    for sequence in 5..=8 {
        small_delta(&engine, sequence);
        store.save_required(&engine, sequence).unwrap();
    }
    store
        .wait_for_merges(Duration::from_secs(10))
        .expect("phase B background merge worker must drain before assertions");
    let merged = current_manifest(&store);
    assert_eq!(
        field_delta_count(&merged, field),
        3,
        "one background merge job folds one adjacent pair and leaves the remaining deltas"
    );
}

#[test]
fn background_merge_compacts_the_whole_delta_stack_in_one_job() {
    assert_whole_delta_stack_compacts_in_one_job(
        capacity_schema(),
        "a_capacity",
        |engine| {
            for row in 0..8000 {
                put_keyword_row(
                    engine,
                    "a_capacity",
                    &format!("wide-{row}"),
                    &format!("wide-value-with-extra-padding-bytes-{row}"),
                );
            }
        },
        |engine, sequence| {
            put_keyword_row(engine, "a_capacity", &format!("spacer-{sequence}"), "s");
        },
        |engine, sequence| {
            put_keyword_row(engine, "a_capacity", "hot", &format!("a-{sequence}"));
        },
    );
}

#[test]
fn background_merge_compacts_the_whole_delta_stack_for_a_flat_cpu_vector_field() {
    assert_whole_delta_stack_compacts_in_one_job(
        vector_capacity_schema(Some(
            crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        )),
        "v_capacity",
        |engine| {
            for row in 0..2000 {
                put_vector(
                    engine,
                    &format!("wide-{row}"),
                    vec![row as f32, 0.0, 0.0, 0.0],
                );
            }
        },
        |engine, sequence| {
            put_vector(
                engine,
                &format!("spacer-{sequence}"),
                vec![0.0, 0.0, 0.0, 0.0],
            );
        },
        |engine, sequence| {
            put_vector(engine, "hot", vec![sequence as f32, 1.0, 0.0, 0.0]);
        },
    );
}

#[test]
fn background_merge_compacts_the_whole_delta_stack_for_an_hnsw_vector_field() {
    assert_whole_delta_stack_compacts_in_one_job(
        vector_capacity_schema(Some(
            crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        )),
        "v_capacity",
        |engine| {
            for row in 0..2000 {
                put_vector(
                    engine,
                    &format!("wide-{row}"),
                    vec![row as f32, 0.0, 0.0, 0.0],
                );
            }
        },
        |engine, sequence| {
            put_vector(
                engine,
                &format!("spacer-{sequence}"),
                vec![0.0, 0.0, 0.0, 0.0],
            );
        },
        |engine, sequence| {
            put_vector(engine, "hot", vec![sequence as f32, 1.0, 0.0, 0.0]);
        },
    );
}
