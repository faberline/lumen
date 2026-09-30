//! Freezing collections for a checkpoint: the frozen payload replays unchanged
//! after live writes, an initial sparse capture needs complete journal
//! provenance, and a freeze exchanges each field backend's journal without
//! cloning the live rows.

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::application::engine::tests::{build_users_schema, item, record_cost_schema};
use crate::index::application::engine::Engine;
use crate::index::application::frozen_checkpoint::FrozenCollectionFiles;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::ExistsQuery;
use crate::shared_kernel::types::query::QueryNode;
use crate::shared_kernel::types::schema::FieldType;
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn frozen_checkpoint_replays_the_same_captured_payload_after_live_mutation() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    engine
        .index(
            "c",
            IndexRequest {
                items: vec![
                    item(
                        "doc",
                        "bio",
                        FieldValue::String("captured biography".into()),
                    ),
                    item(
                        "doc",
                        "email",
                        FieldValue::String("captured@example.test".into()),
                    ),
                    item(
                        "doc",
                        "tags",
                        FieldValue::StringList(vec!["captured".into()]),
                    ),
                    item("doc", "age", FieldValue::Number(42.0)),
                ],
                request_id: None,
            },
        )
        .unwrap();
    // A restored legacy collection has no complete change journal. Keep
    // this test on the full-base retry path; fresh journals have their own
    // sparse ownership and generation round-trip tests below.
    engine.restore(engine.snapshot().unwrap()).unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let first = tempfile::tempdir().unwrap();
    frozen.write(first.path(), 7).unwrap();

    engine
        .index(
            "c",
            IndexRequest {
                items: vec![item(
                    "doc",
                    "email",
                    FieldValue::String("later@example.test".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();
    let replay = tempfile::tempdir().unwrap();
    frozen.write(replay.path(), 7).unwrap();

    let first_engine = Engine::new();
    first_engine.reopen_from_segment_dir(first.path()).unwrap();
    let replay_engine = Engine::new();
    replay_engine
        .reopen_from_segment_dir(replay.path())
        .unwrap();
    for checkpoint in [&first_engine, &replay_engine] {
        let result = checkpoint
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Term(crate::shared_kernel::types::query::TermQuery {
                        field: "email".into(),
                        value: FieldValue::String("captured@example.test".into()),
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].external_id, "doc");
        let text = checkpoint
            .search(
                "c",
                SearchRequest {
                    query: QueryNode::Match(crate::shared_kernel::types::query::MatchQuery {
                        field: "bio".into(),
                        text: "captured".into(),
                        op: crate::shared_kernel::types::query::MatchOp::And,
                    }),
                    limit: 10,
                    offset: 0,
                    cursor: None,
                    routing_key: None,
                    sort: None,
                    track_total: true,
                    collapse: None,
                },
            )
            .unwrap();
        assert_eq!(text.hits.len(), 1, "replayed text field missing");
        for field in ["email", "tags", "age"] {
            let exists = checkpoint
                .search(
                    "c",
                    SearchRequest {
                        query: QueryNode::Exists(ExistsQuery {
                            field: field.into(),
                        }),
                        limit: 10,
                        offset: 0,
                        cursor: None,
                        routing_key: None,
                        sort: None,
                        track_total: true,
                        collapse: None,
                    },
                )
                .unwrap();
            assert_eq!(exists.hits.len(), 1, "replayed field `{field}` missing");
        }
    }
}

#[test]
fn initial_sparse_capture_requires_complete_journal_provenance() {
    let engine = Engine::new();
    engine.create_collection("c", build_users_schema()).unwrap();
    engine
        .index(
            "c",
            IndexRequest {
                items: vec![item(
                    "legacy",
                    "email",
                    FieldValue::String("legacy-value".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();
    engine.restore(engine.snapshot().unwrap()).unwrap();
    engine
        .prepare_checkpoint_namespace(std::path::Path::new("/unused-test-checkpoint-namespace"), 1)
        .unwrap();
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    assert!(
        frozen.capture.initial_sparse.is_empty(),
        "a missing origin must not imply journal completeness"
    );
    assert!(
        matches!(&frozen.files[0].1, FrozenCollectionFiles::Base { eids, .. } if !eids.is_empty())
    );

    let fresh = Engine::new();
    fresh.create_collection("c", build_users_schema()).unwrap();
    fresh.drop_field("c", "age").unwrap();
    let frozen = fresh.freeze_checkpoint_collections(None).unwrap();
    assert!(
        frozen.capture.initial_sparse.is_empty(),
        "schema changes must invalidate initial journal provenance"
    );
    assert!(matches!(
        &frozen.files[0].1,
        FrozenCollectionFiles::Base { .. }
    ));
}

#[test]
fn first_checkpoint_freeze_does_not_clone_live_field_rows() {
    for backend in [
        crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
    ] {
        let engine = Engine::new();
        let mut schema = build_users_schema();
        let mut vector = record_cost_schema(Some(backend));
        schema
            .fields
            .insert("vector".into(), vector.fields.remove("vector").unwrap());
        let mut hash = schema.fields["email"].clone();
        hash.field_type = FieldType::Hash;
        schema.fields.insert("sig".into(), hash);
        engine.create_collection("c", schema).unwrap();
        let fields = [
            ("email", FieldValue::String("captured".into())),
            ("bio", FieldValue::String("captured captured text".into())),
            (
                "tags",
                serde_json::from_value(serde_json::json!(["one", "two"])).unwrap(),
            ),
            ("age", FieldValue::Number(42.0)),
            ("sig", FieldValue::String("000000000000002a".into())),
            (
                "vector",
                serde_json::from_value(serde_json::json!([1.0, 0.0, 0.0])).unwrap(),
            ),
        ];
        engine
            .index(
                "c",
                IndexRequest {
                    items: fields
                        .iter()
                        .map(|(field, value)| item("sparse-first", field, value.clone()))
                        .collect(),
                    request_id: None,
                },
            )
            .unwrap();
        let frozen = engine.freeze_checkpoint_collections(None).unwrap();
        assert!(
            frozen
                .files
                .iter()
                .all(|(_, files)| !matches!(files, FrozenCollectionFiles::Base { .. })),
            "fresh checkpoint must freeze journal handles instead of cloning live field rows"
        );
        engine.delete("c", "sparse-first", None).unwrap();
        let output = tempfile::tempdir().unwrap();
        let capture = frozen.write(output.path(), 0).unwrap();
        for (field, _) in fields {
            let saved = capture.field_deltas["c"][field][0].1.as_ref().unwrap();
            let row = capture.frozen_changes["c"]
                .row(field, "sparse-first")
                .unwrap();
            assert!(
                saved.same_identity(row.value().unwrap()),
                "first checkpoint must retain the captured row handle for {field}"
            );
        }
    }
}

#[test]
fn checkpoint_freeze_exchanges_journal_for_all_field_backends() {
    for backend in [
        crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
    ] {
        let engine = std::sync::Arc::new(Engine::new());
        let mut schema = build_users_schema();
        let mut vector = record_cost_schema(Some(backend));
        schema
            .fields
            .insert("vector".into(), vector.fields.remove("vector").unwrap());
        let mut hash = schema.fields["email"].clone();
        hash.field_type = FieldType::Hash;
        schema.fields.insert("sig".into(), hash);
        engine.create_collection("c", schema).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let store = crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(
            directory.path(),
        )
        .unwrap();
        store.save(&engine, 0).unwrap();
        let fields = [
            ("email", FieldValue::String("captured".into())),
            ("bio", FieldValue::String("captured captured text".into())),
            (
                "tags",
                serde_json::from_value(serde_json::json!(["one", "two"])).unwrap(),
            ),
            (
                "age",
                serde_json::from_value(serde_json::json!(42)).unwrap(),
            ),
            ("sig", FieldValue::String("000000000000002a".into())),
            (
                "vector",
                serde_json::from_value(serde_json::json!([1.0, 0.0, 0.0])).unwrap(),
            ),
        ];
        engine
            .index(
                "c",
                IndexRequest {
                    items: fields
                        .iter()
                        .map(|(name, value)| item("sparse-1000000", name, value.clone()))
                        .collect(),
                    request_id: None,
                },
            )
            .unwrap();
        let lineage = {
            let state = engine.state.read().unwrap();
            let coll = &state.collections["c"];
            for (field, _) in &fields {
                assert!(coll
                    .change_journal
                    .active_revision(field, "sparse-1000000")
                    .is_some());
            }
            coll.checkpoint_lineage
                .as_ref()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf()
        };
        let frozen = engine
            .freeze_checkpoint_collections(Some(&lineage))
            .unwrap();
        {
            let state = engine.state.read().unwrap();
            for (field, _) in &fields {
                assert_eq!(
                    state.collections["c"]
                        .change_journal
                        .active_revision(field, "sparse-1000000"),
                    None,
                    "checkpoint must exchange active journal ownership during capture"
                );
            }
        }
        engine.delete("c", "sparse-1000000", None).unwrap();
        let written = tempfile::tempdir().unwrap();
        let captured = frozen.write(written.path(), 1).unwrap();
        let values = &captured.field_deltas["c"];
        for (field, _) in &fields {
            assert!(
                values[*field][0].1.as_ref().unwrap().same_identity(
                    captured.frozen_changes["c"]
                        .row(field, "sparse-1000000")
                        .unwrap()
                        .value()
                        .unwrap(),
                ),
                "encoding must borrow the frozen typed payload without copying it"
            );
        }
        assert!(
            matches!(values["email"][0].1.as_deref(), Some(CheckpointValue::Keyword(value)) if value == "captured")
        );
        assert!(
            matches!(values["age"][0].1.as_deref(), Some(CheckpointValue::Number(value)) if *value == 42.0)
        );
        assert!(matches!(
            values["sig"][0].1.as_deref(),
            Some(CheckpointValue::Hash(42))
        ));
        assert!(
            matches!(values["tags"][0].1.as_deref(), Some(CheckpointValue::Set(value)) if value == &["one", "two"])
        );
        assert!(
            matches!(values["vector"][0].1.as_deref(), Some(CheckpointValue::Vector(value)) if value == &[1.0, 0.0, 0.0])
        );
        assert!(
            matches!(values["bio"][0].1.as_deref(), Some(CheckpointValue::Text { doc_len: 3, tokens }) if tokens.get("captured") == Some(&2))
        );
        let deleted = engine
            .freeze_checkpoint_collections(Some(&lineage))
            .unwrap();
        let deleted_dir = tempfile::tempdir().unwrap();
        let deleted_capture = deleted.write(deleted_dir.path(), 2).unwrap();
        for (field, _) in &fields {
            assert!(
                deleted_capture.field_deltas["c"][*field][0].1.is_none(),
                "delete must be an explicit frozen tombstone for {field}"
            );
        }
    }
}
