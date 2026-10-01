use std::collections::BTreeMap;

use crate::index::application::apply::committed_index_apply::tests::borrowed_vector_apply_tests::{
    borrowed_vector, vector_engine, vector_request,
};
use crate::index::application::apply::committed_index_apply::tests::item;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::shared_kernel::types::document::{FieldValue, ReplaceDocItem, ReplaceDocsRequest};
use crate::shared_kernel::types::schema::VectorBackend;

#[test]
fn committed_vector_apply_records_lock_wait_and_hnsw_add_by_backend() {
    for (backend, expected_hnsw_adds) in [(VectorBackend::HnswCpu, 1), (VectorBackend::FlatCpu, 0)]
    {
        let engine = vector_engine(backend, false);
        let (handled, outcome) = borrowed_vector(
            &engine,
            &vector_request(
                vec![item(
                    "id",
                    "vec",
                    FieldValue::Vector(vec![0.0, 1.0, 2.0]),
                    Some(1),
                )],
                None,
            ),
            1,
        );
        assert!(handled, "Vector must stay on the borrowed fast ledger");
        assert!(matches!(outcome, Some(Ok(ApplyOutcome::Indexed(_)))));
        assert_eq!(
            engine
                .metrics()
                .engine_state_write_lock_wait_seconds_count
                .get(),
            1,
            "every committed Vector apply acquires the state write lock once"
        );
        assert_eq!(
            engine.metrics().hnsw_add_seconds_count.get(),
            expected_hnsw_adds,
            "only the HNSW backend records live graph-add work"
        );
    }
}

#[test]
fn normal_live_vector_index_and_replace_record_hnsw_timing_by_backend() {
    for (
        backend,
        hnsw_adds_after_index,
        hnsw_adds_after_replace,
        hnsw_lock_observations_after_replace,
        hnsw_rebuilds_after_replace,
    ) in [
        (VectorBackend::HnswCpu, 1, 2, 2, 1),
        (VectorBackend::FlatCpu, 0, 0, 0, 0),
    ] {
        let engine = vector_engine(backend, false);
        engine
            .index(
                "docs",
                vector_request(
                    vec![item(
                        "id",
                        "vec",
                        FieldValue::Vector(vec![0.0, 1.0, 2.0]),
                        Some(1),
                    )],
                    None,
                ),
            )
            .unwrap();
        assert_eq!(
            engine
                .metrics()
                .engine_state_write_lock_wait_seconds_count
                .get(),
            1,
            "normal live Index acquires the state write lock once"
        );
        assert_eq!(
            engine.metrics().hnsw_add_seconds_count.get(),
            hnsw_adds_after_index,
            "normal live Index records only HNSW graph-add work"
        );
        let hnsw_adds_before_replace = engine.metrics().hnsw_add_seconds_count.get();
        let hnsw_lock_waits_before_replace =
            engine.metrics().hnsw_write_lock_wait_seconds_count.get();
        let hnsw_lock_holds_before_replace =
            engine.metrics().hnsw_write_lock_held_seconds_count.get();
        let hnsw_rebuilds_before_replace = engine.metrics().hnsw_graph_rebuild_seconds_count.get();
        let engine_lock_holds_before_replace = engine
            .metrics()
            .engine_state_write_lock_held_seconds_count
            .get();

        engine
            .replace_docs(
                "docs",
                ReplaceDocsRequest {
                    docs: vec![ReplaceDocItem {
                        external_id: "id".to_owned(),
                        version: Some(2),
                        fields: BTreeMap::from([(
                            "vec".to_owned(),
                            FieldValue::Vector(vec![2.0, 1.0, 0.0]),
                        )]),
                    }],
                },
            )
            .unwrap();
        assert_eq!(
            engine
                .metrics()
                .engine_state_write_lock_wait_seconds_count
                .get(),
            2,
            "normal live Replace acquires the state write lock once"
        );
        assert_eq!(
            engine.metrics().hnsw_add_seconds_count.get() - hnsw_adds_before_replace,
            hnsw_adds_after_replace - hnsw_adds_after_index,
            "normal live Replace records only HNSW graph-add work"
        );
        assert_eq!(
            engine.metrics().hnsw_write_lock_wait_seconds_count.get()
                - hnsw_lock_waits_before_replace,
            hnsw_lock_observations_after_replace,
            "replacing a live HNSW vector records its remove and add lock waits"
        );
        assert_eq!(
            engine.metrics().hnsw_write_lock_held_seconds_count.get()
                - hnsw_lock_holds_before_replace,
            hnsw_lock_observations_after_replace,
            "replacing a live HNSW vector records its remove and add lock holds"
        );
        assert_eq!(
            engine.metrics().hnsw_graph_rebuild_seconds_count.get() - hnsw_rebuilds_before_replace,
            hnsw_rebuilds_after_replace,
            "only the HNSW replacement records its graph rebuild"
        );
        assert_eq!(
            engine
                .metrics()
                .engine_state_write_lock_held_seconds_count
                .get()
                - engine_lock_holds_before_replace,
            1,
            "normal live Replace records its engine writer hold once"
        );
    }
}
