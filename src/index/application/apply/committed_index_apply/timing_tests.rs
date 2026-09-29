use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::CreateCollectionRequest;

fn apply_committed_vector(engine: &Engine, vector: Vec<f32>, sequence: u64) {
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "id".into(),
                field: "vec".into(),
                value: FieldValue::Vector(vector),
                version: Some(sequence),
            }],
            request_id: None,
        },
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let mut outcome = None;
    assert!(engine
        .try_apply_committed_index(&scanner, sequence, |apply, result| {
            apply.advance_sequence(sequence);
            outcome = Some(result);
        })
        .unwrap());
    assert!(matches!(outcome, Some(Ok(ApplyOutcome::Indexed(_)))));
}

#[test]
fn replacing_existing_hnsw_vector_records_remove_and_add_lock_observations() {
    let engine = Engine::new();
    engine
        .create_collection_inner(
            "docs",
            CreateCollectionRequest {
                fields: serde_json::from_value(serde_json::json!({
                    "vec": {
                        "type": "vector",
                        "dim": 3,
                        "metric": "l2",
                        "backend": "hnsw-cpu"
                    }
                }))
                .unwrap(),
            },
        )
        .unwrap();
    apply_committed_vector(&engine, vec![0.0, 1.0, 2.0], 1);
    let before = engine.metrics().hnsw_write_lock_wait_seconds_count.get();
    let rebuilds_before = engine.metrics().hnsw_graph_rebuild_seconds_count.get();
    assert_eq!(
        before, 2,
        "the initial committed HNSW write measures its no-op remove and add"
    );
    assert_eq!(rebuilds_before, 0, "the initial HNSW add does not rebuild");

    apply_committed_vector(&engine, vec![2.0, 1.0, 0.0], 2);

    for count in [
        engine.metrics().hnsw_write_lock_wait_seconds_count.get(),
        engine.metrics().hnsw_write_lock_held_seconds_count.get(),
    ] {
        assert_eq!(
            count - before,
            2,
            "an existing-vector replace must record HNSW remove plus add"
        );
    }
    assert_eq!(
        engine.metrics().hnsw_graph_rebuild_seconds_count.get() - rebuilds_before,
        1,
        "the replacement add rebuilds the orphaned HNSW graph once"
    );
}
