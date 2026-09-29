use std::sync::Arc;

use anyhow::Result;

use crate::index::application::apply::committed_index_apply::tests::item;
use crate::index::application::apply::committed_index_apply::BEFORE_ATTACH;
use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, VectorBackend};

fn vector_engine(backend: VectorBackend, sq: bool) -> Arc<Engine> {
    let engine = Arc::new(Engine::with_change_budget(
        crate::ingest::domain::change_budget::ChangeBudget::with_hard_limit(32 * 1024 * 1024),
    ));
    engine.create_collection_inner("docs", CreateCollectionRequest { fields: serde_json::from_value(serde_json::json!({
        "vec": {"type":"vector", "dim":3, "metric":"l2", "backend": if matches!(backend, VectorBackend::FlatCpu) { "flat-cpu" } else { "hnsw-cpu" }, "quantize": if sq { serde_json::json!("sq") } else { serde_json::Value::Null } }
    })).unwrap() }).unwrap();
    engine
}

fn vector_request(items: Vec<IndexItem>, request_id: Option<&str>) -> IndexRequest {
    IndexRequest {
        items,
        request_id: request_id.map(str::to_owned),
    }
}

fn borrowed_vector(
    engine: &Engine,
    req: &IndexRequest,
    sequence: u64,
) -> (bool, Option<Result<ApplyOutcome>>) {
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: req.clone(),
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let mut outcome = None;
    let handled = engine
        .try_apply_committed_index(&scanner, sequence, |apply, result| {
            apply.advance_sequence(sequence);
            outcome = Some(result);
        })
        .unwrap();
    (handled, outcome)
}

fn vector_bits(engine: &Engine, eid: &str) -> Option<Vec<u32>> {
    let state = engine.state.read().unwrap();
    let coll = &state.collections["docs"];
    let FieldIndex::Vector { idx, .. } = &coll.fields["vec"] else {
        unreachable!()
    };
    idx.checkpoint_vector(eid)
        .unwrap()
        .map(|row| row.into_iter().map(f32::to_bits).collect())
}

#[test]
fn borrowed_vector_matches_owned_ledger_for_each_backend_and_quantizer() {
    for backend in [VectorBackend::FlatCpu, VectorBackend::HnswCpu] {
        for sq in [false, true] {
            let actual = vector_engine(backend, sq);
            let owned = vector_engine(backend, sq);
            let ledger = [
                vector_request(
                    vec![item(
                        "id",
                        "vec",
                        FieldValue::Vector(vec![0.0, 1.0, 2.0]),
                        Some(2),
                    )],
                    None,
                ),
                // Older wrong-dimension action is stale and must not replace the first vector.
                vector_request(
                    vec![item("id", "vec", FieldValue::Vector(vec![9.0]), Some(1))],
                    None,
                ),
                // Both actions are valid. The v4 range must affect SQ decoding
                // of the v5 final winner; collapsing to only the winner is wrong.
                vector_request(
                    vec![
                        item(
                            "id",
                            "vec",
                            FieldValue::Vector(vec![-100.0, 0.0, 100.0]),
                            Some(4),
                        ),
                        item(
                            "id",
                            "vec",
                            FieldValue::Vector(vec![0.0, 1.0, 2.0]),
                            Some(5),
                        ),
                        item(
                            "third",
                            "vec",
                            FieldValue::Vector(vec![3.0, 4.0, 5.0]),
                            Some(1),
                        ),
                    ],
                    None,
                ),
                vector_request(
                    vec![item(
                        "id",
                        "vec",
                        FieldValue::Vector(vec![-4.0, 3.0, 9.0]),
                        Some(6),
                    )],
                    Some("once"),
                ),
                // A non-stale dimension error keeps the preceding valid cell.
                vector_request(
                    vec![
                        item(
                            "dimension-prefix",
                            "vec",
                            FieldValue::Vector(vec![6.0, 6.0, 6.0]),
                            Some(1),
                        ),
                        item(
                            "dimension-bad",
                            "vec",
                            FieldValue::Vector(vec![1.0]),
                            Some(1),
                        ),
                    ],
                    None,
                ),
                // Valid prefix is retained, then the existing id is dropped before
                // its wrong-type error, matching owned apply's error-prefix order.
                vector_request(
                    vec![
                        item(
                            "next",
                            "vec",
                            FieldValue::Vector(vec![2.0, 2.0, 2.0]),
                            Some(1),
                        ),
                        item(
                            "id",
                            "vec",
                            FieldValue::String("wrong type".into()),
                            Some(7),
                        ),
                    ],
                    None,
                ),
                vector_request(
                    vec![item(
                        "id",
                        "vec",
                        FieldValue::Vector(vec![7.0, 7.0, 7.0]),
                        Some(8),
                    )],
                    Some("once"),
                ),
            ];
            for (sequence, req) in ledger.iter().enumerate() {
                let (handled, outcome) = borrowed_vector(&actual, req, sequence as u64 + 1);
                assert!(handled, "Vector must stay on the borrowed fast ledger");
                let wanted = owned.index_inner("docs", req.clone(), None, None);
                match (outcome.expect("handled record completes"), wanted) {
                    (Ok(ApplyOutcome::Indexed(got)), Ok(want)) => assert_eq!(
                        serde_json::to_value(got).unwrap(),
                        serde_json::to_value(want).unwrap()
                    ),
                    (Err(got), Err(want)) => assert_eq!(got.to_string(), want.to_string()),
                    (got, want) => panic!("borrowed/owned outcome differs: {got:?} / {want:?}"),
                }
                assert_eq!(vector_bits(&actual, "id"), vector_bits(&owned, "id"));
                assert_eq!(vector_bits(&actual, "next"), vector_bits(&owned, "next"));
                assert_eq!(vector_bits(&actual, "third"), vector_bits(&owned, "third"));
            }
        }
    }
}

fn frozen_vector_bits(engine: &Engine, eid: &str) -> Option<Vec<u32>> {
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    let row = frozen.capture.frozen_changes["docs"].row("vec", eid)?;
    match &**row.value()? {
        CheckpointValue::StagedVector(value) => Some(
            value
                .as_f32_slice()
                .iter()
                .map(|value| value.to_bits())
                .collect(),
        ),
        CheckpointValue::Vector(value) => Some(value.iter().map(|value| value.to_bits()).collect()),
        value => panic!("expected vector journal value, got {value:?}"),
    }
}

#[test]
fn stale_sq_vector_preparation_retries_after_concurrent_codebook_widen() {
    for backend in [VectorBackend::FlatCpu, VectorBackend::HnswCpu] {
        let actual = vector_engine(backend, true);
        let reference = vector_engine(backend, true);
        // Reuse existing IDs so the data/apply version checks detect the
        // codebook race without help from the interner watermark.
        let initial = vector_request(
            vec![
                item(
                    "range",
                    "vec",
                    FieldValue::Vector(vec![0.0, 0.0, 0.0]),
                    None,
                ),
                item(
                    "pending",
                    "vec",
                    FieldValue::Vector(vec![0.0, 0.0, 0.0]),
                    None,
                ),
            ],
            None,
        );
        actual
            .index_inner("docs", initial.clone(), None, None)
            .unwrap();
        reference.index_inner("docs", initial, None, None).unwrap();
        let widening = vector_request(
            vec![item(
                "range",
                "vec",
                FieldValue::Vector(vec![-100.0, 0.0, 100.0]),
                Some(1),
            )],
            None,
        );
        let pending = vector_request(
            vec![item(
                "pending",
                "vec",
                FieldValue::Vector(vec![0.0, 1.0, 2.0]),
                Some(1),
            )],
            None,
        );

        // This is the owned order the eventual retry must reproduce. It is an
        // independent oracle: it does not use detached staging or BEFORE_ATTACH.
        reference
            .index_inner("docs", widening.clone(), None, None)
            .unwrap();
        reference
            .index_inner("docs", pending.clone(), None, None)
            .unwrap();

        let concurrent = actual.clone();
        BEFORE_ATTACH.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                concurrent
                    .index_inner("docs", widening, None, None)
                    .unwrap();
            }));
        });
        let (handled, outcome) = borrowed_vector(&actual, &pending, 41);
        assert!(
            handled,
            "the retried Vector record must stay on the borrowed route"
        );
        assert!(matches!(outcome, Some(Ok(ApplyOutcome::Indexed(_)))));

        assert_eq!(
            vector_bits(&actual, "pending"),
            vector_bits(&reference, "pending"),
            "live SQ storage must use the codebook widened by the concurrent apply"
        );
        assert_eq!(
            frozen_vector_bits(&actual, "pending"),
            frozen_vector_bits(&reference, "pending"),
            "the retained staged checkpoint row must be rebuilt from the widened codebook"
        );
    }
}

mod timing;
