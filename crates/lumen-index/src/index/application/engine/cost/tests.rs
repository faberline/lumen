//! Pricing a log entry: the record cost reads the Engine's schema and known
//! external ids, charges replace tombstones and the coverage an unindex
//! actually removes, skips deduplicated requests, leaves out the vector graph,
//! and bounds the public n-gram tokenizer's output.

use std::collections::BTreeMap;

use crate::index::application::engine::tests::{item, record_cost_schema};
use crate::index::application::engine::Engine;
use crate::index::domain::analysis::tokenize;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest, ReplaceDocsRequest};
use crate::shared_kernel::types::schema::Analyzer;

fn estimated_total(record: crate::ingest::domain::change_record_cost::RecordCost) -> usize {
    record.active + record.frozen + record.prepublish
}

fn ready_record_cost(
    engine: &Engine,
    entry: &crate::shared_kernel::log_entry::RaftLogEntry,
) -> crate::ingest::domain::change_record_cost::RecordCost {
    match engine.estimate_record_cost(entry) {
        crate::ingest::domain::change_record_cost::RecordEstimate::Ready(cost) => cost,
        retained => panic!("expected a decidable record cost, got {retained:?}"),
    }
}

#[test]
fn record_cost_uses_engine_schema_and_known_external_id() {
    let engine = Engine::new();
    engine
        .create_collection("c", record_cost_schema(None))
        .unwrap();
    let entry = crate::shared_kernel::log_entry::RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![item(
                "doc",
                "email",
                FieldValue::String("a@example.test".into()),
            )],
            request_id: None,
        },
    };
    let new_document = ready_record_cost(&engine, &entry);
    engine
        .index(
            "c",
            IndexRequest {
                items: vec![item(
                    "doc",
                    "email",
                    FieldValue::String("a@example.test".into()),
                )],
                request_id: None,
            },
        )
        .unwrap();
    let existing_document = ready_record_cost(&engine, &entry);

    assert!(
        estimated_total(new_document) > estimated_total(existing_document),
        "a new external ID must include interner and coverage ownership"
    );
}

#[test]
fn record_cost_charges_replace_tombstones_and_actual_unindex_coverage() {
    let engine = Engine::new();
    engine
        .create_collection("c", record_cost_schema(None))
        .unwrap();
    engine
        .index(
            "c",
            IndexRequest {
                items: vec![
                    item("doc", "email", FieldValue::String("a@example.test".into())),
                    item("doc", "text", FieldValue::String("alphabet".into())),
                ],
                request_id: None,
            },
        )
        .unwrap();

    let replace = crate::shared_kernel::log_entry::RaftLogEntry::ReplaceDocs {
        collection_id: "c".into(),
        req: ReplaceDocsRequest {
            docs: vec![crate::shared_kernel::types::document::ReplaceDocItem {
                external_id: "doc".into(),
                version: None,
                fields: BTreeMap::from([(
                    "email".into(),
                    FieldValue::String("next@example.test".into()),
                )]),
            }],
        },
    };
    let replace_cost = ready_record_cost(&engine, &replace);
    let unindex = crate::shared_kernel::log_entry::RaftLogEntry::UnindexDocs {
        collection_id: "c".into(),
        req: crate::shared_kernel::types::document::BatchUnindexDocsRequest {
            external_ids: vec!["doc".into()],
        },
    };
    let covered_unindex = ready_record_cost(&engine, &unindex);
    let missing_unindex = ready_record_cost(
        &engine,
        &crate::shared_kernel::log_entry::RaftLogEntry::UnindexDocs {
            collection_id: "c".into(),
            req: crate::shared_kernel::types::document::BatchUnindexDocsRequest {
                external_ids: vec!["missing".into()],
            },
        },
    );

    assert!(
        estimated_total(replace_cost) > 0,
        "omitted text must add a tombstone cost"
    );
    assert!(
        estimated_total(covered_unindex) > estimated_total(missing_unindex),
        "unindex must use current coverage instead of a caller supplied field count"
    );
}

#[test]
fn record_cost_skips_deduplicated_requests_and_excludes_vector_graph() {
    let flat = Engine::new();
    let hnsw = Engine::new();
    flat.create_collection(
        "c",
        record_cost_schema(Some(
            crate::shared_kernel::types::schema::VectorBackend::FlatCpu,
        )),
    )
    .unwrap();
    hnsw.create_collection(
        "c",
        record_cost_schema(Some(
            crate::shared_kernel::types::schema::VectorBackend::HnswCpu,
        )),
    )
    .unwrap();
    let vector_entry = |collection_id: &str| crate::shared_kernel::log_entry::RaftLogEntry::Index {
        collection_id: collection_id.into(),
        req: IndexRequest {
            items: vec![item(
                "doc",
                "vector",
                FieldValue::Vector(vec![1.0, 2.0, 3.0]),
            )],
            request_id: None,
        },
    };
    assert_eq!(
        ready_record_cost(&flat, &vector_entry("c")),
        ready_record_cost(&hnsw, &vector_entry("c")),
        "pending vector payload cost must not charge an HNSW graph"
    );

    let request_id = "request-1";
    let valid_request = IndexRequest {
        items: vec![item(
            "doc",
            "vector",
            FieldValue::Vector(vec![1.0, 2.0, 3.0]),
        )],
        request_id: Some(request_id.into()),
    };
    flat.index("c", valid_request.clone()).unwrap();
    let duplicate = crate::shared_kernel::log_entry::RaftLogEntry::Index {
        collection_id: "c".into(),
        req: valid_request,
    };
    assert_eq!(
        ready_record_cost(&flat, &duplicate),
        crate::ingest::domain::change_record_cost::RecordCost::default()
    );
}

#[test]
fn record_cost_ngram_bound_covers_public_tokenizer_for_ascii_and_unicode() {
    for input in ["abcd", "İstanbul 42"] {
        let actual = tokenize::tokenize(input, Analyzer::Ngram);
        let bound = crate::ingest::domain::change_record_cost::text_upper_bound::text_upper_bound(
            input,
            crate::ingest::domain::change_record_cost::text_upper_bound::AnalyzerKind::Ngram,
            tokenize::DEFAULT_NGRAM_MIN,
            tokenize::DEFAULT_NGRAM_MAX,
        )
        .unwrap();
        assert!(bound.terms >= actual.len());
        assert!(bound.total_utf8_bytes >= actual.iter().map(|term| term.len()).sum::<usize>());
    }
}
