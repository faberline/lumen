use anyhow::Result;

use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::CreateCollectionRequest;
use crate::shared_kernel::types::stats::StatsResponse;

fn engine() -> Engine {
    let engine = Engine::new();
    engine
        .create_collection_inner(
            "docs",
            CreateCollectionRequest {
                fields: serde_json::from_value(serde_json::json!({
                    "body": {"type": "text", "analyzer": "whitespace_lower"},
                    "title": {"type": "text", "analyzer": "whitespace_lower"}
                }))
                .unwrap(),
            },
        )
        .unwrap();
    engine
}

fn item(id: &str, field: &str, value: FieldValue, version: Option<u64>) -> IndexItem {
    IndexItem {
        external_id: id.to_owned(),
        field: field.to_owned(),
        value,
        version,
    }
}

fn request(items: Vec<IndexItem>, request_id: Option<&str>) -> IndexRequest {
    IndexRequest {
        items,
        request_id: request_id.map(str::to_owned),
    }
}

fn search_request(text: &str) -> crate::shared_kernel::types::search::SearchRequest {
    serde_json::from_value(serde_json::json!({
        "query": {"match": {"field": "body", "text": text, "op": "and"}},
        "limit": 10
    }))
    .unwrap()
}

fn search_json(engine: &Engine, text: &str) -> serde_json::Value {
    let mut value =
        serde_json::to_value(engine.search("docs", search_request(text)).unwrap()).unwrap();
    let object = value.as_object_mut().unwrap();
    object.remove("took_ms");
    object.remove("took_us");
    value
}

fn stats_json(engine: &Engine) -> serde_json::Value {
    let mut value = serde_json::to_value(engine.stats("docs").unwrap()).unwrap();
    value.as_object_mut().unwrap().remove("last_indexed_at");
    value
}

fn owned(engine: &Engine, request: IndexRequest) -> Result<ApplyOutcome> {
    engine
        .index_inner("docs", request, None, None)
        .map(ApplyOutcome::Indexed)
}

fn borrowed(
    engine: &Engine,
    request: IndexRequest,
    sequence: u64,
    before_watermark: impl FnOnce(&Engine),
) -> Result<ApplyOutcome> {
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: request,
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let mut completed = None;
    assert!(
        engine.try_apply_committed_index(&scanner, sequence, |apply, outcome| {
            // Dispatch has already changed the public index.  The durable
            // source callback receives that same enclosing apply lease and
            // moves the watermark only after it can observe those changes.
            before_watermark(engine);
            apply.advance_sequence(sequence);
            completed = Some(outcome);
        })?
    );
    completed.expect("borrowed Index must invoke its completion callback")
}

fn assert_same_result(actual: Result<ApplyOutcome>, expected: Result<ApplyOutcome>) {
    match (actual, expected) {
        (Ok(ApplyOutcome::Indexed(actual)), Ok(ApplyOutcome::Indexed(expected))) => {
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap(),
            );
        }
        (Ok(actual), Ok(expected)) => {
            panic!("borrowed and owned returned non-Index outcomes: {actual:?} / {expected:?}")
        }
        (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
        (actual, expected) => panic!("borrowed and owned differ: {actual:?} / {expected:?}"),
    }
}

#[test]
fn borrowed_text_matches_owned_bm25_stats_duplicate_ids_and_versions() {
    let actual = engine();
    let expected = engine();
    let request = request(
        vec![
            item(
                "alpha",
                "body",
                FieldValue::String("rust rust".into()),
                Some(1),
            ),
            item(
                "bravo",
                "body",
                FieldValue::String("rust search".into()),
                Some(1),
            ),
            item(
                "alpha",
                "body",
                FieldValue::String("systems engineer".into()),
                Some(2),
            ),
            item(
                "alpha",
                "body",
                FieldValue::String("stale value".into()),
                Some(1),
            ),
        ],
        Some("text-request"),
    );

    let actual_result = borrowed(&actual, request.clone(), 41, |engine| {
        let visible = search_json(engine, "engineer");
        assert_eq!(visible["hits"][0]["external_id"].as_str(), Some("alpha"));
    });
    let expected_result = owned(&expected, request);
    assert_same_result(actual_result, expected_result);
    assert_eq!(search_json(&actual, "rust"), search_json(&expected, "rust"));
    assert_eq!(
        search_json(&actual, "engineer"),
        search_json(&expected, "engineer")
    );
    assert_eq!(stats_json(&actual), stats_json(&expected));

    let stats: StatsResponse = actual.stats("docs").unwrap();
    assert_eq!(stats.documents_indexed, 2);
    assert_eq!(stats.fields["body"].avg_doc_len, Some(2.0));
    assert_eq!(stats.fields["body"].unique_terms, 4);
}

#[test]
fn borrowed_text_wrong_type_keeps_the_owned_valid_prefix_and_error() {
    let actual = engine();
    let expected = engine();
    let request = request(
        vec![
            item(
                "alpha",
                "body",
                FieldValue::String("valid prefix".into()),
                None,
            ),
            item("alpha", "title", FieldValue::Vector(vec![0.1, 0.2]), None),
        ],
        None,
    );

    assert_same_result(
        borrowed(&actual, request.clone(), 42, |_| {}),
        owned(&expected, request),
    );
    assert_eq!(
        search_json(&actual, "valid"),
        search_json(&expected, "valid")
    );
    assert_eq!(stats_json(&actual), stats_json(&expected));
    assert_eq!(
        search_json(&actual, "valid")["hits"][0]["external_id"].as_str(),
        Some("alpha"),
    );
}

#[test]
fn borrowed_text_unknown_field_keeps_the_owned_valid_prefix_and_error() {
    let actual = engine();
    let expected = engine();
    let request = request(
        vec![
            item(
                "alpha",
                "body",
                FieldValue::String("valid prefix".into()),
                Some(1),
            ),
            item(
                "alpha",
                "later",
                FieldValue::String("unknown".into()),
                Some(1),
            ),
        ],
        None,
    );

    assert_same_result(
        borrowed(&actual, request.clone(), 43, |_| {}),
        owned(&expected, request),
    );
    assert_eq!(
        search_json(&actual, "valid"),
        search_json(&expected, "valid")
    );
    assert_eq!(stats_json(&actual), stats_json(&expected));
    assert_eq!(
        search_json(&actual, "valid")["hits"][0]["external_id"].as_str(),
        Some("alpha"),
    );
}

#[test]
fn borrowed_text_duplicate_request_does_not_replace_the_first_value() {
    let actual = engine();
    let expected = engine();
    let first = request(
        vec![item(
            "alpha",
            "body",
            FieldValue::String("first text".into()),
            Some(1),
        )],
        Some("same-request"),
    );
    let duplicate = request(
        vec![item(
            "alpha",
            "body",
            FieldValue::String("later text".into()),
            Some(2),
        )],
        Some("same-request"),
    );

    assert_same_result(
        borrowed(&actual, first.clone(), 44, |_| {}),
        owned(&expected, first),
    );
    assert_same_result(
        borrowed(&actual, duplicate.clone(), 45, |_| {}),
        owned(&expected, duplicate),
    );
    assert_eq!(
        search_json(&actual, "first"),
        search_json(&expected, "first")
    );
    assert_eq!(
        search_json(&actual, "later"),
        search_json(&expected, "later")
    );
    assert_eq!(stats_json(&actual), stats_json(&expected));
    assert_eq!(
        search_json(&actual, "later")["hits"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}
