//! An empty list (`[]`) is a missing value in the engine write paths: an index write keeps
//! the stored value, and a replace treats the field as omitted. A vector field keeps its
//! dimension check.

use std::collections::BTreeMap;

use crate::index::application::engine::tests::item;
use crate::index::application::engine::Engine;
use crate::index::domain::schema_validation::validate_value;
use crate::shared_kernel::types::document::{
    FieldValue, IndexRequest, ReplaceDocItem, ReplaceDocResult, ReplaceDocsRequest,
};
use crate::shared_kernel::types::query::{
    ExistsQuery, HammingQuery, MatchOp, MatchQuery, QueryNode,
};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType, VectorBackend, VectorMetric,
};
use crate::shared_kernel::types::search::SearchRequest;

const LIST_TYPES: [FieldType; 5] = [
    FieldType::Keyword,
    FieldType::Number,
    FieldType::Set,
    FieldType::Text,
    FieldType::Hash,
];

/// A collection `c` with one field `f` of type `ty` and a second keyword field `k`.
fn schema_of(ty: FieldType) -> CreateCollectionRequest {
    let analyzer = match ty {
        FieldType::Text => Some(Analyzer::WhitespaceLower),
        _ => None,
    };
    let mut fields = BTreeMap::new();
    fields.insert(
        "f".into(),
        FieldSpec {
            field_type: ty,
            analyzer,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
            facetable: false,
        },
    );
    fields.insert(
        "k".into(),
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
            facetable: false,
        },
    );
    CreateCollectionRequest { fields }
}

/// A real value of each list type. It is a hash for `Hash`, so `Hamming` finds it.
fn sample(ty: FieldType) -> FieldValue {
    match ty {
        FieldType::Keyword => FieldValue::String("red".into()),
        FieldType::Number => FieldValue::Number(5.0),
        FieldType::Set => FieldValue::StringList(vec!["a".into()]),
        FieldType::Text => FieldValue::String("hello world".into()),
        FieldType::Hash => FieldValue::String("00000000000000ff".into()),
        _ => unreachable!("sample is for the five list types"),
    }
}

/// The empty value of a type: `Number` and `Hash` take a vector, the others a string list.
fn empty_of(ty: FieldType) -> FieldValue {
    match ty {
        FieldType::Number | FieldType::Hash => FieldValue::Vector(vec![]),
        _ => FieldValue::StringList(vec![]),
    }
}

/// The sorted ids of the hits of one query on the collection `c`.
fn search_ids(e: &Engine, query: QueryNode) -> Vec<String> {
    let response = e
        .search(
            "c",
            SearchRequest {
                query,
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
    let mut ids: Vec<String> = response
        .hits
        .iter()
        .map(|hit| hit.external_id.clone())
        .collect();
    ids.sort();
    ids
}

/// The ids of the documents that have field `f`. `Exists` is refused on text and hash
/// fields, so those use a text match and a Hamming match.
fn present(e: &Engine, ty: FieldType) -> Vec<String> {
    let query = match ty {
        FieldType::Text => QueryNode::Match(MatchQuery {
            field: "f".into(),
            text: "hello".into(),
            op: MatchOp::And,
        }),
        FieldType::Hash => QueryNode::Hamming(HammingQuery {
            field: "f".into(),
            hash: "00000000000000ff".into(),
            max_distance: 0,
        }),
        _ => QueryNode::Exists(ExistsQuery { field: "f".into() }),
    };
    search_ids(e, query)
}

/// The ids of the documents that have field `k`.
fn present_k(e: &Engine) -> Vec<String> {
    search_ids(e, QueryNode::Exists(ExistsQuery { field: "k".into() }))
}

/// Replaces the document `d` with `fields` and returns the per-item results.
fn replace_d(e: &Engine, fields: BTreeMap<String, FieldValue>) -> Vec<ReplaceDocResult> {
    e.replace_docs(
        "c",
        ReplaceDocsRequest {
            docs: vec![ReplaceDocItem {
                external_id: "d".into(),
                version: None,
                fields,
            }],
        },
    )
    .unwrap()
    .results
}

fn replace_empty_list_drops_the_field(ty: FieldType) {
    let e = Engine::new();
    e.create_collection("c", schema_of(ty)).unwrap();
    let mut fields = BTreeMap::new();
    fields.insert("k".into(), FieldValue::String("x".into()));
    fields.insert("f".into(), sample(ty));
    replace_d(&e, fields);
    assert_eq!(present(&e, ty), ["d"]);

    let mut fields = BTreeMap::new();
    fields.insert("k".into(), FieldValue::String("x".into()));
    fields.insert("f".into(), FieldValue::StringList(vec![]));
    let results = replace_d(&e, fields);
    assert!(
        matches!(results[0], ReplaceDocResult::Ok { .. }),
        "{results:?}"
    );
    assert!(present(&e, ty).is_empty());
    assert_eq!(present_k(&e), ["d"]);
}

fn index_empty_list_keeps_the_old_value(ty: FieldType) {
    let e = Engine::new();
    e.create_collection("c", schema_of(ty)).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("d", "f", sample(ty)),
                item("d", "k", FieldValue::String("x".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![item("d", "f", empty_of(ty))],
            request_id: None,
        },
    )
    .unwrap();
    assert_eq!(present(&e, ty), ["d"]);
}

#[test]
fn replace_empty_list_drops_the_field_keyword() {
    replace_empty_list_drops_the_field(FieldType::Keyword);
}

#[test]
fn replace_empty_list_drops_the_field_number() {
    replace_empty_list_drops_the_field(FieldType::Number);
}

#[test]
fn replace_empty_list_drops_the_field_set() {
    replace_empty_list_drops_the_field(FieldType::Set);
}

#[test]
fn replace_empty_list_drops_the_field_text() {
    replace_empty_list_drops_the_field(FieldType::Text);
}

#[test]
fn replace_empty_list_drops_the_field_hash() {
    replace_empty_list_drops_the_field(FieldType::Hash);
}

#[test]
fn index_empty_list_keeps_the_old_value_keyword() {
    index_empty_list_keeps_the_old_value(FieldType::Keyword);
}

#[test]
fn index_empty_list_keeps_the_old_value_number() {
    index_empty_list_keeps_the_old_value(FieldType::Number);
}

#[test]
fn index_empty_list_keeps_the_old_value_set() {
    index_empty_list_keeps_the_old_value(FieldType::Set);
}

#[test]
fn index_empty_list_keeps_the_old_value_text() {
    index_empty_list_keeps_the_old_value(FieldType::Text);
}

#[test]
fn index_empty_list_keeps_the_old_value_hash() {
    index_empty_list_keeps_the_old_value(FieldType::Hash);
}

/// Negative control: a real value on each list type is still indexed and found.
#[test]
fn non_empty_values_are_still_indexed_and_found() {
    for ty in LIST_TYPES {
        let e = Engine::new();
        e.create_collection("c", schema_of(ty)).unwrap();
        e.index(
            "c",
            IndexRequest {
                items: vec![item("d", "f", sample(ty))],
                request_id: None,
            },
        )
        .unwrap();
        assert_eq!(present(&e, ty), ["d"]);
    }
}

#[test]
fn unknown_field_with_an_empty_list_is_still_an_error() {
    let e = Engine::new();
    e.create_collection("c", schema_of(FieldType::Keyword))
        .unwrap();
    let err = e
        .index(
            "c",
            IndexRequest {
                items: vec![item("d", "nope", FieldValue::StringList(vec![]))],
                request_id: None,
            },
        )
        .expect_err("an unknown field is an error");
    assert!(format!("{err:#}").contains("nope"), "{err:#}");

    let mut fields = BTreeMap::new();
    fields.insert("nope".into(), FieldValue::StringList(vec![]));
    let results = replace_d(&e, fields);
    assert!(
        matches!(&results[0], ReplaceDocResult::Error { code, .. } if code == "unknown_field"),
        "{results:?}"
    );
}

fn validate_empty_vector_for(ty: FieldType) -> bool {
    let e = Engine::new();
    e.create_collection("c", schema_of(ty)).unwrap();
    let state = e.state.read().unwrap();
    let fi = state.collections.get("c").unwrap().fields.get("f").unwrap();
    validate_value(fi, &FieldValue::Vector(vec![]), "f").is_ok()
}

#[test]
fn validate_value_accepts_an_empty_list_keyword() {
    assert!(validate_empty_vector_for(FieldType::Keyword));
}

#[test]
fn validate_value_accepts_an_empty_list_number() {
    assert!(validate_empty_vector_for(FieldType::Number));
}

#[test]
fn validate_value_accepts_an_empty_list_set() {
    assert!(validate_empty_vector_for(FieldType::Set));
}

#[test]
fn validate_value_accepts_an_empty_list_text() {
    assert!(validate_empty_vector_for(FieldType::Text));
}

#[test]
fn validate_value_accepts_an_empty_list_hash() {
    assert!(validate_empty_vector_for(FieldType::Hash));
}

/// Negative control: an empty vector on a vector field keeps its dimension check.
#[test]
fn validate_value_still_rejects_an_empty_vector_on_a_vector_field() {
    let mut fields = BTreeMap::new();
    fields.insert(
        "v".into(),
        FieldSpec {
            field_type: FieldType::Vector,
            analyzer: None,
            multi: None,
            dim: Some(3),
            metric: Some(VectorMetric::Cosine),
            backend: Some(VectorBackend::HnswCpu),
            quantize: None,
            facetable: false,
        },
    );
    let e = Engine::new();
    e.create_collection("c", CreateCollectionRequest { fields })
        .unwrap();
    let state = e.state.read().unwrap();
    let fi = state.collections.get("c").unwrap().fields.get("v").unwrap();
    assert!(validate_value(fi, &FieldValue::Vector(vec![]), "v").is_err());
}
