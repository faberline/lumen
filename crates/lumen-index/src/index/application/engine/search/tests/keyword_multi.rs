//! A keyword field with `multi: true` keeps the keyword type and has set
//! semantics, and a persisted `{type: set}` schema still loads.

use std::collections::BTreeMap;

use crate::index::application::engine::tests::item;
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, TermQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

fn tags_schema(spec: FieldSpec) -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("tags".into(), spec);
    CreateCollectionRequest { fields }
}

fn term_total(e: &Engine, value: &str) -> (u64, Vec<String>) {
    let resp = e
        .search(
            "c",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "tags".into(),
                    value: FieldValue::String(value.into()),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
                scoring: None,
                filter: None,
            },
        )
        .unwrap();
    let eids = resp.hits.iter().map(|h| h.external_id.clone()).collect();
    (resp.total, eids)
}

#[test]
fn keyword_multi_values_are_a_set() {
    let e = Engine::new();
    let spec = FieldSpec {
        field_type: FieldType::Keyword,
        analyzer: None,
        multi: Some(true),
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
        facetable: false,
    }
    .normalize();
    e.create_collection("c", tags_schema(spec)).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item(
                    "d1",
                    "tags",
                    FieldValue::StringList(vec!["a".into(), "b".into(), "a".into()]),
                ),
                item(
                    "d2",
                    "tags",
                    FieldValue::StringList(vec!["b".into(), "a".into()]),
                ),
            ],
            request_id: None,
        },
    )
    .unwrap();
    assert_eq!(term_total(&e, "a"), (2, vec!["d1".into(), "d2".into()]));
    assert_eq!(term_total(&e, "b"), (2, vec!["d1".into(), "d2".into()]));
    assert_eq!(term_total(&e, "c"), (0, vec![]));
}

#[test]
fn legacy_set_schema_still_loads() {
    let spec = serde_json::from_str::<FieldSpec>(r#"{"type":"set"}"#)
        .unwrap()
        .normalize();
    let e = Engine::new();
    e.create_collection("c", tags_schema(spec)).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![item(
                "d1",
                "tags",
                FieldValue::StringList(vec!["a".into(), "b".into()]),
            )],
            request_id: None,
        },
    )
    .unwrap();
    let snap = e.snapshot().unwrap();
    let restored = Engine::new();
    restored.restore(snap).unwrap();
    assert_eq!(term_total(&restored, "a"), (1, vec!["d1".into()]));
    assert_eq!(term_total(&restored, "b"), (1, vec!["d1".into()]));
}
