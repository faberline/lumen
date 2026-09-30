//! #183: multi-key sort cap raised to MAX_SORT_KEYS (4). The generic plan
//! compares every key in priority order; > 4 keys is rejected.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::index::domain::query::sort::MAX_SORT_KEYS;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, RangeQuery, SortMissing, SortOrder, SortSpec};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

fn schema() -> CreateCollectionRequest {
    let num = || FieldSpec {
        field_type: FieldType::Number,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    };
    let mut fields = BTreeMap::new();
    fields.insert("a".into(), num());
    fields.insert("b".into(), num());
    fields.insert("c".into(), num());
    CreateCollectionRequest { fields }
}

fn idx(e: &Engine, eid: &str, a: f64, b: f64, c: f64) {
    let item = |field: &str, v: f64| IndexItem {
        external_id: eid.into(),
        field: field.into(),
        value: FieldValue::Number(v),
        version: None,
    };
    e.index(
        "c",
        IndexRequest {
            items: vec![item("a", a), item("b", b), item("c", c)],
            request_id: None,
        },
    )
    .unwrap();
}

fn sort_asc(fields: &[&str]) -> Vec<SortSpec> {
    fields
        .iter()
        .map(|f| SortSpec {
            field: (*f).into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        })
        .collect()
}

fn all() -> QueryNode {
    QueryNode::Range(RangeQuery {
        field: "a".into(),
        gte: None,
        gt: None,
        lte: None,
        lt: None,
    })
}

/// R1: a 3-key sort orders by each key in priority.
#[test]
fn three_key_sort_orders() {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    idx(&e, "d0", 1.0, 1.0, 1.0);
    idx(&e, "d1", 1.0, 1.0, 2.0);
    idx(&e, "d2", 1.0, 2.0, 1.0);
    idx(&e, "d3", 2.0, 1.0, 1.0);
    let r = e
        .search(
            "c",
            SearchRequest {
                query: all(),
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(sort_asc(&["a", "b", "c"])),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
    let ordered: Vec<String> = r.hits.iter().map(|h| h.external_id.clone()).collect();
    assert_eq!(ordered, vec!["d0", "d1", "d2", "d3"]);
    assert_eq!(r.total, 4);
}

/// R2: more than MAX_SORT_KEYS keys is rejected with UnsupportedSort.
#[test]
fn over_four_keys_rejected() {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    idx(&e, "d0", 1.0, 1.0, 1.0);
    let err = e
        .search(
            "c",
            SearchRequest {
                query: all(),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(sort_asc(&["a", "b", "c", "a", "b"])), // 5 keys
                track_total: true,
                collapse: None,
            },
        )
        .unwrap_err();
    let se = err.downcast_ref::<StorageError>().expect("StorageError");
    assert!(
        matches!(se, StorageError::UnsupportedSort(_)),
        "more than {MAX_SORT_KEYS} keys must be rejected, got {se:?}"
    );
}
