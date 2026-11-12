//! #297: `float64` is a validated alias of `number`. It has the same index,
//! the same exact, range and sort results, and it refuses NaN and infinity
//! on write with `StorageError::InvalidNumber`.

use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::query::{
    QueryNode, RangeBound, RangeQuery, SortMissing, SortOrder, SortSpec, TermQuery,
};
use crate::shared_kernel::types::schema::CreateCollectionRequest;
use crate::shared_kernel::types::search::SearchRequest;

/// A collection `c` with one number-like field `price` declared as `type_name`,
/// seeded with four docs: lo=-1.0, negz=-0.0, mid=10.0, big=f64::MAX.
fn seed(type_name: &str) -> Engine {
    let schema: CreateCollectionRequest = serde_json::from_str(&format!(
        r#"{{"fields":{{"price":{{"type":"{type_name}"}}}}}}"#
    ))
    .unwrap();
    let e = Engine::new();
    e.create_collection("c", schema).unwrap();
    for (id, v) in [
        ("lo", -1.0_f64),
        ("negz", -0.0),
        ("mid", 10.0),
        ("big", f64::MAX),
    ] {
        e.index(
            "c",
            IndexRequest {
                items: vec![IndexItem {
                    external_id: id.into(),
                    field: "price".into(),
                    value: FieldValue::Number(v),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }
    e
}

fn search(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> Vec<String> {
    let resp = e
        .search(
            "c",
            SearchRequest {
                query,
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
    resp.hits.iter().map(|h| h.external_id.clone()).collect()
}

fn exact(v: f64) -> QueryNode {
    QueryNode::Term(TermQuery {
        field: "price".into(),
        value: FieldValue::Number(v),
    })
}

fn at_least(v: f64) -> QueryNode {
    QueryNode::Range(RangeQuery {
        field: "price".into(),
        gte: Some(RangeBound::Number(v)),
        lt: None,
        gt: None,
        lte: None,
    })
}

fn sort_price(order: SortOrder) -> Option<Vec<SortSpec>> {
    Some(vec![SortSpec {
        field: "price".into(),
        order,
        missing: SortMissing::Exclude,
    }])
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn assert_exact_range_and_sort(type_name: &str) {
    let e = seed(type_name);
    assert_eq!(search(&e, exact(f64::MAX), None), vec!["big"]);
    assert_eq!(search(&e, exact(-1.0), None), vec!["lo"]);
    assert_eq!(
        sorted(search(&e, at_least(5.0), None)),
        vec!["big".to_string(), "mid".to_string()]
    );
    assert_eq!(
        search(&e, at_least(-2.0), sort_price(SortOrder::Asc)),
        vec!["lo", "negz", "mid", "big"]
    );
    assert_eq!(
        search(&e, at_least(-2.0), sort_price(SortOrder::Desc)),
        vec!["big", "mid", "negz", "lo"]
    );
}

#[test]
fn float64_exact_range_and_sort_work_at_boundaries() {
    assert_exact_range_and_sort("float64");
}

#[test]
fn float64_refuses_nan_and_infinity_on_write() {
    let e = seed("float64");
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = e
            .index(
                "c",
                IndexRequest {
                    items: vec![IndexItem {
                        external_id: "bad".into(),
                        field: "price".into(),
                        value: FieldValue::Number(bad),
                        version: None,
                    }],
                    request_id: None,
                },
            )
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<StorageError>(),
                Some(StorageError::InvalidNumber(_))
            ),
            "expected InvalidNumber for {bad}, got {err:?}"
        );
    }
    let resp = e
        .search(
            "c",
            SearchRequest {
                query: at_least(-2.0),
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
    assert_eq!(resp.total, 4);
}

#[test]
fn number_field_behaves_like_float64() {
    assert_exact_range_and_sort("number");
}
