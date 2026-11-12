//! #297: `float64` is a validated alias of `number`. Exact, range and sort
//! work at the boundaries, and NaN or infinity is refused on write.

use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::query::{
    QueryNode, RangeBound, RangeQuery, SortMissing, SortOrder, SortSpec, TermQuery,
};
use crate::shared_kernel::types::schema::CreateCollectionRequest;
use crate::shared_kernel::types::search::SearchRequest;

/// A collection `c` with one field `price` of the given type name, seeded
/// with four docs: `lo` = -1.0, `negz` = -0.0, `mid` = 10.0, `big` = f64::MAX.
fn seeded(type_name: &str) -> Engine {
    let e = Engine::new();
    let schema: CreateCollectionRequest = serde_json::from_str(&format!(
        r#"{{"fields":{{"price":{{"type":"{type_name}"}}}}}}"#
    ))
    .unwrap();
    e.create_collection("c", schema).unwrap();
    for (id, p) in [
        ("lo", -1.0_f64),
        ("negz", -0.0),
        ("mid", 10.0),
        ("big", f64::MAX),
    ] {
        e.index("c", price_request(id, p)).unwrap();
    }
    e
}

fn price_request(id: &str, value: f64) -> IndexRequest {
    IndexRequest {
        items: vec![IndexItem {
            external_id: id.into(),
            field: "price".into(),
            value: FieldValue::Number(value),
            version: None,
        }],
        request_id: None,
    }
}

fn request(query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchRequest {
    SearchRequest {
        query,
        limit: 100,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort,
        track_total: true,
        collapse: None,
    }
}

fn term(value: f64) -> QueryNode {
    QueryNode::Term(TermQuery {
        field: "price".into(),
        value: FieldValue::Number(value),
    })
}

fn gte(value: f64) -> QueryNode {
    QueryNode::Range(RangeQuery {
        field: "price".into(),
        gt: None,
        gte: Some(RangeBound::Number(value)),
        lt: None,
        lte: None,
    })
}

fn sort_by(order: SortOrder) -> Vec<SortSpec> {
    vec![SortSpec {
        field: "price".into(),
        order,
        missing: SortMissing::Exclude,
    }]
}

fn ids(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> Vec<String> {
    e.search("c", request(query, sort))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| h.external_id)
        .collect()
}

/// Exact, range and sort results on the seeded docs, in a fixed order:
/// exact `-0.0`, exact `f64::MAX`, exact `-1.0`, range sorted by id,
/// sort ascending, sort descending.
fn observed(e: &Engine) -> Vec<Vec<String>> {
    let mut range = ids(e, gte(5.0), None);
    range.sort();
    vec![
        ids(e, term(-0.0), None),
        ids(e, term(f64::MAX), None),
        ids(e, term(-1.0), None),
        range,
        ids(e, gte(-2.0), Some(sort_by(SortOrder::Asc))),
        ids(e, gte(-2.0), Some(sort_by(SortOrder::Desc))),
    ]
}

fn expected() -> Vec<Vec<String>> {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    vec![
        s(&["negz"]),
        s(&["big"]),
        s(&["lo"]),
        s(&["big", "mid"]),
        s(&["lo", "negz", "mid", "big"]),
        s(&["big", "mid", "negz", "lo"]),
    ]
}

#[test]
fn float64_exact_range_and_sort_work_at_boundaries() {
    assert_eq!(observed(&seeded("float64")), expected());
}

#[test]
fn float64_refuses_nan_and_infinity_on_write() {
    let e = seeded("float64");
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = e.index("c", price_request("bad", bad)).unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<StorageError>(),
                Some(StorageError::InvalidNumber(_))
            ),
            "{bad}: got {err}"
        );
    }
    let total = e.search("c", request(gte(-2.0), None)).unwrap().total;
    assert_eq!(total, 4);
}

#[test]
fn number_field_behaves_like_float64() {
    assert_eq!(observed(&seeded("number")), expected());
}
