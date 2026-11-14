//! `float64` is a validated alias of `number`: the same exact, range and sort
//! behavior, and NaN or infinity refused on write.

use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::query::{
    QueryNode, RangeBound, RangeQuery, SortMissing, SortOrder, SortSpec, TermQuery,
};
use crate::shared_kernel::types::schema::CreateCollectionRequest;
use crate::shared_kernel::types::search::SearchRequest;

/// Builds collection `c` whose `price` field has the given type name, and
/// seeds four docs: `lo` = -1.0, `negz` = -0.0, `mid` = 10.0, `big` = f64::MAX.
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
        put(&e, id, v).unwrap();
    }
    e
}

fn put(e: &Engine, id: &str, v: f64) -> anyhow::Result<()> {
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
    .map(|_| ())
}

fn search(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> (Vec<String>, u64) {
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
    (
        resp.hits.iter().map(|h| h.external_id.clone()).collect(),
        resp.total,
    )
}

fn term(v: f64) -> QueryNode {
    QueryNode::Term(TermQuery {
        field: "price".into(),
        value: FieldValue::Number(v),
    })
}

fn gte(v: f64) -> QueryNode {
    QueryNode::Range(RangeQuery {
        field: "price".into(),
        gt: None,
        gte: Some(RangeBound::Number(v)),
        lt: None,
        lte: None,
    })
}

fn sort_price(order: SortOrder) -> Vec<SortSpec> {
    vec![SortSpec {
        field: "price".into(),
        order,
        missing: SortMissing::Exclude,
    }]
}

fn assert_exact_range_and_sort(e: &Engine) {
    assert_eq!(search(e, term(f64::MAX), None).0, vec!["big".to_string()]);
    assert_eq!(search(e, term(-1.0), None).0, vec!["lo".to_string()]);

    let mut ranged = search(e, gte(5.0), None).0;
    ranged.sort();
    assert_eq!(ranged, vec!["big".to_string(), "mid".to_string()]);

    assert_eq!(
        search(e, gte(-2.0), Some(sort_price(SortOrder::Asc))).0,
        ["lo", "negz", "mid", "big"]
    );
    assert_eq!(
        search(e, gte(-2.0), Some(sort_price(SortOrder::Desc))).0,
        ["big", "mid", "negz", "lo"]
    );
}

#[test]
fn float64_exact_range_and_sort_work_at_boundaries() {
    assert_exact_range_and_sort(&seed("float64"));
}

#[test]
fn float64_refuses_nan_and_infinity_on_write() {
    let e = seed("float64");
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let err = put(&e, "bad", bad).expect_err("NaN and infinity must be refused");
        assert!(
            matches!(
                err.downcast_ref::<StorageError>(),
                Some(StorageError::InvalidNumber(_))
            ),
            "expected InvalidNumber for {bad}, got {err:?}"
        );
    }
    let (_, total) = search(&e, gte(-2.0), None);
    assert_eq!(total, 4, "the four seed docs remain after the refusals");
}

#[test]
fn number_field_behaves_like_float64() {
    assert_exact_range_and_sort(&seed("number"));
}
