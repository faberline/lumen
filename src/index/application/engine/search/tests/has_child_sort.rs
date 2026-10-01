//! #181: a has_child query may be combined with sort. It resolves to a parent
//! bitmap via the materialized path, which is then sorted by a parent field.
//! knn/rrf/hamming + sort stay rejected.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::index::domain::query::sort_missing::MATERIALIZED_SORT_RETAINED_HIGH_WATER;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{
    HasChildQuery, KnnQuery, QueryNode, SortMissing, SortOrder, SortSpec, TermQuery,
};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

fn kw() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Keyword,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}
fn num() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Number,
        ..kw()
    }
}

fn order(e: &Engine, eid: &str, ts: f64, status: &str) {
    e.index(
        "orders",
        IndexRequest {
            items: vec![
                IndexItem {
                    external_id: eid.into(),
                    field: "ts".into(),
                    value: FieldValue::Number(ts),
                    version: None,
                },
                IndexItem {
                    external_id: eid.into(),
                    field: "status".into(),
                    value: FieldValue::String(status.into()),
                    version: None,
                },
            ],
            request_id: None,
        },
    )
    .unwrap();
}

fn child(e: &Engine, parent: &str, sku: &str) {
    e.index(
        "items",
        IndexRequest {
            items: vec![
                IndexItem {
                    external_id: format!("{parent}#0"),
                    field: "parent".into(),
                    value: FieldValue::String(parent.into()),
                    version: None,
                },
                IndexItem {
                    external_id: format!("{parent}#0"),
                    field: "sku".into(),
                    value: FieldValue::String(sku.into()),
                    version: None,
                },
            ],
            request_id: None,
        },
    )
    .unwrap();
}

/// orders o1(ts100,open) o2(ts200,closed) o3(ts300,open); items link each
/// order; o1,o2 have sku=S0, o3 has sku=X.
fn setup() -> Engine {
    let e = Engine::new();
    let mut pf = BTreeMap::new();
    pf.insert("status".into(), kw());
    pf.insert("rank".into(), kw());
    pf.insert("ts".into(), num());
    e.create_collection("orders", CreateCollectionRequest { fields: pf })
        .unwrap();
    let mut cf = BTreeMap::new();
    cf.insert("parent".into(), kw());
    cf.insert("sku".into(), kw());
    e.create_collection("items", CreateCollectionRequest { fields: cf })
        .unwrap();
    order(&e, "o1", 100.0, "open");
    order(&e, "o2", 200.0, "closed");
    order(&e, "o3", 300.0, "open");
    child(&e, "o1", "S0");
    child(&e, "o2", "S0");
    child(&e, "o3", "X");
    e
}

fn has_child_s0() -> QueryNode {
    QueryNode::HasChild(HasChildQuery {
        collection: "items".into(),
        field: "parent".into(),
        query: Box::new(QueryNode::Term(TermQuery {
            field: "sku".into(),
            value: FieldValue::String("S0".into()),
        })),
    })
}

fn sort_ts_desc() -> Option<Vec<SortSpec>> {
    Some(vec![SortSpec {
        field: "ts".into(),
        order: SortOrder::Desc,
        missing: SortMissing::Exclude,
    }])
}

fn run(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchResponse {
    e.search(
        "orders",
        SearchRequest {
            query,
            limit: 100,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort,
            track_total: true,
            collapse: None,
        },
    )
    .unwrap()
}

fn ids(r: &SearchResponse) -> Vec<String> {
    r.hits.iter().map(|h| h.external_id.clone()).collect()
}

/// R1: has_child + sort returns matching parents ordered by the parent field.
#[test]
fn has_child_sort_orders_parents() {
    let e = setup();
    let r = run(&e, has_child_s0(), sort_ts_desc());
    assert_eq!(ids(&r), vec!["o2", "o1"]); // ts 200, 100 desc
    assert_eq!(r.total, 2);
}

/// R2: has_child AND a parent-field filter, sorted, intersect + exact total.
#[test]
fn has_child_sort_composes_with_filter() {
    let e = setup();
    let q = QueryNode::And(vec![
        has_child_s0(),
        QueryNode::Term(TermQuery {
            field: "status".into(),
            value: FieldValue::String("open".into()),
        }),
    ]);
    let r = run(&e, q, sort_ts_desc());
    assert_eq!(ids(&r), vec!["o1"]); // o2 is closed
    assert_eq!(r.total, 1);
}

/// #3997: a child query keeps the exact bounded materialized fallback,
/// even when its one sort key could otherwise use the keyword stream.
#[test]
fn has_child_missing_keyword_sort_keeps_materialized_fallback() {
    let e = setup();
    e.index(
        "orders",
        IndexRequest {
            items: vec![IndexItem {
                external_id: "o1".into(),
                field: "rank".into(),
                value: FieldValue::String("a".into()),
                version: None,
            }],
            request_id: None,
        },
    )
    .unwrap();
    MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.set(0));

    let r = run(
        &e,
        has_child_s0(),
        Some(vec![SortSpec {
            field: "rank".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Last,
        }]),
    );

    assert_eq!(ids(&r), vec!["o1", "o2"]);
    assert_eq!(r.total, 2);
    assert_eq!(
        MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.get()),
        2,
        "has_child must retain its bounded materialized fallback"
    );
}

/// R3: sort + knn is still rejected (400 UnsupportedSort).
#[test]
fn knn_sort_still_rejected() {
    let e = setup();
    let err = e
        .search(
            "orders",
            SearchRequest {
                query: QueryNode::Knn(KnnQuery {
                    field: "v".into(),
                    vector: vec![0.1, 0.2],
                    k: 5,
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: sort_ts_desc(),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap_err();
    let se = err.downcast_ref::<StorageError>().expect("StorageError");
    assert!(
        matches!(se, StorageError::UnsupportedSort(_)),
        "knn + sort must stay rejected, got {se:?}"
    );
}
