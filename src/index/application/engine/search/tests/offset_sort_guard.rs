//! #179: an offset cursor combined with `sort` must be REJECTED (400), never
//! silently fall through to score ranking and ignore the sort. Sequential
//! sorted paging uses the keyset cursor handed back in the response; native
//! `offset` provides direct page jumps without client over-fetch.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::index::domain::query::page_cursor::make_cursor;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, SortMissing, SortOrder, SortSpec, TermQuery};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

fn fieldspec(t: FieldType) -> FieldSpec {
    FieldSpec {
        field_type: t,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("price".into(), fieldspec(FieldType::Number));
    fields.insert("cat".into(), fieldspec(FieldType::Keyword));
    fields.insert("code".into(), fieldspec(FieldType::Keyword));
    CreateCollectionRequest { fields }
}

/// Five docs d0..d4 with prices 10,20,30,40,50, all `cat = "x"`.
fn seed() -> Engine {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    for (i, p) in [10.0_f64, 20.0, 30.0, 40.0, 50.0].iter().enumerate() {
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: format!("d{i}"),
                        field: "price".into(),
                        value: FieldValue::Number(*p),
                        version: None,
                    },
                    IndexItem {
                        external_id: format!("d{i}"),
                        field: "cat".into(),
                        value: FieldValue::String("x".into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: format!("d{i}"),
                        field: "code".into(),
                        value: FieldValue::String(format!("k{i}")),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }
    e
}

fn cat_x() -> QueryNode {
    QueryNode::Term(TermQuery {
        field: "cat".into(),
        value: FieldValue::String("x".into()),
    })
}

fn base() -> SearchRequest {
    SearchRequest {
        query: cat_x(),
        limit: 2,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

fn sort_price_asc() -> Vec<SortSpec> {
    vec![SortSpec {
        field: "price".into(),
        order: SortOrder::Asc,
        missing: SortMissing::Exclude,
    }]
}

fn ids(resp: &SearchResponse) -> Vec<String> {
    resp.hits.iter().map(|h| h.external_id.clone()).collect()
}

/// R1: an offset cursor (N>0) + a non-empty sort → 400 UnsupportedSort,
/// instead of silently mis-ordering by score.
#[test]
fn offset_cursor_with_sort_is_rejected() {
    let e = seed();
    let mut r = base();
    r.sort = Some(sort_price_asc());
    r.cursor = Some(make_cursor(2)); // {"offset":2}
    let err = e.search("c", r).unwrap_err();
    let se = err
        .downcast_ref::<StorageError>()
        .expect("offset+sort error must be a StorageError");
    assert!(
        matches!(se, StorageError::UnsupportedSort(_)),
        "offset+sort must map to UnsupportedSort (HTTP 400), got {se:?}"
    );
}

/// R2: an offset cursor WITHOUT sort still paginates relevance/constant
/// results — the guard must not regress the unsorted offset path.
#[test]
fn offset_cursor_without_sort_paginates() {
    let e = seed();
    let mut r = base();
    r.cursor = Some(make_cursor(2));
    let resp = e
        .search("c", r)
        .expect("offset cursor without sort must succeed");
    assert_eq!(resp.total, 5, "exact total across the unsorted match set");
    assert!(resp.hits.len() <= 2, "page honors the limit");
}

#[test]
fn native_offset_applies_after_numeric_sort() {
    let e = seed();
    let mut r = base();
    r.sort = Some(sort_price_asc());
    r.offset = 2;
    let resp = e.search("c", r).expect("native sorted offset succeeds");
    assert_eq!(ids(&resp), ["d2", "d3"]);
    assert_eq!(resp.total, 5);
    assert!(
        resp.cursor.is_none(),
        "an offset jump does not emit a cursor"
    );
}

#[test]
fn native_offset_applies_after_keyword_and_composite_sort() {
    let e = seed();
    let mut r = base();
    r.offset = 1;
    r.sort = Some(vec![
        SortSpec {
            field: "cat".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        },
        SortSpec {
            field: "code".into(),
            order: SortOrder::Desc,
            missing: SortMissing::Exclude,
        },
    ]);
    let resp = e
        .search("c", r)
        .expect("native keyword/composite offset succeeds");
    assert_eq!(ids(&resp), ["d3", "d2"]);
}

#[test]
fn native_offset_applies_after_score_ordering() {
    let e = seed();
    let mut r = base();
    r.offset = 2;
    let resp = e.search("c", r).expect("native score offset succeeds");
    assert_eq!(ids(&resp), ["d2", "d3"]);
}

#[test]
fn nonzero_native_offset_and_cursor_are_rejected() {
    let e = seed();
    let mut r = base();
    r.offset = 1;
    r.cursor = Some(make_cursor(1));
    let err = e.search("c", r).unwrap_err();
    assert!(matches!(
        err.downcast_ref::<StorageError>(),
        Some(StorageError::InvalidPagination(_))
    ));
}

/// R3: a keyset cursor combined with sort paginates correctly (page 1 with
/// no cursor hands back a keyset cursor; following it yields the next page).
#[test]
fn keyset_cursor_with_sort_paginates() {
    let e = seed();

    let mut p1 = base();
    p1.sort = Some(sort_price_asc());
    let r1 = e.search("c", p1).expect("sorted page 1 must succeed");
    assert_eq!(ids(&r1), vec!["d0".to_string(), "d1".to_string()]);
    let cursor = r1
        .cursor
        .expect("a full sorted page must hand back a keyset cursor");

    let mut p2 = base();
    p2.sort = Some(sort_price_asc());
    p2.cursor = Some(cursor);
    let r2 = e
        .search("c", p2)
        .expect("keyset + sort page 2 must succeed");
    assert_eq!(ids(&r2), vec!["d2".to_string(), "d3".to_string()]);
}
