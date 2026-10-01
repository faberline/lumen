//! Exists / Duplicated query primitives (DataTable composite search)

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{DuplicatedQuery, ExistsQuery};
use crate::shared_kernel::types::query::{QueryNode, RangeBound, RangeQuery};
use crate::shared_kernel::types::search::SearchRequest;

fn search_ids(e: &Engine, coll: &str, query: QueryNode) -> (u64, Vec<String>) {
    let resp = e
        .search(
            coll,
            SearchRequest {
                query,
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
    let mut ids: Vec<String> = resp.hits.iter().map(|h| h.external_id.clone()).collect();
    ids.sort();
    (resp.total, ids)
}

#[test]
fn exists_filters_missing_field() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u1", "age", FieldValue::Number(30.0)),
                // u2 carries no email — only age. Exists("email") must skip it.
                item("u2", "age", FieldValue::Number(40.0)),
                item("u3", "email", FieldValue::String("c@z.com".into())),
                // u4 multi-valued set; Exists must see it via element_postings.
                item(
                    "u4",
                    "tags",
                    FieldValue::StringList(vec!["a".into(), "b".into()]),
                ),
            ],
            request_id: None,
        },
    )
    .unwrap();

    // Keyword field: only docs that actually hold an email value.
    let (total, ids) = search_ids(
        &e,
        "users",
        QueryNode::Exists(ExistsQuery {
            field: "email".into(),
        }),
    );
    assert_eq!(total, 2);
    assert_eq!(ids, vec!["u1", "u3"]);

    // Set field: presence via element postings.
    let (total, ids) = search_ids(
        &e,
        "users",
        QueryNode::Exists(ExistsQuery {
            field: "tags".into(),
        }),
    );
    assert_eq!(total, 1);
    assert_eq!(ids, vec!["u4"]);

    // Number field.
    let (total, ids) = search_ids(
        &e,
        "users",
        QueryNode::Exists(ExistsQuery {
            field: "age".into(),
        }),
    );
    assert_eq!(total, 2);
    assert_eq!(ids, vec!["u1", "u2"]);
}

#[test]
fn exists_composes_with_boolean() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u1", "age", FieldValue::Number(30.0)),
                item("u2", "email", FieldValue::String("b@y.com".into())),
                item("u2", "age", FieldValue::Number(50.0)),
                item("u3", "age", FieldValue::Number(30.0)), // no email
            ],
            request_id: None,
        },
    )
    .unwrap();
    // has-email AND age in [25,40): u1 only (u2 too old, u3 no email).
    let q = QueryNode::And(vec![
        QueryNode::Exists(ExistsQuery {
            field: "email".into(),
        }),
        QueryNode::Range(RangeQuery {
            field: "age".into(),
            gte: Some(RangeBound::Number(25.0)),
            lt: Some(RangeBound::Number(40.0)),
            gt: None,
            lte: None,
        }),
    ]);
    let (total, ids) = search_ids(&e, "users", q);
    assert_eq!(total, 1);
    assert_eq!(ids, vec!["u1"]);

    // Inverse: missing email (NOT Exists) → u3.
    let q = QueryNode::Not(Box::new(QueryNode::Exists(ExistsQuery {
        field: "email".into(),
    })));
    let (total, ids) = search_ids(&e, "users", q);
    assert_eq!(total, 1);
    assert_eq!(ids, vec!["u3"]);
}

#[test]
fn duplicated_as_query_leaf() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u2", "email", FieldValue::String("a@x.com".into())),
                item("u3", "email", FieldValue::String("a@x.com".into())),
                item("u4", "email", FieldValue::String("b@y.com".into())),
                item("u5", "email", FieldValue::String("b@y.com".into())),
                item("u6", "email", FieldValue::String("c@z.com".into())), // unique
            ],
            request_id: None,
        },
    )
    .unwrap();

    // min_group_size defaults to >=2: every doc whose email collides.
    let (total, ids) = search_ids(
        &e,
        "users",
        QueryNode::Duplicated(DuplicatedQuery {
            field: "email".into(),
            min_group_size: 2,
        }),
    );
    assert_eq!(total, 5);
    assert_eq!(ids, vec!["u1", "u2", "u3", "u4", "u5"]);

    // Raise the threshold: only the 3-way group survives.
    let (total, ids) = search_ids(
        &e,
        "users",
        QueryNode::Duplicated(DuplicatedQuery {
            field: "email".into(),
            min_group_size: 3,
        }),
    );
    assert_eq!(total, 3);
    assert_eq!(ids, vec!["u1", "u2", "u3"]);
}

#[test]
fn duplicated_composes_with_boolean() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u1", "age", FieldValue::Number(30.0)),
                item("u2", "email", FieldValue::String("a@x.com".into())),
                item("u2", "age", FieldValue::Number(60.0)),
                item("u3", "email", FieldValue::String("a@x.com".into())),
                item("u3", "age", FieldValue::Number(35.0)),
                item("u4", "email", FieldValue::String("u@u.com".into())), // unique email
                item("u4", "age", FieldValue::Number(30.0)),
            ],
            request_id: None,
        },
    )
    .unwrap();
    // duplicate-email AND age<40: u1, u3 (u2 too old, u4 not a duplicate).
    let q = QueryNode::And(vec![
        QueryNode::Duplicated(DuplicatedQuery {
            field: "email".into(),
            min_group_size: 2,
        }),
        QueryNode::Range(RangeQuery {
            field: "age".into(),
            gte: None,
            lt: Some(RangeBound::Number(40.0)),
            gt: None,
            lte: None,
        }),
    ]);
    let (total, ids) = search_ids(&e, "users", q);
    assert_eq!(total, 2);
    assert_eq!(ids, vec!["u1", "u3"]);
}

#[test]
fn duplicated_min_group_size_floor_is_two() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u2", "email", FieldValue::String("a@x.com".into())),
                item("u3", "email", FieldValue::String("solo@x.com".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    // min_group_size 0/1 would make every doc a "duplicate"; the leaf floors it
    // at 2 so a singleton never matches.
    let (total, ids) = search_ids(
        &e,
        "users",
        QueryNode::Duplicated(DuplicatedQuery {
            field: "email".into(),
            min_group_size: 0,
        }),
    );
    assert_eq!(total, 2);
    assert_eq!(ids, vec!["u1", "u2"]);
}

#[test]
fn exists_on_text_field_rejected() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let err = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Exists(ExistsQuery {
                    field: "bio".into(),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("text"), "unexpected error: {msg}");
}

mod segments;
