//! Term and match queries, an AND of text, term and range, an exact number
//! term, and the result cache an index write clears.

use std::collections::BTreeSet;

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{
    MatchOp, MatchQuery, QueryNode, RangeBound, RangeQuery, TermQuery,
};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn index_and_term_search_keyword() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "email", FieldValue::String("a@x.com".into())),
                item("u2", "email", FieldValue::String("b@y.com".into())),
                item("u3", "email", FieldValue::String("a@x.com".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String("a@x.com".into()),
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
        .unwrap();
    assert_eq!(resp.total, 2);
    let eids: Vec<_> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(eids, vec!["u1", "u3"]);
}

#[test]
fn match_query_finds_text() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item(
                    "u1",
                    "bio",
                    FieldValue::String("senior engineer in Taipei".into()),
                ),
                item(
                    "u2",
                    "bio",
                    FieldValue::String("designer in Hsinchu".into()),
                ),
                item("u3", "bio", FieldValue::String("engineer in Tokyo".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "bio".into(),
                    text: "engineer taipei".into(),
                    op: MatchOp::And,
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
        .unwrap();
    assert_eq!(resp.total, 1);
    assert_eq!(resp.hits[0].external_id, "u1");
}

#[test]
fn search_result_cache_is_cleared_by_index_write() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![item(
                "u1",
                "bio",
                FieldValue::String("engineer in Taipei".into()),
            )],
            request_id: None,
        },
    )
    .unwrap();

    let req = SearchRequest {
        query: QueryNode::Match(MatchQuery {
            field: "bio".into(),
            text: "engineer".into(),
            op: MatchOp::Or,
        }),
        limit: 10,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    };
    let first = e.search("users", req.clone()).unwrap();
    assert_eq!(first.total, 1);
    {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("users").unwrap();
        assert_eq!(coll.search_cache.read().unwrap().len(), 1);
    }

    e.index(
        "users",
        IndexRequest {
            items: vec![item(
                "u2",
                "bio",
                FieldValue::String("engineer in Tokyo".into()),
            )],
            request_id: None,
        },
    )
    .unwrap();
    {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("users").unwrap();
        assert!(coll.search_cache.read().unwrap().is_empty());
    }

    let second = e.search("users", req).unwrap();
    assert_eq!(second.total, 2);
}

#[test]
fn and_combines_text_term_range() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "bio", FieldValue::String("rust engineer".into())),
                item(
                    "u1",
                    "tags",
                    FieldValue::StringList(vec!["rust".into(), "db".into()]),
                ),
                item("u1", "age", FieldValue::Number(30.0)),
                item("u2", "bio", FieldValue::String("rust engineer".into())),
                item("u2", "tags", FieldValue::StringList(vec!["go".into()])),
                item("u2", "age", FieldValue::Number(30.0)),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let q = QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "bio".into(),
            text: "rust".into(),
            op: MatchOp::And,
        }),
        QueryNode::Term(TermQuery {
            field: "tags".into(),
            value: FieldValue::String("rust".into()),
        }),
        QueryNode::Range(RangeQuery {
            field: "age".into(),
            gte: Some(RangeBound::Number(25.0)),
            lt: Some(RangeBound::Number(40.0)),
            gt: None,
            lte: None,
        }),
    ]);
    let resp = e
        .search(
            "users",
            SearchRequest {
                query: q,
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
    assert_eq!(resp.total, 1);
    assert_eq!(resp.hits[0].external_id, "u1");
}

#[test]
fn number_exact_term_query_matches_only_that_value() {
    // Exercises the (Number, Number) arm of eval_term — number
    // *exact* match, distinct from range. Without this, deleting
    // that match arm goes uncaught (the range tests never hit it).
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("a", "age", FieldValue::Number(30.0)),
                item("b", "age", FieldValue::Number(30.0)),
                item("c", "age", FieldValue::Number(31.0)),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "age".into(),
                    value: FieldValue::Number(30.0),
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
        .unwrap();
    assert_eq!(resp.total, 2);
    let ids: BTreeSet<&str> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert!(ids.contains("a") && ids.contains("b") && !ids.contains("c"));
}
