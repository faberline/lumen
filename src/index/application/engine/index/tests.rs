//! Indexing: a field repeated in one request, or indexed again later, replaces
//! the old value, and a repeated request id is skipped.

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, MatchQuery, QueryNode, TermQuery};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn duplicate_field_in_one_index_request_is_replacement() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "bio", FieldValue::String("old token".into())),
                item("u1", "bio", FieldValue::String("new token".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();

    let old = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "bio".into(),
                    text: "old".into(),
                    op: MatchOp::Or,
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
    let new = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "bio".into(),
                    text: "new".into(),
                    op: MatchOp::Or,
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

    assert_eq!(old.total, 0);
    assert_eq!(new.total, 1);
    assert_eq!(new.hits[0].external_id, "u1");
}

#[test]
fn reindex_replaces_field_value() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![item("u1", "email", FieldValue::String("old@x.com".into()))],
            request_id: None,
        },
    )
    .unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![item("u1", "email", FieldValue::String("new@x.com".into()))],
            request_id: None,
        },
    )
    .unwrap();
    // Old value gone, new value present.
    let r_old = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String("old@x.com".into()),
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
    assert_eq!(r_old.total, 0);
    let r_new = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Term(TermQuery {
                    field: "email".into(),
                    value: FieldValue::String("new@x.com".into()),
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
    assert_eq!(r_new.total, 1);
}

#[test]
fn idempotency_skips_duplicate_request_id() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let req = IndexRequest {
        items: vec![item("u1", "email", FieldValue::String("a@x.com".into()))],
        request_id: Some("req-1".into()),
    };
    e.index("users", req.clone()).unwrap();
    let r = e.index("users", req).unwrap();
    assert_eq!(r.indexed, 0);
}
