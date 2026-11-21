//! Indexing: a field repeated in one request, or indexed again later, replaces
//! the old value, and a repeated request id is skipped.

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, MatchQuery, QueryNode, TermQuery};
use crate::shared_kernel::types::search::{SearchRequest, TrackTotal};

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
                track_total: lumen_shared_kernel::types::search::TrackTotal::Exact,
                collapse: None,
                scoring: None,
                filter: None,
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
                track_total: lumen_shared_kernel::types::search::TrackTotal::Exact,
                collapse: None,
                scoring: None,
                filter: None,
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
                track_total: lumen_shared_kernel::types::search::TrackTotal::Exact,
                collapse: None,
                scoring: None,
                filter: None,
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
                track_total: lumen_shared_kernel::types::search::TrackTotal::Exact,
                collapse: None,
                scoring: None,
                filter: None,
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

mod external_version_lww;

#[test]
fn up_to_total_is_a_lower_bound_exact_total_is_full() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let items: Vec<_> = (0..50)
        .map(|i| {
            item(
                &format!("u{i:02}"),
                "bio",
                FieldValue::String("shared token".into()),
            )
        })
        .collect();
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();

    let search = |track_total: TrackTotal| {
        e.search(
            "users",
            SearchRequest {
                query: QueryNode::Match(MatchQuery {
                    field: "bio".into(),
                    text: "token".into(),
                    op: MatchOp::Or,
                }),
                limit: 5,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total,
                collapse: None,
                scoring: None,
                filter: None,
            },
        )
        .unwrap()
    };

    let exact = search(TrackTotal::Exact);
    let up_to = search(TrackTotal::UpTo { limit: 10_000 });
    let none = search(TrackTotal::None);

    assert_eq!(exact.total, 50);
    assert_eq!(exact.hits.len(), 5);
    assert!(
        up_to.total >= 5 && up_to.total <= 50,
        "up_to total {}",
        up_to.total
    );
    assert!(
        none.total >= 5 && none.total <= 50,
        "none total {}",
        none.total
    );
}
