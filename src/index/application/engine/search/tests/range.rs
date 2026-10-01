//! Range queries: number bounds, keyword bounds compared byte by byte, a bound
//! of the wrong type rejected, and exclusive against inclusive bounds.

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, RangeBound, RangeQuery};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn range_query_on_number() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let items = (1..=5)
        .map(|i| item(&format!("u{i}"), "age", FieldValue::Number(i as f64 * 10.0)))
        .collect();
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
    let resp = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Range(RangeQuery {
                    field: "age".into(),
                    gte: Some(RangeBound::Number(20.0)),
                    lt: Some(RangeBound::Number(50.0)),
                    gt: None,
                    lte: None,
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
    assert_eq!(resp.total, 3);
}

/// #1307 AC1: string `gt`/`gte`/`lt`/`lte` bounds on a `keyword` field
/// (`email`) filter by byte/lexicographic comparison, matching a
/// reference sort of the same values — the ordering ISO-8601
/// date/datetime strings rely on for chronological sort.
#[test]
fn range_query_on_keyword_byte_lexicographic() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let emails = [
        "alice@example.com",
        "bob@example.com",
        "carol@example.com",
        "dave@example.com",
        "erin@example.com",
    ];
    let items = emails
        .iter()
        .enumerate()
        .map(|(i, addr)| {
            item(
                &format!("u{i}"),
                "email",
                FieldValue::String((*addr).into()),
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

    let run = |gte: Option<&str>, lt: Option<&str>| {
        e.search(
            "users",
            SearchRequest {
                query: QueryNode::Range(RangeQuery {
                    field: "email".into(),
                    gt: None,
                    gte: gte.map(|s| RangeBound::Keyword(s.into())),
                    lt: lt.map(|s| RangeBound::Keyword(s.into())),
                    lte: None,
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
        .unwrap()
    };

    // bob..dave (exclusive) → {bob, carol} = 2, matching a reference sort
    // of the same strings.
    let resp = run(Some("bob@example.com"), Some("dave@example.com"));
    assert_eq!(resp.total, 2);
    let mut hit_ids: Vec<&str> = resp.hits.iter().map(|h| h.external_id.as_str()).collect();
    hit_ids.sort();
    assert_eq!(hit_ids, vec!["u1", "u2"]);

    // Unbounded above "carol@example.com" (inclusive) → {carol, dave, erin} = 3.
    let resp = run(Some("carol@example.com"), None);
    assert_eq!(resp.total, 3);
}

/// #1307 AC2: a numeric bound against a non-`number` (`keyword`) field, or
/// a string bound against a non-`keyword` (`number`) field, returns an
/// error (mapped to 400 at the API layer, not a silent misparse or
/// panic) rather than a result set.
#[test]
fn range_query_bound_type_mismatch_rejected() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![
                item("u1", "age", FieldValue::Number(30.0)),
                item("u1", "email", FieldValue::String("a@example.com".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();

    // String bound against the `number` field `age`.
    let err = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Range(RangeQuery {
                    field: "age".into(),
                    gt: None,
                    gte: Some(RangeBound::Keyword("20".into())),
                    lt: None,
                    lte: None,
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
    assert!(
        err.to_string().contains("numeric bound"),
        "unexpected error: {err}"
    );

    // Numeric bound against the `keyword` field `email`.
    let err = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Range(RangeQuery {
                    field: "email".into(),
                    gt: None,
                    gte: Some(RangeBound::Number(1.0)),
                    lt: None,
                    lte: None,
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
    assert!(
        err.to_string().contains("string bound"),
        "unexpected error: {err}"
    );
}

#[test]
fn range_bounds_are_exclusive_vs_inclusive() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let items = (0..=10)
        .map(|i| item(&format!("u{i}"), "age", FieldValue::Number(i as f64)))
        .collect();
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();

    let run = |q: RangeQuery| {
        e.search(
            "users",
            SearchRequest {
                query: QueryNode::Range(q),
                limit: 50,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: None,
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
        .total
    };

    // gte=2, lte=5 → {2,3,4,5} = 4
    assert_eq!(
        run(RangeQuery {
            field: "age".into(),
            gt: None,
            gte: Some(RangeBound::Number(2.0)),
            lt: None,
            lte: Some(RangeBound::Number(5.0))
        }),
        4
    );
    // gt=2, lt=5 → {3,4} = 2  (exclusive both ends)
    assert_eq!(
        run(RangeQuery {
            field: "age".into(),
            gt: Some(RangeBound::Number(2.0)),
            gte: None,
            lt: Some(RangeBound::Number(5.0)),
            lte: None
        }),
        2
    );
    // gte=2, lt=5 → {2,3,4} = 3  (mixed)
    assert_eq!(
        run(RangeQuery {
            field: "age".into(),
            gt: None,
            gte: Some(RangeBound::Number(2.0)),
            lt: Some(RangeBound::Number(5.0)),
            lte: None
        }),
        3
    );
}
