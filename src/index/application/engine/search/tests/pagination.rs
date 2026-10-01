//! Sorted search pages: an unsupported text sort is rejected, and keyword,
//! composite and tied sort keys page by keyset to the full ordering.

use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::index::domain::query::page_cursor::{parse_page_cursor, PageCursor};
use crate::index::domain::query::sort::SortValue;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, QueryNode, SortMissing, SortOrder};
use crate::shared_kernel::types::search::SearchRequest;

/// Walk every page through the returned cursors; assert the
/// concatenation equals one exhaustive query (order included), with no
/// duplicates or gaps — the keyset-pagination contract.
fn walk_pages(e: &Engine, base: &SearchRequest) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..1000 {
        let mut req = base.clone();
        req.cursor = cursor.clone();
        let resp = e.search("users", req).unwrap();
        let n = resp.hits.len();
        out.extend(resp.hits.into_iter().map(|h| h.external_id));
        match resp.cursor {
            Some(c) => cursor = Some(c),
            None => return out,
        }
        if n == 0 {
            return out;
        }
    }
    panic!("cursor never exhausted");
}

#[test]
fn unsupported_text_sort_is_rejected_instead_of_silent_score_ranking() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    e.index(
        "users",
        IndexRequest {
            items: vec![item("u1", "bio", FieldValue::String("rust".into()))],
            request_id: None,
        },
    )
    .unwrap();
    let err = e
        .search(
            "users",
            SearchRequest {
                query: QueryNode::Match(crate::shared_kernel::types::query::MatchQuery {
                    field: "bio".into(),
                    text: "rust".into(),
                    op: MatchOp::And,
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                    field: "bio".into(),
                    order: SortOrder::Asc,
                    missing: SortMissing::Exclude,
                }]),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<StorageError>(),
            Some(StorageError::UnsupportedSort(_))
        ),
        "text sort must be a 400-class unsupported sort error: {err:?}"
    );
}

#[test]
fn keyword_sort_keyset_pagination_walks_lexicographically() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let docs = [
        ("u00", "delta", 4.0),
        ("u01", "alpha", 1.0),
        ("u02", "charlie", 3.0),
        ("u03", "alpha", 2.0),
        ("u04", "bravo", 5.0),
    ];
    let mut items = Vec::new();
    for (eid, email, age) in docs {
        items.push(item(eid, "email", FieldValue::String(email.into())));
        items.push(item(eid, "age", FieldValue::Number(age)));
    }
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();

    for (order, expected) in [
        (SortOrder::Asc, vec!["u01", "u03", "u04", "u02", "u00"]),
        (SortOrder::Desc, vec!["u00", "u02", "u04", "u01", "u03"]),
    ] {
        let base = SearchRequest {
            query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                field: "age".into(),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }),
            limit: 2,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                field: "email".into(),
                order,
                missing: SortMissing::Exclude,
            }]),
            track_total: true,
            collapse: None,
        };
        let first = e.search("users", base.clone()).unwrap();
        match parse_page_cursor(first.cursor.as_deref().expect("more pages")).unwrap() {
            PageCursor::SortValuesKeyset { values, .. } => {
                assert!(matches!(values.as_slice(), [SortValue::Keyword(_)]));
            }
            other => panic!("expected keyword sort cursor, got {other:?}"),
        }
        assert_eq!(walk_pages(&e, &base), expected, "order {order:?}");
    }
}

#[test]
fn composite_keyword_number_sort_keyset_paginates_to_oracle() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let docs = [
        ("u00", "todo", 10.0),
        ("u01", "done", 20.0),
        ("u02", "todo", 30.0),
        ("u03", "done", 15.0),
        ("u04", "blocked", 40.0),
        ("u05", "todo", 30.0),
    ];
    let mut items = Vec::new();
    for (eid, status, age) in docs {
        items.push(item(eid, "email", FieldValue::String(status.into())));
        items.push(item(eid, "age", FieldValue::Number(age)));
    }
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
    let base = SearchRequest {
        query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
            field: "age".into(),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        }),
        limit: 2,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: Some(vec![
            crate::shared_kernel::types::query::SortSpec {
                field: "email".into(),
                order: SortOrder::Asc,
                missing: SortMissing::Exclude,
            },
            crate::shared_kernel::types::query::SortSpec {
                field: "age".into(),
                order: SortOrder::Desc,
                missing: SortMissing::Exclude,
            },
        ]),
        track_total: true,
        collapse: None,
    };
    let first = e.search("users", base.clone()).unwrap();
    match parse_page_cursor(first.cursor.as_deref().expect("more pages")).unwrap() {
        PageCursor::SortValuesKeyset { values, .. } => {
            assert!(matches!(
                values.as_slice(),
                [SortValue::Keyword(_), SortValue::Number(_)]
            ));
        }
        other => panic!("expected composite sort cursor, got {other:?}"),
    }
    assert_eq!(
        walk_pages(&e, &base),
        vec!["u04", "u01", "u03", "u02", "u05", "u00"]
    );
}

#[test]
fn sorted_keyset_pagination_walks_exhaustively_with_ties() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    // 97 docs, age = i % 10 → heavy duplicate sort keys exercise the
    // (value, docid) tie-break across page boundaries.
    let items: Vec<_> = (0..97)
        .flat_map(|i| {
            vec![item(
                &format!("u{i:03}"),
                "age",
                FieldValue::Number((i % 10) as f64),
            )]
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

    for order in [SortOrder::Asc, SortOrder::Desc] {
        let base = SearchRequest {
            query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
                field: "age".into(),
                gt: None,
                gte: None,
                lt: None,
                lte: None,
            }),
            limit: 7,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
                field: "age".into(),
                order,
                missing: SortMissing::Exclude,
            }]),
            track_total: true,
            collapse: None,
        };
        // One exhaustive page as the oracle.
        let mut oracle_req = base.clone();
        oracle_req.limit = 1000;
        let oracle: Vec<String> = e
            .search("users", oracle_req)
            .unwrap()
            .hits
            .into_iter()
            .map(|h| h.external_id)
            .collect();
        assert_eq!(oracle.len(), 97);

        let paged = walk_pages(&e, &base);
        assert_eq!(paged, oracle, "order {order:?}");
    }
}

mod cursors;
mod sealed;
