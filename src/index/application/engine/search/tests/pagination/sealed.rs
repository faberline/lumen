//! Sorted pagination over a field sealed to a disk segment plus its live tail.

use crate::index::application::engine::search::tests::pagination::walk_pages;
use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, SortMissing, SortOrder};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn keyword_sort_paginates_over_sealed_segment_plus_live_tail() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let mut sealed = Vec::new();
    for (eid, email, age) in [
        ("u00", "delta", 4.0),
        ("u01", "alpha", 1.0),
        ("u02", "charlie", 3.0),
    ] {
        sealed.push(item(eid, "email", FieldValue::String(email.into())));
        sealed.push(item(eid, "age", FieldValue::Number(age)));
    }
    e.index(
        "users",
        IndexRequest {
            items: sealed,
            request_id: None,
        },
    )
    .unwrap();
    e.__seal_keyword_field_to_segment("users", "email", dir.path())
        .unwrap();
    let mut tail = Vec::new();
    for (eid, email, age) in [("u03", "alpha", 2.0), ("u04", "bravo", 5.0)] {
        tail.push(item(eid, "email", FieldValue::String(email.into())));
        tail.push(item(eid, "age", FieldValue::Number(age)));
    }
    e.index(
        "users",
        IndexRequest {
            items: tail,
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
        sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
            field: "email".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        }]),
        track_total: true,
        collapse: None,
    };
    assert_eq!(
        walk_pages(&e, &base),
        vec!["u01", "u03", "u04", "u02", "u00"]
    );
}

#[test]
fn sorted_keyset_pagination_over_sealed_segment_plus_tail() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    // 50 sealed docs with duplicate keys, then a live tail of 13 more —
    // the keyset walk must seek correctly across BOTH sources.
    let items: Vec<_> = (0..50)
        .map(|i| {
            item(
                &format!("u{i:03}"),
                "age",
                FieldValue::Number((i % 7) as f64),
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
    e.__seal_number_field_to_segment("users", "age", dir.path())
        .unwrap();
    let tail: Vec<_> = (50..63)
        .map(|i| {
            item(
                &format!("u{i:03}"),
                "age",
                FieldValue::Number((i % 7) as f64),
            )
        })
        .collect();
    e.index(
        "users",
        IndexRequest {
            items: tail,
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
            limit: 5,
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
        let mut oracle_req = base.clone();
        oracle_req.limit = 1000;
        let oracle: Vec<String> = e
            .search("users", oracle_req)
            .unwrap()
            .hits
            .into_iter()
            .map(|h| h.external_id)
            .collect();
        assert_eq!(oracle.len(), 63);
        let paged = walk_pages(&e, &base);
        assert_eq!(paged, oracle, "order {order:?}");
    }
}
