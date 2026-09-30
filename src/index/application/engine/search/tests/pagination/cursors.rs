//! Page cursors: the sorted keyset cursor's v2 form, the score keyset, the
//! legacy offset cursor, and a page's latency not growing with its depth.

use crate::index::application::engine::search::tests::pagination::walk_pages;
use crate::index::application::engine::tests::{build_users_schema, item};
use crate::index::application::engine::Engine;
use crate::index::domain::query::page_cursor::{
    make_cursor, make_sort_cursor, parse_page_cursor, PageCursor,
};
use crate::index::domain::sortable_f64::SortableF64;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, QueryNode, RangeBound, SortMissing, SortOrder};
use crate::shared_kernel::types::search::SearchRequest;

#[test]
fn sorted_keyset_cursor_is_v2_and_filtered_walks_match() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let mut items = Vec::new();
    for i in 0..60 {
        items.push(item(
            &format!("u{i:03}"),
            "age",
            FieldValue::Number(i as f64),
        ));
        items.push(item(
            &format!("u{i:03}"),
            "email",
            FieldValue::String(format!("{}@x.com", if i % 2 == 0 { "even" } else { "odd" })),
        ));
    }
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();

    // Filtered (query predicate) + sorted + paged.
    let base = SearchRequest {
        query: QueryNode::Term(crate::shared_kernel::types::query::TermQuery {
            field: "email".into(),
            value: FieldValue::String("even@x.com".into()),
        }),
        limit: 4,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
            field: "age".into(),
            order: SortOrder::Desc,
            missing: SortMissing::Exclude,
        }]),
        track_total: true,
        collapse: None,
    };
    let first = e.search("users", base.clone()).unwrap();
    // The first page of a sorted query hands out a v2 keyset cursor.
    let cursor = first.cursor.clone().expect("more pages");
    match parse_page_cursor(&cursor).expect("parseable") {
        PageCursor::SortKeyset { .. } => {}
        _ => panic!("expected a sort keyset cursor"),
    }

    let paged = walk_pages(&e, &base);
    let expected: Vec<String> = (0..60)
        .rev()
        .filter(|i| i % 2 == 0)
        .map(|i| format!("u{i:03}"))
        .collect();
    assert_eq!(paged, expected);
}

#[test]
fn score_keyset_pagination_matches_full_ranking() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let items: Vec<_> = (0..45)
        .map(|i| {
            item(
                &format!("u{i:03}"),
                "bio",
                FieldValue::String(format!(
                    "engineer {}",
                    if i % 3 == 0 { "rust rust" } else { "rust" }
                )),
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

    let base = SearchRequest {
        query: QueryNode::Match(crate::shared_kernel::types::query::MatchQuery {
            field: "bio".into(),
            text: "rust".into(),
            op: MatchOp::And,
        }),
        limit: 6,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
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
    assert_eq!(oracle.len(), 45);

    let first = e.search("users", base.clone()).unwrap();
    match parse_page_cursor(&first.cursor.clone().unwrap()).unwrap() {
        PageCursor::ScoreKeyset { .. } => {}
        _ => panic!("expected a score keyset cursor"),
    }
    let paged = walk_pages(&e, &base);
    assert_eq!(paged, oracle);
}

#[test]
fn legacy_offset_cursor_still_pages() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let items: Vec<_> = (0..30)
        .map(|i| item(&format!("u{i:03}"), "age", FieldValue::Number(i as f64)))
        .collect();
    e.index(
        "users",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
    let req = SearchRequest {
        query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
            field: "age".into(),
            gt: None,
            gte: Some(RangeBound::Number(0.0)),
            lt: None,
            lte: None,
        }),
        limit: 10,
        offset: 0,
        cursor: Some(make_cursor(25)),
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    };
    let resp = e.search("users", req).unwrap();
    assert_eq!(resp.hits.len(), 5);
    assert_eq!(resp.total, 30);
    assert!(resp.cursor.is_none());
}

/// Deep-pagination latency proof (run explicitly, release):
/// `cargo test -p lumen --release --lib deep_pagination_depth_invariance -- --ignored --nocapture`
#[test]
#[ignore]
fn deep_pagination_depth_invariance() {
    let e = Engine::new();
    e.create_collection("users", build_users_schema()).unwrap();
    let n = 100_000;
    for chunk in (0..n).collect::<Vec<_>>().chunks(5000) {
        let items: Vec<_> = chunk
            .iter()
            .map(|i| item(&format!("u{i:06}"), "age", FieldValue::Number(*i as f64)))
            .collect();
        e.index(
            "users",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }
    let base = SearchRequest {
        query: QueryNode::Range(crate::shared_kernel::types::query::RangeQuery {
            field: "age".into(),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        }),
        limit: 10,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
            field: "age".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        }]),
        track_total: false,
        collapse: None,
    };

    // Page 1, then jump a keyset cursor to depth ~50_000 and time a page.
    let t0 = std::time::Instant::now();
    let first = e.search("users", base.clone()).unwrap();
    let first_us = t0.elapsed().as_micros();
    assert_eq!(first.hits.len(), 10);

    let deep_cursor = make_sort_cursor(
        SortableF64::new(50_000.0).unwrap().bits(),
        0, // before any docid at that key
    );
    let mut deep_req = base.clone();
    deep_req.cursor = Some(deep_cursor);
    let t1 = std::time::Instant::now();
    let deep = e.search("users", deep_req).unwrap();
    let deep_us = t1.elapsed().as_micros();
    assert_eq!(deep.hits.len(), 10);
    assert_eq!(deep.hits[0].external_id, "u050000");

    // Legacy offset to the same depth for contrast.
    let mut offset_req = base.clone();
    offset_req.cursor = Some(make_cursor(50_000));
    let t2 = std::time::Instant::now();
    let via_offset = e.search("users", offset_req).unwrap();
    let offset_us = t2.elapsed().as_micros();

    eprintln!(
        "page#1 {first_us}us | keyset@50k {deep_us}us | offset@50k {offset_us}us (hits {})",
        via_offset.hits.len()
    );
    // The keyset deep page must be the same order of magnitude as page 1 —
    // depth invariance. (Loose 20x bound to survive CI jitter.)
    assert!(
        deep_us < first_us.max(1) * 20,
        "keyset deep page degraded: first={first_us}us deep={deep_us}us"
    );
}
