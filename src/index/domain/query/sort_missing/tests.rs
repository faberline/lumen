//! #180: opt-in `missing: first|last|exclude` on a sort key. exclude (default)
//! drops rows lacking the value (today's behavior); first/last keep them, placed
//! before/after the present rows, and count them in an exact total.

use std::collections::BTreeMap;

use crate::index::application::engine::Engine;
use crate::index::domain::query::sort_missing::{
    MATERIALIZED_SORT_COMPARISONS, MATERIALIZED_SORT_RETAINED_HIGH_WATER,
};
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{ExistsQuery, SortMissing};
use crate::shared_kernel::types::query::{QueryNode, SortOrder, SortSpec, TermQuery};
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
    fields.insert("kw".into(), fieldspec(FieldType::Keyword));
    CreateCollectionRequest { fields }
}

fn idx(e: &Engine, eid: &str, price: Option<f64>) {
    let mut items = vec![IndexItem {
        external_id: eid.into(),
        field: "cat".into(),
        value: FieldValue::String("x".into()),
        version: None,
    }];
    if let Some(p) = price {
        items.push(IndexItem {
            external_id: eid.into(),
            field: "price".into(),
            value: FieldValue::Number(p),
            version: None,
        });
    }
    e.index(
        "c",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
}

/// d0=10, d1=20 have a price; d2 has none (all share cat="x").
fn seed() -> Engine {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    idx(&e, "d0", Some(10.0));
    idx(&e, "d1", Some(20.0));
    idx(&e, "d2", None);
    e
}

fn search(e: &Engine, missing: SortMissing, limit: u32, cursor: Option<String>) -> SearchResponse {
    e.search(
        "c",
        SearchRequest {
            query: QueryNode::Term(TermQuery {
                field: "cat".into(),
                value: FieldValue::String("x".into()),
            }),
            limit,
            offset: 0,
            cursor,
            routing_key: None,
            sort: Some(vec![SortSpec {
                field: "price".into(),
                order: SortOrder::Asc,
                missing,
            }]),
            track_total: true,
            collapse: None,
        },
    )
    .unwrap()
}

fn ids(r: &SearchResponse) -> Vec<String> {
    r.hits.iter().map(|h| h.external_id.clone()).collect()
}

/// R1: missing:last places the value-less row after present rows, counted.
#[test]
fn missing_last_placed_after_and_counted() {
    let e = seed();
    let r = search(&e, SortMissing::Last, 100, None);
    assert_eq!(ids(&r), vec!["d0", "d1", "d2"]);
    assert_eq!(r.total, 3);
}

/// R2: missing:first places the value-less row before present rows.
#[test]
fn missing_first_placed_before() {
    let e = seed();
    let r = search(&e, SortMissing::First, 100, None);
    assert_eq!(ids(&r), vec!["d2", "d0", "d1"]);
    assert_eq!(r.total, 3);
}

/// R3: default exclude drops the value-less row from results and total.
#[test]
fn exclude_default_drops_missing() {
    let e = seed();
    let r = search(&e, SortMissing::Exclude, 100, None);
    assert_eq!(ids(&r), vec!["d0", "d1"]);
    assert_eq!(r.total, 2);
}

/// R4: the missing-inclusive order paginates, each row once, exact total.
#[test]
fn missing_paginates_each_once() {
    let e = seed();
    let p1 = search(&e, SortMissing::Last, 2, None);
    assert_eq!(ids(&p1), vec!["d0", "d1"]);
    assert_eq!(p1.total, 3);
    let cursor = p1.cursor.expect("a full page hands back a cursor");
    let p2 = search(&e, SortMissing::Last, 2, Some(cursor));
    assert_eq!(ids(&p2), vec!["d2"]);
    assert_eq!(p2.total, 3);
}

/// #3997: a single high-cardinality keyword key with `missing:last` must
/// not send a small page through the generic full tuple-sort fallback.
/// Values are deliberately permuted so the old all-row sort needs many
/// comparisons; the new keyword planner streams dictionary buckets.
#[test]
fn keyword_missing_last_small_page_bypasses_full_tuple_sort() {
    const DOCS: usize = 1_024;
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    for i in 0..DOCS {
        let mut items = vec![IndexItem {
            external_id: format!("d{i:04}"),
            field: "cat".into(),
            value: FieldValue::String("x".into()),
            version: None,
        }];
        if i % 8 != 0 {
            items.push(IndexItem {
                external_id: format!("d{i:04}"),
                field: "kw".into(),
                value: FieldValue::String(format!("k{:04}", DOCS - i)),
                version: None,
            });
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
    }

    let segment_dir = tempfile::tempdir().expect("keyword segment tempdir");
    e.__seal_keyword_field_to_segment("c", "kw", segment_dir.path())
        .expect("seal high-cardinality keyword field");
    e.__seal_keyword_field_to_segment("c", "cat", segment_dir.path())
        .expect("seal exact exists filter field");

    MATERIALIZED_SORT_COMPARISONS.with(|comparisons| comparisons.set(0));
    let response = e
        .search(
            "c",
            SearchRequest {
                query: QueryNode::Exists(ExistsQuery {
                    field: "cat".into(),
                }),
                limit: 10,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(vec![SortSpec {
                    field: "kw".into(),
                    order: SortOrder::Asc,
                    missing: SortMissing::Last,
                }]),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
    assert_eq!(response.total, DOCS as u64);
    assert!(
        response.hits.iter().all(|hit| hit.score == 1.0),
        "keyword-present rows retain the constant filter score"
    );
    assert_eq!(
        ids(&response),
        (0..DOCS)
            .rev()
            .filter(|i| i % 8 != 0)
            .take(10)
            .map(|i| format!("d{i:04}"))
            .collect::<Vec<_>>()
    );
    assert!(
        MATERIALIZED_SORT_COMPARISONS.with(|comparisons| comparisons.get()) <= DOCS as u64,
        "small-page keyword sort must not comparison-sort all {DOCS} matches"
    );
}

/// A sealed ordinal stream must merge tail-only values, an equal tail value,
/// and a post-seal tombstone without materializing either dictionary.
#[test]
fn sealed_keyword_bucket_walk_merges_tombstone_and_live_tail_both_orders() {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    let write = |eid: &str, keyword: &str| {
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: eid.into(),
                        field: "cat".into(),
                        value: FieldValue::String("x".into()),
                        version: None,
                    },
                    IndexItem {
                        external_id: eid.into(),
                        field: "kw".into(),
                        value: FieldValue::String(keyword.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    };
    write("base-a", "a");
    write("base-b", "b");
    write("base-c", "c");
    let segment_dir = tempfile::tempdir().unwrap();
    e.__seal_keyword_field_to_segment("c", "kw", segment_dir.path())
        .unwrap();
    e.delete("c", "base-b", None).unwrap();
    write("tail-aa", "aa");
    write("tail-c", "c");
    write("tail-z", "z");

    let run = |order| {
        e.search(
            "c",
            SearchRequest {
                query: QueryNode::Exists(ExistsQuery {
                    field: "cat".into(),
                }),
                limit: 100,
                offset: 0,
                cursor: None,
                routing_key: None,
                sort: Some(vec![SortSpec {
                    field: "kw".into(),
                    order,
                    missing: SortMissing::Last,
                }]),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap()
    };
    let asc = run(SortOrder::Asc);
    assert_eq!(
        ids(&asc),
        ["base-a", "tail-aa", "base-c", "tail-c", "tail-z"]
    );
    assert!(asc.hits.iter().all(|hit| hit.score == 1.0));
    let desc = run(SortOrder::Desc);
    assert_eq!(
        ids(&desc),
        ["tail-z", "base-c", "tail-c", "tail-aa", "base-a"]
    );
    assert!(desc.hits.iter().all(|hit| hit.score == 1.0));
}

/// Non-keyword/multi-key missing sorts use the exact bounded fallback.
/// Counting may scan all matches, but retained tuples must never exceed the
/// requested native prefix.
#[test]
fn missing_sort_fallback_retains_at_most_offset_plus_limit() {
    const DOCS: usize = 1_024;
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    for i in 0..DOCS {
        idx(
            &e,
            &format!("d{i:04}"),
            (i % 5 != 0).then_some((DOCS - i) as f64),
        );
    }
    MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.set(0));
    let response = e
        .search(
            "c",
            SearchRequest {
                query: QueryNode::Exists(ExistsQuery {
                    field: "cat".into(),
                }),
                limit: 13,
                offset: 7,
                cursor: None,
                routing_key: None,
                sort: Some(vec![SortSpec {
                    field: "price".into(),
                    order: SortOrder::Asc,
                    missing: SortMissing::Last,
                }]),
                track_total: true,
                collapse: None,
            },
        )
        .unwrap();
    assert_eq!(response.total, DOCS as u64);
    assert_eq!(response.hits.len(), 13);
    assert!(
        MATERIALIZED_SORT_RETAINED_HIGH_WATER.with(|high_water| high_water.get()) <= 20,
        "fallback retained more than offset + limit tuples"
    );
}
