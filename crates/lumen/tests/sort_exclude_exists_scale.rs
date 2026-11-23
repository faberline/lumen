//! Timing and correctness of a sort with `missing: exclude` and an `exists` query.
//!
//! Its own test binary: it measures wall time, so no other case may share the
//! process and slow it down.
//!
//! Reproduces faberline/lumen#578. `query_predicate` for `Exists` rebuilt the
//! field's whole presence bitmap for every document the sort walk visited, so a
//! walk over N documents cost O(N^2).

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use lumen::storage::Engine;
use lumen::types::{
    CreateCollectionRequest, ExistsQuery, FieldSpec, FieldType, FieldValue, IndexItem,
    IndexRequest, QueryNode, SearchRequest, SearchResponse, SortMissing, SortOrder, SortSpec,
    TrackTotal,
};

const N: u64 = 20_000;
/// Documents per index request (3 items each).
const CHUNK: u64 = 250;
/// Snapshot and restore after every this many documents, to stay under the
/// pending-change budget during setup.
const SNAP_EVERY: u64 = 5_000;

/// Splitmix64 — a pure mixing function over `i`.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn spec(field_type: FieldType) -> FieldSpec {
    FieldSpec {
        field_type,
        analyzer: None,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
        facetable: false,
    }
}

fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("_created_at".to_string(), spec(FieldType::Number));
    fields.insert("field_004".to_string(), spec(FieldType::Number));
    fields.insert("field_001".to_string(), spec(FieldType::Keyword));
    CreateCollectionRequest { fields }
}

fn item(eid: &str, field: &str, value: FieldValue) -> IndexItem {
    IndexItem {
        external_id: eid.to_string(),
        field: field.to_string(),
        value,
        version: None,
    }
}

/// Engine `c` with `n` documents `d<i>`, all with `_created_at` and `field_001`,
/// and `field_004` on every document.
fn build(n: u64) -> Engine {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    for start in (0..n).step_by(CHUNK as usize) {
        let end = (start + CHUNK).min(n);
        let mut items = Vec::new();
        for i in start..end {
            let eid = format!("d{i}");
            items.push(item(
                &eid,
                "_created_at",
                FieldValue::Number(1_700_000_000_000.0 + i as f64),
            ));
            items.push(item(
                &eid,
                "field_004",
                FieldValue::Number((mix(i ^ 4) % 1_000_000) as f64),
            ));
            items.push(item(
                &eid,
                "field_001",
                FieldValue::String(format!("row-{i:08}-{:012x}", mix(i))),
            ));
        }
        e.index(
            "c",
            IndexRequest {
                items,
                request_id: None,
            },
        )
        .unwrap();
        if end.is_multiple_of(SNAP_EVERY) || end == n {
            let snap = e.snapshot().unwrap();
            e.restore(snap).unwrap();
        }
    }
    e
}

fn sort_by(field: &str, order: SortOrder, missing: SortMissing) -> SortSpec {
    SortSpec {
        field: field.to_string(),
        order,
        missing,
    }
}

/// Search `c` for `exists(query_field)`, exact total, 20 hits, the given sort.
fn exists_sorted(
    e: &Engine,
    query_field: &str,
    sort: Vec<SortSpec>,
) -> Result<SearchResponse, String> {
    e.search(
        "c",
        SearchRequest {
            query: QueryNode::Exists(ExistsQuery {
                field: query_field.to_string(),
            }),
            limit: 20,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: Some(sort),
            track_total: TrackTotal::Exact,
            collapse: None,
            scoring: None,
            filter: None,
        },
    )
    .map_err(|e| format!("{e:?}"))
}

#[test]
fn exclude_sort_with_exists_query_is_not_quadratic() {
    let e = build(N);
    for (field, order) in [
        ("field_004", SortOrder::Desc),
        ("field_001", SortOrder::Asc),
        ("_created_at", SortOrder::Asc),
    ] {
        let start = Instant::now();
        let r = exists_sorted(
            &e,
            "_created_at",
            vec![sort_by(field, order, SortMissing::Exclude)],
        )
        .unwrap();
        let elapsed = start.elapsed();
        assert_eq!(r.total, N, "{field}: exact total");
        assert_eq!(r.hits.len(), 20, "{field}: page size");
        assert!(
            elapsed < Duration::from_secs(3),
            "{field}: took {elapsed:?}, bound 3 s"
        );
    }
}

#[test]
fn exclude_and_last_return_the_same_total_and_first_hit() {
    let e = build(N);
    let exclude = exists_sorted(
        &e,
        "_created_at",
        vec![sort_by("field_004", SortOrder::Desc, SortMissing::Exclude)],
    )
    .unwrap();
    let last = exists_sorted(
        &e,
        "_created_at",
        vec![sort_by("field_004", SortOrder::Desc, SortMissing::Last)],
    )
    .unwrap();
    assert_eq!(exclude.total, N);
    assert_eq!(last.total, N);
    assert_eq!(exclude.hits[0].external_id, last.hits[0].external_id);
}

#[test]
fn exists_on_unknown_field_still_errors() {
    let e = build(200);
    let result = exists_sorted(
        &e,
        "no_such_field",
        vec![sort_by("field_004", SortOrder::Desc, SortMissing::Exclude)],
    );
    assert!(result.is_err(), "an unknown field must still be an error");
}

#[test]
fn exists_on_a_field_some_docs_lack_still_filters() {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    // Ten documents; only the even ones have `field_004`.
    let mut items = Vec::new();
    for i in 0..10u64 {
        let eid = format!("d{i}");
        items.push(item(
            &eid,
            "_created_at",
            FieldValue::Number(1_700_000_000_000.0 + i as f64),
        ));
        if i % 2 == 0 {
            items.push(item(&eid, "field_004", FieldValue::Number(i as f64)));
        }
    }
    e.index(
        "c",
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
    let r = exists_sorted(
        &e,
        "field_004",
        vec![sort_by("_created_at", SortOrder::Asc, SortMissing::Exclude)],
    )
    .unwrap();
    assert_eq!(r.total, 5);
    let ids: Vec<&str> = r.hits.iter().map(|h| h.external_id.as_str()).collect();
    assert_eq!(ids, ["d0", "d2", "d4", "d6", "d8"]);
}
