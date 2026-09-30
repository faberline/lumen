//! #182: native `ids` query — filter by a set of external_ids, resolved through
//! the interner. Constant-scored, predicable, composes under and/or/not + sort.

use std::collections::{BTreeMap, BTreeSet};

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::IndexItem;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::IdsQuery;
use crate::shared_kernel::types::query::{
    QueryNode, SortMissing, SortOrder, SortSpec, TermQuery, TermsQuery,
};
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
    fields.insert("status".into(), fieldspec(FieldType::Keyword));
    CreateCollectionRequest { fields }
}

/// d0(10,open) d1(20,closed) d2(30,open).
fn seed() -> Engine {
    let e = Engine::new();
    e.create_collection("c", schema()).unwrap();
    for (eid, price, status) in [
        ("d0", 10.0, "open"),
        ("d1", 20.0, "closed"),
        ("d2", 30.0, "open"),
    ] {
        e.index(
            "c",
            IndexRequest {
                items: vec![
                    IndexItem {
                        external_id: eid.into(),
                        field: "price".into(),
                        value: FieldValue::Number(price),
                        version: None,
                    },
                    IndexItem {
                        external_id: eid.into(),
                        field: "status".into(),
                        value: FieldValue::String(status.into()),
                        version: None,
                    },
                ],
                request_id: None,
            },
        )
        .unwrap();
    }
    e
}

fn ids_q(vals: &[&str]) -> QueryNode {
    QueryNode::Ids(IdsQuery {
        values: vals.iter().map(|s| s.to_string()).collect(),
    })
}

fn run(e: &Engine, query: QueryNode, sort: Option<Vec<SortSpec>>) -> SearchResponse {
    e.search(
        "c",
        SearchRequest {
            query,
            limit: 100,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort,
            track_total: true,
            collapse: None,
        },
    )
    .unwrap()
}

fn id_set(r: &SearchResponse) -> BTreeSet<String> {
    r.hits.iter().map(|h| h.external_id.clone()).collect()
}

/// R1: returns exactly the named existing ids, skipping unknown ones.
#[test]
fn ids_returns_named_set_skips_unknown() {
    let e = seed();
    let r = run(&e, ids_q(&["d0", "d2", "does-not-exist"]), None);
    assert_eq!(
        id_set(&r),
        ["d0".to_string(), "d2".to_string()].into_iter().collect()
    );
    assert_eq!(r.total, 2);
}

/// R2: composes under a boolean AND with another clause.
#[test]
fn ids_composes_under_and() {
    let e = seed();
    let q = QueryNode::And(vec![
        ids_q(&["d0", "d1", "d2"]),
        QueryNode::Term(TermQuery {
            field: "status".into(),
            value: FieldValue::String("open".into()),
        }),
    ]);
    let r = run(&e, q, None);
    assert_eq!(
        id_set(&r),
        ["d0".to_string(), "d2".to_string()].into_iter().collect(),
        "d1 is closed, so the AND drops it"
    );
}

/// R3: combines with sort (ids is a predicable filter).
#[test]
fn ids_combines_with_sort() {
    let e = seed();
    let r = run(
        &e,
        ids_q(&["d2", "d0"]),
        Some(vec![SortSpec {
            field: "price".into(),
            order: SortOrder::Asc,
            missing: SortMissing::Exclude,
        }]),
    );
    let ordered: Vec<String> = r.hits.iter().map(|h| h.external_id.clone()).collect();
    assert_eq!(ordered, vec!["d0".to_string(), "d2".to_string()]); // 10 then 30
}

/// #1487/R1: a fully-deleted doc (all fields removed) must not match an
/// `ids` query, consistent with `term`/`terms` on the same state.
#[test]
fn ids_excludes_fully_deleted_doc() {
    let e = seed();
    e.delete("c", "d1", None).unwrap();
    let r = run(&e, ids_q(&["d0", "d1", "d2"]), None);
    assert_eq!(
        id_set(&r),
        ["d0".to_string(), "d2".to_string()].into_iter().collect(),
        "d1 was fully deleted and must not be a hit"
    );
    assert_eq!(r.total, 2);

    // Same doc-state, term query on the surviving docs' field agrees.
    let term_r = run(
        &e,
        QueryNode::Terms(TermsQuery {
            field: "status".into(),
            values: vec![
                FieldValue::String("open".into()),
                FieldValue::String("closed".into()),
            ],
        }),
        None,
    );
    assert_eq!(
        id_set(&term_r),
        ["d0".to_string(), "d2".to_string()].into_iter().collect()
    );
}

/// #1487: mixed batch — a request naming live and deleted ids together
/// returns only the live subset.
#[test]
fn ids_mixed_batch_returns_only_live_subset() {
    let e = seed();
    e.delete("c", "d0", None).unwrap();
    e.delete("c", "d2", None).unwrap();
    let r = run(&e, ids_q(&["d0", "d1", "d2", "does-not-exist"]), None);
    assert_eq!(
        id_set(&r),
        ["d1".to_string()].into_iter().collect(),
        "only the still-live doc survives, deleted + unknown ids drop out"
    );
    assert_eq!(r.total, 1);
}

/// #1487: partial-field deletion — a doc with SOME fields deleted but at
/// least one field still live stays a hit under `ids` (matches the
/// engine's liveness definition used by `term`: live iff any field
/// lives).
#[test]
fn ids_matches_doc_with_partial_field_deletion() {
    let e = seed();
    // Delete only the `price` field on d0 — `status` is still live.
    e.delete("c", "d0", Some("price")).unwrap();
    let r = run(&e, ids_q(&["d0", "d1", "d2"]), None);
    assert_eq!(
        id_set(&r),
        ["d0".to_string(), "d1".to_string(), "d2".to_string()]
            .into_iter()
            .collect(),
        "d0 still has a live field (status), so it remains a hit"
    );
    assert_eq!(r.total, 3);

    // Now delete the remaining field too — d0 becomes fully dead.
    e.delete("c", "d0", Some("status")).unwrap();
    let r2 = run(&e, ids_q(&["d0", "d1", "d2"]), None);
    assert_eq!(
        id_set(&r2),
        ["d1".to_string(), "d2".to_string()].into_iter().collect(),
        "d0 has no live fields left, so it drops out"
    );
}
