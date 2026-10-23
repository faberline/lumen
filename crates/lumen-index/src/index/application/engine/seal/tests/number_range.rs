//! Dual-path diff test: the segment-backed Number RANGE index (Phase 2h-3, WS2
//! BKD) must answer range / exact / boolean / sort queries byte-identically to
//! the in-RAM `values: BTreeMap<SortableF64, RoaringBitmap>` range walk. The crux
//! is the on-disk SORTED-VALUE column (`ROLE_NUMBER_SORTED`) binary-searched by
//! `number_range` honoring every inclusive/exclusive bound + open-endedness case,
//! with the 2h tombstone reused for delete-after-seal.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{
    QueryNode, RangeBound, RangeQuery, SortMissing, SortOrder, TermQuery, TermsQuery,
};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec, FieldType};
use crate::shared_kernel::types::search::SearchRequest;

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

/// `price` (Number, the field we seal) + `cat` (Keyword, for boolean
/// cross-field And/Or algebra).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("price".into(), fieldspec(FieldType::Number));
    fields.insert("cat".into(), fieldspec(FieldType::Keyword));
    fields.insert("kw".into(), fieldspec(FieldType::Keyword));
    CreateCollectionRequest { fields }
}

fn req(query: QueryNode) -> SearchRequest {
    SearchRequest {
        query,
        limit: 100_000,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

fn req_sort(query: QueryNode, field: &str, order: SortOrder) -> SearchRequest {
    SearchRequest {
        query,
        limit: 100_000,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: Some(vec![crate::shared_kernel::types::query::SortSpec {
            field: field.into(),
            order,
            missing: SortMissing::Exclude,
        }]),
        track_total: true,
        collapse: None,
    }
}

fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
    e.search("c", req(query))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score))
        .collect()
}

/// Sort-driven page (drives `try_plan`'s number-sort path over the
/// sorted-value column). Returns the ORDERED external_ids.
fn run_sorted(e: &Engine, query: QueryNode, field: &str, order: SortOrder) -> Vec<String> {
    e.search("c", req_sort(query, field, order))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| h.external_id)
        .collect()
}

fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
    rows.iter().map(|(e, _)| e.clone()).collect()
}

fn search_total(e: &Engine, query: QueryNode) -> u64 {
    e.search("c", req(query)).unwrap().total
}

fn index_num(e: &Engine, eid: &str, price: Option<f64>, cat: Option<&str>) {
    let mut items = Vec::new();
    if let Some(p) = price {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "price".into(),
            value: FieldValue::Number(p),
            version: None,
        });
    }
    if let Some(c) = cat {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "cat".into(),
            value: FieldValue::String(c.into()),
            version: None,
        });
    }
    // Ensure the doc is interned even when both fields are absent.
    if items.is_empty() {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "cat".into(),
            value: FieldValue::String("zzz_filler".into()),
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

fn rangeq(gte: Option<f64>, gt: Option<f64>, lte: Option<f64>, lt: Option<f64>) -> QueryNode {
    QueryNode::Range(RangeQuery {
        field: "price".into(),
        gte: gte.map(RangeBound::Number),
        gt: gt.map(RangeBound::Number),
        lte: lte.map(RangeBound::Number),
        lt: lt.map(RangeBound::Number),
    })
}

fn termnum(v: f64) -> QueryNode {
    QueryNode::Term(TermQuery {
        field: "price".into(),
        value: FieldValue::Number(v),
    })
}

fn termsnum(vs: &[f64]) -> QueryNode {
    QueryNode::Terms(TermsQuery {
        field: "price".into(),
        values: vs.iter().map(|v| FieldValue::Number(*v)).collect(),
    })
}

fn termkw(field: &str, v: &str) -> QueryNode {
    QueryNode::Term(TermQuery {
        field: field.into(),
        value: FieldValue::String(v.into()),
    })
}

/// The full battery of range/exact/boolean queries the dual-path test runs.
/// Every inclusive/exclusive bound + open-endedness combination, exact-match,
/// empty ranges, and boolean composition with another field.
fn all_queries() -> Vec<(&'static str, QueryNode)> {
    vec![
        // closed ranges (inclusive both)
        ("closed_incl", rangeq(Some(-5.0), None, Some(5.0), None)),
        // closed range exclusive both
        ("closed_excl", rangeq(None, Some(-5.0), None, Some(5.0))),
        // closed range mixed inclusivity
        ("mixed_gte_lt", rangeq(Some(-2.0), None, None, Some(7.0))),
        ("mixed_gt_lte", rangeq(None, Some(-2.0), Some(7.0), None)),
        // open-low (..hi]
        ("open_low_incl", rangeq(None, None, Some(3.0), None)),
        ("open_low_excl", rangeq(None, None, None, Some(3.0))),
        // open-high [lo..
        ("open_high_incl", rangeq(Some(-3.0), None, None, None)),
        ("open_high_excl", rangeq(None, Some(-3.0), None, None)),
        // fully open
        ("fully_open", rangeq(None, None, None, None)),
        // exact via inclusive lo==hi (single-value)
        ("single_val", rangeq(Some(0.0), None, Some(0.0), None)),
        // empty range: lo > hi
        ("empty_inverted", rangeq(Some(5.0), None, Some(-5.0), None)),
        // empty range: exclusive at the same point
        ("empty_excl_point", rangeq(None, Some(1.0), None, Some(1.0))),
        // exact-match Term (lo==hi semantics)
        ("exact_term_0", termnum(0.0)),
        ("exact_term_neg", termnum(-3.0)),
        ("exact_term_missing", termnum(999.0)),
        // multi-value Terms
        ("terms_multi", termsnum(&[-3.0, 0.0, 3.0])),
        // boolean And with another field
        (
            "and_range_cat",
            QueryNode::And(vec![
                rangeq(Some(-5.0), None, Some(5.0), None),
                termkw("cat", "red"),
            ]),
        ),
        // boolean Or of two ranges
        (
            "or_two_ranges",
            QueryNode::Or(vec![
                rangeq(None, None, None, Some(-2.0)),
                rangeq(Some(2.0), None, None, None),
            ]),
        ),
        // boolean And of exact + range (drives the cheapest-clause planner)
        (
            "and_exact_range",
            QueryNode::And(vec![
                termnum(0.0),
                rangeq(Some(-5.0), None, Some(5.0), None),
            ]),
        ),
    ]
}

/// Assert the Number field `price` has an EMPTY in-RAM `values` index but an
/// attached segment — the RAM-bounded invariant after a seal.
fn assert_values_dropped(e: &Engine) {
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
        panic!("price must be a Number field");
    };
    assert!(n.segment.is_some(), "segment must be attached");
    assert!(
        n.values.is_empty(),
        "RAM `values` index must be DROPPED after seal (got {} entries)",
        n.values.len()
    );
    assert!(
        n.forward.is_empty(),
        "RAM `forward` must be dropped after seal"
    );
}

fn uniq(e: &Engine, field: &str) -> u64 {
    e.stats("c")
        .unwrap()
        .fields
        .get(field)
        .unwrap()
        .unique_terms
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// PATH A (segment OFF, in-RAM `values` range walk) must equal PATH B
    /// (sealed: RAM `values` DROPPED, range/exact driven from the on-disk
    /// SORTED-VALUE column binary-search) for the WHOLE battery: closed /
    /// open-low / open-high / fully-open ranges, EXCLUSIVE vs INCLUSIVE
    /// bounds, empty ranges, exact-match (lo==hi), single-value, multi-Terms,
    /// boolean And/Or with another field, AND the asc/desc number-sort page.
    /// Byte-identical result SETS + totals on both paths. Stress varied f64:
    /// negatives, zeros (±0.0), duplicates, large/small magnitudes.
    #[test]
    fn range_segment_matches_live(
        docs in proptest::collection::vec(
            (
                proptest::option::weighted(
                    0.9,
                    prop::sample::select(vec![
                        -1000.5_f64, -7.0, -3.0, -2.0, -0.0, 0.0, 1.0, 2.0, 3.0,
                        3.0, 0.0, -3.0, 7.0, 42.0, 1e9, -1e9, 0.5, -0.5,
                    ]),
                ),
                proptest::option::weighted(
                    0.7,
                    prop::sample::select(vec!["red", "green", "blue"]),
                ),
            ),
            1..80,
        ),
    ) {
        // --- PATH A: in-RAM range index (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (price, cat)) in docs.iter().enumerate() {
            index_num(&e, &format!("d{i}"), *price, *cat);
        }

        let qs = all_queries();
        let a_sets: Vec<BTreeSet<String>> =
            qs.iter().map(|(_, q)| set_of(&run(&e, q.clone()))).collect();
        let a_totals: Vec<u64> =
            qs.iter().map(|(_, q)| search_total(&e, q.clone())).collect();
        let a_sort_asc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Asc);
        let a_sort_desc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Desc);
        let a_uniq = uniq(&e, "price");

        // --- PATH B: seal `price` (drops RAM `values`), rerun from the mmap. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_number_field_to_segment("c", "price", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");
        assert_values_dropped(&e); // RAM-bounded: values index gone

        for (i, (name, q)) in qs.iter().enumerate() {
            let b_set = set_of(&run(&e, q.clone()));
            prop_assert_eq!(&a_sets[i], &b_set, "result SET diverged for `{}`", name);
            let b_total = search_total(&e, q.clone());
            prop_assert_eq!(a_totals[i], b_total, "total diverged for `{}`", name);
        }
        let b_sort_asc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Asc);
        let b_sort_desc = run_sorted(&e, rangeq(None, None, None, None), "price", SortOrder::Desc);
        // Sort order is the field-sorted walk — must be value-identical (the
        // ORDERED sequence, not just the set), since `try_plan` drives the page
        // straight off the sorted-value column.
        prop_assert_eq!(&a_sort_asc, &b_sort_asc, "asc number-sort page diverged");
        prop_assert_eq!(&a_sort_desc, &b_sort_desc, "desc number-sort page diverged");
        prop_assert_eq!(a_uniq, uniq(&e, "price"), "unique_terms diverged");
    }
}

mod deletes;
mod reopen;
