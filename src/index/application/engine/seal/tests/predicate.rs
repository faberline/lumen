//! Dual-path diff test: segment-backed Number predicate read must be
//! byte-identical to the live in-RAM read (Stage 2 Phase 2c).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{
    MatchOp, MatchQuery, QueryNode, RangeBound, RangeQuery, TermQuery,
};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};
use crate::shared_kernel::types::search::SearchRequest;

fn fieldspec(t: FieldType, analyzer: Option<Analyzer>) -> FieldSpec {
    FieldSpec {
        field_type: t,
        analyzer,
        multi: None,
        dim: None,
        metric: None,
        backend: None,
        quantize: None,
    }
}

/// `age` (Number, the field we seal), `kw` (Keyword), `body` (Text).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("age".into(), fieldspec(FieldType::Number, None));
    fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
    fields.insert(
        "body".into(),
        fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
    );
    CreateCollectionRequest { fields }
}

fn req(query: QueryNode) -> SearchRequest {
    SearchRequest {
        query,
        limit: 100_000, // larger than any corpus → page == full match set
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

/// (external_id, score) pairs for a query, mirroring planner_diff's shape.
fn run(e: &Engine, query: QueryNode) -> Vec<(String, f32)> {
    e.search("c", req(query))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score))
        .collect()
}

fn set_of(rows: &[(String, f32)]) -> BTreeSet<String> {
    rows.iter().map(|(e, _)| e.clone()).collect()
}

/// Scores keyed by external_id — so we can assert score byte-equality
/// independent of result ordering.
fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
    rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
}

/// Index one doc: always writes `kw` + `body`; writes `age` only when
/// `age` is `Some` (so absent-value docs are part of the corpus).
fn index_doc(e: &Engine, eid: &str, age: Option<f64>, kw: &str, tok: bool) {
    let mut items = vec![
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "kw".into(),
            value: FieldValue::String(kw.into()),
            version: None,
        },
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "body".into(),
            value: FieldValue::String(if tok {
                "tok filler".into()
            } else {
                "filler".into()
            }),
            version: None,
        },
    ];
    if let Some(a) = age {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "age".into(),
            value: FieldValue::Number(a),
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

/// A match-DRIVEN AND so the `range age` conjunct is applied as a per-doc
/// PREDICATE (`clause_matches` → `NumberIndex::number_at`), which is the
/// segment-backed read site under test. `tok` is the rare driver token.
fn bool_filter(gte: f64, lt: f64) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Range(RangeQuery {
            field: "age".into(),
            gt: None,
            gte: Some(RangeBound::Number(gte)),
            lt: Some(RangeBound::Number(lt)),
            lte: None,
        }),
    ])
}

/// filtered_search: match-driven AND with BOTH a `term kw` and a
/// `range age` predicate — exercises the Term-Number and Range-Number
/// segment predicate sites together.
fn filtered_search(kw: &str, gte: f64, lt: f64) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Term(TermQuery {
            field: "kw".into(),
            value: FieldValue::String(kw.into()),
        }),
        QueryNode::Range(RangeQuery {
            field: "age".into(),
            gt: None,
            gte: Some(RangeBound::Number(gte)),
            lt: Some(RangeBound::Number(lt)),
            lte: None,
        }),
    ])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, live in-RAM) must equal PATH B (Number field
    /// sealed to an mmap segment, then served from it) — same result SET
    /// and byte-identical scores — for both query shapes over a randomized
    /// corpus (varied N, value distribution, absent-`age` docs).
    #[test]
    fn segment_read_matches_live_read(
        docs in proptest::collection::vec(
            (
                // age: ~1-in-5 docs have NO age value (absent column entry).
                proptest::option::weighted(0.8, 0u32..30),
                prop::sample::select(vec!["a", "b", "c", "d"]),
                any::<bool>(),
            ),
            1..60,
        ),
        lo in 0u32..30,
        span in 1u32..30,
    ) {
        let hi = lo + span;

        // --- PATH A: build the live engine, run both shapes (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (age, kw, tok)) in docs.iter().enumerate() {
            index_doc(&e, &format!("d{i}"), age.map(|a| a as f64), kw, *tok);
        }

        let a_bool = run(&e, bool_filter(lo as f64, hi as f64));
        let a_filt = run(&e, filtered_search("c", lo as f64, hi as f64));

        // --- PATH B: seal `age` to a segment, flip it ON, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_number_field_to_segment("c", "age", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

        let b_bool = run(&e, bool_filter(lo as f64, hi as f64));
        let b_filt = run(&e, filtered_search("c", lo as f64, hi as f64));

        // Result SET equality.
        prop_assert_eq!(set_of(&a_bool), set_of(&b_bool), "bool_filter set diverged");
        prop_assert_eq!(set_of(&a_filt), set_of(&b_filt), "filtered_search set diverged");
        // Scores byte-identical (f64::to_bits keyed by eid).
        prop_assert_eq!(scores_of(&a_bool), scores_of(&b_bool), "bool_filter scores diverged");
        prop_assert_eq!(scores_of(&a_filt), scores_of(&b_filt), "filtered_search scores diverged");
    }
}

/// A doc indexed AFTER sealing (docid >= segment n_docs) lives in the live
/// `forward` tail and must still match through `number_at`'s fallback.
#[test]
fn doc_indexed_after_sealing_served_from_live_tail() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    // Two docs sealed into the segment.
    index_doc(&e, "sealed_in", Some(10.0), "c", true);
    index_doc(&e, "sealed_out", Some(99.0), "c", true);

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_number_field_to_segment("c", "age", dir.path())
        .unwrap();
    assert_eq!(n, 2, "two docs sealed");

    // A NEW doc after sealing → docid 2 (>= n_docs) → lives in the live tail.
    index_doc(&e, "tail", Some(15.0), "c", true);

    // Range [12,20): only the tail doc qualifies. It is NOT in the segment,
    // so this proves number_at's `id >= n_docs` fallback to `forward`.
    let got = set_of(&run(&e, bool_filter(12.0, 20.0)));
    let want: BTreeSet<String> = ["tail".to_string()].into_iter().collect();
    assert_eq!(got, want, "tail doc must match via live fallback");

    // Range [8,12): only the sealed doc qualifies — served from the segment.
    let got = set_of(&run(&e, bool_filter(8.0, 12.0)));
    let want: BTreeSet<String> = ["sealed_in".to_string()].into_iter().collect();
    assert_eq!(got, want, "sealed doc must match via segment read");
}

/// Direct check that `NumberIndex::number_at` reads from the segment for
/// sealed ids and falls back to the live tail past `n_docs` — independent
/// of the query planner.
#[test]
fn number_at_segment_then_live_split() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_doc(&e, "a", Some(1.5), "x", false); // id 0
    index_doc(&e, "b", None, "x", false); // id 1, absent age
    index_doc(&e, "c", Some(-3.0), "x", false); // id 2

    let dir = tempfile::tempdir().unwrap();
    e.__seal_number_field_to_segment("c", "age", dir.path())
        .unwrap();
    index_doc(&e, "d", Some(7.0), "x", false); // id 3, live tail

    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Number(n) = coll.fields.get("age").unwrap() else {
        panic!("age must be a Number field");
    };
    assert!(n.segment.is_some(), "segment attached");
    assert_eq!(n.number_at(0), Some(SortableF64::new(1.5).unwrap())); // segment
    assert_eq!(n.number_at(1), None); // segment, absent
    assert_eq!(n.number_at(2), Some(SortableF64::new(-3.0).unwrap())); // segment
    assert_eq!(n.number_at(3), Some(SortableF64::new(7.0).unwrap())); // live tail
    assert_eq!(n.number_at(99), None); // unknown
}
