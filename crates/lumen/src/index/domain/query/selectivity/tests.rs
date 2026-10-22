//! Exact hamming as an AND filter (#4246): `and[hamming(max_distance 0), match]`
//! must plan as filter-driven (bitmap driver / per-doc predicate) and return
//! the SAME hit set with byte-identical scores as the materialize-and-intersect
//! fallback, which a fuzzy hamming (`max_distance > 0`) still takes.

use std::collections::BTreeMap;
use std::sync::Arc;

use roaring::RoaringBitmap;

use crate::index::application::engine::Engine;
use crate::index::domain::query::clause::{clause_matches, eval_filter_bitmap};
use crate::index::domain::query::knn::eval_hamming;
use crate::index::domain::query::selectivity::{
    estimate_selectivity, is_exact_hamming, is_predicable,
};
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{HammingQuery, MatchOp, MatchQuery, QueryNode};
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

/// `sig` (Hash) + `body` (Text, whitespace-lower).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
    fields.insert(
        "body".into(),
        fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
    );
    CreateCollectionRequest { fields }
}

/// Even-weight code `(i << 1) | parity(i)`: every pair of hashes sits at
/// Hamming distance ≥ 2, so `max_distance 1` (fallback path) and
/// `max_distance 0` (filter path) select exactly the same docs.
fn sig(i: u32) -> u64 {
    ((i as u64) << 1) | (i.count_ones() & 1) as u64
}

/// Every doc shares most tokens (like the durable workload's ngram text)
/// and carries one unique token, so a full-text `match … op=and` selects
/// exactly one doc while every token's posting spans the corpus.
fn body(i: u32) -> String {
    let parity = if i % 2 == 0 { "even" } else { "odd" };
    format!("doc {i} shared token stream alpha beta gamma {parity}")
}

fn index(e: &Engine, eid: &str, hash: u64, text: &str) {
    e.index(
        "c",
        IndexRequest {
            items: vec![
                crate::shared_kernel::types::document::IndexItem {
                    external_id: eid.into(),
                    field: "sig".into(),
                    value: FieldValue::String(format!("{hash:016x}")),
                    version: None,
                },
                crate::shared_kernel::types::document::IndexItem {
                    external_id: eid.into(),
                    field: "body".into(),
                    value: FieldValue::String(text.into()),
                    version: None,
                },
            ],
            request_id: None,
        },
    )
    .unwrap();
}

fn seed(n: u32) -> Arc<Engine> {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    for i in 0..n {
        index(&e, &format!("d{i}"), sig(i), &body(i));
    }
    e
}

fn hamming_raw(hash: u64, max: u32) -> QueryNode {
    QueryNode::Hamming(HammingQuery {
        field: "sig".into(),
        hash: format!("{hash:016x}"),
        max_distance: max,
    })
}

fn hamming(i: u32, max: u32) -> QueryNode {
    hamming_raw(sig(i), max)
}

fn matchq(text: &str) -> QueryNode {
    QueryNode::Match(MatchQuery {
        field: "body".into(),
        text: text.into(),
        op: MatchOp::And,
    })
}

/// (external_id, score_bits) — order-independent, byte-exact.
fn run(e: &Engine, query: QueryNode) -> BTreeMap<String, u32> {
    e.search(
        "c",
        SearchRequest {
            query,
            limit: 100,
            offset: 0,
            cursor: None,
            routing_key: None,
            sort: None,
            track_total: true,
            collapse: None,
        },
    )
    .unwrap()
    .hits
    .into_iter()
    .map(|h| (h.external_id, h.score.to_bits()))
    .collect()
}

#[test]
fn exact_hamming_is_a_filter_and_fuzzy_is_not() {
    assert!(is_exact_hamming(&hamming(3, 0)));
    assert!(is_predicable(&hamming(3, 0)));
    assert!(!is_exact_hamming(&hamming(3, 1)));
    assert!(!is_predicable(&hamming(3, 1)));

    let e = seed(8);
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    assert_eq!(estimate_selectivity(coll, &hamming(3, 0)), 1);
    assert_eq!(estimate_selectivity(coll, &hamming(3, 1)), u64::MAX);
    // The exact hamming drives the AND ahead of a corpus-wide match, so
    // the match is scored over the hash hits, never materialized.
    assert!(
        estimate_selectivity(coll, &hamming(3, 0))
            <= estimate_selectivity(coll, &matchq("shared token")),
        "exact hamming must be the cheaper driver"
    );
}

#[test]
fn exact_hamming_bitmap_and_predicate_match_eval_hamming() {
    let e = seed(16);
    // Seal the hash field so `hash_at` serves ids from the segment, then
    // add a live-tail doc: the filter reads must cover both sources.
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        e.__seal_hash_field_to_segment("c", "sig", dir.path())
            .unwrap(),
        16
    );
    index(&e, "d16", sig(16), &body(16));

    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    for i in [0u32, 5, 15, 16, 40] {
        let q = hamming(i, 0);
        let QueryNode::Hamming(hq) = &q else {
            unreachable!()
        };
        let want: RoaringBitmap = eval_hamming(coll, hq).unwrap().into_keys().collect();
        assert_eq!(want.len(), u64::from(i < 17), "sig({i}) hit count");
        let got = eval_filter_bitmap(coll, &q).unwrap();
        assert_eq!(got, want, "bitmap for sig({i})");
        for id in 0..17u32 {
            let pred = clause_matches(coll, &q, id).unwrap();
            assert_eq!(
                pred,
                want.contains(id).then_some(1.0),
                "predicate sig({i}) on id {id}"
            );
        }
    }
}

#[test]
fn exact_hamming_and_match_scores_are_byte_identical_to_fallback() {
    let e = seed(24);
    for i in [0u32, 7, 23] {
        let full = body(i);
        // Fallback: a fuzzy hamming is never predicable, and the pairwise
        // distance ≥ 2 makes `max_distance 1` select the same single doc.
        let fallback = run(&e, QueryNode::And(vec![hamming(i, 1), matchq(&full)]));
        assert_eq!(fallback.len(), 1, "sig({i}) selects exactly its doc");
        assert!(fallback.contains_key(&format!("d{i}")));

        // Filter path, top-k entry (`eval_predicable_and_topk`).
        let topk = run(&e, QueryNode::And(vec![hamming(i, 0), matchq(&full)]));
        assert_eq!(topk, fallback, "top-k filter path vs fallback for d{i}");

        // Filter path, general `eval_query` AND branch (reached through a
        // single-child `or`), with the conjuncts in the other order.
        let general = run(
            &e,
            QueryNode::Or(vec![QueryNode::And(vec![matchq(&full), hamming(i, 0)])]),
        );
        let general_fallback = run(
            &e,
            QueryNode::Or(vec![QueryNode::And(vec![matchq(&full), hamming(i, 1)])]),
        );
        assert_eq!(
            general, general_fallback,
            "eval_query filter path vs fallback for d{i}"
        );
        assert_eq!(
            general, fallback,
            "conjunct order must not change the score for d{i}"
        );

        // A wrong-field text under the same hash misses on every path.
        let wrong = format!("readback mismatch window {i} body");
        assert!(run(&e, QueryNode::And(vec![hamming(i, 0), matchq(&wrong)])).is_empty());
        assert!(run(&e, QueryNode::And(vec![hamming(i, 1), matchq(&wrong)])).is_empty());
    }
}

#[test]
fn fuzzy_hamming_keeps_its_graded_score_on_the_fallback() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index(&e, "near", 0, "tok");
    index(&e, "far", 1, "tok"); // distance 1 from the query hash 0

    let bm25 = run(&e, matchq("tok"));
    let got = run(&e, QueryNode::And(vec![hamming_raw(0, 1), matchq("tok")]));
    assert_eq!(got.len(), 2);
    assert_eq!(
        got["near"],
        (1.0f32 + f32::from_bits(bm25["near"])).to_bits()
    );
    assert_eq!(
        got["far"],
        ((64 - 1) as f32 / 64.0 + f32::from_bits(bm25["far"])).to_bits()
    );
    // And the exact form drops the distance-1 doc.
    let exact = run(&e, QueryNode::And(vec![hamming_raw(0, 0), matchq("tok")]));
    assert_eq!(exact.len(), 1);
    assert_eq!(exact["near"], got["near"]);
}

mod sparse_planning;
