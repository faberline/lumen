//! Dual-path diff test: the segment-backed BM25 scan (stored postings + DocLen
//! column + header scalars) must be byte-identical to the live in-RAM scan
//! (Stage 2 Phase 2e-B). Text tf is NOT rebuildable, so the postings are STORED.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, MatchQuery, QueryNode, RangeBound, RangeQuery};
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

/// `body` (Text, the field we seal) + `price` (Number, a filter for the
/// filtered_search shape).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "body".into(),
        fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
    );
    fields.insert("price".into(), fieldspec(FieldType::Number, None));
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

/// Key the f32 score BITS by external id, so the dual-path compare proves
/// byte-identical scores (not just approximate equality).
fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
    rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
}

/// Index one doc's `body` text and (optionally) a `price` filter value. The
/// body is a tf-realistic bag: each chosen token is repeated `tf` times so
/// the stored term-frequencies vary across the corpus.
fn index_doc(e: &Engine, eid: &str, body: &str, price: Option<f64>) {
    let mut items = vec![crate::shared_kernel::types::document::IndexItem {
        external_id: eid.into(),
        field: "body".into(),
        value: FieldValue::String(body.into()),
        version: None,
    }];
    if let Some(p) = price {
        items.push(crate::shared_kernel::types::document::IndexItem {
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

/// Build a tf-realistic body string from `(token, repeat)` pairs.
fn body_from(parts: &[(&str, u32)]) -> String {
    let mut out: Vec<&str> = Vec::new();
    for (tok, rep) in parts {
        for _ in 0..*rep {
            out.push(tok);
        }
    }
    out.join(" ")
}

/// text_bm25: a single-token OR match — the pure BM25 scan over one token's
/// posting list (the `score_token` hot loop).
fn bm25_single(tok: &str) -> QueryNode {
    QueryNode::Match(MatchQuery {
        field: "body".into(),
        text: tok.into(),
        op: MatchOp::Or,
    })
}

/// text_and: a 2-token AND match — the intersect-and-sum path (drives from
/// the rarer token, probes the other by binary-search).
fn text_and(a: &str, b: &str) -> QueryNode {
    QueryNode::Match(MatchQuery {
        field: "body".into(),
        text: format!("{a} {b}"),
        op: MatchOp::And,
    })
}

/// filtered_search: a `match` AND a `price` range filter. The match BM25 is
/// scored over the candidate set; both the bitmap-driven and match-driven
/// AND plans route through `eval_match` / `match_doc_score`, so the sealed
/// postings/doc-len feed the scoring on either plan.
fn filtered(tok: &str, lo: f64, hi: f64) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: tok.into(),
            op: MatchOp::Or,
        }),
        QueryNode::Range(RangeQuery {
            field: "price".into(),
            gte: Some(RangeBound::Number(lo)),
            lte: Some(RangeBound::Number(hi)),
            gt: None,
            lt: None,
        }),
    ])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, live in-RAM postings) must equal PATH B (Text
    /// field sealed to a token DICT + STORED posting blocks + DocLen column,
    /// then served entirely from it) — same result SET and BYTE-IDENTICAL
    /// f32 scores — across text_bm25 (single token), text_and (2-token AND),
    /// and filtered_search (match + range) over a tf-realistic corpus with
    /// varied token frequencies and document lengths.
    #[test]
    fn segment_bm25_matches_live_scan(
        docs in proptest::collection::vec(
            (
                // tf of "alpha" (0 == token absent), 0..=4.
                0u32..5,
                // tf of "beta", 0..=4.
                0u32..5,
                // tf of "gamma" (filler that varies doc_len), 0..=3.
                0u32..4,
                // price (always present so the range filter is meaningful).
                0u32..100,
            ),
            1..50,
        ),
    ) {
        // --- PATH A: build the live engine, run all shapes (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (a, b, g, price)) in docs.iter().enumerate() {
            // Every doc has SOME body token so doc_count tracks all docs;
            // a doc with all-zero tfs still posts a single "filler" token.
            let mut parts: Vec<(&str, u32)> = Vec::new();
            if *a > 0 { parts.push(("alpha", *a)); }
            if *b > 0 { parts.push(("beta", *b)); }
            if *g > 0 { parts.push(("gamma", *g)); }
            if parts.is_empty() { parts.push(("filler", 1)); }
            index_doc(&e, &format!("d{i}"), &body_from(&parts), Some(*price as f64));
        }

        let a_single = run(&e, bm25_single("alpha"));
        let a_and = run(&e, text_and("alpha", "beta"));
        let a_filt = run(&e, filtered("alpha", 10.0, 80.0));

        // --- PATH B: seal `body` to a segment, flip it ON, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_text_field_to_segment("c", "body", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

        let b_single = run(&e, bm25_single("alpha"));
        let b_and = run(&e, text_and("alpha", "beta"));
        let b_filt = run(&e, filtered("alpha", 10.0, 80.0));

        prop_assert_eq!(set_of(&a_single), set_of(&b_single), "single-token set diverged");
        prop_assert_eq!(set_of(&a_and), set_of(&b_and), "2-token AND set diverged");
        prop_assert_eq!(set_of(&a_filt), set_of(&b_filt), "filtered set diverged");
        prop_assert_eq!(scores_of(&a_single), scores_of(&b_single), "single-token scores diverged");
        prop_assert_eq!(scores_of(&a_and), scores_of(&b_and), "2-token AND scores diverged");
        prop_assert_eq!(scores_of(&a_filt), scores_of(&b_filt), "filtered scores diverged");
    }
}

/// Direct planner-free check that the sealed `TextIndex` reads postings /
/// doc-len / df / corpus scalars from the segment, bit-identical to live.
#[test]
fn text_reads_from_segment_after_seal() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_doc(&e, "a", &body_from(&[("alpha", 3), ("beta", 1)]), Some(5.0)); // id 0, len 4
    index_doc(&e, "b", &body_from(&[("alpha", 1)]), Some(6.0)); // id 1, len 1
    index_doc(&e, "c", &body_from(&[("gamma", 2)]), Some(7.0)); // id 2, len 2

    // Capture live BM25 scores before sealing.
    let live = scores_of(&run(&e, bm25_single("alpha")));

    let dir = tempfile::tempdir().unwrap();
    e.__seal_text_field_to_segment("c", "body", dir.path())
        .unwrap();

    // After sealing, the same query must yield byte-identical scores.
    let sealed_scores = scores_of(&run(&e, bm25_single("alpha")));
    assert_eq!(
        live, sealed_scores,
        "sealed BM25 must be bit-identical to live"
    );

    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
        panic!("body must be a Text field");
    };
    assert!(idx.segment.is_some(), "segment attached");
    // Postings come from the segment.
    let p = idx.tok_postings("alpha").unwrap();
    assert_eq!(p.docids(), &[0u32, 1]);
    assert_eq!(p.tfs(), &[3u32, 1]);
    // doc_len routes through the segment DocLen column.
    assert_eq!(idx.doc_len(0), 4);
    assert_eq!(idx.doc_len(1), 1);
    assert_eq!(idx.doc_len(2), 2);
    // df from the segment posting length.
    assert_eq!(idx.tok_df("alpha"), Some(2));
    assert_eq!(idx.tok_df("durian"), None);
    // corpus scalars now read the LIVE counters (initialized from the header
    // at seal: doc_count=3, total_doc_len=4+1+2). Phase 2h-4.
    assert_eq!(idx.bm25_corpus(), (3, 4 + 1 + 2));
    // Phase 2h-4: `distinct` (and `tokens`/`lens`) are DROPPED at seal — no RAM
    // rebuild. `drop_eid` tombstones a sealed base id instead of consuming
    // `distinct`. The BM25 scan answers entirely from the mmap segment.
    assert!(
        idx.tokens.is_empty(),
        "tokens dropped at seal (postings on disk)"
    );
    assert!(
        idx.distinct_is_empty(),
        "distinct dropped at seal (drop_eid uses tombstones)"
    );
    assert!(
        idx.lens.is_empty(),
        "lens dropped at seal (doc_len reads the segment column)"
    );
    assert!(idx.tombstones.is_empty(), "no deletes yet");
}

mod deletes;
mod overlay;
mod reopen;
mod reseal;
