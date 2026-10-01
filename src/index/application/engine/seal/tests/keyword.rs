//! Dual-path diff test: segment-backed Keyword predicate read must be
//! byte-identical to the live in-RAM read (Stage 2 Phase 2e-A).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, MatchQuery, QueryNode, TermQuery, TermsQuery};
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

/// `kw` (Keyword, the field we seal) + `body` (Text, the AND driver).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
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

fn scores_of(rows: &[(String, f32)]) -> BTreeMap<String, u32> {
    rows.iter().map(|(e, s)| (e.clone(), s.to_bits())).collect()
}

/// Index one doc: always writes `body`; writes `kw` only when `kw` is
/// `Some` (so absent-keyword docs are part of the corpus).
fn index_doc(e: &Engine, eid: &str, kw: Option<&str>, tok: bool) {
    let mut items = vec![crate::shared_kernel::types::document::IndexItem {
        external_id: eid.into(),
        field: "body".into(),
        value: FieldValue::String(if tok {
            "tok filler".into()
        } else {
            "filler".into()
        }),
        version: None,
    }];
    if let Some(k) = kw {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "kw".into(),
            value: FieldValue::String(k.into()),
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

/// A match-DRIVEN AND so the `term kw` conjunct is applied as a per-doc
/// PREDICATE (`clause_matches` → `KeywordIndex::keyword_at`) — the
/// segment-backed read site under test. `tok` is the rare driver token.
fn term_conjunct(kw: &str) -> QueryNode {
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
    ])
}

/// A match-DRIVEN AND with a `terms kw` (multi-value OR-of-terms) conjunct
/// — exercises the Terms-Keyword segment predicate site (`keyword_at` in
/// the `Terms` arm of `clause_matches`).
fn terms_conjunct(kws: &[&str]) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Terms(TermsQuery {
            field: "kw".into(),
            values: kws
                .iter()
                .map(|s| FieldValue::String((*s).into()))
                .collect(),
        }),
    ])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, live in-RAM `forward`) must equal PATH B
    /// (Keyword field sealed to a var-width DICT + dict-id segment, then
    /// served from it) — same result SET and byte-identical scores — for
    /// both the `term kw` conjunct and the `terms kw` conjunct over a
    /// randomized corpus (varied N, value distribution, absent-`kw` docs).
    #[test]
    fn segment_read_matches_live_read(
        docs in proptest::collection::vec(
            (
                // kw: ~1-in-5 docs have NO keyword (absent column entry).
                proptest::option::weighted(
                    0.8,
                    prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]),
                ),
                any::<bool>(),
            ),
            1..60,
        ),
    ) {
        // --- PATH A: build the live engine, run both shapes (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (kw, tok)) in docs.iter().enumerate() {
            index_doc(&e, &format!("d{i}"), *kw, *tok);
        }

        let a_term = run(&e, term_conjunct("alpha"));
        let a_terms = run(&e, terms_conjunct(&["beta", "gamma"]));

        // --- PATH B: seal `kw` to a segment, flip it ON, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_keyword_field_to_segment("c", "kw", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

        let b_term = run(&e, term_conjunct("alpha"));
        let b_terms = run(&e, terms_conjunct(&["beta", "gamma"]));

        prop_assert_eq!(set_of(&a_term), set_of(&b_term), "term conjunct set diverged");
        prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "terms conjunct set diverged");
        prop_assert_eq!(scores_of(&a_term), scores_of(&b_term), "term scores diverged");
        prop_assert_eq!(scores_of(&a_terms), scores_of(&b_terms), "terms scores diverged");
    }
}

/// A doc indexed AFTER sealing (docid >= segment n_docs) lives in the live
/// `forward` tail and must still match through `keyword_at`'s fallback.
#[test]
fn doc_indexed_after_sealing_served_from_live_tail() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_doc(&e, "sealed", Some("alpha"), true);
    index_doc(&e, "absent", None, true); // present-but-no-kw, id 1

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_keyword_field_to_segment("c", "kw", dir.path())
        .unwrap();
    assert_eq!(n, 2, "two docs sealed");

    // A NEW doc after sealing → docid 2 (>= n_docs) → lives in the live tail.
    index_doc(&e, "tail", Some("beta"), true);

    // term beta: only the tail doc qualifies, NOT in the segment → proves
    // keyword_at's id >= n_docs fallback to forward.
    let got = set_of(&run(&e, term_conjunct("beta")));
    let want: BTreeSet<String> = ["tail".to_string()].into_iter().collect();
    assert_eq!(got, want, "tail doc must match via live fallback");

    // term alpha: only the sealed doc qualifies → served from the segment.
    let got = set_of(&run(&e, term_conjunct("alpha")));
    let want: BTreeSet<String> = ["sealed".to_string()].into_iter().collect();
    assert_eq!(got, want, "sealed doc must match via segment read");
}

/// Direct planner-free check that `KeywordIndex::keyword_at` reads from the
/// segment for sealed ids (incl an absent doc) and falls back to the live
/// tail past `n_docs`.
#[test]
fn keyword_at_segment_then_live_split() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_doc(&e, "a", Some("alpha"), false); // id 0
    index_doc(&e, "b", None, false); // id 1, absent kw
    index_doc(&e, "c", Some("gamma"), false); // id 2

    let dir = tempfile::tempdir().unwrap();
    e.__seal_keyword_field_to_segment("c", "kw", dir.path())
        .unwrap();
    index_doc(&e, "d", Some("delta"), false); // id 3, live tail

    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
        panic!("kw must be a Keyword field");
    };
    assert!(k.segment.is_some(), "segment attached");
    assert_eq!(k.keyword_at(0).as_deref(), Some("alpha")); // segment
    assert_eq!(k.keyword_at(1), None); // segment, absent
    assert_eq!(k.keyword_at(2).as_deref(), Some("gamma")); // segment
    assert_eq!(k.keyword_at(3).as_deref(), Some("delta")); // live tail
    assert_eq!(k.keyword_at(99), None); // unknown
}
