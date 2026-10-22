//! Dual-path diff test: segment-backed Set membership read must be
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

/// `tags` (Set, the field we seal) + `body` (Text, the AND driver).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("tags".into(), fieldspec(FieldType::Set, None));
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

/// Index one doc: always writes `body`; writes `tags` only when `tags` is
/// `Some` (so absent-set docs are part of the corpus). A present-but-empty
/// set is `Some(&[])`.
fn index_doc(e: &Engine, eid: &str, tags: Option<&[&str]>, tok: bool) {
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
    if let Some(ts) = tags {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "tags".into(),
            value: FieldValue::StringList(ts.iter().map(|s| (*s).to_string()).collect()),
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

/// A match-DRIVEN AND so the `term tags` membership conjunct is applied as a
/// per-doc PREDICATE (`clause_matches` → `SetIndex::set_contains`).
fn term_conjunct(el: &str) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Term(TermQuery {
            field: "tags".into(),
            value: FieldValue::String(el.into()),
        }),
    ])
}

/// A match-DRIVEN AND with a `terms tags` (OR-of-members) conjunct —
/// exercises `SetIndex::set_contains_any`.
fn terms_conjunct(els: &[&str]) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Terms(TermsQuery {
            field: "tags".into(),
            values: els
                .iter()
                .map(|s| FieldValue::String((*s).into()))
                .collect(),
        }),
    ])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, live in-RAM `forward`) must equal PATH B (Set
    /// field sealed to a shared DICT + CSR offsets + packed segment, then
    /// served from it) — same result SET and byte-identical scores — for
    /// both the `term tags` and `terms tags` membership conjuncts over a
    /// randomized multi-valued corpus (varied N, cardinality incl 0,
    /// absent-`tags` docs).
    #[test]
    fn segment_read_matches_live_read(
        docs in proptest::collection::vec(
            (
                // tags: ~1-in-5 docs have NO set value; others get 0..4
                // members drawn from a small pool (multi-valued, deduped).
                proptest::option::weighted(
                    0.8,
                    proptest::collection::vec(
                        prop::sample::select(vec!["x", "y", "z", "w", "v"]),
                        0..4,
                    ),
                ),
                any::<bool>(),
            ),
            1..60,
        ),
    ) {
        // --- PATH A: build the live engine, run both shapes (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (tags, tok)) in docs.iter().enumerate() {
            let slice: Option<Vec<&str>> =
                tags.as_ref().map(|v| v.iter().map(|s| *s).collect());
            index_doc(&e, &format!("d{i}"), slice.as_deref(), *tok);
        }

        let a_term = run(&e, term_conjunct("x"));
        let a_terms = run(&e, terms_conjunct(&["y", "z"]));

        // --- PATH B: seal `tags` to a segment, flip it ON, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_set_field_to_segment("c", "tags", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");

        let b_term = run(&e, term_conjunct("x"));
        let b_terms = run(&e, terms_conjunct(&["y", "z"]));

        prop_assert_eq!(set_of(&a_term), set_of(&b_term), "term conjunct set diverged");
        prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "terms conjunct set diverged");
        prop_assert_eq!(scores_of(&a_term), scores_of(&b_term), "term scores diverged");
        prop_assert_eq!(scores_of(&a_terms), scores_of(&b_terms), "terms scores diverged");
    }
}

/// A doc indexed AFTER sealing (docid >= segment n_docs) lives in the live
/// `forward` tail and must still match through `set_contains`'s fallback.
#[test]
fn doc_indexed_after_sealing_served_from_live_tail() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_doc(&e, "sealed", Some(&["x", "y"]), true); // id 0
    index_doc(&e, "empty", Some(&[]), true); // id 1, present-but-empty

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_set_field_to_segment("c", "tags", dir.path())
        .unwrap();
    assert_eq!(n, 2, "two docs sealed");

    // New doc after sealing → docid 2 (>= n_docs) → lives in the live tail.
    index_doc(&e, "tail", Some(&["z"]), true);

    // term z: only the tail doc qualifies (NOT in the segment) → proves the
    // set_contains id >= n_docs fallback.
    let got = set_of(&run(&e, term_conjunct("z")));
    let want: BTreeSet<String> = ["tail".to_string()].into_iter().collect();
    assert_eq!(got, want, "tail doc must match via live fallback");

    // term x: only the sealed doc qualifies → served from the segment.
    let got = set_of(&run(&e, term_conjunct("x")));
    let want: BTreeSet<String> = ["sealed".to_string()].into_iter().collect();
    assert_eq!(got, want, "sealed doc must match via segment read");
}

/// Direct planner-free check that `SetIndex::set_contains` reads CSR-packed
/// members from the segment (incl multi-valued + present-empty + absent
/// docs) and falls back to the live tail past `n_docs`.
#[test]
fn set_contains_segment_then_live_split() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_doc(&e, "a", Some(&["x", "y"]), false); // id 0, multi
    index_doc(&e, "b", None, false); // id 1, absent
    index_doc(&e, "c", Some(&[]), false); // id 2, present-empty

    let dir = tempfile::tempdir().unwrap();
    e.__seal_set_field_to_segment("c", "tags", dir.path())
        .unwrap();
    index_doc(&e, "d", Some(&["z"]), false); // id 3, live tail

    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
        panic!("tags must be a Set field");
    };
    assert!(s.segment.is_some(), "segment attached");
    // doc 0: {x, y} from the segment (CSR slice of 2 members).
    assert!(s.set_contains(0, "x"));
    assert!(s.set_contains(0, "y"));
    assert!(!s.set_contains(0, "z"));
    // doc 1: absent → no membership.
    assert!(!s.set_contains(1, "x"));
    // doc 2: present-but-empty → no membership but is present.
    assert!(!s.set_contains(2, "x"));
    // doc 3: {z} from the live tail (id >= n_docs).
    assert!(s.set_contains(3, "z"));
    assert!(!s.set_contains(3, "x"));
    // unknown id.
    assert!(!s.set_contains(99, "x"));
}
