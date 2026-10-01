//! Dual-path diff test for the INVERTED Set driver (Stage 2 Phase 2h-2): the
//! segment-driven membership / Terms / boolean RoaringBitmap algebra
//! (`element_postings` / `element_df`) must be byte-identical to the in-RAM
//! `elements` index, AND after a seal the RAM index is DROPPED (the disk=all
//! win) while queries keep serving entirely from the mmap segment. This is the
//! Set analogue of `seal::tests::keyword_inverted` and the keystone test
//! for Phase 2h-2 — it reuses the same query-time tombstone mechanism.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{QueryNode, TermQuery, TermsQuery};
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

/// `tags` (Set, sealed) + `cat` (a second Set, for boolean AND/OR
/// cross-field algebra). Both are inverted-driver fields.
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("tags".into(), fieldspec(FieldType::Set, None));
    fields.insert("cat".into(), fieldspec(FieldType::Set, None));
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

fn search_total(e: &Engine, query: QueryNode) -> u64 {
    e.search("c", req(query)).unwrap().total
}

/// Index one doc: writes `tags` (multi-valued) when `Some`, `cat` when
/// `Some`. A doc with neither still interns via a filler `tags` member so it
/// is part of the corpus (mirrors the keyword module's filler).
fn index_set(e: &Engine, eid: &str, tags: Option<&[&str]>, cat: Option<&[&str]>) {
    let mut items = Vec::new();
    if let Some(ts) = tags {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "tags".into(),
            value: FieldValue::StringList(ts.iter().map(|s| (*s).to_string()).collect()),
            version: None,
        });
    }
    if let Some(cs) = cat {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "cat".into(),
            value: FieldValue::StringList(cs.iter().map(|s| (*s).to_string()).collect()),
            version: None,
        });
    }
    if items.is_empty() {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "tags".into(),
            value: FieldValue::StringList(vec!["zzz_filler".into()]),
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

fn term(field: &str, v: &str) -> QueryNode {
    QueryNode::Term(TermQuery {
        field: field.into(),
        value: FieldValue::String(v.into()),
    })
}

fn terms(field: &str, vs: &[&str]) -> QueryNode {
    QueryNode::Terms(TermsQuery {
        field: field.into(),
        values: vs.iter().map(|s| FieldValue::String((*s).into())).collect(),
    })
}

/// Assert the Set field `tags` has an EMPTY in-RAM `elements` index but an
/// attached segment — the RAM-bounded invariant after a seal/reopen.
fn assert_elements_dropped(e: &Engine) {
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
        panic!("tags must be a Set field");
    };
    assert!(s.segment.is_some(), "segment must be attached");
    assert!(
        s.elements.is_empty(),
        "RAM `elements` index must be DROPPED after seal (got {} entries)",
        s.elements.len()
    );
    assert!(
        s.forward.is_empty(),
        "RAM `forward` must be dropped after seal"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, in-RAM `elements`) must equal PATH B (sealed: RAM
    /// `elements` DROPPED, driven from the mmap posting column) for the whole
    /// inverted-driver surface: standalone `Term`, standalone `Terms`,
    /// boolean `Or` of two members, boolean `And` across two set fields, and
    /// the `try_plan` standalone-Term total. Byte-identical result sets AND
    /// identical totals on both paths.
    #[test]
    fn inverted_segment_matches_live(
        docs in proptest::collection::vec(
            (
                // tags: ~1-in-5 docs absent; others 0..4 deduped members.
                proptest::option::weighted(
                    0.8,
                    proptest::collection::vec(
                        prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]),
                        0..4,
                    ),
                ),
                // cat: ~1-in-3 docs absent; others 0..3 members.
                proptest::option::weighted(
                    0.7,
                    proptest::collection::vec(
                        prop::sample::select(vec!["red", "green"]),
                        0..3,
                    ),
                ),
            ),
            1..70,
        ),
    ) {
        // --- PATH A: in-RAM inverted index (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (tags, cat)) in docs.iter().enumerate() {
            let ts: Option<Vec<&str>> = tags.as_ref().map(|v| v.iter().map(|s| *s).collect());
            let cs: Option<Vec<&str>> = cat.as_ref().map(|v| v.iter().map(|s| *s).collect());
            index_set(&e, &format!("d{i}"), ts.as_deref(), cs.as_deref());
        }

        let a_term = run(&e, term("tags", "alpha"));
        let a_terms = run(&e, terms("tags", &["beta", "gamma"]));
        let a_or = run(&e, QueryNode::Or(vec![term("tags", "alpha"), term("tags", "delta")]));
        let a_and = run(&e, QueryNode::And(vec![term("tags", "beta"), term("cat", "red")]));
        let a_total = search_total(&e, term("tags", "gamma"));

        // --- PATH B: seal `tags` (drops RAM `elements`), rerun from the mmap. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_set_field_to_segment("c", "tags", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");
        assert_elements_dropped(&e); // RAM-bounded: elements index gone

        let b_term = run(&e, term("tags", "alpha"));
        let b_terms = run(&e, terms("tags", &["beta", "gamma"]));
        let b_or = run(&e, QueryNode::Or(vec![term("tags", "alpha"), term("tags", "delta")]));
        let b_and = run(&e, QueryNode::And(vec![term("tags", "beta"), term("cat", "red")]));
        let b_total = search_total(&e, term("tags", "gamma"));

        prop_assert_eq!(set_of(&a_term), set_of(&b_term), "Term set diverged");
        prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "Terms set diverged");
        prop_assert_eq!(set_of(&a_or), set_of(&b_or), "Or set diverged");
        prop_assert_eq!(set_of(&a_and), set_of(&b_and), "And set diverged");
        prop_assert_eq!(a_total, b_total, "standalone-term total diverged");
    }
}

/// RAM-BOUNDED + REOPEN-NO-REBUILD: seal the WHOLE collection to disk, reopen
/// from the segments alone (no CBOR snapshot), and assert the reopened Set
/// field's `elements` map is EMPTY while membership/Terms queries still return
/// correct results entirely from the mmap segment.
#[test]
fn reopen_drives_from_segment_with_empty_elements() {
    let dir = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_set(&e, "a", Some(&["alpha", "beta"]), Some(&["red"]));
    index_set(&e, "b", Some(&["beta"]), Some(&["green"]));
    index_set(&e, "c2", Some(&["gamma"]), None);
    index_set(&e, "d", Some(&["alpha"]), Some(&["red"]));
    index_set(&e, "e", None, Some(&["green"])); // present-but-no-tags

    let want_alpha = set_of(&run(&e, term("tags", "alpha")));
    let want_beta_gamma = set_of(&run(&e, terms("tags", &["beta", "gamma"])));

    e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
    let schema = e.__collection_schema("c").unwrap();
    let e2 = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();

    // The reopened Set `elements` map must be EMPTY (no RAM rebuild) ...
    {
        let state = e2.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags must be a Set field");
        };
        assert!(s.segment.is_some(), "reopened segment attached");
        assert!(
            s.elements.is_empty(),
            "reopen must NOT rebuild `elements` in RAM (got {} entries)",
            s.elements.len()
        );
    }

    // ... yet the inverted queries still resolve, entirely from the mmap.
    assert_eq!(
        set_of(&run(&e2, term("tags", "alpha"))),
        want_alpha,
        "Term post-reopen"
    );
    assert_eq!(
        set_of(&run(&e2, terms("tags", &["beta", "gamma"]))),
        want_beta_gamma,
        "Terms post-reopen"
    );
}

/// LIVE-TAIL UNION: seal, then index more docs (tail into the live
/// `elements`), and assert a Term query returns base (segment) + tail (RAM)
/// composed, plus the `element_df` sum.
#[test]
fn live_tail_unions_with_segment_base() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_set(&e, "base0", Some(&["alpha"]), None); // id 0, sealed base
    index_set(&e, "base1", Some(&["beta"]), None); // id 1, sealed base

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_set_field_to_segment("c", "tags", dir.path())
        .unwrap();
    assert_eq!(n, 2);
    assert_elements_dropped(&e); // base index now on disk

    // Tail docs after seal → land in the live `elements` tail (ids >= n_docs).
    index_set(&e, "tail0", Some(&["alpha"]), None); // id 2, SAME element as base0
    index_set(&e, "tail1", Some(&["gamma"]), None); // id 3, NEW element

    // element alpha: base id (base0) UNION tail id (tail0).
    let got = set_of(&run(&e, term("tags", "alpha")));
    let want: BTreeSet<String> = ["base0".into(), "tail0".into()].into_iter().collect();
    assert_eq!(got, want, "alpha must union segment base + live tail");

    // element gamma: ONLY the tail (not in the segment).
    let got = set_of(&run(&e, term("tags", "gamma")));
    let want: BTreeSet<String> = ["tail1".into()].into_iter().collect();
    assert_eq!(got, want, "gamma must come from the live tail alone");

    // element beta: ONLY the segment base.
    let got = set_of(&run(&e, term("tags", "beta")));
    let want: BTreeSet<String> = ["base1".into()].into_iter().collect();
    assert_eq!(got, want, "beta must come from the segment base alone");

    // df composition: element_df(alpha) = 1 (base) + 1 (tail) = 2.
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
        panic!("tags must be a Set field");
    };
    assert_eq!(
        s.element_df("alpha"),
        2,
        "df must sum segment base + live tail"
    );
    assert_eq!(s.element_df("beta"), 1, "df beta segment-only");
    assert_eq!(s.element_df("gamma"), 1, "df gamma tail-only");
    assert_eq!(s.element_df("missing"), 0, "df absent element");
}

mod deletes;
