//! Dual-path diff test for the INVERTED Keyword driver (Stage 2 Phase 2h-1):
//! the segment-driven Term/Terms/boolean RoaringBitmap algebra (`term_postings`
//! / `term_df`) must be byte-identical to the in-RAM `terms` index, AND after a
//! seal the RAM index is DROPPED (the disk=all win) while queries keep serving
//! entirely from the mmap segment. This is the keystone test for Phase 2h-1.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{PrefixQuery, QueryNode, TermQuery, TermsQuery};
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

/// `kw` (Keyword, sealed) + `cat` (a second Keyword, for boolean AND/OR
/// cross-field algebra). Both are inverted-driver fields.
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
    fields.insert("cat".into(), fieldspec(FieldType::Keyword, None));
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

/// (external_id, total) result for a query — drives the `try_plan`
/// standalone-Term page+total path too.
fn search_total(e: &Engine, query: QueryNode) -> u64 {
    e.search("c", req(query)).unwrap().total
}

fn index_kw(e: &Engine, eid: &str, kw: Option<&str>, cat: Option<&str>) {
    let mut items = Vec::new();
    if let Some(k) = kw {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "kw".into(),
            value: FieldValue::String(k.into()),
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
    // A doc with neither field would have no postings anywhere; ensure at
    // least the kw field so the doc is interned.
    if items.is_empty() {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "kw".into(),
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

/// Assert the Keyword field `kw` has an EMPTY in-RAM `terms` index but an
/// attached segment — the RAM-bounded invariant after a seal/reopen.
fn assert_terms_dropped(e: &Engine) {
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
        panic!("kw must be a Keyword field");
    };
    assert!(k.segment.is_some(), "segment must be attached");
    assert!(
        k.terms.is_empty(),
        "RAM `terms` index must be DROPPED after seal (got {} entries)",
        k.terms.len()
    );
    assert!(
        k.forward.is_empty(),
        "RAM `forward` must be dropped after seal"
    );
    assert!(
        k.dense_forward.is_empty(),
        "RAM dense `forward` must be dropped after seal"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PATH A (segment OFF, in-RAM `terms`) must equal PATH B (sealed: RAM
    /// `terms` DROPPED, driven from the mmap posting column) for the whole
    /// inverted-driver surface: standalone `Term`, standalone `Terms`,
    /// boolean `Or` of two terms, boolean `And` across two keyword fields,
    /// and the `try_plan` standalone-Term total. Byte-identical result sets
    /// AND identical totals on both paths.
    #[test]
    fn inverted_segment_matches_live(
        docs in proptest::collection::vec(
            (
                proptest::option::weighted(
                    0.85,
                    prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]),
                ),
                proptest::option::weighted(
                    0.7,
                    prop::sample::select(vec!["red", "green"]),
                ),
            ),
            1..70,
        ),
    ) {
        // --- PATH A: in-RAM inverted index (segment OFF). ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (kw, cat)) in docs.iter().enumerate() {
            index_kw(&e, &format!("d{i}"), *kw, *cat);
        }

        let a_term = run(&e, term("kw", "alpha"));
        let a_terms = run(&e, terms("kw", &["beta", "gamma"]));
        let a_or = run(&e, QueryNode::Or(vec![term("kw", "alpha"), term("kw", "delta")]));
        let a_and = run(&e, QueryNode::And(vec![term("kw", "beta"), term("cat", "red")]));
        let a_total = search_total(&e, term("kw", "gamma"));

        // --- PATH B: seal `kw` (drops RAM `terms`), rerun from the mmap. ---
        let dir = tempfile::tempdir().unwrap();
        let sealed = e.__seal_keyword_field_to_segment("c", "kw", dir.path()).unwrap();
        prop_assert_eq!(sealed as usize, docs.len(), "all docs sealed");
        assert_terms_dropped(&e); // RAM-bounded: terms index gone

        let b_term = run(&e, term("kw", "alpha"));
        let b_terms = run(&e, terms("kw", &["beta", "gamma"]));
        let b_or = run(&e, QueryNode::Or(vec![term("kw", "alpha"), term("kw", "delta")]));
        let b_and = run(&e, QueryNode::And(vec![term("kw", "beta"), term("cat", "red")]));
        let b_total = search_total(&e, term("kw", "gamma"));

        prop_assert_eq!(set_of(&a_term), set_of(&b_term), "Term set diverged");
        prop_assert_eq!(set_of(&a_terms), set_of(&b_terms), "Terms set diverged");
        prop_assert_eq!(set_of(&a_or), set_of(&b_or), "Or set diverged");
        prop_assert_eq!(set_of(&a_and), set_of(&b_and), "And set diverged");
        prop_assert_eq!(a_total, b_total, "standalone-term total diverged");
    }
}

/// RAM-BOUNDED + REOPEN-NO-REBUILD: seal the WHOLE collection to disk, reopen
/// from the segments alone (no CBOR snapshot), and assert the reopened
/// Keyword field's `terms` map is EMPTY while Term/Terms queries still return
/// correct results entirely from the mmap segment.
#[test]
fn reopen_drives_from_segment_with_empty_terms() {
    let dir = tempfile::tempdir().unwrap();
    // Build the collection and capture the wanted answers (segment OFF).
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_kw(&e, "a", Some("alpha"), Some("red"));
    index_kw(&e, "b", Some("beta"), Some("green"));
    index_kw(&e, "c2", Some("gamma"), None);
    index_kw(&e, "d", Some("alpha"), Some("red"));
    index_kw(&e, "e", None, Some("green")); // present-but-no-kw

    let want_alpha = set_of(&run(&e, term("kw", "alpha")));
    let want_beta_gamma = set_of(&run(&e, terms("kw", &["beta", "gamma"])));

    // PRODUCTION whole-collection seal (drops every field's RAM driver), then
    // reopen from the segments alone (no CBOR snapshot, no whole-collection
    // load) via the production `open_from_segments`.
    e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
    let schema = e.__collection_schema("c").unwrap();
    let e2 = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();

    // The reopened Keyword `terms` map must be EMPTY (no RAM rebuild) ...
    {
        let state = e2.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw must be a Keyword field");
        };
        assert!(k.segment.is_some(), "reopened segment attached");
        assert!(
            k.terms.is_empty(),
            "reopen must NOT rebuild `terms` in RAM (got {} entries)",
            k.terms.len()
        );
    }

    // ... yet the inverted queries still resolve, entirely from the mmap.
    assert_eq!(
        set_of(&run(&e2, term("kw", "alpha"))),
        want_alpha,
        "Term post-reopen"
    );
    assert_eq!(
        set_of(&run(&e2, terms("kw", &["beta", "gamma"]))),
        want_beta_gamma,
        "Terms post-reopen"
    );
}

/// LIVE-TAIL UNION: seal, then index more docs (tail into the live `terms`),
/// and assert a Term query returns base (segment) + tail (RAM) composed.
#[test]
fn live_tail_unions_with_segment_base() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_kw(&e, "base0", Some("alpha"), None); // id 0, sealed base
    index_kw(&e, "base1", Some("beta"), None); // id 1, sealed base

    let dir = tempfile::tempdir().unwrap();
    let n = e
        .__seal_keyword_field_to_segment("c", "kw", dir.path())
        .unwrap();
    assert_eq!(n, 2);
    assert_terms_dropped(&e); // base index now on disk

    // Tail docs after seal → land in the live `terms` tail (ids >= n_docs).
    index_kw(&e, "tail0", Some("alpha"), None); // id 2, tail, SAME term as base0
    index_kw(&e, "tail1", Some("gamma"), None); // id 3, tail, NEW term

    // term alpha: base id (base0) UNION tail id (tail0).
    let got = set_of(&run(&e, term("kw", "alpha")));
    let want: BTreeSet<String> = ["base0".into(), "tail0".into()].into_iter().collect();
    assert_eq!(got, want, "alpha must union segment base + live tail");

    // term gamma: ONLY the tail (not in the segment).
    let got = set_of(&run(&e, term("kw", "gamma")));
    let want: BTreeSet<String> = ["tail1".into()].into_iter().collect();
    assert_eq!(got, want, "gamma must come from the live tail alone");

    // term beta: ONLY the segment base.
    let got = set_of(&run(&e, term("kw", "beta")));
    let want: BTreeSet<String> = ["base1".into()].into_iter().collect();
    assert_eq!(got, want, "beta must come from the segment base alone");

    // df composition: term_df(alpha) = 1 (base) + 1 (tail) = 2.
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
        panic!("kw must be a Keyword field");
    };
    assert_eq!(
        k.term_df("alpha"),
        2,
        "df must sum segment base + live tail"
    );
    assert_eq!(k.term_df("beta"), 1, "df beta segment-only");
    assert_eq!(k.term_df("gamma"), 1, "df gamma tail-only");
    assert_eq!(k.term_df("missing"), 0, "df absent term");
}

#[test]
fn prefix_composes_segment_tail_and_delete_tombstones() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_kw(&e, "sealed-keep", Some("台北市/大安區"), None);
    index_kw(&e, "sealed-delete", Some("台北市/信義區"), None);
    index_kw(&e, "sealed-other", Some("新北市/板橋區"), None);

    let dir = tempfile::tempdir().unwrap();
    e.__seal_keyword_field_to_segment("c", "kw", dir.path())
        .unwrap();
    index_kw(&e, "tail-keep", Some("台北市/中正區"), None);
    index_kw(&e, "tail-other", Some("桃園市/桃園區"), None);
    e.delete("c", "sealed-delete", None).unwrap();

    let query = QueryNode::Prefix(PrefixQuery {
        field: "kw".into(),
        value: "台北市/".into(),
    });
    let got = set_of(&run(&e, query));
    let want: BTreeSet<String> = ["sealed-keep".into(), "tail-keep".into()]
        .into_iter()
        .collect();
    assert_eq!(got, want);
}

mod deletes;
