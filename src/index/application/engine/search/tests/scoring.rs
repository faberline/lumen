//! Ordering contract (Contract 1 of the coverage goal).
//!
//! These assert the *order* and relative magnitude of scores, not
//! just membership — so a mutated BM25 / score-combination operator
//! (e.g. `+`→`-`, `/`→`*`, `cmp(a,b)`→`cmp(b,a)`) makes at least one
//! of them fail. Membership-only tests above cannot catch those.

use std::collections::BTreeMap;

use crate::index::application::engine::search::tests::score_of;
use crate::index::application::engine::tests::item;
use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{MatchOp, MatchQuery, QueryNode};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};
use crate::shared_kernel::types::search::{SearchHit, SearchRequest};

fn text_only_schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert(
        "body".into(),
        FieldSpec {
            field_type: FieldType::Text,
            analyzer: Some(Analyzer::WhitespaceLower),
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    CreateCollectionRequest { fields }
}

fn search_match(e: &Engine, coll: &str, text: &str, op: MatchOp) -> Vec<SearchHit> {
    e.search(
        coll,
        SearchRequest {
            query: QueryNode::Match(MatchQuery {
                field: "body".into(),
                text: text.into(),
                op,
            }),
            limit: 50,
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
}

#[test]
fn bm25_higher_tf_scores_strictly_higher() {
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                // identical doc length, differing only in TF of "rust"
                item(
                    "hi",
                    "body",
                    FieldValue::String("rust rust rust pad pad".into()),
                ),
                item(
                    "lo",
                    "body",
                    FieldValue::String("rust pad pad pad pad".into()),
                ),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let hits = search_match(&e, "c", "rust", MatchOp::Or);
    // hi has TF=3, lo has TF=1 → hi must score strictly higher AND
    // sort first. Kills `tf` numerator / denominator operator flips.
    assert_eq!(hits[0].external_id, "hi");
    assert!(
        score_of(&hits, "hi") > score_of(&hits, "lo"),
        "higher TF must score higher: {hits:?}"
    );
}

#[test]
fn bm25_shorter_doc_scores_higher_for_same_tf() {
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                // same TF(rust)=1, but `short` is a shorter doc →
                // length normalization gives it the higher score.
                item("short", "body", FieldValue::String("rust pad".into())),
                item(
                    "long",
                    "body",
                    FieldValue::String("rust pad pad pad pad pad pad pad".into()),
                ),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let hits = search_match(&e, "c", "rust", MatchOp::Or);
    assert_eq!(hits[0].external_id, "short");
    assert!(
        score_of(&hits, "short") > score_of(&hits, "long"),
        "BM25 length-norm: shorter doc ranks higher at equal TF: {hits:?}"
    );
}

#[test]
fn bm25_rarer_term_contributes_more_idf() {
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    // "common" appears in every doc (low IDF); "rare" in just one
    // (high IDF). A doc matched by "rare" must outscore one matched
    // only by "common" — kills IDF numerator/denominator flips.
    let mut items = vec![
        item("rare_doc", "body", FieldValue::String("common rare".into())),
        item(
            "common_doc",
            "body",
            FieldValue::String("common filler".into()),
        ),
    ];
    for i in 0..8 {
        items.push(item(
            &format!("bg{i}"),
            "body",
            FieldValue::String("common filler".into()),
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
    let hits = search_match(&e, "c", "rare common", MatchOp::Or);
    assert_eq!(
        hits[0].external_id, "rare_doc",
        "doc matching the rare (high-IDF) term must rank first: {hits:?}"
    );
    assert!(score_of(&hits, "rare_doc") > score_of(&hits, "common_doc"));
}

#[test]
fn or_combines_scores_additively_doc_matching_both_ranks_first() {
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("both", "body", FieldValue::String("alpha beta".into())),
                item(
                    "alpha_only",
                    "body",
                    FieldValue::String("alpha gamma".into()),
                ),
                item("beta_only", "body", FieldValue::String("beta gamma".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let hits = search_match(&e, "c", "alpha beta", MatchOp::Or);
    // `both` matched on two tokens → its summed score must exceed
    // either single-token doc. Kills the OR `+=`→`-=`/`*=` mutants.
    assert_eq!(
        hits[0].external_id, "both",
        "doc matching both tokens ranks first: {hits:?}"
    );
    let s_both = score_of(&hits, "both");
    assert!(s_both > score_of(&hits, "alpha_only"));
    assert!(s_both > score_of(&hits, "beta_only"));
}

#[test]
fn and_sums_matched_token_scores() {
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("d1", "body", FieldValue::String("alpha beta".into())),
                item(
                    "d2",
                    "body",
                    FieldValue::String("alpha beta gamma delta eps".into()),
                ),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let and_hits = search_match(&e, "c", "alpha beta", MatchOp::And);
    // Both docs contain both tokens → both returned, and the AND
    // score equals the sum of the two per-token contributions. The
    // shorter doc (d1) wins on length-norm. Kills AND `score + s`
    // → `score - s` (which would invert or zero the combination).
    assert_eq!(and_hits.len(), 2);
    assert_eq!(and_hits[0].external_id, "d1");
    assert!(score_of(&and_hits, "d1") > 0.0);
    assert!(score_of(&and_hits, "d1") >= score_of(&and_hits, "d2"));
}

#[test]
fn search_sorts_by_score_desc_then_eid_asc() {
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    // Two docs with identical content → identical score → tie
    // broken by external_id ascending. Kills the tie-break
    // `a.cmp(b)`→`b.cmp(a)` mutant and the score-cmp flip.
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("zeta", "body", FieldValue::String("rust rust".into())),
                item("alpha", "body", FieldValue::String("rust rust".into())),
                item("solo", "body", FieldValue::String("rust".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let hits = search_match(&e, "c", "rust", MatchOp::Or);
    // solo has lower TF → lowest score → must be last.
    assert_eq!(hits.last().unwrap().external_id, "solo");
    // zeta & alpha tie on score → alpha first (eid asc).
    let alpha_pos = hits.iter().position(|h| h.external_id == "alpha").unwrap();
    let zeta_pos = hits.iter().position(|h| h.external_id == "zeta").unwrap();
    assert!(alpha_pos < zeta_pos, "tie broken by eid asc: {hits:?}");
}

#[test]
fn bm25_exact_golden_scores() {
    // Pins the formula to textbook BM25 (K1=1.2, B=0.75). Ordering
    // assertions can't catch magnitude-only mutations that preserve
    // relative order (e.g. `(n-df+0.5)/(df+0.5)` → `*`); a
    // hand-computed reference does.
    //
    // Corpus (field "body", whitespace_lower):
    //   a = "x y"      → tf(x)=1, doc_len=2
    //   b = "x x z w"  → tf(x)=2, doc_len=4
    // n=2, total_doc_len=6, avgdl=3, df(x)=2
    // idf = ln((2-2+0.5)/(2+0.5) + 1) = ln(1.2)             = 0.1823215
    // a: denom = 1 + 1.2*(1-0.75 + 0.75*2/3) = 1.9
    //    score = 0.1823215 * 1 * 2.2 / 1.9                  = 0.211110
    // b: denom = 2 + 1.2*(1-0.75 + 0.75*4/3) = 3.5
    //    score = 0.1823215 * 2 * 2.2 / 3.5                  = 0.229204
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![
                item("a", "body", FieldValue::String("x y".into())),
                item("b", "body", FieldValue::String("x x z w".into())),
            ],
            request_id: None,
        },
    )
    .unwrap();
    let hits = search_match(&e, "c", "x", MatchOp::Or);
    let sa = score_of(&hits, "a");
    let sb = score_of(&hits, "b");
    assert!(
        (sa - 0.211110).abs() < 5e-4,
        "BM25(a) golden mismatch: got {sa}, want ≈0.211110"
    );
    assert!(
        (sb - 0.229204).abs() < 5e-4,
        "BM25(b) golden mismatch: got {sb}, want ≈0.229204"
    );
}

#[test]
fn and_match_score_is_sum_not_product_of_token_scores() {
    // Single doc "p q" (the only doc). Each token scores identically:
    //   n=1, df=1, idf=ln((0.5)/(1.5)+1)=ln(1.3333)=0.287682
    //   denom = 1 + 1.2*(1-0.75 + 0.75*2/2) = 2.2
    //   per-token = 0.287682 * 1 * 2.2 / 2.2 = 0.287682
    // AND(p,q) sums the two ⇒ 0.575364.
    // The `score + s` → `score * s` mutant would give
    // 0.287682² = 0.082761, so an exact assert kills it.
    let e = Engine::new();
    e.create_collection("c", text_only_schema()).unwrap();
    e.index(
        "c",
        IndexRequest {
            items: vec![item("d", "body", FieldValue::String("p q".into()))],
            request_id: None,
        },
    )
    .unwrap();
    let hits = search_match(&e, "c", "p q", MatchOp::And);
    assert_eq!(hits.len(), 1);
    let s = score_of(&hits, "d");
    assert!(
        (s - 0.575364).abs() < 5e-4,
        "AND match must SUM token scores (≈0.575364), got {s} (product would be ≈0.0828)"
    );
}
