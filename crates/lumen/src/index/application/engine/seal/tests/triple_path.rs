//! Triple-path diff test (Stage 2 Phase 2f-1): the RAM=hot / disk=all keystone.
//!
//! PATH A = a pure live engine (segments OFF).
//! PATH B = the SAME engine after `seal_to_segments` + forward-payload drop.
//! PATH C = a fresh engine whose collection is `open_from_segments`'d from B's
//!          directory (NO CBOR snapshot, NO whole-collection load).
//!
//! Over a randomized multi-field corpus (Number + Keyword + Set + Text + Hash +
//! Vector) the three paths must return IDENTICAL result-SETS, byte-identical f32
//! scores (to_bits), identical retrieved field values, and identical kNN
//! ordering — for point lookups, range, term, set-membership, BM25, kNN, AND
//! direct value retrieval. A missed forward-read site or a wrong inverted-driver
//! rebuild MUST fail this. The test also asserts the forward payload provably
//! left RAM after the seal-and-drop.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use proptest::prelude::*;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{
    MatchOp, MatchQuery, QueryNode, RangeBound, RangeQuery, TermQuery, TermsQuery,
};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};
use crate::shared_kernel::types::schema::{VectorBackend, VectorMetric};
use crate::shared_kernel::types::search::SearchRequest;

const DIM: usize = 6;

/// One full query-battery snapshot of an engine: the seven query legs plus
/// the direct value-retrieval leg. Fields (in order): range, term, setmem,
/// point, bm25, knn (ordered), hamming, retrieval.
type Snapshot = (
    Vec<(String, u32)>,
    Vec<(String, u32)>,
    Vec<(String, u32)>,
    Vec<(String, u32)>,
    Vec<(String, u32)>,
    Vec<(String, u32)>,
    Vec<(String, u32)>,
    Vec<(
        String,
        Option<u64>,
        Option<String>,
        Option<Vec<String>>,
        Option<u64>,
    )>,
);

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

fn vec_fieldspec() -> FieldSpec {
    FieldSpec {
        field_type: FieldType::Vector,
        analyzer: None,
        multi: None,
        dim: Some(DIM as u32),
        metric: Some(VectorMetric::L2),
        backend: Some(VectorBackend::FlatCpu),
        quantize: None,
    }
}

/// Multi-field corpus: num (Number), kw (Keyword), tags (Set), body (Text),
/// sig (Hash), emb (Vector).
fn schema() -> CreateCollectionRequest {
    let mut fields = BTreeMap::new();
    fields.insert("num".into(), fieldspec(FieldType::Number, None));
    fields.insert("kw".into(), fieldspec(FieldType::Keyword, None));
    fields.insert("tags".into(), fieldspec(FieldType::Set, None));
    fields.insert(
        "body".into(),
        fieldspec(FieldType::Text, Some(Analyzer::WhitespaceLower)),
    );
    fields.insert("sig".into(), fieldspec(FieldType::Hash, None));
    fields.insert("emb".into(), vec_fieldspec());
    CreateCollectionRequest { fields }
}

fn req(query: QueryNode, limit: u32) -> SearchRequest {
    SearchRequest {
        query,
        limit,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

fn run(e: &Engine, query: QueryNode, limit: u32) -> Vec<(String, u32)> {
    e.search("c", req(query, limit))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
}

/// Result SET (eids only) — order-independent assertion for filter queries.
fn set_of(rows: &[(String, u32)]) -> BTreeSet<String> {
    rows.iter().map(|(e, _)| e.clone()).collect()
}

/// Scores keyed by eid (f32 bits) — order-independent score byte-equality.
fn scores_of(rows: &[(String, u32)]) -> BTreeMap<String, u32> {
    rows.iter().map(|(e, s)| (e.clone(), *s)).collect()
}

/// Index one doc across all six fields. `tok` selects the rare driver token
/// so BM25 has a non-trivial corpus.
#[allow(clippy::too_many_arguments)]
fn index_doc(
    e: &Engine,
    eid: &str,
    num: Option<f64>,
    kw: &str,
    tags: &[&str],
    tok: bool,
    sig: u64,
    emb: &[f32],
) {
    let mut items = vec![
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "kw".into(),
            value: FieldValue::String(kw.into()),
            version: None,
        },
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "tags".into(),
            value: FieldValue::StringList(tags.iter().map(|s| s.to_string()).collect()),
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
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "sig".into(),
            value: FieldValue::String(format!("{sig:016x}")),
            version: None,
        },
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "emb".into(),
            value: FieldValue::Vector(emb.to_vec()),
            version: None,
        },
    ];
    if let Some(n) = num {
        items.push(crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "num".into(),
            value: FieldValue::Number(n),
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

/// A match-DRIVEN AND so the non-text conjunct is applied as a per-doc
/// PREDICATE (`number_at`/`keyword_at`/`set_contains` — the segment-backed
/// read sites), not a posting-walk.
fn driven(extra: QueryNode) -> QueryNode {
    QueryNode::And(vec![
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        extra,
    ])
}

/// Direct field-value RETRIEVAL through the segment-aware accessors, for
/// every docid — the "value retrieval" leg of the contract. Resolves each
/// docid to (eid, num, kw, tags, sig). Routes through `number_at` /
/// `keyword_at` / `set_members` / `hash_at`, which after a seal-and-drop must
/// read the segment, not the (empty) forward map.
fn retrieve_all(
    e: &Engine,
) -> Vec<(
    String,
    Option<u64>,
    Option<String>,
    Option<Vec<String>>,
    Option<u64>,
)> {
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let n = coll.interner.to_eid.len() as u32;
    let mut out = Vec::with_capacity(n as usize);
    for id in 0..n {
        let eid = coll.interner.resolve(id).to_string();
        let num = match coll.fields.get("num") {
            Some(FieldIndex::Number(nx)) => nx.number_at(id).map(|s| s.to_f64().to_bits()),
            _ => None,
        };
        let kw = match coll.fields.get("kw") {
            Some(FieldIndex::Keyword(k)) => k.keyword_at(id),
            _ => None,
        };
        let tags = match coll.fields.get("tags") {
            Some(FieldIndex::Set(s)) => s.set_members(id).map(|m| m.into_iter().collect()),
            _ => None,
        };
        let sig = match coll.fields.get("sig") {
            Some(FieldIndex::Hash(h)) => h.hash_at(id),
            _ => None,
        };
        out.push((eid, num, kw, tags, sig));
    }
    // Order by eid so the comparison is interner-order-independent (PATH C
    // re-interns in docid order, which equals PATH A/B docid order, but
    // sorting makes the contract explicit).
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(80))]

    #[test]
    fn triple_path_a_eq_b_eq_c(
        docs in proptest::collection::vec(
            (
                proptest::option::weighted(0.8, 0u32..30),          // num (some absent)
                prop::sample::select(vec!["a", "b", "c", "d"]),     // kw
                proptest::collection::vec(
                    prop::sample::select(vec!["red", "green", "blue", "x"]),
                    0..3,
                ),                                                  // tags
                any::<bool>(),                                      // tok
                0u64..64,                                           // sig (low bits)
                proptest::collection::vec(-3.0f32..3.0, DIM..=DIM), // emb
            ),
            1..40,
        ),
        lo in 0u32..30,
        span in 1u32..30,
        qsig in 0u64..64,
        qraw in proptest::collection::vec(-3.0f32..3.0, DIM..=DIM),
        kw_pick in prop::sample::select(vec!["a", "b", "c", "d"]),
        tag_pick in prop::sample::select(vec!["red", "green", "blue", "x"]),
    ) {
        let hi = lo + span;

        // --- PATH A: live engine, segments OFF. ---
        let e = Arc::new(Engine::new());
        e.create_collection("c", schema()).unwrap();
        for (i, (num, kw, tags, tok, sig, emb)) in docs.iter().enumerate() {
            let tagrefs: Vec<&str> = tags.iter().copied().collect();
            index_doc(
                &e,
                &format!("d{i}"),
                num.map(|n| n as f64),
                kw,
                &tagrefs,
                *tok,
                *sig,
                emb,
            );
        }

        // The query battery (built once, reused across paths).
        let q_range = || driven(QueryNode::Range(RangeQuery {
            field: "num".into(), gt: None, gte: Some(RangeBound::Number(lo as f64)), lt: Some(RangeBound::Number(hi as f64)), lte: None,
        }));
        let q_term = || driven(QueryNode::Term(TermQuery {
            field: "kw".into(), value: FieldValue::String(kw_pick.to_string()),
        }));
        let q_setmem = || driven(QueryNode::Terms(TermsQuery {
            field: "tags".into(),
            values: vec![FieldValue::String(tag_pick.to_string())],
        }));
        let q_point = || QueryNode::Term(TermQuery {
            field: "kw".into(), value: FieldValue::String(kw_pick.to_string()),
        });
        let q_bm25 = || QueryNode::Match(MatchQuery {
            field: "body".into(), text: "tok".into(), op: MatchOp::And,
        });
        let q_knn = || QueryNode::Knn(crate::shared_kernel::types::query::KnnQuery {
            field: "emb".into(), vector: qraw.clone(), k: 8,
        });
        let q_ham = || QueryNode::Hamming(crate::shared_kernel::types::query::HammingQuery {
            field: "sig".into(), hash: format!("{qsig:016x}"), max_distance: 6,
        });

        let snapshot = |e: &Engine| -> Snapshot {
            (
                run(e, q_range(), 100_000),
                run(e, q_term(), 100_000),
                run(e, q_setmem(), 100_000),
                run(e, q_point(), 100_000),
                run(e, q_bm25(), 100_000),
                run(e, q_knn(), 8),
                run(e, q_ham(), 100_000),
                retrieve_all(e),
            )
        };

        let a = snapshot(&e);

        // --- PATH B: production seal_to_segments + drop, rerun. ---
        let dir = tempfile::tempdir().unwrap();
        e.__seal_collection_to_segments("c", dir.path(), 1).unwrap();
        let b = snapshot(&e);

        // The forward payload provably LEFT RAM (and the inverted driver did
        // NOT): every dropped field's forward map is empty / tokens dropped,
        // yet a segment is attached and queries still answer.
        for f in ["num", "kw", "tags", "sig"] {
            let (fwd, _toks, has_seg) = e.__field_forward_probe("c", f).unwrap();
            prop_assert_eq!(fwd, 0, "field `{}` forward map not freed after drop", f);
            prop_assert!(has_seg, "field `{}` has no segment after seal", f);
        }
        let (_f, toks, has_seg) = e.__field_forward_probe("c", "body").unwrap();
        prop_assert_eq!(toks, 0, "text tokens not freed after drop");
        prop_assert!(has_seg, "text field has no segment after seal");

        // --- PATH C: reopen from segments (no snapshot), rerun. ---
        let schema = e.__collection_schema("c").unwrap();
        let ce = Engine::__open_collection_from_segments("c", dir.path(), schema, 1).unwrap();
        let c = snapshot(&ce);

        // A == B and B == C, leg by leg. Filter legs compare result SET +
        // byte-identical scores; kNN compares the full ordered ranked vec;
        // retrieval compares the resolved field values. Filter tuple fields:
        // 0 range, 1 term, 2 setmem, 3 point, 4 bm25, 6 ham (5 knn, 7 retrieval
        // are compared separately because their contract is ordered / value).
        let filt = |x: &Snapshot| {
            vec![
                (set_of(&x.0), scores_of(&x.0)),
                (set_of(&x.1), scores_of(&x.1)),
                (set_of(&x.2), scores_of(&x.2)),
                (set_of(&x.3), scores_of(&x.3)),
                (set_of(&x.4), scores_of(&x.4)),
                (set_of(&x.6), scores_of(&x.6)),
            ]
        };
        let names = ["range", "term", "setmem", "point", "bm25", "hamming"];
        let (fa, fb, fc) = (filt(&a), filt(&b), filt(&c));
        for (i, name) in names.iter().enumerate() {
            prop_assert_eq!(&fa[i].0, &fb[i].0, "A!=B {} set", name);
            prop_assert_eq!(&fa[i].1, &fb[i].1, "A!=B {} scores", name);
            prop_assert_eq!(&fb[i].0, &fc[i].0, "B!=C {} set", name);
            prop_assert_eq!(&fb[i].1, &fc[i].1, "B!=C {} scores", name);
        }
        // kNN is RANKED: the whole ordered vec must match bit-for-bit.
        prop_assert_eq!(&a.5, &b.5, "A!=B knn ordering/score");
        prop_assert_eq!(&b.5, &c.5, "B!=C knn ordering/score");
        // Value retrieval through the segment-aware accessors.
        prop_assert_eq!(&a.7, &b.7, "A!=B value retrieval");
        prop_assert_eq!(&b.7, &c.7, "B!=C value retrieval");
    }
}
