//! Engine-level checkpoint (Stage 2 Phase 2f-2): the disk engine as the running
//! binary's persistence. Two contracts:
//!   (a) ENGINE REOPEN — a multi-collection, all-field-type engine, flushed to a
//!       checkpoint dir and reopened into a FRESH engine, answers every query leg
//!       identically (the disk engine IS a faithful persistence).
//!   (b) IDEMPOTENT DOUBLE-FLUSH — flush, index MORE docs, flush again, reopen
//!       yields ALL docs (base + tail) identical to a pure-live engine. This is
//!       the re-seal-after-drop proof: the second flush reads base docs from the
//!       prior segment (their live forward was dropped), not the empty forward map.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};
use crate::shared_kernel::types::query::{
    KnnQuery, MatchOp, MatchQuery, RangeQuery, TermQuery, TermsQuery,
};
use crate::shared_kernel::types::query::{QueryNode, RangeBound};
use crate::shared_kernel::types::schema::{
    Analyzer, CreateCollectionRequest, FieldSpec, FieldType,
};
use crate::shared_kernel::types::schema::{VectorBackend, VectorMetric};
use crate::shared_kernel::types::search::SearchRequest;

const DIM: usize = 4;

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

/// Multi-field corpus matching the triple-path schema: num (Number), kw
/// (Keyword), tags (Set), body (Text), sig (Hash), emb (Vector).
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

fn index_doc(
    e: &Engine,
    coll: &str,
    eid: &str,
    n: f64,
    kw: &str,
    tag: &str,
    tok: bool,
    sig: u64,
    emb: &[f32],
) {
    let items = vec![
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "num".into(),
            value: FieldValue::Number(n),
            version: None,
        },
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "kw".into(),
            value: FieldValue::String(kw.into()),
            version: None,
        },
        crate::shared_kernel::types::document::IndexItem {
            external_id: eid.into(),
            field: "tags".into(),
            value: FieldValue::StringList(vec![tag.into()]),
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
    e.index(
        coll,
        IndexRequest {
            items,
            request_id: None,
        },
    )
    .unwrap();
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

fn run(e: &Engine, coll: &str, query: QueryNode, limit: u32) -> Vec<(String, u32)> {
    e.search(coll, req(query, limit))
        .unwrap()
        .hits
        .into_iter()
        .map(|h| (h.external_id, h.score.to_bits()))
        .collect()
}

fn set_of(rows: &[(String, u32)]) -> BTreeSet<String> {
    rows.iter().map(|(e, _)| e.clone()).collect()
}
fn scores_of(rows: &[(String, u32)]) -> BTreeMap<String, u32> {
    rows.iter().map(|(e, s)| (e.clone(), *s)).collect()
}

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

/// The full query battery for one collection (predicate legs go through the
/// segment-aware per-doc accessors; kNN/hamming/bm25 through the segment scan).
fn battery(e: &Engine, coll: &str) -> Vec<(BTreeSet<String>, BTreeMap<String, u32>)> {
    let legs = vec![
        driven(QueryNode::Range(RangeQuery {
            field: "num".into(),
            gt: None,
            gte: Some(RangeBound::Number(2.0)),
            lt: Some(RangeBound::Number(8.0)),
            lte: None,
        })),
        driven(QueryNode::Term(TermQuery {
            field: "kw".into(),
            value: FieldValue::String("a".into()),
        })),
        driven(QueryNode::Terms(TermsQuery {
            field: "tags".into(),
            values: vec![FieldValue::String("red".into())],
        })),
        QueryNode::Term(TermQuery {
            field: "kw".into(),
            value: FieldValue::String("b".into()),
        }),
        QueryNode::Match(MatchQuery {
            field: "body".into(),
            text: "tok".into(),
            op: MatchOp::And,
        }),
        QueryNode::Hamming(crate::shared_kernel::types::query::HammingQuery {
            field: "sig".into(),
            hash: format!("{:016x}", 0u64),
            max_distance: 8,
        }),
    ];
    legs.into_iter()
        .map(|q| {
            let r = run(e, coll, q, 100_000);
            (set_of(&r), scores_of(&r))
        })
        .collect()
}

fn knn(e: &Engine, coll: &str, q: &[f32]) -> Vec<(String, u32)> {
    run(
        e,
        coll,
        QueryNode::Knn(KnnQuery {
            field: "emb".into(),
            vector: q.to_vec(),
            k: 8,
        }),
        8,
    )
}

// Some fixed multi-collection corpus. Two collections, all field types.
fn seed(e: &Engine) {
    e.create_collection("alpha", schema()).unwrap();
    e.create_collection("beta", schema()).unwrap();
    let docs = [
        ("d0", 1.0, "a", "red", true, 0u64, [0.1f32, 0.2, 0.3, 0.4]),
        ("d1", 3.0, "b", "blue", true, 3, [0.9, 0.8, 0.7, 0.6]),
        ("d2", 5.0, "a", "red", false, 7, [0.5, 0.5, 0.5, 0.5]),
        ("d3", 7.0, "c", "green", true, 1, [0.2, 0.4, 0.6, 0.8]),
    ];
    for (eid, n, kw, tag, tok, sig, emb) in docs {
        index_doc(e, "alpha", eid, n, kw, tag, tok, sig, &emb);
        index_doc(
            e,
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        );
    }
}

// ----- (a) ENGINE REOPEN -------------------------------------------------
#[test]
fn flush_then_reopen_into_fresh_engine_is_identical() {
    let live = Arc::new(Engine::new());
    seed(&live);

    let qa = [0.15f32, 0.25, 0.35, 0.45];
    let live_battery_alpha = battery(&live, "alpha");
    let live_battery_beta = battery(&live, "beta");
    let live_knn_alpha = knn(&live, "alpha", &qa);

    let dir = tempfile::tempdir().unwrap();
    live.flush_to_segments(dir.path(), 11).unwrap();

    // Fresh engine reopened ONLY from the checkpoint dir (no CBOR, no log).
    let reopened = Arc::new(Engine::new());
    let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
    assert_eq!(
        seq, 11,
        "applied_seq must round-trip through the checkpoint"
    );

    assert_eq!(
        reopened.list_collections().unwrap().len(),
        2,
        "both collections reopened"
    );
    assert_eq!(
        battery(&reopened, "alpha"),
        live_battery_alpha,
        "alpha legs diverged after reopen"
    );
    assert_eq!(
        battery(&reopened, "beta"),
        live_battery_beta,
        "beta legs diverged after reopen"
    );
    assert_eq!(
        knn(&reopened, "alpha", &qa),
        live_knn_alpha,
        "alpha kNN diverged after reopen"
    );
}

// ----- (b) IDEMPOTENT DOUBLE-FLUSH (re-seal-after-drop proof) -------------
#[test]
fn double_flush_with_tail_matches_pure_live() {
    // The persisted engine: seed, FLUSH (drops forward for base docs), index
    // MORE docs (the live tail), FLUSH AGAIN, reopen.
    let persisted = Arc::new(Engine::new());
    seed(&persisted);
    let dir = tempfile::tempdir().unwrap();
    persisted.flush_to_segments(dir.path(), 4).unwrap(); // first checkpoint

    // After the first flush the base docs' forward maps are dropped. Add a
    // tail of new docs whose docids are > the sealed n_docs.
    let tail = [
        (
            "d4",
            2.5,
            "b",
            "red",
            true,
            0u64,
            [0.11f32, 0.22, 0.33, 0.44],
        ),
        ("d5", 6.5, "a", "blue", true, 7, [0.6, 0.6, 0.6, 0.6]),
    ];
    for (eid, n, kw, tag, tok, sig, emb) in tail {
        index_doc(&persisted, "alpha", eid, n, kw, tag, tok, sig, &emb);
        index_doc(
            &persisted,
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        );
    }
    // SECOND flush: this RE-SEALS. Base docs must be gathered from the prior
    // segment (their forward is empty), the tail from the live forward. If the
    // gather read raw `forward`, the base docs would seal as ABSENT here.
    persisted.flush_to_segments(dir.path(), 6).unwrap();

    let reopened = Arc::new(Engine::new());
    let seq = reopened.reopen_from_segment_dir(dir.path()).unwrap();
    assert_eq!(seq, 6, "second checkpoint's seq must win");

    // The oracle: a pure-live engine that NEVER flushed, with the SAME docs.
    let pure = Arc::new(Engine::new());
    seed(&pure);
    for (eid, n, kw, tag, tok, sig, emb) in tail {
        index_doc(&pure, "alpha", eid, n, kw, tag, tok, sig, &emb);
        index_doc(
            &pure,
            "beta",
            &format!("b{eid}"),
            n + 1.0,
            kw,
            tag,
            tok,
            sig + 1,
            &emb,
        );
    }

    let qa = [0.15f32, 0.25, 0.35, 0.45];
    // EVERY leg of BOTH collections must match the pure-live oracle — base AND
    // tail docs (sets AND byte-identical scores). This is the re-seal-after-drop
    // correctness proof: the second flush gathered base docs from the prior
    // segment (their forward was dropped) and the tail from the live state.
    assert_eq!(
        battery(&reopened, "alpha"),
        battery(&pure, "alpha"),
        "alpha legs diverged after double-flush"
    );
    assert_eq!(
        battery(&reopened, "beta"),
        battery(&pure, "beta"),
        "beta legs diverged after double-flush"
    );
    assert_eq!(
        knn(&reopened, "alpha", &qa),
        knn(&pure, "alpha", &qa),
        "alpha kNN diverged after double-flush"
    );
    assert_eq!(
        knn(&reopened, "beta", &qa),
        knn(&pure, "beta", &qa),
        "beta kNN diverged after double-flush"
    );

    // And direct doc-count parity (base 4 + tail 2 = 6 per collection).
    assert_eq!(reopened.stats("alpha").unwrap().documents_indexed, 6);
    assert_eq!(reopened.stats("beta").unwrap().documents_indexed, 6);
}

mod mutations;
