use std::collections::BTreeSet;
use std::sync::Arc;

use crate::index::application::engine::seal::tests::text::{
    bm25_single, body_from, filtered, index_doc, run, schema, scores_of, set_of, text_and,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::query::QueryNode;

/// DELETE-AFTER-SEAL BM25 — THE CRUX (Phase 2h-4): seal a Text corpus, DELETE
/// base docs (NO re-seal), and assert the segment-ON BM25 equals an in-RAM
/// oracle that indexed the SAME docs and physically deleted the SAME ones —
/// byte-identical f32 scores AND result-sets for single/multi-token + the
/// corpus-sensitive filtered case. Deleting a doc shifts doc_count/avgdl/df, so
/// EVERY surviving score changes; the tombstone subtraction + live corpus must
/// reproduce that shift exactly. `tombstones.len()` matches the deletes.
#[test]
fn delete_after_seal_bm25_matches_oracle() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        // A corpus where the deleted docs carry the query terms, so removing
        // them moves df AND the corpus length factor.
        index_doc(
            e,
            "d0",
            &body_from(&[("alpha", 3), ("beta", 1)]),
            Some(20.0),
        );
        index_doc(
            e,
            "d1",
            &body_from(&[("alpha", 1), ("gamma", 4)]),
            Some(35.0),
        );
        index_doc(
            e,
            "d2",
            &body_from(&[("alpha", 2), ("beta", 2)]),
            Some(50.0),
        );
        index_doc(
            e,
            "d3",
            &body_from(&[("beta", 3), ("gamma", 1)]),
            Some(65.0),
        );
        index_doc(
            e,
            "d4",
            &body_from(&[("alpha", 1), ("beta", 1)]),
            Some(80.0),
        );
        index_doc(e, "d5", &body_from(&[("gamma", 5)]), Some(95.0));
    }
    // Delete docs that DO carry alpha/beta so df + avgdl both shift.
    let to_delete = ["d1", "d2", "d4"];

    // ORACLE: in-RAM, physically delete, NEVER sealed.
    let oracle = Arc::new(Engine::new());
    build(&oracle);
    for d in to_delete {
        oracle.delete("c", d, None).unwrap();
    }

    // SUBJECT: seal body + price, THEN delete (no re-seal).
    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir.path(), 1)
        .unwrap();
    for d in to_delete {
        subject.delete("c", d, None).unwrap();
    }

    // The deletes are tombstoned, NOT removed from the on-disk postings; the
    // LIVE corpus scalars were decremented (so avgdl shifts).
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body");
        };
        assert!(idx.tokens.is_empty(), "sealed: tokens still dropped");
        assert_eq!(idx.tombstones.len(), 3, "three base deletes tombstoned");
        // 6 docs - 3 deleted = 3 live; total_doc_len = d0(4)+d3(4)+d5(5)=13.
        assert_eq!(
            idx.bm25_corpus(),
            (3, 4 + 4 + 5),
            "live corpus reflects deletes (drives avgdl)"
        );
        // The oracle and subject must agree on the live corpus EXACTLY.
        let ostate = oracle.state.read().unwrap();
        let ocoll = ostate.collections.get("c").unwrap();
        let FieldIndex::Text { idx: oidx, .. } = ocoll.fields.get("body").unwrap() else {
            panic!("oracle body");
        };
        assert_eq!(
            idx.bm25_corpus(),
            (oidx.doc_count, oidx.total_doc_len),
            "corpus must match oracle"
        );
    }

    // BYTE-IDENTICAL f32 scores AND sets across the corpus-sensitive shapes.
    // (Deleting d1/d2/d4 changes N, avgdl, and df(alpha)/df(beta) — every
    // surviving doc's score shifts, and it must match the oracle bit-for-bit.)
    let s_single = run(&subject, bm25_single("alpha"));
    let o_single = run(&oracle, bm25_single("alpha"));
    let s_beta = run(&subject, bm25_single("beta"));
    let o_beta = run(&oracle, bm25_single("beta"));
    let s_and = run(&subject, text_and("alpha", "beta"));
    let o_and = run(&oracle, text_and("alpha", "beta"));
    let s_filt = run(&subject, filtered("alpha", 10.0, 90.0));
    let o_filt = run(&oracle, filtered("alpha", 10.0, 90.0));

    assert_eq!(
        set_of(&s_single),
        set_of(&o_single),
        "alpha set leaked a deleted doc"
    );
    assert_eq!(
        scores_of(&s_single),
        scores_of(&o_single),
        "alpha scores diverged (corpus shift not reproduced)"
    );
    assert_eq!(
        set_of(&s_beta),
        set_of(&o_beta),
        "beta set leaked a deleted doc"
    );
    assert_eq!(
        scores_of(&s_beta),
        scores_of(&o_beta),
        "beta scores diverged"
    );
    assert_eq!(
        set_of(&s_and),
        set_of(&o_and),
        "AND set leaked a deleted doc"
    );
    assert_eq!(scores_of(&s_and), scores_of(&o_and), "AND scores diverged");
    assert_eq!(
        set_of(&s_filt),
        set_of(&o_filt),
        "filtered set leaked a deleted doc"
    );
    assert_eq!(
        scores_of(&s_filt),
        scores_of(&o_filt),
        "filtered scores diverged"
    );

    // Concrete spot-check: alpha now only d0 survives (d1, d2, d4 deleted).
    assert_eq!(
        set_of(&s_single),
        ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "alpha must be only the surviving d0"
    );
    // beta: d0 and d3 survive (d2, d4 deleted).
    assert_eq!(
        set_of(&s_beta),
        ["d0".to_string(), "d3".to_string()]
            .into_iter()
            .collect::<BTreeSet<_>>(),
        "beta must be d0 + d3"
    );
}

/// AND-STREAMING PERF FIX regression (the 500k-hot-doc `match … op: "and"`
/// fix): seal `body` to a segment, add MORE docs afterward (a live-tail
/// overlay on top of the sealed base — one of the four sources
/// `TokProbe`/`eval_match_topk`'s streaming intersection must compose),
/// THEN delete some of the sealed docs (tombstones — the segment, live
/// tail, AND tombstones are all simultaneously non-empty, matching the
/// perf brief's corpus shape). The AND result set and f32 scores through
/// BOTH `eval_match_topk` (`run`, the search hot path) and the nested
/// `eval_match` map path (`QueryNode::And([Match])`, which is NOT the
/// `eval_match_topk` fast path) must equal an in-RAM oracle that indexed
/// only the surviving docs and never sealed.
#[test]
fn and_query_matches_oracle_over_segment_plus_live_tail_plus_tombstones() {
    let subject = Arc::new(Engine::new());
    subject.create_collection("c", schema()).unwrap();
    // Sealed base: d0..d3.
    index_doc(
        &subject,
        "d0",
        &body_from(&[("alpha", 3), ("beta", 1)]),
        Some(10.0),
    );
    index_doc(
        &subject,
        "d1",
        &body_from(&[("alpha", 1), ("beta", 2)]),
        Some(20.0),
    );
    index_doc(&subject, "d2", &body_from(&[("alpha", 2)]), Some(30.0));
    index_doc(&subject, "d3", &body_from(&[("beta", 3)]), Some(40.0));
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_text_field_to_segment("c", "body", dir.path())
        .unwrap();
    // Live tail, added AFTER the seal (never sealed — pure live overlay).
    index_doc(
        &subject,
        "d4",
        &body_from(&[("alpha", 4), ("beta", 1)]),
        Some(50.0),
    );
    index_doc(
        &subject,
        "d5",
        &body_from(&[("alpha", 1), ("beta", 4)]),
        Some(60.0),
    );
    // Tombstone a sealed base doc that carries BOTH query tokens.
    subject.delete("c", "d1", None).unwrap();

    let oracle = Arc::new(Engine::new());
    oracle.create_collection("c", schema()).unwrap();
    index_doc(
        &oracle,
        "d0",
        &body_from(&[("alpha", 3), ("beta", 1)]),
        Some(10.0),
    );
    index_doc(&oracle, "d2", &body_from(&[("alpha", 2)]), Some(30.0));
    index_doc(&oracle, "d3", &body_from(&[("beta", 3)]), Some(40.0));
    index_doc(
        &oracle,
        "d4",
        &body_from(&[("alpha", 4), ("beta", 1)]),
        Some(50.0),
    );
    index_doc(
        &oracle,
        "d5",
        &body_from(&[("alpha", 1), ("beta", 4)]),
        Some(60.0),
    );

    // The `eval_match_topk` hot path (through `search`, top-level Match).
    let s_topk = run(&subject, text_and("alpha", "beta"));
    let o_topk = run(&oracle, text_and("alpha", "beta"));
    assert_eq!(set_of(&s_topk), set_of(&o_topk), "topk AND set diverged");
    assert_eq!(
        scores_of(&s_topk),
        scores_of(&o_topk),
        "topk AND scores diverged"
    );
    // d1 is gone (tombstoned); d0/d4/d5 carry both tokens, d2/d3 carry only one.
    assert_eq!(
        set_of(&s_topk),
        ["d0", "d4", "d5"]
            .into_iter()
            .map(String::from)
            .collect::<BTreeSet<_>>(),
        "AND must keep exactly the docs carrying both surviving tokens"
    );

    // The `eval_match` map path: nest the Match under a single-child And so
    // the top-level query is NOT `QueryNode::Match` and the search router's
    // `eval_match_topk` fast path is bypassed, reaching `eval_query`'s
    // `QueryNode::Match(m) => eval_match(coll, m)?` instead.
    let wrapped = QueryNode::And(vec![text_and("alpha", "beta")]);
    let s_map = run(&subject, wrapped.clone());
    let o_map = run(&oracle, wrapped);
    assert_eq!(set_of(&s_map), set_of(&o_map), "map-path AND set diverged");
    assert_eq!(
        scores_of(&s_map),
        scores_of(&o_map),
        "map-path AND scores diverged"
    );
}
