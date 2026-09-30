use std::collections::BTreeSet;
use std::sync::Arc;

use crate::index::application::engine::seal::tests::text::{
    bm25_single, body_from, filtered, index_doc, run, schema, scores_of, set_of, text_and,
};
use crate::index::application::engine::Engine;
use crate::index::domain::analysis::tokenize;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::schema::Analyzer;

/// BM25 REOPEN BYTE-IDENTICAL + RAM-BOUNDED (Phase 2h-4): seal the WHOLE
/// collection to disk, reopen into a FRESH engine from the segments alone (no
/// CBOR snapshot), and assert (a) the reopened Text field has `tokens` AND
/// `distinct` EMPTY (no RAM rebuild — RAM is O(live tail), not O(corpus)), yet
/// (b) the BM25 scan — text_bm25 (single), text_and (multi), filtered_search —
/// is byte-identical f32 (to_bits) AND same result-set as an in-RAM oracle,
/// driven entirely from the mmap with `tokens.is_empty()`.
#[test]
fn reopen_drives_bm25_from_segment_with_empty_tokens() {
    // Build the same tf-realistic corpus into a SEAL-then-REOPEN subject and an
    // in-RAM oracle.
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_doc(
            e,
            "d0",
            &body_from(&[("alpha", 3), ("beta", 1)]),
            Some(20.0),
        );
        index_doc(
            e,
            "d1",
            &body_from(&[("alpha", 1), ("gamma", 2)]),
            Some(40.0),
        );
        index_doc(e, "d2", &body_from(&[("beta", 4)]), Some(55.0));
        index_doc(
            e,
            "d3",
            &body_from(&[("alpha", 2), ("beta", 1), ("gamma", 1)]),
            Some(70.0),
        );
        index_doc(e, "d4", &body_from(&[("gamma", 3)]), Some(90.0));
    }

    let oracle = Arc::new(Engine::new());
    build(&oracle);
    let o_single = run(&oracle, bm25_single("alpha"));
    let o_and = run(&oracle, text_and("alpha", "beta"));
    let o_filt = run(&oracle, filtered("alpha", 10.0, 80.0));

    // Subject: build, seal the whole collection, reopen into a FRESH engine.
    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir.path(), 1)
        .unwrap();
    let sch = subject.__collection_schema("c").unwrap();
    let reopened = Engine::__open_collection_from_segments("c", dir.path(), sch, 1).unwrap();

    // RAM-BOUNDED: the reopened Text field drives BM25 from the mmap with NO
    // in-RAM tokens/distinct rebuild.
    {
        let state = reopened.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be a Text field");
        };
        assert!(idx.segment.is_some(), "reopened segment attached");
        assert!(
            idx.tokens.is_empty(),
            "reopen must NOT rebuild `tokens` (got {})",
            idx.tokens.len()
        );
        assert!(
            idx.distinct_is_empty(),
            "reopen must NOT rebuild `distinct` (got {})",
            idx.distinct_iter().count()
        );
        assert!(
            idx.lens.is_empty(),
            "reopen must NOT rebuild `lens` (doc_len reads the segment column)"
        );
        // Live corpus scalars were initialized from the header.
        assert_eq!(
            idx.bm25_corpus(),
            (5, 4 + 3 + 4 + 4 + 3),
            "live corpus initialized from header"
        );
    }

    // BYTE-IDENTICAL BM25 from the mmap, same sets.
    let r_single = run(&reopened, bm25_single("alpha"));
    let r_and = run(&reopened, text_and("alpha", "beta"));
    let r_filt = run(&reopened, filtered("alpha", 10.0, 80.0));
    assert_eq!(
        set_of(&o_single),
        set_of(&r_single),
        "reopen single-token set diverged"
    );
    assert_eq!(
        set_of(&o_and),
        set_of(&r_and),
        "reopen 2-token AND set diverged"
    );
    assert_eq!(
        set_of(&o_filt),
        set_of(&r_filt),
        "reopen filtered set diverged"
    );
    assert_eq!(
        scores_of(&o_single),
        scores_of(&r_single),
        "reopen single-token scores diverged"
    );
    assert_eq!(
        scores_of(&o_and),
        scores_of(&r_and),
        "reopen 2-token AND scores diverged"
    );
    assert_eq!(
        scores_of(&o_filt),
        scores_of(&r_filt),
        "reopen filtered scores diverged"
    );
}

#[test]
fn ngram_streamed_write_survives_checkpoint_and_cold_reopen() {
    let engine = Arc::new(Engine::new());
    let mut ngram_schema = schema();
    ngram_schema.fields.get_mut("body").unwrap().analyzer = Some(Analyzer::Ngram);
    engine.create_collection("c", ngram_schema).unwrap();

    let text = "İstanbul ABcd";
    index_doc(&engine, "unicode", text, Some(1.0));
    let expected_len = u32::try_from(tokenize::tokenize(text, Analyzer::Ngram).len()).unwrap();
    let doc_len = |subject: &Engine| {
        let state = subject.state.read().unwrap();
        let collection = state.collections.get("c").unwrap();
        let id = collection.interner.id("unicode").unwrap();
        let FieldIndex::Text { idx, .. } = collection.fields.get("body").unwrap() else {
            panic!("body must be text");
        };
        idx.doc_len(id)
    };
    assert_eq!(doc_len(&engine), expected_len);
    let before = run(&engine, bm25_single("ABCD"));
    assert_eq!(set_of(&before), BTreeSet::from(["unicode".to_owned()]));

    let directory = tempfile::tempdir().unwrap();
    engine
        .__seal_collection_to_segments("c", directory.path(), 1)
        .unwrap();
    let cold = Engine::__open_collection_from_segments(
        "c",
        directory.path(),
        engine.__collection_schema("c").unwrap(),
        1,
    )
    .unwrap();
    assert_eq!(doc_len(&cold), expected_len);
    assert_eq!(
        scores_of(&run(&cold, bm25_single("ABCD"))),
        scores_of(&before),
        "cold search must retain the ngram stream postings and document length"
    );
}

#[test]
fn sealed_text_reseal_and_cold_reopen_match_ram_scores() {
    let oracle = Arc::new(Engine::new());
    let subject = Arc::new(Engine::new());
    oracle.create_collection("c", schema()).unwrap();
    subject.create_collection("c", schema()).unwrap();
    for (eid, body, price) in [("d0", "shared base", 1.0), ("d1", "base", 2.0)] {
        index_doc(&oracle, eid, body, Some(price));
        index_doc(&subject, eid, body, Some(price));
    }
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir.path(), 1)
        .unwrap();
    for (eid, body, price) in [
        ("d0", "shared overlay", 3.0),
        ("d2", "shared tailonly", 4.0),
    ] {
        index_doc(&oracle, eid, body, Some(price));
        index_doc(&subject, eid, body, Some(price));
    }
    let dir2 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    let schema = subject.__collection_schema("c").unwrap();
    let reopened = Engine::__open_collection_from_segments("c", dir2.path(), schema, 2).unwrap();
    for token in ["shared", "overlay", "tailonly"] {
        let query = bm25_single(token);
        assert_eq!(
            set_of(&run(&reopened, query.clone())),
            set_of(&run(&oracle, query.clone()))
        );
        assert_eq!(
            scores_of(&run(&reopened, query.clone())),
            scores_of(&run(&oracle, query))
        );
    }
    assert_eq!(run(&reopened, bm25_single("shared")).len(), 2);
}
