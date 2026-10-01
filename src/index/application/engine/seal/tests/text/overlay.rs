use std::sync::Arc;

use crate::index::application::engine::seal::tests::text::{
    bm25_single, body_from, filtered, index_doc, run, schema, scores_of, set_of, text_and,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};

#[test]
fn sealed_text_unions_tail_and_reused_overlay_postings() {
    let oracle = Arc::new(Engine::new());
    let subject = Arc::new(Engine::new());
    oracle.create_collection("c", schema()).unwrap();
    subject.create_collection("c", schema()).unwrap();
    index_doc(
        &oracle,
        "d0",
        &body_from(&[("shared", 2), ("base", 1)]),
        Some(1.0),
    );
    index_doc(
        &subject,
        "d0",
        &body_from(&[("shared", 2), ("base", 1)]),
        Some(1.0),
    );
    index_doc(&oracle, "d1", &body_from(&[("base", 1)]), Some(2.0));
    index_doc(&subject, "d1", &body_from(&[("base", 1)]), Some(2.0));
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_text_field_to_segment("c", "body", dir.path())
        .unwrap();

    // d0 reuses a sealed id, while d2 is a pure live tail. Both must be
    // visible through the same active posting accessor.
    index_doc(
        &oracle,
        "d0",
        &body_from(&[("shared", 1), ("overlay", 2)]),
        Some(3.0),
    );
    index_doc(
        &subject,
        "d0",
        &body_from(&[("shared", 1), ("overlay", 2)]),
        Some(3.0),
    );
    index_doc(
        &oracle,
        "d2",
        &body_from(&[("shared", 1), ("tailonly", 1)]),
        Some(4.0),
    );
    index_doc(
        &subject,
        "d2",
        &body_from(&[("shared", 1), ("tailonly", 1)]),
        Some(4.0),
    );

    for token in ["shared", "base", "overlay", "tailonly"] {
        let query = bm25_single(token);
        assert_eq!(
            set_of(&run(&subject, query.clone())),
            set_of(&run(&oracle, query.clone()))
        );
        assert_eq!(
            scores_of(&run(&subject, query.clone())),
            scores_of(&run(&oracle, query))
        );
    }
    for query in [text_and("shared", "overlay"), filtered("shared", 3.0, 3.0)] {
        assert_eq!(
            set_of(&run(&subject, query.clone())),
            set_of(&run(&oracle, query.clone()))
        );
        assert_eq!(
            scores_of(&run(&subject, query.clone())),
            scores_of(&run(&oracle, query))
        );
    }
    assert_eq!(
        set_of(&run(&subject, bm25_single("shared"))),
        ["d0", "d2"].into_iter().map(String::from).collect()
    );

    // Snapshot restore must preserve both the appended tail and the
    // replacement overlay on the reused base id.
    let restored = Arc::new(Engine::new());
    restored.restore(subject.snapshot().unwrap()).unwrap();
    for token in ["shared", "overlay", "tailonly"] {
        let query = bm25_single(token);
        assert_eq!(
            set_of(&run(&restored, query.clone())),
            set_of(&run(&subject, query.clone()))
        );
        assert_eq!(
            scores_of(&run(&restored, query.clone())),
            scores_of(&run(&subject, query))
        );
    }

    let state = subject.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
        panic!("body must be text");
    };
    assert_eq!(idx.tok_df("shared"), Some(2));
    assert_eq!(idx.tok_df("tailonly"), Some(1));
    assert_eq!(idx.tok_df("base"), Some(1));
    let posting = idx.tok_postings("shared").unwrap();
    assert_eq!(posting.docids(), &[0, 2]);
    assert_eq!(posting.tfs(), &[1, 1]);
}

#[test]
fn sealed_text_replacement_twice_keeps_latest_overlay_and_snapshot_state() {
    let oracle = Arc::new(Engine::new());
    let subject = Arc::new(Engine::new());
    oracle.create_collection("c", schema()).unwrap();
    subject.create_collection("c", schema()).unwrap();
    index_doc(&oracle, "d0", "old", Some(1.0));
    index_doc(&subject, "d0", "old", Some(1.0));
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_text_field_to_segment("c", "body", dir.path())
        .unwrap();

    for engine in [&oracle, &subject] {
        index_doc(engine, "d0", "new", Some(2.0));
        index_doc(engine, "d0", "latest", Some(3.0));
    }
    assert_eq!(
        scores_of(&run(&subject, bm25_single("old"))),
        scores_of(&run(&oracle, bm25_single("old")))
    );
    assert!(run(&subject, bm25_single("new")).is_empty());
    assert_eq!(
        set_of(&run(&subject, bm25_single("latest"))),
        ["d0"].into_iter().map(String::from).collect()
    );
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be text");
        };
        assert!(idx.tok_postings("new").is_none());
        assert_eq!(idx.tok_df("latest"), Some(1));
        assert_eq!(idx.bm25_corpus(), (1, 1));
        assert!(idx
            .distinct_at(0)
            .is_some_and(|tokens| tokens.iter().next().is_some()));
        assert_eq!(idx.doc_len(0), 1);
        assert!(idx.tombstones.contains(0));
    }

    // Snapshot must retain the latest live overlay and restore the same
    // query state without a segment.
    let restored = Arc::new(Engine::new());
    restored.restore(subject.snapshot().unwrap()).unwrap();
    assert_eq!(
        run(&restored, bm25_single("latest")),
        run(&subject, bm25_single("latest"))
    );
    let state = restored.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
        panic!("body must be text");
    };
    assert!(idx.distinct.get(0).is_some_and(|tokens| {
        tokens
            .as_ref()
            .is_some_and(|tokens| tokens.iter().next().is_some())
    }));
    assert_eq!(idx.doc_len(0), 1);
}

#[test]
fn sealed_text_absent_base_replacement_does_not_tombstone_overlay() {
    fn index_body(e: &Engine, eid: &str, body: &str) {
        e.index(
            "c",
            IndexRequest {
                items: vec![crate::shared_kernel::types::document::IndexItem {
                    external_id: eid.into(),
                    field: "body".into(),
                    value: FieldValue::String(body.into()),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }
    fn index_price(e: &Engine, eid: &str, price: f64) {
        e.index(
            "c",
            IndexRequest {
                items: vec![crate::shared_kernel::types::document::IndexItem {
                    external_id: eid.into(),
                    field: "price".into(),
                    value: FieldValue::Number(price),
                    version: None,
                }],
                request_id: None,
            },
        )
        .unwrap();
    }

    let oracle = Arc::new(Engine::new());
    let subject = Arc::new(Engine::new());
    oracle.create_collection("c", schema()).unwrap();
    subject.create_collection("c", schema()).unwrap();
    for engine in [&oracle, &subject] {
        index_price(engine, "d0", 1.0);
        index_body(engine, "d1", "seed");
    }
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir.path(), 1)
        .unwrap();

    // d0 had no body at the seal. Its first post-seal body write creates a
    // live overlay without a base tombstone; the second write must remove
    // that overlay before applying the latest value.
    for engine in [&oracle, &subject] {
        index_body(engine, "d0", "old");
        index_body(engine, "d0", "new");
    }
    let old = bm25_single("old");
    let new = bm25_single("new");
    assert!(run(&subject, old.clone()).is_empty());
    assert_eq!(
        set_of(&run(&subject, new.clone())),
        ["d0"].into_iter().map(String::from).collect()
    );
    assert_eq!(
        scores_of(&run(&subject, new.clone())),
        scores_of(&run(&oracle, new.clone()))
    );
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be text");
        };
        assert!(idx.tombstones.is_empty());
        assert_eq!(idx.bm25_corpus(), (2, 2));
        let posting = idx.tok_postings("new").unwrap();
        assert_eq!(posting.docids(), &[0]);
        assert_eq!(posting.tfs(), &[1]);
    }

    let restored = Arc::new(Engine::new());
    restored.restore(subject.snapshot().unwrap()).unwrap();
    assert_eq!(
        scores_of(&run(&restored, new.clone())),
        scores_of(&run(&subject, new.clone()))
    );
    assert!(run(&restored, old.clone()).is_empty());

    let dir2 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    let schema = subject.__collection_schema("c").unwrap();
    let reopened = Engine::__open_collection_from_segments("c", dir2.path(), schema, 2).unwrap();
    assert_eq!(
        scores_of(&run(&reopened, new)),
        scores_of(&run(&oracle, bm25_single("new")))
    );
    assert!(run(&reopened, old).is_empty());
}
