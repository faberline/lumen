use std::collections::BTreeSet;
use std::sync::Arc;

use crate::index::application::engine::seal::tests::text::{
    bm25_single, body_from, index_doc, run, schema, scores_of, set_of,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::document::{FieldValue, IndexRequest};

/// `unique_terms` for a field via the stats surface (Phase 2h-4 `unique_terms`).
fn uniq(e: &Engine, field: &str) -> u64 {
    e.stats("c")
        .unwrap()
        .fields
        .get(field)
        .unwrap()
        .unique_terms
}

/// RE-SEAL after delete (Phase 2h-4): once re-sealed the deletes are BAKED into
/// the new segment (2g-A live(id) GC), the tombstone is CLEARED, and a fresh
/// reopen excludes the deleted docs with a correct corpus.
#[test]
fn reseal_bakes_text_deletes_and_clears_tombstone() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_doc(
            e,
            "d0",
            &body_from(&[("alpha", 2), ("beta", 1)]),
            Some(20.0),
        );
        index_doc(e, "d1", &body_from(&[("alpha", 3)]), Some(40.0));
        index_doc(
            e,
            "d2",
            &body_from(&[("beta", 2), ("gamma", 1)]),
            Some(60.0),
        );
        index_doc(
            e,
            "d3",
            &body_from(&[("alpha", 1), ("gamma", 2)]),
            Some(80.0),
        );
    }
    let to_delete = ["d1", "d2"];

    let oracle = Arc::new(Engine::new());
    build(&oracle);
    for d in to_delete {
        oracle.delete("c", d, None).unwrap();
    }

    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir.path(), 1)
        .unwrap();
    for d in to_delete {
        subject.delete("c", d, None).unwrap();
    }
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body");
        };
        assert_eq!(idx.tombstones.len(), 2, "deletes tombstoned before re-seal");
    }

    // RE-SEAL into a new dir: deletes baked in, tombstone cleared.
    let dir2 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body");
        };
        assert!(
            idx.tombstones.is_empty(),
            "tombstone must be CLEARED after re-seal"
        );
    }

    // Reopen from the RE-SEALED dir: deleted docs gone, corpus correct.
    let sch = subject.__collection_schema("c").unwrap();
    let reopened = Engine::__open_collection_from_segments("c", dir2.path(), sch, 2).unwrap();
    {
        let state = reopened.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body");
        };
        // 4 docs - 2 deleted = 2 live; total = d0(3) + d3(3) = 6.
        assert_eq!(
            idx.bm25_corpus(),
            (2, 3 + 3),
            "re-sealed corpus excludes deletes"
        );
    }

    let s_single = run(&reopened, bm25_single("alpha"));
    let o_single = run(&oracle, bm25_single("alpha"));
    assert_eq!(
        set_of(&s_single),
        set_of(&o_single),
        "post-re-seal alpha set diverged"
    );
    assert_eq!(
        scores_of(&s_single),
        scores_of(&o_single),
        "post-re-seal alpha scores diverged"
    );
    // beta fully deleted only via d2; d0 still has beta → survives.
    assert_eq!(
        set_of(&run(&reopened, bm25_single("beta"))),
        ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "beta must be only the surviving d0 after re-seal"
    );
    assert_eq!(
        uniq(&reopened, "body"),
        uniq(&oracle, "body"),
        "unique_terms diverged after re-seal"
    );
}

#[test]
fn sealed_empty_text_presence_survives_reseal_snapshot_and_delete() {
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
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_body(e, "d0", "");
        index_price(e, "d0", 1.0);
        index_body(e, "d1", "control");
        index_price(e, "d1", 2.0);
    }
    fn body_corpus(e: &Engine) -> (u64, u64) {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Text { idx, .. } = coll.fields.get("body").unwrap() else {
            panic!("body must be text");
        };
        idx.bm25_corpus()
    }
    fn body_is_covered(e: &Engine, eid: &str) -> bool {
        let state = e.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let id = coll.interner.id(eid).unwrap();
        coll.eid_fields
            .get(&id)
            .is_some_and(|fields| fields.contains("body"))
    }

    let oracle = Arc::new(Engine::new());
    build(&oracle);

    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir1 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir1.path(), 1)
        .unwrap();
    let schema1 = subject.__collection_schema("c").unwrap();
    let cold = Engine::__open_collection_from_segments("c", dir1.path(), schema1, 1).unwrap();
    assert!(
        body_is_covered(&cold, "d0"),
        "empty body coverage after cold reopen"
    );
    assert!(body_is_covered(&cold, "d1"));
    assert_eq!(body_corpus(&cold), body_corpus(&oracle));

    // A re-seal must carry the explicit empty presence bit forward, even
    // though d0's DocLen and posting list are both empty.
    let dir2 = tempfile::tempdir().unwrap();
    cold.__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    let schema2 = cold.__collection_schema("c").unwrap();
    let cold2 = Engine::__open_collection_from_segments("c", dir2.path(), schema2, 2).unwrap();
    assert!(
        body_is_covered(&cold2, "d0"),
        "empty body coverage after re-seal"
    );
    assert_eq!(body_corpus(&cold2), body_corpus(&oracle));
    let cold_snapshot = cold2.snapshot().unwrap();

    // The cold segment-backed path must recognize the empty base value as
    // covered. Updating d0 must tombstone that base once before adding the
    // replacement posting.
    index_body(&oracle, "d0", "updated");
    index_body(&cold2, "d0", "updated");
    assert_eq!(body_corpus(&cold2), body_corpus(&oracle));
    let updated = bm25_single("updated");
    assert_eq!(
        set_of(&run(&cold2, updated.clone())),
        set_of(&run(&oracle, updated.clone()))
    );
    assert_eq!(
        scores_of(&run(&cold2, updated.clone())),
        scores_of(&run(&oracle, updated.clone()))
    );
    let control = bm25_single("control");
    assert_eq!(
        set_of(&run(&cold2, control.clone())),
        set_of(&run(&oracle, control.clone()))
    );
    assert_eq!(
        scores_of(&run(&cold2, control.clone())),
        scores_of(&run(&oracle, control.clone()))
    );

    // Delete the formerly empty doc's field and compare exact corpus, IDs,
    // and score bits with the pure-RAM path.
    oracle.delete("c", "d0", Some("body")).unwrap();
    cold2.delete("c", "d0", Some("body")).unwrap();
    assert_eq!(body_corpus(&cold2), body_corpus(&oracle));
    assert!(run(&cold2, updated.clone()).is_empty());
    assert_eq!(
        set_of(&run(&cold2, control.clone())),
        set_of(&run(&oracle, control.clone()))
    );
    assert_eq!(
        scores_of(&run(&cold2, control.clone())),
        scores_of(&run(&oracle, control.clone()))
    );

    // A snapshot taken after the cold reopen must retain empty-field
    // coverage and permit a later replacement through the RAM path.
    let restored = Arc::new(Engine::new());
    restored.restore(cold_snapshot).unwrap();
    assert!(
        body_is_covered(&restored, "d0"),
        "snapshot lost empty coverage"
    );
    let snapshot_oracle = Arc::new(Engine::new());
    build(&snapshot_oracle);
    index_body(&restored, "d0", "snapshot-updated");
    index_body(&snapshot_oracle, "d0", "snapshot-updated");
    assert_eq!(body_corpus(&restored), body_corpus(&snapshot_oracle));
    let snapshot_updated = bm25_single("snapshot-updated");
    assert_eq!(
        set_of(&run(&restored, snapshot_updated.clone())),
        set_of(&run(&snapshot_oracle, snapshot_updated.clone()))
    );
    assert_eq!(
        scores_of(&run(&restored, snapshot_updated.clone())),
        scores_of(&run(&snapshot_oracle, snapshot_updated))
    );
}
