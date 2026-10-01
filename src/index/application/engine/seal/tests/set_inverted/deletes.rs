use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::application::engine::seal::tests::set_inverted::{
    assert_elements_dropped, index_set, run, schema, set_of, term, terms,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::query::QueryNode;

// -----------------------------------------------------------------------
// Phase 2h-2: delete-after-seal query-time tombstone (REUSED from 2h-1).
// After the seal drops the in-RAM `elements` index, `drop_eid` on a SEALED
// base docid is a NO-OP on the immutable on-disk posting, so segment-driven
// Set queries would LEAK the deleted doc until the next re-seal. The fix
// records sealed-base deletes in the per-field `tombstones` RoaringBitmap
// (the same field shape as the Keyword index) that every segment-ON accessor
// subtracts. These tests pin the fix and prove its teeth.
// -----------------------------------------------------------------------

/// (value → external_ids) of every duplicate group, as a comparable map.
fn dup_map(e: &Engine, field: &str) -> BTreeMap<String, BTreeSet<String>> {
    e.duplicates(
        "c",
        crate::shared_kernel::types::search::DuplicatesRequest {
            field: field.into(),
            min_group_size: 2,
            limit: 100_000,
            offset: 0,
        },
    )
    .unwrap()
    .groups
    .into_iter()
    .map(|g| {
        let v = g.value.as_str().unwrap().to_string();
        (v, g.external_ids.into_iter().collect::<BTreeSet<String>>())
    })
    .collect()
}

/// `unique_terms` of `field` via the public stats surface.
fn uniq(e: &Engine, field: &str) -> u64 {
    e.stats("c")
        .unwrap()
        .fields
        .get(field)
        .unwrap()
        .unique_terms
}

/// DELETE-AFTER-SEAL **WITHOUT** RE-SEAL: seal a Set corpus, delete several
/// BASE docs (no re-seal), then assert every segment-ON query equals an
/// in-RAM ORACLE built from the identical op sequence but NEVER sealed —
/// byte-identical result SETS for standalone Term, multi-member Terms,
/// boolean Or, cross-field And, plus identical `duplicates`/`unique_terms`.
/// This is the window the seal-time GC does NOT cover; the tombstone closes it.
#[test]
fn delete_after_seal_without_reseal_matches_oracle() {
    // Shared corpus builder so the oracle and the sealed engine see the SAME
    // index sequence. Multi-valued: alpha in 3 docs, beta in 2, red in 3.
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_set(e, "d0", Some(&["alpha", "beta"]), Some(&["red"]));
        index_set(e, "d1", Some(&["alpha"]), Some(&["red"]));
        index_set(e, "d2", Some(&["beta"]), Some(&["green"]));
        index_set(e, "d3", Some(&["alpha"]), Some(&["red"]));
        index_set(e, "d4", Some(&["beta", "gamma"]), Some(&["green"]));
        index_set(e, "d5", Some(&["gamma"]), None);
    }
    let to_delete = ["d1", "d3", "d4"];

    // --- ORACLE: in-RAM, never sealed. Same build + same deletes. ---
    let oracle = Arc::new(Engine::new());
    build(&oracle);
    for d in to_delete {
        oracle.delete("c", d, None).unwrap();
    }

    // --- SUBJECT: seal tags + cat to segments, THEN delete (no re-seal). ---
    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_set_field_to_segment("c", "tags", dir.path())
        .unwrap();
    subject
        .__seal_set_field_to_segment("c", "cat", dir.path())
        .unwrap();
    assert_elements_dropped(&subject); // RAM `elements` gone — drives from mmap
    for d in to_delete {
        subject.delete("c", d, None).unwrap();
    }

    // The deleted ids are now tombstoned (base ids < n_docs), NOT removed
    // from any on-disk posting. Confirm the bitmap actually recorded them.
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags");
        };
        assert!(s.elements.is_empty(), "sealed: elements still dropped");
        assert_eq!(s.tombstones.len(), 3, "three base deletes tombstoned");
    }

    // Result SETS must match the oracle on every driver surface.
    let q_term = term("tags", "alpha");
    let q_terms = terms("tags", &["alpha", "beta"]);
    let q_or = QueryNode::Or(vec![term("tags", "alpha"), term("tags", "beta")]);
    let q_and = QueryNode::And(vec![term("tags", "alpha"), term("cat", "red")]);

    assert_eq!(
        set_of(&run(&subject, q_term.clone())),
        set_of(&run(&oracle, q_term)),
        "standalone Term leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_terms.clone())),
        set_of(&run(&oracle, q_terms)),
        "multi-member Terms leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_or.clone())),
        set_of(&run(&oracle, q_or)),
        "boolean Or leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_and.clone())),
        set_of(&run(&oracle, q_and)),
        "cross-field And leaked a deleted doc"
    );

    // duplicates + unique_terms must match the oracle on sealed data.
    assert_eq!(
        dup_map(&subject, "tags"),
        dup_map(&oracle, "tags"),
        "find_duplicates(tags) diverged on sealed-after-delete"
    );
    assert_eq!(
        dup_map(&subject, "cat"),
        dup_map(&oracle, "cat"),
        "find_duplicates(cat) diverged on sealed-after-delete"
    );
    assert_eq!(
        uniq(&subject, "tags"),
        uniq(&oracle, "tags"),
        "unique_terms(tags) diverged on sealed-after-delete"
    );
    assert_eq!(
        uniq(&subject, "cat"),
        uniq(&oracle, "cat"),
        "unique_terms(cat) diverged on sealed-after-delete"
    );

    // Concrete spot-checks: deleted docs GONE; a fully-deleted element yields
    // None (empty result), not a leak. After deleting d1,d3,d4:
    // alpha={d0}, beta={d2}, gamma={d5}.
    let alpha = set_of(&run(&subject, term("tags", "alpha")));
    assert_eq!(
        alpha,
        ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "alpha must be only the surviving d0"
    );
    assert!(
        run(&subject, term("tags", "beta"))
            .iter()
            .all(|(eid, _)| eid != "d4"),
        "deleted d4 must not appear under beta"
    );
    // gamma was on d4 (deleted) and d5 (alive) → only d5 survives.
    let gamma = set_of(&run(&subject, term("tags", "gamma")));
    assert_eq!(
        gamma,
        ["d5".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "gamma must drop deleted d4, keep d5"
    );
}

/// RE-SEAL (CHECKPOINT) after delete: once the field is re-sealed the
/// deletions are BAKED into the new segment (via the live(id) GC), the
/// tombstone is CLEARED, and queries still match the oracle.
#[test]
fn reseal_bakes_deletes_and_clears_tombstone() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        // beta lives ONLY on d2 (deleted) so it is fully removed after re-seal;
        // alpha lives on d0 (kept) and d1 (deleted).
        index_set(e, "d0", Some(&["alpha"]), Some(&["red"]));
        index_set(e, "d1", Some(&["alpha"]), Some(&["red"]));
        index_set(e, "d2", Some(&["beta"]), Some(&["green"]));
        index_set(e, "d3", Some(&["gamma"]), None);
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
    // Tombstone holds the two base deletes pre-re-seal.
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags");
        };
        assert_eq!(s.tombstones.len(), 2, "deletes tombstoned before re-seal");
    }
    // RE-SEAL: the live(id) gather excludes the tombstoned ids, so the NEW
    // segment has them absent; the tombstone is reset to empty.
    let dir2 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Set(s) = coll.fields.get("tags").unwrap() else {
            panic!("tags");
        };
        assert!(
            s.tombstones.is_empty(),
            "tombstone must be CLEARED after re-seal (deletes baked in)"
        );
    }

    // Post-re-seal queries match the oracle, now with an EMPTY tombstone.
    let q_term = term("tags", "alpha");
    assert_eq!(
        set_of(&run(&subject, q_term.clone())),
        set_of(&run(&oracle, q_term)),
        "post-re-seal Term diverged from oracle"
    );
    assert_eq!(
        set_of(&run(&subject, term("tags", "beta"))),
        BTreeSet::new(),
        "beta fully deleted — must be empty after re-seal"
    );
    assert_eq!(
        uniq(&subject, "tags"),
        uniq(&oracle, "tags"),
        "unique_terms diverged after re-seal"
    );
}
