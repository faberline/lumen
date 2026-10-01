use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::application::engine::seal::tests::keyword_inverted::{
    assert_terms_dropped, index_kw, run, schema, set_of, term, terms,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::query::QueryNode;

// -----------------------------------------------------------------------
// Phase 2h-1 FIX: delete-after-seal query-time tombstone. After 2h-1 dropped
// the in-RAM `terms` index at seal, `drop_eid` on a SEALED base docid was a
// NO-OP (the on-disk posting is immutable), so segment-driven Keyword queries
// LEAKED the deleted doc until the next re-seal. The fix records sealed-base
// deletes in a per-field `tombstones` RoaringBitmap that every segment-ON
// accessor subtracts. These tests pin the fix and prove its teeth.
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

/// DELETE-AFTER-SEAL **WITHOUT** RE-SEAL: seal a Keyword corpus, delete
/// several BASE docs (no re-seal), then assert every segment-ON query equals
/// an in-RAM ORACLE built from the identical op sequence but NEVER sealed —
/// byte-identical result SETS for standalone Term, multi-term Terms, boolean
/// Or, cross-field And, plus identical `duplicates`/`unique_terms`. This is
/// the window 2g-A's seal-time GC does NOT cover; the tombstone closes it.
#[test]
fn delete_after_seal_without_reseal_matches_oracle() {
    // Shared corpus builder so the oracle and the sealed engine see the
    // SAME index sequence. d0..d5 on `kw`/`cat`; alpha appears 3x, beta 2x,
    // red 3x → real duplicate groups before any delete.
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_kw(e, "d0", Some("alpha"), Some("red"));
        index_kw(e, "d1", Some("alpha"), Some("red"));
        index_kw(e, "d2", Some("beta"), Some("green"));
        index_kw(e, "d3", Some("alpha"), Some("red"));
        index_kw(e, "d4", Some("beta"), Some("green"));
        index_kw(e, "d5", Some("gamma"), None);
    }
    // The deletes to apply on BOTH paths (whole-doc deletes).
    let to_delete = ["d1", "d3", "d4"];

    // --- ORACLE: in-RAM, never sealed. Same build + same deletes. ---
    let oracle = Arc::new(Engine::new());
    build(&oracle);
    for d in to_delete {
        oracle.delete("c", d, None).unwrap();
    }

    // --- SUBJECT: seal kw + cat to segments, THEN delete (no re-seal). ---
    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_keyword_field_to_segment("c", "kw", dir.path())
        .unwrap();
    subject
        .__seal_keyword_field_to_segment("c", "cat", dir.path())
        .unwrap();
    assert_terms_dropped(&subject); // RAM `terms` gone — drives from mmap
                                    // Delete AFTER the seal, with NO re-seal → exercises the tombstone path.
    for d in to_delete {
        subject.delete("c", d, None).unwrap();
    }

    // The deleted ids are now tombstoned (base ids < n_docs), NOT removed
    // from any on-disk posting. Confirm the bitmap actually recorded them.
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw");
        };
        assert!(k.terms.is_empty(), "sealed: terms still dropped");
        assert_eq!(k.tombstones.len(), 3, "three base deletes tombstoned");
    }

    // Result SETS must match the oracle on every driver surface.
    let q_term = term("kw", "alpha");
    let q_terms = terms("kw", &["alpha", "beta"]);
    let q_or = QueryNode::Or(vec![term("kw", "alpha"), term("kw", "beta")]);
    let q_and = QueryNode::And(vec![term("kw", "alpha"), term("cat", "red")]);

    assert_eq!(
        set_of(&run(&subject, q_term.clone())),
        set_of(&run(&oracle, q_term)),
        "standalone Term leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_terms.clone())),
        set_of(&run(&oracle, q_terms)),
        "multi-term Terms leaked a deleted doc"
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
    // After deleting d1,d3,d4: alpha={d0}, beta={d2}, gamma={d5} on kw →
    // NO kw duplicate group survives; on cat red={d0}, green={d2} → none.
    assert_eq!(
        dup_map(&subject, "kw"),
        dup_map(&oracle, "kw"),
        "find_duplicates(kw) diverged on sealed-after-delete"
    );
    assert_eq!(
        dup_map(&subject, "cat"),
        dup_map(&oracle, "cat"),
        "find_duplicates(cat) diverged on sealed-after-delete"
    );
    assert_eq!(
        uniq(&subject, "kw"),
        uniq(&oracle, "kw"),
        "unique_terms(kw) diverged on sealed-after-delete"
    );
    assert_eq!(
        uniq(&subject, "cat"),
        uniq(&oracle, "cat"),
        "unique_terms(cat) diverged on sealed-after-delete"
    );

    // Concrete spot-checks (not just oracle-equality): the deleted docs are
    // GONE and a fully-deleted term yields None (empty result), not a leak.
    let alpha = set_of(&run(&subject, term("kw", "alpha")));
    assert_eq!(
        alpha,
        ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "alpha must be only the surviving d0"
    );
    assert!(
        run(&subject, term("kw", "beta"))
            .iter()
            .all(|(eid, _)| eid != "d4"),
        "deleted d4 must not appear under beta"
    );
}

/// RE-SEAL (CHECKPOINT) after delete: once the field is re-sealed the
/// deletions are BAKED into the new segment (via 2g-A's live(id) GC), the
/// tombstone is CLEARED, and queries still match the oracle — composing the
/// query-time tombstone with the seal-time GC.
#[test]
fn reseal_bakes_deletes_and_clears_tombstone() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_kw(e, "d0", Some("alpha"), Some("red"));
        index_kw(e, "d1", Some("alpha"), Some("red"));
        index_kw(e, "d2", Some("beta"), Some("green"));
        index_kw(e, "d3", Some("gamma"), None);
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
    // First seal (the WHOLE collection so re-seal has a working live(id)/
    // eid_fields GC), then delete, then RE-SEAL (checkpoint).
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
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw");
        };
        assert_eq!(k.tombstones.len(), 2, "deletes tombstoned before re-seal");
    }
    // RE-SEAL: the live(id) gather (2g-A) excludes the tombstoned ids, so the
    // NEW segment has them absent; the tombstone is reset to empty.
    let dir2 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Keyword(k) = coll.fields.get("kw").unwrap() else {
            panic!("kw");
        };
        assert!(
            k.tombstones.is_empty(),
            "tombstone must be CLEARED after re-seal (deletes baked in)"
        );
    }

    // Post-re-seal queries match the oracle, now with an EMPTY tombstone (the
    // new segment itself has the deleted docs absent).
    let q_term = term("kw", "alpha");
    assert_eq!(
        set_of(&run(&subject, q_term.clone())),
        set_of(&run(&oracle, q_term)),
        "post-re-seal Term diverged from oracle"
    );
    assert_eq!(
        set_of(&run(&subject, term("kw", "beta"))),
        BTreeSet::new(),
        "beta fully deleted — must be empty after re-seal"
    );
    assert_eq!(
        uniq(&subject, "kw"),
        uniq(&oracle, "kw"),
        "unique_terms diverged after re-seal"
    );
}
