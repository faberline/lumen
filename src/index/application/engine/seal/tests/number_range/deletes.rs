use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::application::engine::seal::tests::number_range::{
    assert_values_dropped, index_num, rangeq, run, run_sorted, schema, set_of, termkw, termnum,
    termsnum, uniq,
};
use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::shared_kernel::types::query::{QueryNode, SortOrder};

/// (value → external_ids) duplicate groups for a Number field, comparable.
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
        // Number duplicate group values serialize as JSON numbers.
        let v = g.value.to_string();
        (v, g.external_ids.into_iter().collect::<BTreeSet<String>>())
    })
    .collect()
}

/// DELETE-AFTER-SEAL **WITHOUT** RE-SEAL: seal a Number corpus, delete several
/// BASE docs (no re-seal), then assert every segment-ON query equals an in-RAM
/// ORACLE built from the identical op sequence but NEVER sealed — byte-identical
/// result SETS for range, exact Term, multi Terms, boolean Or/And, plus
/// duplicates/unique_terms. The 2h tombstone closes the immutable-posting gap.
#[test]
fn delete_after_seal_without_reseal_matches_oracle() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_num(e, "d0", Some(0.0), Some("red"));
        index_num(e, "d1", Some(0.0), Some("red")); // dup 0.0
        index_num(e, "d2", Some(5.0), Some("green"));
        index_num(e, "d3", Some(0.0), Some("red")); // dup 0.0
        index_num(e, "d4", Some(5.0), Some("green")); // dup 5.0
        index_num(e, "d5", Some(-3.0), None);
    }
    let to_delete = ["d1", "d3", "d4"];

    // --- ORACLE: in-RAM, never sealed. ---
    let oracle = Arc::new(Engine::new());
    build(&oracle);
    for d in to_delete {
        oracle.delete("c", d, None).unwrap();
    }

    // --- SUBJECT: seal price + cat, THEN delete (no re-seal). ---
    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_number_field_to_segment("c", "price", dir.path())
        .unwrap();
    subject
        .__seal_keyword_field_to_segment("c", "cat", dir.path())
        .unwrap();
    assert_values_dropped(&subject);
    for d in to_delete {
        subject.delete("c", d, None).unwrap();
    }

    // The deleted base ids are tombstoned, NOT removed from any on-disk posting.
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
            panic!("price");
        };
        assert!(n.values.is_empty(), "sealed: values still dropped");
        assert_eq!(n.tombstones.len(), 3, "three base deletes tombstoned");
    }

    // Result SETS must match the oracle on every driver surface.
    let q_range = rangeq(Some(-10.0), None, Some(10.0), None);
    let q_exact = termnum(0.0);
    let q_terms = termsnum(&[0.0, 5.0]);
    let q_or = QueryNode::Or(vec![termnum(0.0), termnum(5.0)]);
    let q_and = QueryNode::And(vec![
        rangeq(Some(-1.0), None, Some(1.0), None),
        termkw("cat", "red"),
    ]);
    // Or-of-RANGES forces the MATERIALIZED `eval_range` → `range_postings`
    // path (NOT the `try_plan` standalone shortcut), so the range tombstone
    // subtraction is exercised: each range child is fully materialized and the
    // deleted base docids must be subtracted from the on-disk union.
    let q_or_ranges = QueryNode::Or(vec![
        rangeq(Some(-1.0), None, Some(1.0), None),
        rangeq(Some(4.0), None, Some(6.0), None),
    ]);

    assert_eq!(
        set_of(&run(&subject, q_range.clone())),
        set_of(&run(&oracle, q_range)),
        "range leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_exact.clone())),
        set_of(&run(&oracle, q_exact)),
        "exact Term leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_terms.clone())),
        set_of(&run(&oracle, q_terms)),
        "Terms leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_or.clone())),
        set_of(&run(&oracle, q_or)),
        "Or leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_and.clone())),
        set_of(&run(&oracle, q_and)),
        "And leaked a deleted doc"
    );
    assert_eq!(
        set_of(&run(&subject, q_or_ranges.clone())),
        set_of(&run(&oracle, q_or_ranges)),
        "Or-of-ranges (eval_range) leaked a deleted doc"
    );
    assert_eq!(
        dup_map(&subject, "price"),
        dup_map(&oracle, "price"),
        "duplicates(price) diverged after delete"
    );
    assert_eq!(
        uniq(&subject, "price"),
        uniq(&oracle, "price"),
        "unique_terms(price) diverged after delete"
    );

    // Concrete spot-check: 0.0 now only d0 survives (d1, d3 deleted).
    let zero = set_of(&run(&subject, termnum(0.0)));
    assert_eq!(
        zero,
        ["d0".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "0.0 must be only the surviving d0"
    );
    // 5.0: only d2 survives (d4 deleted).
    let five = set_of(&run(&subject, termnum(5.0)));
    assert_eq!(
        five,
        ["d2".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "5.0 must be only the surviving d2"
    );
}

/// SORT-after-delete: the segment sort-via-sorted-index walk (Phase 2m) must
/// drop tombstoned base docids per value, so the ORDERED page matches the
/// in-RAM oracle exactly (asc + desc). Guards the sort walk's tombstone
/// subtraction — without it the walk emits deleted base docs (proven by
/// temporarily disabling the subtraction).
#[test]
fn sort_after_delete_matches_oracle() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_num(e, "d0", Some(0.0), Some("red"));
        index_num(e, "d1", Some(0.0), Some("red"));
        index_num(e, "d2", Some(5.0), Some("green"));
        index_num(e, "d3", Some(0.0), Some("red"));
        index_num(e, "d4", Some(5.0), Some("green"));
        index_num(e, "d5", Some(-3.0), None);
    }
    let to_delete = ["d1", "d3", "d4"];
    let oracle = Arc::new(Engine::new());
    build(&oracle);
    for d in to_delete {
        oracle.delete("c", d, None).unwrap();
    }
    let subject = Arc::new(Engine::new());
    build(&subject);
    let dir = tempfile::tempdir().unwrap();
    subject
        .__seal_number_field_to_segment("c", "price", dir.path())
        .unwrap();
    for d in to_delete {
        subject.delete("c", d, None).unwrap();
    }
    let o_asc = run_sorted(
        &oracle,
        rangeq(None, None, None, None),
        "price",
        SortOrder::Asc,
    );
    let s_asc = run_sorted(
        &subject,
        rangeq(None, None, None, None),
        "price",
        SortOrder::Asc,
    );
    assert_eq!(o_asc, s_asc, "asc sort leaked a deleted doc");
    let o_desc = run_sorted(
        &oracle,
        rangeq(None, None, None, None),
        "price",
        SortOrder::Desc,
    );
    let s_desc = run_sorted(
        &subject,
        rangeq(None, None, None, None),
        "price",
        SortOrder::Desc,
    );
    assert_eq!(o_desc, s_desc, "desc sort leaked a deleted doc");
}

/// RE-SEAL (CHECKPOINT) after delete: once re-sealed the deletions are BAKED
/// into the new segment (2g-A live(id) GC), the tombstone is CLEARED, and
/// queries still match the oracle.
#[test]
fn reseal_bakes_deletes_and_clears_tombstone() {
    fn build(e: &Engine) {
        e.create_collection("c", schema()).unwrap();
        index_num(e, "d0", Some(0.0), Some("red"));
        index_num(e, "d1", Some(0.0), Some("red"));
        index_num(e, "d2", Some(5.0), Some("green"));
        index_num(e, "d3", Some(-3.0), None);
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
        let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
            panic!("price");
        };
        assert_eq!(n.tombstones.len(), 2, "deletes tombstoned before re-seal");
    }
    let dir2 = tempfile::tempdir().unwrap();
    subject
        .__seal_collection_to_segments("c", dir2.path(), 2)
        .unwrap();
    {
        let state = subject.state.read().unwrap();
        let coll = state.collections.get("c").unwrap();
        let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
            panic!("price");
        };
        assert!(
            n.tombstones.is_empty(),
            "tombstone must be CLEARED after re-seal"
        );
    }

    let q_range = rangeq(Some(-10.0), None, Some(10.0), None);
    assert_eq!(
        set_of(&run(&subject, q_range.clone())),
        set_of(&run(&oracle, q_range)),
        "post-re-seal range diverged"
    );
    // value 5.0 fully deleted (d2 gone) → empty.
    assert_eq!(
        set_of(&run(&subject, termnum(5.0))),
        BTreeSet::new(),
        "5.0 fully deleted — empty after re-seal"
    );
    assert_eq!(
        uniq(&subject, "price"),
        uniq(&oracle, "price"),
        "unique_terms diverged after re-seal"
    );
}

/// TEETH: a wrong binary-search bound (inclusivity flip) MUST diverge from the
/// in-RAM oracle. This pins that `number_range_window` honors INCLUSIVE vs
/// EXCLUSIVE exactly: a value sitting exactly on a bound is the difference
/// between the two, so flipping inclusivity changes the result SET.
#[test]
fn teeth_inclusivity_flip_changes_result() {
    let e = Arc::new(Engine::new());
    e.create_collection("c", schema()).unwrap();
    index_num(&e, "lo", Some(0.0), None);
    index_num(&e, "mid", Some(5.0), None);
    index_num(&e, "hi", Some(10.0), None);
    let dir = tempfile::tempdir().unwrap();
    e.__seal_number_field_to_segment("c", "price", dir.path())
        .unwrap();
    assert_values_dropped(&e);

    // [0, 10] inclusive → {lo, mid, hi}; (0, 10) exclusive → {mid}. If the
    // window math ignored inclusivity these would be equal — they are NOT, so
    // this is the teeth assertion the spec asks for.
    let incl = set_of(&run(&e, rangeq(Some(0.0), None, Some(10.0), None)));
    let excl = set_of(&run(&e, rangeq(None, Some(0.0), None, Some(10.0))));
    assert_eq!(
        incl.len(),
        3,
        "[0,10] inclusive must include both endpoints"
    );
    assert_eq!(
        excl,
        ["mid".to_string()].into_iter().collect::<BTreeSet<_>>(),
        "(0,10) exclusive must drop both endpoints"
    );
    assert_ne!(
        incl, excl,
        "inclusive and exclusive bounds MUST differ on boundary values"
    );

    // Direct reader-level teeth: an off-by-one in `number_range_window` would
    // make Included(5.0)..=Included(5.0) miss the exact value. Pin it.
    let state = e.state.read().unwrap();
    let coll = state.collections.get("c").unwrap();
    let FieldIndex::Number(n) = coll.fields.get("price").unwrap() else {
        panic!("price")
    };
    let seg = n.segment.as_ref().unwrap();
    let b5 = SortableF64::new(5.0).unwrap().bits();
    let r = seg
        .number_range(Some((b5, true)), Some((b5, true)))
        .unwrap();
    assert_eq!(
        r.len(),
        1,
        "[5,5] inclusive must select exactly the one 5.0 doc"
    );
    let r_excl = seg
        .number_range(Some((b5, false)), Some((b5, false)))
        .unwrap();
    assert_eq!(r_excl.len(), 0, "(5,5) exclusive must be empty");
}
