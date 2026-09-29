use crate::persistence::infrastructure::composed_segment::tests::{keyword, text};
use crate::persistence::infrastructure::composed_segment::{
    reset_text_posting_clones, text_posting_clones, ComposedSegmentReader,
};
use roaring::RoaringBitmap;
use std::sync::Arc;

#[test]
fn prepared_replacement_reuses_row_maps_and_preserves_later_layers() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(
        &dir.path().join("prepared-base"),
        &[Some("base"), Some("keep")],
    );
    let first = keyword(&dir.path().join("prepared-first"), &[None]);
    let second = keyword(&dir.path().join("prepared-second"), &[Some("added")]);
    let later = keyword(&dir.path().join("prepared-later"), &[Some("latest")]);
    let captured = ComposedSegmentReader::from_base(base.clone())
        .with_delta(first.clone(), vec![0])
        .unwrap()
        .with_delta(second.clone(), vec![1_000_000])
        .unwrap();
    let live = captured.with_delta(later.clone(), vec![1_000_000]).unwrap();
    for includes_base in [false, true] {
        let (values, ids) = if includes_base {
            (
                vec![Some("added"), Some("keep"), None],
                vec![1_000_000, 1, 0],
            )
        } else {
            (vec![Some("added"), None], vec![1_000_000, 0])
        };
        let output = keyword(
            &dir.path().join(format!("prepared-{includes_base}")),
            &values,
        );
        let prepared = captured
            .prepare_replacement(
                includes_base.then_some(&base),
                &[first.clone(), second.clone()],
                output,
                ids,
            )
            .unwrap();
        let next = live.install_prepared_replacement(&prepared).unwrap();
        assert_eq!(next.keyword_at(0), None);
        assert_eq!(next.keyword_at(1).as_deref(), Some("keep"));
        assert_eq!(next.keyword_at(1_000_000).as_deref(), Some("latest"));
        assert!(Arc::ptr_eq(&next.layers.last().unwrap().reader, &later));
        let installed = if includes_base {
            next.base_map.as_ref().unwrap()
        } else {
            &next.layers[0]
        };
        assert!(
            Arc::ptr_eq(installed, &prepared.replacement),
            "binding must reuse the prepared row map, not rebuild it"
        );
        assert!(
            next.install_prepared_replacement(&prepared).is_err(),
            "already replaced input identities must be rejected"
        );
        let incomplete = ComposedSegmentReader::from_base(base.clone())
            .with_delta(first.clone(), vec![0])
            .unwrap();
        assert!(
            incomplete.install_prepared_replacement(&prepared).is_err(),
            "a missing input must be rejected without slicing past the layer list"
        );
    }
}

#[test]
fn mapped_base_replacement_preserves_newer_layers_and_rejects_stale_inputs() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("old"), Some("keep")]);
    let delta = keyword(&dir.path().join("delta"), &[None, Some("added")]);
    let later = keyword(&dir.path().join("later"), &[Some("later")]);
    let merged = keyword(
        &dir.path().join("merged"),
        &[Some("added"), Some("keep"), None],
    );
    let view = ComposedSegmentReader::from_base(base.clone())
        .with_delta(delta.clone(), vec![0, 1_000_000])
        .unwrap()
        .with_delta(later.clone(), vec![1_000_000])
        .unwrap();
    let replaced = view
        .replace_base_inputs(
            &base,
            &[delta.clone()],
            merged.clone(),
            vec![1_000_000, 1, 0],
        )
        .expect("replace captured base and prefix");
    assert_eq!(replaced.keyword_at(0), None);
    assert_eq!(replaced.keyword_at(1).as_deref(), Some("keep"));
    assert_eq!(replaced.keyword_at(1_000_000).as_deref(), Some("later"));
    assert_eq!(replaced.delta_readers().len(), 1);
    assert!(Arc::ptr_eq(&replaced.delta_readers()[0], &later));
    assert!(replaced
        .replace_base_inputs(&base, &[delta], merged.clone(), vec![1_000_000, 1, 0])
        .is_err());
    drop(view);
    assert_eq!(
        Arc::strong_count(&base),
        1,
        "replaced view must release old base"
    );
    assert_eq!(replaced.immutable_base_reader().as_ref().n_docs(), 3);
}

#[test]
fn replacement_keeps_newer_layers_and_rejects_coverage_drift() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("base"), Some("base")]);
    let first = keyword(&dir.path().join("first"), &[Some("one")]);
    let second = keyword(&dir.path().join("second"), &[Some("two")]);
    let newer = keyword(&dir.path().join("newer"), &[Some("newest")]);
    let merged = keyword(&dir.path().join("merged"), &[Some("one"), Some("two")]);
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(first, vec![0])
        .unwrap()
        .with_delta(second, vec![1])
        .unwrap()
        .with_delta(newer, vec![1])
        .unwrap();
    let replaced = view
        .replace_delta_range(0, 2, merged.clone(), vec![0, 1])
        .unwrap();
    assert_eq!(replaced.layers.len(), 2);
    assert_eq!(replaced.keyword_at(0).as_deref(), Some("one"));
    assert_eq!(replaced.keyword_at(1).as_deref(), Some("newest"));
    assert!(view.replace_delta_range(0, 2, merged, vec![0]).is_err());
}

/// #4246: the live-term walk is exact under layer coverage AND pending
/// tombstones, agrees with the materialized composition, and copies
/// nothing.
#[test]
fn live_text_term_count_is_exact_under_coverage_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let rows: &[Option<&[(&str, u32)]>] = &[
        Some(&[("a", 1), ("b", 1)]),
        Some(&[("a", 1), ("c", 1)]),
        Some(&[("e", 1)]),
    ];
    let base = text(&dir.path().join("base"), rows);
    let delta_rows: &[Option<&[(&str, u32)]>] = &[Some(&[("d", 1), ("a", 1)])];
    let delta = text(&dir.path().join("delta"), delta_rows);
    // Row 1 is rewritten by the delta: `c` is gone, `a` survives twice.
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![1])
        .unwrap();
    let brute = |dead: &RoaringBitmap| {
        view.text_tokens_all()
            .unwrap()
            .into_iter()
            .filter(|(_, ids, _)| ids.iter().any(|id| !dead.contains(*id)))
            .count() as u64
    };
    let none = RoaringBitmap::new();
    let dead: RoaringBitmap = [0u32].into_iter().collect();
    let all: RoaringBitmap = [0u32, 1, 2].into_iter().collect();
    assert_eq!(brute(&none), 4, "a b d e");
    assert_eq!(
        brute(&dead),
        3,
        "b dies with row 0; a survives in the delta row"
    );
    assert_eq!(brute(&all), 0);

    reset_text_posting_clones();
    assert_eq!(view.live_text_term_count(&none), Some(4));
    assert_eq!(view.live_text_term_count(&dead), Some(3));
    assert_eq!(view.live_text_term_count(&all), Some(0));
    assert_eq!(view.text_term_has_live_doc("a", &none), Some(true));
    assert_eq!(
        view.text_term_has_live_doc("c", &none),
        Some(false),
        "hidden by coverage"
    );
    assert_eq!(
        view.text_term_has_live_doc("b", &dead),
        Some(false),
        "hidden by a tombstone"
    );
    assert_eq!(view.text_term_has_live_doc("a", &dead), Some(true));
    assert_eq!(
        view.text_term_has_live_doc("zzz", &none),
        Some(false),
        "absent"
    );
    assert_eq!(
        text_posting_clones(),
        0,
        "liveness never materializes a posting"
    );
}

/// #4246: a window merge of additive layers changes no live term, so the
/// O(1) count survives it; a merge that folds the base in is re-derived
/// from the merged dictionary; a hidden row still voids it.
#[test]
fn compaction_keeps_or_refolds_the_distinct_term_count() {
    let dir = tempfile::tempdir().unwrap();
    let base = text(&dir.path().join("base"), &[Some(&[("a", 1)])]);
    let first = text(&dir.path().join("first"), &[Some(&[("b", 1)])]);
    let second = text(&dir.path().join("second"), &[Some(&[("c", 1), ("a", 1)])]);
    let third = text(&dir.path().join("third"), &[Some(&[("d", 1)])]);
    let view = ComposedSegmentReader::from_base(base.clone())
        .with_delta(first.clone(), vec![1])
        .unwrap()
        .with_delta(second.clone(), vec![2])
        .unwrap()
        .with_delta(third, vec![3])
        .unwrap();
    assert_eq!(view.known_distinct_terms(), Some(4), "a b c d");
    let none = RoaringBitmap::new();

    // Window merge of `first` + `second` (rows 1 and 2); the base stays.
    let merged_rows: &[Option<&[(&str, u32)]>] = &[Some(&[("b", 1)]), Some(&[("c", 1), ("a", 1)])];
    let merged = text(&dir.path().join("merged"), merged_rows);
    assert_eq!(
        view.replace_delta_range(0, 2, merged.clone(), vec![1, 2])
            .unwrap()
            .known_distinct_terms(),
        Some(4)
    );
    let prepared = view
        .prepare_replacement(
            None,
            &[first.clone(), second.clone()],
            merged.clone(),
            vec![1, 2],
        )
        .unwrap();
    let windowed = view.install_prepared_replacement(&prepared).unwrap();
    assert_eq!(windowed.known_distinct_terms(), Some(4));
    assert_eq!(windowed.live_text_term_count(&none), Some(4));

    // Base-including merge of base + `first` + `second` into a dense local
    // space; the remaining `third` layer is re-folded on top.
    let rebased_rows: &[Option<&[(&str, u32)]>] = &[
        Some(&[("a", 1)]),
        Some(&[("b", 1)]),
        Some(&[("c", 1), ("a", 1)]),
    ];
    let rebased = text(&dir.path().join("rebased"), rebased_rows);
    let prepared = view
        .prepare_replacement(
            Some(&base),
            &[first.clone(), second.clone()],
            rebased,
            vec![0, 1, 2],
        )
        .unwrap();
    let folded = view.install_prepared_replacement(&prepared).unwrap();
    assert_eq!(folded.known_distinct_terms(), Some(4));
    assert_eq!(folded.live_text_term_count(&none), Some(4));

    // An update hiding row 0 voids the count; a window merge below it
    // cannot restore what it never had, and the walk still answers.
    let update = text(&dir.path().join("update"), &[Some(&[("z", 1)])]);
    let hidden = view.with_delta(update, vec![0]).unwrap();
    assert_eq!(hidden.known_distinct_terms(), None);
    assert_eq!(hidden.live_text_term_count(&none), Some(5), "a b c d z");
    let prepared = hidden
        .prepare_replacement(None, &[first, second], merged, vec![1, 2])
        .unwrap();
    let still_hidden = hidden.install_prepared_replacement(&prepared).unwrap();
    assert_eq!(still_hidden.known_distinct_terms(), None);
    assert_eq!(still_hidden.live_text_term_count(&none), Some(5));
}
