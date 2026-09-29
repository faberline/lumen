use super::*;
use std::path::Path;

fn keyword(path: &Path, values: &[Option<&str>]) -> Arc<SegmentReader> {
    let mut postings = BTreeMap::<String, RoaringBitmap>::new();
    for (row, value) in values.iter().enumerate() {
        if let Some(value) = value {
            postings
                .entry((*value).to_owned())
                .or_default()
                .insert(row as u32);
        }
    }
    crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
        path, 7, values, &postings,
    )
    .unwrap();
    Arc::new(SegmentReader::open(path).unwrap())
}

#[test]
fn publication_keeps_newer_private_value_and_tombstone() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("old"), Some("old")]);
    let stage = keyword(&dir.path().join("stage"), &[Some("cut"), Some("cut")]);
    let captured = Arc::new(
        ComposedSegmentReader::from_base(base.clone())
            .with_private_delta(stage, vec![0, 1])
            .unwrap(),
    );
    let cut = captured.checkpoint_cut().unwrap();
    let delta = keyword(
        &dir.path().join("delta"),
        &[Some("cut"), Some("cut"), Some("tail")],
    );
    let prepared = cut.prepare_delta(delta.clone(), vec![0, 1, 10]).unwrap();
    let later = keyword(&dir.path().join("later"), &[Some("new"), None]);
    let live = captured.with_private_delta(later, vec![0, 1]).unwrap();
    let bound = live.install_checkpoint_publication(&prepared).unwrap();
    assert_eq!(
        bound.keyword_at(0).as_deref(),
        Some("new"),
        "publication must retain a post-cut private value"
    );
    assert_eq!(
        bound.keyword_at(1),
        None,
        "publication must retain a post-cut private tombstone"
    );
    assert_eq!(bound.keyword_at(10).as_deref(), Some("tail"));
    assert_eq!(bound.delta_readers().len(), 1);
    assert!(Arc::ptr_eq(&bound.delta_readers()[0], &delta));
    assert!(Arc::ptr_eq(&bound.catalog_base_reader().unwrap(), &base));
    assert_eq!(captured.keyword_at(0).as_deref(), Some("cut"));
}

#[test]
fn initial_full_publication_installs_real_base_and_keeps_newer_private_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let empty = keyword(&dir.path().join("empty-private"), &[]);
    let stage = keyword(&dir.path().join("stage"), &[Some("cut")]);
    let captured = Arc::new(
        ComposedSegmentReader::from_private_empty_base(empty)
            .unwrap()
            .with_private_delta(stage, vec![90])
            .unwrap(),
    );
    assert!(captured.catalog_base_reader().is_none());
    assert!(captured.delta_readers().is_empty());
    let cut = captured.checkpoint_cut().unwrap();
    let real = keyword(&dir.path().join("real-base"), &[]);
    let delta = keyword(&dir.path().join("delta"), &[Some("cut")]);
    assert!(cut.prepare_delta(delta.clone(), vec![90]).is_err());
    let catalog = ComposedSegmentReader::from_base(real.clone())
        .with_delta(delta, vec![90])
        .unwrap();
    let prepared = cut.prepare_full(catalog).unwrap();
    let newer = keyword(&dir.path().join("newer"), &[Some("new")]);
    let live = captured.with_private_delta(newer, vec![90]).unwrap();
    let bound = live.install_checkpoint_publication(&prepared).unwrap();
    assert!(Arc::ptr_eq(&bound.catalog_base_reader().unwrap(), &real));
    assert_eq!(bound.keyword_at(90).as_deref(), Some("new"));
    assert_eq!(bound.keyword_at(0), None);
    assert_eq!(bound.delta_readers().len(), 1);
}

#[test]
fn missing_private_tombstone_coverage_and_changed_prefix_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("old")]);
    let stage = keyword(&dir.path().join("stage"), &[None]);
    let captured = Arc::new(
        ComposedSegmentReader::from_base(base.clone())
            .with_private_delta(stage, vec![90])
            .unwrap(),
    );
    let cut = captured.checkpoint_cut().unwrap();
    let missing = keyword(&dir.path().join("missing"), &[]);
    assert!(
        cut.prepare_delta(missing, vec![]).is_err(),
        "captured private tombstone coverage must not be discarded"
    );
    let delta = keyword(&dir.path().join("delta"), &[None]);
    let prepared = cut.prepare_delta(delta.clone(), vec![90]).unwrap();
    let different = ComposedSegmentReader::from_base(base)
        .with_private_delta(delta, vec![90])
        .unwrap();
    assert!(
        different.install_checkpoint_publication(&prepared).is_err(),
        "reader byte equality cannot replace captured layer identity"
    );
}

#[test]
fn generic_append_cannot_publish_behind_private_rows_and_compaction_preserves_them() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("old")]);
    let one = keyword(&dir.path().join("one"), &[Some("one")]);
    let two = keyword(&dir.path().join("two"), &[Some("two")]);
    let private = keyword(&dir.path().join("private"), &[Some("private")]);
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(one.clone(), vec![0])
        .unwrap()
        .with_delta(two.clone(), vec![0])
        .unwrap()
        .with_private_delta(private.clone(), vec![0])
        .unwrap();
    assert!(view.with_delta(two.clone(), vec![0]).is_err());
    let merged = keyword(&dir.path().join("merged"), &[Some("two")]);
    let plan = view
        .prepare_replacement(None, &[one, two], merged.clone(), vec![0])
        .unwrap();
    let bound = view.install_prepared_replacement(&plan).unwrap();
    assert_eq!(bound.keyword_at(0).as_deref(), Some("private"));
    assert_eq!(bound.delta_readers().len(), 1);
    assert!(Arc::ptr_eq(&bound.delta_readers()[0], &merged));
    assert!(bound
        .prepare_replacement(None, &[private], merged, vec![0])
        .is_err());
}

#[test]
fn empty_cut_keeps_a_first_private_composition_created_after_capture() {
    let dir = tempfile::tempdir().unwrap();
    let cut = ScalarCheckpointCut::empty();
    let real = keyword(&dir.path().join("catalog"), &[Some("captured-overlay")]);
    let prepared = cut
        .prepare_full(ComposedSegmentReader::from_base(real.clone()))
        .unwrap();
    assert_eq!(
        prepared
            .install_without_composition()
            .unwrap()
            .keyword_at(0)
            .as_deref(),
        Some("captured-overlay")
    );
    let empty = keyword(&dir.path().join("initial"), &[]);
    let newer = keyword(&dir.path().join("newer"), &[Some("post-cut")]);
    let live = ComposedSegmentReader::from_private_empty_base(empty)
        .unwrap()
        .with_private_delta(newer, vec![0])
        .unwrap();
    let bound = live.install_checkpoint_publication(&prepared).unwrap();
    assert_eq!(
        bound.keyword_at(0).as_deref(),
        Some("post-cut"),
        "an empty cut must preserve private layers first installed after capture"
    );
    assert!(Arc::ptr_eq(&bound.catalog_base_reader().unwrap(), &real));
    assert!(ComposedSegmentReader::from_base(real)
        .install_checkpoint_publication(&prepared)
        .is_err());
}

// Append inside `#[cfg(test)] mod tests` in
// `src/composed_segment/checkpoint_publication.rs`.
// It uses that module's existing `keyword` helper and imports.

#[test]
fn private_delta_layers_cap_at_sixteen_and_refusal_keeps_prior_view_readable() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("base")]);
    let mut view = ComposedSegmentReader::from_base(base);

    for layer in 0..16 {
        let value = format!("layer-{layer}");
        let reader = keyword(&dir.path().join(format!("layer-{layer}")), &[Some(&value)]);
        view = view.with_private_delta(reader, vec![0]).unwrap();
        assert_eq!(
            view.keyword_at(0).as_deref(),
            Some(value.as_str()),
            "each accepted private layer remains the current winner"
        );
    }

    let before = view.clone();
    let seventeenth = keyword(&dir.path().join("layer-16"), &[Some("layer-16")]);
    let refused = view.with_private_delta(seventeenth, vec![0]);
    assert!(
        refused.is_err(),
        "the seventeenth incremental layer must be refused before attachment"
    );
    assert_eq!(
        view.keyword_at(0).as_deref(),
        Some("layer-15"),
        "refusal must leave the original view readable and unchanged"
    );
    assert_eq!(
        before.keyword_at(0).as_deref(),
        Some("layer-15"),
        "the previously shared view must remain readable after refusal"
    );
}

#[test]
fn catalog_and_private_delta_layers_share_the_sixteen_layer_cap() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("base")]);
    let mut catalog = ComposedSegmentReader::from_base(base);

    for layer in 0..15 {
        let value = format!("catalog-{layer}");
        let reader = keyword(
            &dir.path().join(format!("catalog-{layer}")),
            &[Some(&value)],
        );
        catalog = catalog.with_delta(reader, vec![0]).unwrap();
    }
    assert_eq!(catalog.keyword_at(0).as_deref(), Some("catalog-14"));

    let first_private = keyword(&dir.path().join("private-0"), &[Some("private-0")]);
    let private_view = catalog.with_private_delta(first_private, vec![0]).unwrap();
    assert_eq!(private_view.keyword_at(0).as_deref(), Some("private-0"));

    let before_refusal = private_view.clone();
    let second_private = keyword(&dir.path().join("private-1"), &[Some("private-1")]);
    let refused = private_view.with_private_delta(second_private, vec![0]);
    assert!(
        refused.is_err(),
        "fifteen catalog layers plus one private layer exhaust the shared cap"
    );
    assert_eq!(
        catalog.keyword_at(0).as_deref(),
        Some("catalog-14"),
        "the catalog-only view remains readable after private append refusal"
    );
    assert_eq!(
        private_view.keyword_at(0).as_deref(),
        Some("private-0"),
        "the accepted private view remains readable after refusal"
    );
    assert_eq!(
        before_refusal.keyword_at(0).as_deref(),
        Some("private-0"),
        "previously shared private views remain readable after refusal"
    );
}
