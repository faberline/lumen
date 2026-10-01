use crate::persistence::infrastructure::composed_segment::replacement::compose_checkpoint_layers;
use crate::persistence::infrastructure::composed_segment::tests::{bits, keyword, text};
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::SegmentReader;
use std::sync::Arc;

#[test]
fn compaction_private_rows_keep_deletes_and_sorted_external_ids() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("z-old"), Some("a-value")]);
    let delta = keyword(&dir.path().join("delta"), &[Some("m-value"), None]);
    let (view, ids) = compose_checkpoint_layers(vec![
        (base, vec!["z".into(), "a".into()]),
        (delta, vec!["m".into(), "z".into()]),
    ])
    .unwrap();
    assert_eq!(ids, ["a", "m", "z"]);
    assert_eq!(view.n_docs(), 3);
    assert_eq!(view.keyword_at(0).as_deref(), Some("a-value"));
    assert_eq!(view.keyword_at(1).as_deref(), Some("m-value"));
    assert_eq!(
        view.keyword_at(2),
        None,
        "partial compaction must preserve the deletion"
    );
    assert!(view.keyword_postings("z-old").is_none());
    let out = dir.path().join("merged");
    crate::persistence::infrastructure::segment::stream::keyword::write_keyword_stream(
        &out, 9, &view,
    )
    .unwrap();
    let reader = SegmentReader::open(&out).unwrap();
    assert_eq!(reader.n_docs(), 3);
    assert_eq!(reader.keyword_at(0).as_deref(), Some("a-value"));
    assert_eq!(reader.keyword_at(1).as_deref(), Some("m-value"));
    assert_eq!(reader.keyword_at(2), None);
}

#[test]
fn compaction_private_rows_reject_invalid_input_maps() {
    let dir = tempfile::tempdir().unwrap();
    let reader = keyword(&dir.path().join("base"), &[Some("one"), Some("two")]);
    assert!(compose_checkpoint_layers(vec![]).is_err());
    assert!(compose_checkpoint_layers(vec![(reader.clone(), vec!["a".into()])]).is_err());
    let error =
        compose_checkpoint_layers(vec![(reader, vec!["a".into(), "a".into()])]).unwrap_err();
    assert!(error.to_string().contains("duplicate external ID"));
}

#[test]
fn mapped_keyword_base_does_not_leak_local_row_ids() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("a"), None, Some("b")]);
    let view = ComposedSegmentReader::from_mapped_base(base, vec![90, 10, 20]).unwrap();
    assert_eq!(view.n_docs(), 91);
    assert_eq!(view.keyword_at(0), None);
    assert_eq!(view.keyword_at(10), None);
    assert_eq!(view.keyword_at(90).as_deref(), Some("a"));
    assert_eq!(view.keyword_postings("b"), Some([20].into_iter().collect()));
    assert_eq!(view.keyword_df("a"), Some(1));
    assert!(view.base_reader().is_none());
    let newer = keyword(&dir.path().join("newer"), &[None, Some("c")]);
    let view = view.with_delta(newer, vec![90, 10]).unwrap();
    assert_eq!(view.keyword_postings("a"), None);
    assert_eq!(view.keyword_at(10).as_deref(), Some("c"));
    assert_eq!(view.keyword_postings("b"), Some([20].into_iter().collect()));
}

#[test]
fn mapped_text_base_sorts_ids_and_keeps_tfs_attached() {
    let dir = tempfile::tempdir().unwrap();
    let base = text(
        &dir.path().join("base"),
        &[Some(&[("same", 7)]), Some(&[("same", 3)]), None],
    );
    let view = ComposedSegmentReader::from_mapped_base(base, vec![90, 10, 20]).unwrap();
    assert_eq!(
        *view.text_postings_arc("same").unwrap(),
        (vec![10, 90], vec![3, 7])
    );
    assert_eq!(view.text_doc_len(90), 7);
    assert!(
        Arc::ptr_eq(
            &view.text_postings_arc("same").unwrap(),
            &view.text_postings_arc("same").unwrap()
        ),
        "a repeated token lookup must be served from the per-composition cache"
    );
    assert!(!view.text_is_present(0));
    assert!(!view.text_is_present(20));
    let lens = view.text_doc_lens().unwrap();
    assert_eq!(lens.len(), view.n_docs() as usize);
    for id in 0..view.n_docs() {
        assert_eq!(lens[id as usize], view.text_doc_len(id), "id {id}");
    }
    let newer = text(&dir.path().join("newer"), &[Some(&[("same", 11)]), None]);
    let view = view.with_delta(newer, vec![20, 90]).unwrap();
    assert_eq!(
        *view.text_postings_arc("same").unwrap(),
        (vec![10, 20], vec![3, 11])
    );
    assert_eq!(view.text_token_df("same"), 2);
}

#[test]
fn mapped_numeric_base_translates_ranges_and_materialized_values() {
    let dir = tempfile::tempdir().unwrap();
    let bp = dir.path().join("base");
    write_number_segment(&bp, 1, &[Some(3.0), Some(-2.0), None]).unwrap();
    let view = ComposedSegmentReader::from_mapped_base(
        Arc::new(SegmentReader::open(&bp).unwrap()),
        vec![90, 10, 20],
    )
    .unwrap();
    assert_eq!(view.number_at(0), None);
    assert_eq!(view.number_at(90), Some(3.0));
    assert_eq!(
        view.number_range(None, None),
        Some([10, 90].into_iter().collect())
    );
    assert_eq!(
        view.number_range(Some((bits(0.0), true)), None),
        Some([90].into_iter().collect())
    );
    assert_eq!(view.number_range_df(None, None), Some(2));
    assert_eq!(view.number_range_distinct_count(None, None), Some(2));
    assert_eq!(
        view.number_values_all().unwrap(),
        vec![
            (bits(-2.0), [10].into_iter().collect()),
            (bits(3.0), [90].into_iter().collect())
        ]
    );
}

#[test]
fn sparse_keyword_deltas_hide_older_values_and_translate_postings() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("old"), Some("same")]);
    let one = keyword(&dir.path().join("one"), &[Some("new"), Some("same"), None]);
    let two = keyword(&dir.path().join("two"), &[None, Some("last")]);
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(one, vec![1_000_000, 1, 0])
        .unwrap()
        .with_delta(two, vec![1, 1_000_000])
        .unwrap();
    assert_eq!(view.n_docs(), 1_000_001);
    assert_eq!(view.keyword_at(0), None);
    assert_eq!(view.keyword_at(1), None);
    assert_eq!(view.keyword_at(1_000_000), Some("last".to_owned()));
    for stale in ["old", "same", "new"] {
        assert_eq!(view.keyword_postings(stale), None);
    }
    assert_eq!(
        view.keyword_postings("last"),
        Some([1_000_000].into_iter().collect())
    );
    assert_eq!(
        view.layers
            .iter()
            .map(|layer| layer.ids.len())
            .sum::<usize>(),
        5
    );
    assert!(view.base_reader().is_none());
    crate::persistence::infrastructure::segment::DICTIONARY_SEARCHES.with(|count| count.set(0));
    assert_eq!(
        view.keyword_terms_all().unwrap(),
        vec![("last".to_owned(), [1_000_000].into_iter().collect())]
    );
    crate::persistence::infrastructure::segment::DICTIONARY_SEARCHES.with(|count| {
        assert_eq!(
            count.get(),
            0,
            "enumeration must reuse known dictionary ordinals instead of repeating term searches"
        )
    });
}
