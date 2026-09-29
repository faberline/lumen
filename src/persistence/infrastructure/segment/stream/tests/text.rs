use std::borrow::Cow;
use std::sync::Arc;

use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::stream::tests::{source, Projection};
use crate::persistence::infrastructure::segment::stream::text_projection::{
    write_text_projection, write_text_stream, TextStreamView,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment::*;

#[test]
fn text_projection_lends_a_large_raw_term_through_composed_second_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let large = "x".repeat(VAR_BLOCK_BYTES + 1);
    let base_path = dir.path().join("raw-text-base.lseg");
    let base_view = Projection {
        rows: vec![Some(3)],
        terms: vec![(large.clone(), Arc::new((vec![0], vec![3])))],
    };
    write_text_projection(&base_path, 70, &base_view).unwrap();
    let base = Arc::new(SegmentReader::open(&base_path).unwrap());
    assert!(matches!(
        base.keyword_term_at_ordinal_cow(0),
        Some(Cow::Borrowed(term)) if term.len() == large.len()
    ));

    let composed = ComposedSegmentReader::from_base(base);
    let mut terms = TextStreamView::terms(&composed).unwrap();
    let term: Cow<'_, str> = terms.next().unwrap().unwrap().into();
    assert!(matches!(term, Cow::Borrowed(term) if term.len() == large.len()));
    assert!(terms.next().is_none());

    let target = dir.path().join("raw-text-second.lseg");
    write_text_stream(&target, 71, &composed).unwrap();
    let reader = SegmentReader::open(&target).unwrap();
    assert!(matches!(
        reader.keyword_term_at_ordinal_cow(0),
        Some(Cow::Borrowed(term)) if term.len() == large.len()
    ));
    assert_eq!(reader.text_postings(&large), Some((vec![0], vec![3])));
    assert_eq!(reader.text_doc_len(0), 3);
    assert_eq!(reader.text_doc_count(), 1);
    assert_eq!(reader.text_total_doc_len(), 3);
}

#[test]
fn stream_text_projection_preserves_sparse_empty_deleted_rows_and_tf() {
    let dir = tempfile::tempdir().unwrap();
    let view = Projection {
        // Row 1 is deleted/absent. Row 2 is explicitly empty.
        rows: vec![Some(3), None, Some(0), Some(2)],
        terms: vec![
            ("alpha".into(), Arc::new((vec![0, 3], vec![2, 1]))),
            ("zeta".into(), Arc::new((vec![0], vec![1]))),
        ],
    };
    let path = dir.path().join("projection.lseg");
    write_text_projection(&path, 17, &view).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.applied_seq(), 17);
    assert!(reader.text_is_present(0));
    assert!(!reader.text_is_present(1));
    assert!(reader.text_is_present(2));
    assert!(reader.text_is_present(3));
    assert_eq!(reader.text_doc_len(2), 0);
    assert_eq!(reader.text_doc_count(), 3);
    assert_eq!(reader.text_total_doc_len(), 5);
    assert_eq!(
        reader.text_postings("alpha"),
        Some((vec![0, 3], vec![2, 1]))
    );
    assert_eq!(reader.text_postings("zeta"), Some((vec![0], vec![1])));
}

#[test]
fn stream_text_writes_normal_terms_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let base = source(
        &dir.path().join("base"),
        &[Some(&[("ant", 2)]), Some(&[("bee", 1)])],
    );
    let path = dir.path().join("stream");
    write_text_stream(&path, 9, &ComposedSegmentReader::from_base(base)).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.applied_seq(), 9);
    assert_eq!(reader.text_postings("ant"), Some((vec![0], vec![2])));
    assert_eq!(reader.text_postings("bee"), Some((vec![1], vec![1])));
    assert_eq!(reader.text_doc_count(), 2);
    assert_eq!(reader.text_total_doc_len(), 3);
}

#[test]
fn stream_text_keeps_empty_present_distinct_from_absent() {
    let dir = tempfile::tempdir().unwrap();
    let base = source(&dir.path().join("base"), &[None, Some(&[])]);
    let path = dir.path().join("stream");
    write_text_stream(&path, 1, &ComposedSegmentReader::from_base(base)).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert!(!reader.text_is_present(0));
    assert!(reader.text_is_present(1));
    assert_eq!(reader.text_doc_len(0), 0);
    assert_eq!(reader.text_doc_len(1), 0);
    assert_eq!(reader.text_doc_count(), 1);
    assert_eq!(reader.text_total_doc_len(), 0);
    assert_eq!(reader.text_postings("missing"), None);
}

#[test]
fn stream_text_single_row_aligns_present_bitset() {
    let dir = tempfile::tempdir().unwrap();
    let base = source(&dir.path().join("base"), &[Some(&[])]);
    let path = dir.path().join("stream");
    write_text_stream(&path, 2, &ComposedSegmentReader::from_base(base)).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 1);
    assert!(reader.text_is_present(0));
    assert_eq!(reader.text_doc_len(0), 0);
    assert_eq!(reader.text_doc_count(), 1);
    assert_eq!(reader.text_total_doc_len(), 0);
}

#[test]
fn stream_text_zero_rows_has_empty_fixed_columns_and_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let base = source(&dir.path().join("base"), &[]);
    let path = dir.path().join("stream");
    write_text_stream(&path, 3, &ComposedSegmentReader::from_base(base)).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 0);
    assert!(!reader.text_is_present(0));
    assert_eq!(reader.text_doc_len(0), 0);
    assert_eq!(reader.text_doc_count(), 0);
    assert_eq!(reader.text_total_doc_len(), 0);
}

#[test]
fn stream_text_resolves_sparse_delta_before_seal() {
    let dir = tempfile::tempdir().unwrap();
    let base = source(
        &dir.path().join("base"),
        &[Some(&[("old", 1)]), Some(&[("stay", 2)])],
    );
    let delta = source(
        &dir.path().join("delta"),
        &[Some(&[("new", 3)]), None, Some(&[])],
    );
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![0, 1, 4])
        .unwrap();
    let path = dir.path().join("stream");
    write_text_stream(&path, 11, &view).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 5);
    assert_eq!(reader.text_postings("old"), None);
    assert_eq!(reader.text_postings("stay"), None);
    assert_eq!(reader.text_postings("new"), Some((vec![0], vec![3])));
    assert!(reader.text_is_present(0));
    assert!(!reader.text_is_present(1));
    assert!(!reader.text_is_present(2));
    assert!(reader.text_is_present(4));
    assert_eq!(reader.text_doc_len(4), 0);
    assert_eq!(reader.text_doc_count(), 2);
    assert_eq!(reader.text_total_doc_len(), 3);
}
