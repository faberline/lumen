use std::sync::Arc;

use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::sortable_bits;
use crate::persistence::infrastructure::segment::hash_writer::write_hash_segment;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::stream::dictionary_spool::{
    DictionarySpool, STREAM_SPOOL_LOOKUPS,
};
use crate::persistence::infrastructure::segment::stream::hash::write_hash_stream;
use crate::persistence::infrastructure::segment::stream::keyword::write_keyword_stream;
use crate::persistence::infrastructure::segment::stream::number::write_number_stream;
use crate::persistence::infrastructure::segment::stream::set::write_set_stream;
use crate::persistence::infrastructure::segment::stream::tests::{keyword_source, set_source};
use crate::persistence::infrastructure::segment::stream::vector::write_vector_stream;
use crate::persistence::infrastructure::segment::SegmentReader;

#[test]
fn stream_keyword_resolves_layered_update_delete_and_empty_odd_rows() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword_source(&dir.path().join("base"), &[Some("old"), Some("keep"), None]);
    let delta = keyword_source(&dir.path().join("delta"), &[Some("new"), None, Some("")]);
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![0, 1, 4])
        .unwrap();
    let path = dir.path().join("stream");
    write_keyword_stream(&path, 8, &view).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 5);
    assert_eq!(reader.keyword_at(0), Some("new".to_owned()));
    assert_eq!(reader.keyword_at(1), None);
    assert_eq!(reader.keyword_at(4), Some(String::new()));
    assert_eq!(reader.keyword_postings("old"), None);
    assert_eq!(
        reader
            .keyword_postings("new")
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
}

#[test]
fn stream_set_resolves_layered_delete_and_explicit_empty_odd_rows() {
    let dir = tempfile::tempdir().unwrap();
    let base = set_source(
        &dir.path().join("base"),
        &[Some(vec!["old".to_owned()]), Some(Vec::new()), None],
    );
    let delta = set_source(
        &dir.path().join("delta"),
        &[None, Some(vec!["new".to_owned()]), Some(Vec::new())],
    );
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![0, 1, 4])
        .unwrap();
    let path = dir.path().join("stream");
    write_set_stream(&path, 8, &view).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 5);
    assert_eq!(reader.set_at(0), None);
    assert_eq!(reader.set_at(1), Some(vec!["new".to_owned()]));
    assert_eq!(reader.set_at(4), Some(Vec::new()));
    assert_eq!(reader.set_postings("old"), None);
    assert_eq!(
        reader
            .set_postings("new")
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![1]
    );
}

#[test]
fn stream_number_resolves_layered_updates_and_sorted_postings() {
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base");
    write_number_segment(&base_path, 4, &[Some(-2.0), Some(4.0), None]).unwrap();
    let delta_path = dir.path().join("delta");
    write_number_segment(&delta_path, 5, &[Some(3.0), None, Some(-1.0)]).unwrap();
    let view = ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&base_path).unwrap()))
        .with_delta(
            Arc::new(SegmentReader::open(&delta_path).unwrap()),
            vec![0, 1, 4],
        )
        .unwrap();
    let path = dir.path().join("stream");
    write_number_stream(&path, 8, &view).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 5);
    assert_eq!(reader.number_at(0), Some(3.0));
    assert_eq!(reader.number_at(1), None);
    assert_eq!(reader.number_at(4), Some(-1.0));
    assert_eq!(
        reader
            .number_value_postings(sortable_bits(-1.0))
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![4]
    );
    assert!(reader.number_value_postings(sortable_bits(4.0)).is_none());
}

#[test]
fn stream_hash_resolves_zero_delete_and_odd_rows() {
    let dir = tempfile::tempdir().unwrap();
    let base_path = dir.path().join("base");
    write_hash_segment(&base_path, 4, &[Some(7), Some(0), None]).unwrap();
    let delta_path = dir.path().join("delta");
    write_hash_segment(&delta_path, 5, &[None, Some(9), Some(0)]).unwrap();
    let view = ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&base_path).unwrap()))
        .with_delta(
            Arc::new(SegmentReader::open(&delta_path).unwrap()),
            vec![0, 1, 4],
        )
        .unwrap();
    let path = dir.path().join("stream");
    write_hash_stream(&path, 8, &view).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 5);
    assert_eq!(reader.hash_at(0), None);
    assert_eq!(reader.hash_at(1), Some(9));
    assert_eq!(reader.hash_at(4), Some(0));
}

#[test]
fn stream_keyword_large_dictionary_uses_one_binary_lookup_per_row() {
    let dir = tempfile::tempdir().unwrap();
    let values: Vec<String> = (0..257).map(|id| format!("term-{id:04}")).collect();
    let rows: Vec<Option<&str>> = values.iter().map(|value| Some(value.as_str())).collect();
    let base = keyword_source(&dir.path().join("base"), &rows);
    let path = dir.path().join("stream");
    STREAM_SPOOL_LOOKUPS.with(|lookups| lookups.set(0));
    write_keyword_stream(&path, 9, &ComposedSegmentReader::from_base(base)).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.keyword_at(0), Some("term-0000".to_owned()));
    assert_eq!(reader.keyword_at(256), Some("term-0256".to_owned()));
    // The spool has 257 distinct terms, but every forward row calls exactly
    // one SegmentReader binary lookup.  It never performs a term walk.
    STREAM_SPOOL_LOOKUPS.with(|lookups| assert_eq!(lookups.get(), 257));
}

#[test]
fn dictionary_spool_removes_only_its_created_temp() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword_source(&dir.path().join("base"), &[Some("one")]);
    let view = ComposedSegmentReader::from_base(base);
    let spool = DictionarySpool::build(&dir.path().join("target"), 9, &view, |term| {
        view.keyword_postings(term).is_some()
    })
    .unwrap();
    let path = spool.path.clone();
    assert!(path.exists());
    drop(spool);
    assert!(!path.exists());
}

#[test]
fn stream_vector_round_trips_none_zero_vector_and_odd_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stream");
    write_vector_stream(&path, 12, 3, 2, |id| {
        Ok(match id {
            0 => Some(vec![1.0, -2.0]),
            1 => None,
            2 => Some(vec![0.0, 0.0]),
            _ => unreachable!(),
        })
    })
    .unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.n_docs(), 3);
    assert_eq!(reader.vector_at(0, 2), Some(&[1.0, -2.0][..]));
    assert_eq!(reader.vector_at(1, 2), None);
    assert_eq!(reader.vector_at(2, 2), Some(&[0.0, 0.0][..]));
}

#[test]
fn stream_vector_zero_rows_does_not_call_callback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stream");
    let calls = std::cell::Cell::new(0);
    write_vector_stream(&path, 12, 0, 2, |_| {
        calls.set(calls.get() + 1);
        Ok(Some(vec![1.0, 2.0]))
    })
    .unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(calls.get(), 0);
    assert_eq!(reader.n_docs(), 0);
    assert_eq!(reader.vectors_slice(2), Some(&[][..]));
}

#[test]
fn stream_vector_bad_row_removes_its_temp_and_target() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("target");
    assert!(write_vector_stream(&path, 12, 1, 2, |_| Ok(Some(vec![1.0]))).is_err());
    assert!(!path.exists());
    assert!(std::fs::read_dir(dir.path()).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains("target.stream")));
    assert!(write_vector_stream(&path, 12, 1, 2, |_| Ok(Some(vec![f32::NAN, 1.0]))).is_err());
    assert!(!path.exists());
}
