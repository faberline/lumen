use crate::persistence::infrastructure::composed_segment::tests::{bits, keyword, text};
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::hash_writer::write_hash_segment;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::set_writer::write_set_segment;
use crate::persistence::infrastructure::segment::SegmentReader;
use roaring::RoaringBitmap;
use std::collections::BTreeMap;
use std::sync::Arc;

#[test]
fn string_cursor_merges_once_in_both_orders() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("a"), Some("c")]);
    let delta = keyword(
        &dir.path().join("delta"),
        &[Some("b"), Some("c"), Some("d")],
    );
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![9, 8, 7])
        .unwrap();
    for (descending, expected) in [
        (false, vec!["a", "b", "c", "d"]),
        (true, vec!["d", "c", "b", "a"]),
    ] {
        let mut cursor = view.string_terms(descending).unwrap();
        let mut result = Vec::new();
        while let Some(term) = cursor.next().unwrap() {
            result.push(term);
        }
        assert_eq!(result, expected);
        assert!(cursor.next().unwrap().is_none());
    }
}

#[test]
fn numeric_range_and_cursor_use_each_layers_rows_and_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let bp = dir.path().join("base");
    let dp = dir.path().join("delta");
    write_number_segment(&bp, 1, &[Some(-2.0), Some(4.0), Some(9.0)]).unwrap();
    write_number_segment(&dp, 2, &[Some(3.0), None, Some(4.0)]).unwrap();
    let base = Arc::new(SegmentReader::open(&bp).unwrap());
    let delta = Arc::new(SegmentReader::open(&dp).unwrap());
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![0, 2, 100])
        .unwrap();
    assert_eq!(view.number_at(0), Some(3.0));
    assert_eq!(view.number_at(2), None);
    assert_eq!(
        view.number_range(Some((bits(3.0), true)), Some((bits(4.0), true))),
        Some([0, 1, 100].into_iter().collect())
    );
    assert_eq!(
        view.number_range(Some((bits(3.0), false)), Some((bits(4.0), false))),
        Some(RoaringBitmap::new())
    );
    let mut keys = view
        .number_keys(Some((bits(3.0), true)), Some((bits(4.0), true)), true)
        .unwrap();
    assert_eq!(keys.next().unwrap(), Some(bits(4.0)));
    assert_eq!(keys.next().unwrap(), Some(bits(3.0)));
    assert_eq!(keys.next().unwrap(), None);
    assert_eq!(view.number_range_distinct_count(None, None), Some(2));
}

#[test]
fn text_tf_length_and_presence_choose_the_same_version() {
    let dir = tempfile::tempdir().unwrap();
    let base = text(
        &dir.path().join("base"),
        &[Some(&[("old", 3)]), Some(&[("shared", 2)])],
    );
    let delta = text(
        &dir.path().join("delta"),
        &[Some(&[("shared", 5), ("new", 1)]), None, Some(&[])],
    );
    let view = ComposedSegmentReader::from_base(base)
        .with_delta(delta, vec![0, 1, 50])
        .unwrap();
    assert!(view.text_postings_arc("old").is_none());
    let p = view.text_postings_arc("shared").unwrap();
    assert_eq!(*p, (vec![0], vec![5]));
    assert_eq!(view.text_token_df("shared"), 1);
    assert_eq!(view.text_doc_len(0), 6);
    assert!(!view.text_is_present(1));
    assert!(view.text_is_present(50));
    assert_eq!(view.text_doc_len(50), 0);
    let lens = view.text_doc_lens().unwrap();
    assert_eq!(lens.len(), view.n_docs() as usize);
    for id in 0..view.n_docs() {
        assert_eq!(lens[id as usize], view.text_doc_len(id), "id {id}");
    }
    assert_eq!(view.text_tokens_all().unwrap().len(), 2);
}

#[test]
fn set_and_hash_point_reads_keep_explicit_empty_and_absent_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let bp = dir.path().join("base");
    let dp = dir.path().join("delta");
    let members = vec!["x".to_owned()];
    let postings = BTreeMap::from([("x".to_owned(), [0].into_iter().collect())]);
    write_set_segment(&bp, 1, &[Some(&members)], &postings).unwrap();
    write_set_segment(&dp, 2, &[None, Some(&[])], &BTreeMap::new()).unwrap();
    let view = ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&bp).unwrap()))
        .with_delta(Arc::new(SegmentReader::open(&dp).unwrap()), vec![0, 10])
        .unwrap();
    assert_eq!(view.set_at(0), None);
    assert_eq!(view.set_at(10), Some(vec![]));
    assert_eq!(view.set_postings("x"), None);
    let hp = dir.path().join("hashbase");
    let hd = dir.path().join("hashdelta");
    write_hash_segment(&hp, 1, &[Some(99)]).unwrap();
    write_hash_segment(&hd, 2, &[None, Some(0)]).unwrap();
    let hashes = ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&hp).unwrap()))
        .with_delta(Arc::new(SegmentReader::open(&hd).unwrap()), vec![0, 30])
        .unwrap();
    assert_eq!(hashes.hash_at(0), None);
    assert_eq!(hashes.hash_at(30), Some(0));
}

#[test]
fn rejects_invalid_sparse_mapping_and_keeps_base_fast_path() {
    let dir = tempfile::tempdir().unwrap();
    let base = keyword(&dir.path().join("base"), &[Some("a"), None]);
    let view = ComposedSegmentReader::from_base(base.clone());
    assert!(view.with_delta(base.clone(), vec![0]).is_err());
    assert!(view.with_delta(base.clone(), vec![1, 1]).is_err());
    assert!(view.with_delta(base.clone(), vec![0, u32::MAX]).is_err());
    assert!(Arc::ptr_eq(view.base_reader().unwrap(), &base));
    assert_eq!(view.keyword_postings("a"), base.keyword_postings("a"));
    assert_eq!(view.keyword_at(1), base.keyword_at(1));
}
