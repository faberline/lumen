use std::sync::Arc;

use crate::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::format::sortable_bits;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::stream::keyword::{
    write_keyword_projection, write_keyword_stream,
};
use crate::persistence::infrastructure::segment::stream::number::{
    write_number_projection, write_number_stream,
};
use crate::persistence::infrastructure::segment::stream::projection_columns::{
    projection_dictionary, projection_posting,
};
use crate::persistence::infrastructure::segment::stream::scalar_projection::{
    ScalarProjectionScratch, ScalarProjectionScratchRequired,
};
use crate::persistence::infrastructure::segment::stream::set::{
    write_set_projection, write_set_stream,
};
use crate::persistence::infrastructure::segment::stream::tests::{
    keyword_source, set_source, BorrowedKeywordRows,
};
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment::*;

#[test]
fn scalar_projection_keyword_matches_normal_writer_for_sparse_duplicates_and_large_term() {
    let dir = tempfile::tempdir().unwrap();
    let large = "x".repeat(VAR_BLOCK_BYTES + 17);
    let rows = vec![
        Some("z"),
        None,
        Some(large.as_str()),
        Some("a"),
        Some(large.as_str()),
    ];
    let base = keyword_source(&dir.path().join("base"), &rows);
    let view = ComposedSegmentReader::from_base(base.clone());
    let ordinary = dir.path().join("ordinary");
    let projected = dir.path().join("projected");
    write_keyword_stream(&ordinary, 31, &view).unwrap();
    write_keyword_projection(
        &projected,
        31,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(usize::MAX),
    )
    .unwrap();
    let left = SegmentReader::open(&ordinary).unwrap();
    let right = SegmentReader::open(&projected).unwrap();
    for id in 0..5 {
        assert_eq!(left.keyword_at(id), right.keyword_at(id));
    }
    for term in ["a", "z", large.as_str()] {
        assert_eq!(left.keyword_postings(term), right.keyword_postings(term));
    }
}

#[test]
fn scalar_projection_set_matches_normal_writer_for_sparse_empty_duplicates_and_sorted_terms() {
    let dir = tempfile::tempdir().unwrap();
    let large = "q".repeat(VAR_BLOCK_BYTES + 3);
    let rows = vec![
        Some(vec!["a".to_owned(), "z".to_owned()]),
        None,
        Some(Vec::new()),
        Some(vec!["a".to_owned(), large.clone()]),
    ];
    let base = set_source(&dir.path().join("base"), &rows);
    let view = ComposedSegmentReader::from_base(base);
    let ordinary = dir.path().join("ordinary");
    let projected = dir.path().join("projected");
    write_set_stream(&ordinary, 32, &view).unwrap();
    write_set_projection(
        &projected,
        32,
        &view,
        ScalarProjectionScratch::new(usize::MAX),
    )
    .unwrap();
    let left = SegmentReader::open(&ordinary).unwrap();
    let right = SegmentReader::open(&projected).unwrap();
    for id in 0..4 {
        assert_eq!(left.set_at(id), right.set_at(id));
    }
    for term in ["a", "z", large.as_str()] {
        assert_eq!(left.set_postings(term), right.set_postings(term));
    }
}

#[test]
fn scalar_projection_number_matches_normal_writer_for_sparse_and_sorted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("base");
    write_number_segment(
        &source,
        4,
        &[Some(-3.0), None, Some(0.0), Some(-3.0), Some(8.5)],
    )
    .unwrap();
    let view = ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&source).unwrap()));
    let ordinary = dir.path().join("ordinary");
    let projected = dir.path().join("projected");
    write_number_stream(&ordinary, 33, &view).unwrap();
    write_number_projection(
        &projected,
        33,
        &view,
        ScalarProjectionScratch::new(usize::MAX),
    )
    .unwrap();
    let left = SegmentReader::open(&ordinary).unwrap();
    let right = SegmentReader::open(&projected).unwrap();
    for id in 0..5 {
        assert_eq!(left.number_at(id), right.number_at(id));
    }
    for value in [-3.0, 0.0, 8.5] {
        assert_eq!(
            left.number_value_postings(sortable_bits(value)),
            right.number_value_postings(sortable_bits(value))
        );
    }
}

#[test]
fn scalar_projection_refuses_uncharged_var_entry_before_target_visibility() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let large = "x".repeat(VAR_BLOCK_BYTES + 17);
    let error = projection_dictionary(
        &target,
        34,
        ScalarProjectionScratch::new(VAR_BLOCK_BYTES),
        |emit| emit(&large),
    )
    .err()
    .expect("unreserved dictionary term must fail before publishing a target");
    let shortage = error
        .downcast_ref::<ScalarProjectionScratchRequired>()
        .unwrap();
    assert_eq!(shortage.kind, "dictionary term");
    assert_eq!(shortage.required, large.len());
    assert!(!target.exists());
}

#[test]
fn scalar_projection_refuses_changed_posting_replay() {
    let mut pass = false;
    let error = projection_posting(
        3,
        ScalarProjectionScratch::new(32),
        "keyword posting",
        |emit| {
            if pass {
                emit(1)?;
                emit(2)?;
            } else {
                pass = true;
                emit(1)?;
            }
            Ok(true)
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("changed between rewindable posting passes"));
}
#[test]
fn scalar_projection_refuses_same_size_changed_posting_replay() {
    let mut replay = false;
    let result = projection_posting(
        4,
        ScalarProjectionScratch::new(32),
        "keyword posting",
        |emit| {
            emit(if replay { 2 } else { 1 })?;
            replay = true;
            Ok(true)
        },
    );
    assert!(
        result.is_err(),
        "same-size posting replay must keep the exact row identity"
    );
}
#[test]
fn scalar_projection_shared_prefix_cannot_expand_a_whole_dictionary_block() {
    let root = tempfile::tempdir().unwrap();
    let terms: Vec<_> = (0..32)
        .map(|n| format!("{}-{n:04}", "x".repeat(8192)))
        .collect();
    let max = terms.iter().map(String::len).max().unwrap();
    let target = root.path().join("projection.lseg");
    let spool = projection_dictionary(&target, 1, ScalarProjectionScratch::new(max), |emit| {
        for term in &terms {
            emit(term)?;
        }
        Ok(())
    })
    .unwrap();
    let (first_block, _) = spool.reader.dict_block_at(ROLE_DICT, 0).unwrap();
    let full_bytes: usize = first_block.iter().map(Vec::len).sum();
    assert!(
        full_bytes <= VAR_BLOCK_BYTES + max,
        "projection prefix decoding must stay within one full-entry block"
    );
    assert!(first_block.len() < terms.len());
    for (ordinal, term) in terms.iter().enumerate() {
        assert_eq!(spool.dict_id(term).unwrap(), ordinal as u32);
    }
}

#[test]
fn composed_raw_keyword_projection_lends_large_row_and_term() {
    let root = tempfile::tempdir().unwrap();
    let large = "q".repeat(256 * 1024);
    let rows = vec![Some("a"), Some(large.as_str()), None];
    let source = root.path().join("raw-composed-keyword-source");
    write_keyword_projection(
        &source,
        81,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let view = ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&source).unwrap()));
    assert!(
        matches!(view.keyword_at_cow(1), Some(std::borrow::Cow::Borrowed(value)) if value.len() == large.len())
    );
    let mut terms = view.string_terms(false).unwrap();
    assert!(matches!(
        terms.next_cow().unwrap(),
        Some(std::borrow::Cow::Borrowed("a"))
    ));
    assert!(
        matches!(terms.next_cow().unwrap(), Some(std::borrow::Cow::Borrowed(value)) if value.len() == large.len())
    );
    let target = root.path().join("raw-composed-keyword-target");
    write_keyword_projection(
        &target,
        82,
        &view,
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let reader = SegmentReader::open(&target).unwrap();
    assert_eq!(reader.keyword_at(1).as_deref(), Some(large.as_str()));
    assert_eq!(
        reader.keyword_postings(large.as_str()),
        Some([1].into_iter().collect())
    );
}

#[test]
fn composed_raw_set_projection_lends_members_and_accepts_legacy_source() {
    let root = tempfile::tempdir().unwrap();
    let large = "z".repeat(256 * 1024);
    let rows = vec![
        Some(vec!["a".to_owned(), large.clone()]),
        Some(Vec::new()),
        None,
    ];
    let legacy = set_source(&root.path().join("legacy-set"), &rows);
    let legacy_view = ComposedSegmentReader::from_base(legacy);
    // LZ4 is a one-entry owned fallback, yet its output may select raw.
    assert!(matches!(
        legacy_view.set_member_at_cow(0, 1),
        Some(std::borrow::Cow::Owned(_))
    ));
    let source = root.path().join("raw-composed-set-source");
    write_set_projection(
        &source,
        83,
        &legacy_view,
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let raw_view =
        ComposedSegmentReader::from_base(Arc::new(SegmentReader::open(&source).unwrap()));
    assert!(
        matches!(raw_view.set_member_at_cow(0, 1), Some(std::borrow::Cow::Borrowed(value)) if value.len() == large.len())
    );
    let target = root.path().join("raw-composed-set-target");
    write_set_projection(
        &target,
        84,
        &raw_view,
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let reader = SegmentReader::open(&target).unwrap();
    assert_eq!(reader.set_at(0), Some(vec!["a".to_owned(), large.clone()]));
    assert_eq!(
        reader.set_postings(large.as_str()),
        Some([0].into_iter().collect())
    );
}
