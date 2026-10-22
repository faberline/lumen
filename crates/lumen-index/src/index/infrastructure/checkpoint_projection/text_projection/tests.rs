use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::infrastructure::checkpoint_projection::text_projection::rows::RowsProjection;
use crate::index::infrastructure::checkpoint_projection::text_projection::write_checkpoint_rows;
use crate::persistence::infrastructure::segment::stream::text_projection::{
    write_text_projection, TextStreamView,
};
use crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::shared_kernel::types::schema::Analyzer;

use std::cell::Cell;

fn ordinary(
    doc_len: u32,
    terms: &[(&str, u32)],
) -> crate::ingest::domain::change_journal::SharedValue<CheckpointValue> {
    crate::ingest::domain::change_journal::SharedValue::new(
        Arc::new(CheckpointValue::Text {
            doc_len,
            tokens: terms
                .iter()
                .map(|(term, tf)| ((*term).to_owned(), *tf))
                .collect::<BTreeMap<_, _>>(),
        }),
        None,
    )
}

fn staged(input: &str) -> crate::ingest::domain::change_journal::SharedValue<CheckpointValue> {
    let row = crate::index::infrastructure::staging::staged_text_row::StagedTextRow::stage(
        input,
        Analyzer::WhitespaceLower,
        TextRowStageOptions::minimum_scratch_bytes() + 4096,
        |_| Ok(()),
    )
    .unwrap();
    crate::ingest::domain::change_journal::SharedValue::new(
        Arc::new(CheckpointValue::StagedText(Arc::new(row))),
        None,
    )
}

#[test]
fn rows_projection_merges_current_term_postings_without_rescanning_rows() {
    let rows = vec![
        (
            "zero".to_owned(),
            Some(ordinary(3, &[("alpha", 2), ("beta", 1)])),
        ),
        ("deleted".to_owned(), None),
        ("empty".to_owned(), Some(ordinary(0, &[]))),
        (
            "three".to_owned(),
            Some(ordinary(4, &[("alpha", 1), ("gamma", 3)])),
        ),
        ("four".to_owned(), Some(staged("beta beta delta"))),
    ];
    let view = RowsProjection {
        rows: &rows,
        current: RefCell::new(None),
        rows_examined: Cell::new(0),
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rows.lseg");

    write_text_projection(&path, 41, &view).unwrap();

    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(
        reader.text_postings("alpha"),
        Some((vec![0, 3], vec![2, 1]))
    );
    assert_eq!(reader.text_postings("beta"), Some((vec![0, 4], vec![1, 2])));
    assert_eq!(reader.text_postings("delta"), Some((vec![4], vec![1])));
    assert_eq!(reader.text_postings("gamma"), Some((vec![3], vec![3])));
    assert!(reader.text_is_present(0));
    assert!(!reader.text_is_present(1));
    assert!(reader.text_is_present(2));
    assert_eq!(reader.text_doc_len(2), 0);
    assert_eq!(
        view.rows_examined(),
        0,
        "streaming writer must use the current-term cache"
    );
}

#[test]
fn rows_projection_keeps_point_lookup_when_the_term_is_not_current() {
    let rows = vec![
        (
            "zero".to_owned(),
            Some(ordinary(2, &[("alpha", 1), ("zeta", 1)])),
        ),
        ("one".to_owned(), Some(ordinary(1, &[("beta", 3)]))),
    ];
    let view = RowsProjection {
        rows: &rows,
        current: RefCell::new(None),
        rows_examined: Cell::new(0),
    };

    let mut terms = view.terms().unwrap();
    assert_eq!(terms.next().unwrap().unwrap(), "alpha");
    assert_eq!(
        view.text_postings("alpha").unwrap().as_deref(),
        Some(&(vec![0], vec![1]))
    );
    assert_eq!(view.rows_examined(), 0);
    assert_eq!(
        view.text_postings("zeta").unwrap().as_deref(),
        Some(&(vec![0], vec![1]))
    );
    assert_eq!(view.rows_examined(), rows.len());
}

#[test]
fn checkpoint_rows_reject_non_text_values_before_streaming() {
    let rows = vec![(
        "wrong".to_owned(),
        Some(crate::ingest::domain::change_journal::SharedValue::new(
            Arc::new(CheckpointValue::Keyword("not text".to_owned())),
            None,
        )),
    )];
    let dir = tempfile::tempdir().unwrap();
    assert!(write_checkpoint_rows(&dir.path().join("wrong.lseg"), 1, &rows).is_err());
}
