use std::sync::Arc;

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::infrastructure::checkpoint_projection::scalar_projection::write_checkpoint_rows;
use crate::ingest::domain::change_journal::SharedValue;
use crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment;
use crate::persistence::infrastructure::segment::number_writer::write_number_segment;
use crate::persistence::infrastructure::segment::set_writer::write_set_segment;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::shared_kernel::types::schema::FieldType;

fn owned(value: CheckpointValue) -> SharedValue<CheckpointValue> {
    SharedValue::new(Arc::new(value), None)
}
fn staged(reader: Arc<SegmentReader>, row: u32) -> SharedValue<CheckpointValue> {
    owned(CheckpointValue::StagedScalar { reader, row })
}
fn open(path: &std::path::Path) -> Arc<SegmentReader> {
    Arc::new(SegmentReader::open(path).unwrap())
}

#[test]
fn keyword_mixes_staged_replacement_and_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lseg");
    write_keyword_segment(
        &source,
        1,
        &[Some("old"), Some("keep"), Some("gone")],
        &[
            ("gone".into(), [2].into_iter().collect()),
            ("keep".into(), [1].into_iter().collect()),
            ("old".into(), [0].into_iter().collect()),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    let reader = open(&source);
    let out = dir.path().join("out.lseg");
    write_checkpoint_rows(
        &out,
        2,
        FieldType::Keyword,
        &[
            ("a".into(), Some(staged(reader.clone(), 0))),
            (
                "b".into(),
                Some(owned(CheckpointValue::Keyword("new".into()))),
            ),
            ("c".into(), None),
            ("d".into(), Some(staged(reader, 1))),
        ],
    )
    .unwrap();
    let got = open(&out);
    assert_eq!(got.keyword_at(0).as_deref(), Some("old"));
    assert_eq!(got.keyword_at(1).as_deref(), Some("new"));
    assert_eq!(got.keyword_at(2), None);
    assert_eq!(got.keyword_at(3).as_deref(), Some("keep"));
    assert_eq!(got.keyword_postings("gone"), None);
}

#[test]
fn set_keeps_empty_and_removes_duplicates_and_deleted_rows() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lseg");
    let values = vec![
        vec!["a".into(), "b".into()],
        vec![],
        vec!["a".into(), "z".into()],
    ];
    let postings = [
        ("a".into(), [0, 2].into_iter().collect()),
        ("b".into(), [0].into_iter().collect()),
        ("z".into(), [2].into_iter().collect()),
    ]
    .into_iter()
    .collect();
    write_set_segment(
        &source,
        1,
        &[
            Some(values[0].as_slice()),
            Some(values[1].as_slice()),
            Some(values[2].as_slice()),
        ],
        &postings,
    )
    .unwrap();
    let out = dir.path().join("out.lseg");
    write_checkpoint_rows(
        &out,
        2,
        FieldType::Set,
        &[
            ("a".into(), Some(staged(open(&source), 0))),
            ("b".into(), Some(staged(open(&source), 1))),
            ("c".into(), None),
            ("d".into(), Some(staged(open(&source), 2))),
        ],
    )
    .unwrap();
    let got = open(&out);
    assert_eq!(got.set_at(0), Some(vec!["a".into(), "b".into()]));
    assert_eq!(got.set_at(1), Some(vec![]));
    assert_eq!(got.set_at(2), None);
    assert_eq!(got.set_at(3), Some(vec!["a".into(), "z".into()]));
    assert_eq!(
        got.set_postings("a").unwrap().iter().collect::<Vec<_>>(),
        vec![0, 3]
    );
}

#[test]
fn number_sparse_keys_preserve_exact_numeric_order() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lseg");
    write_number_segment(&source, 1, &[Some(-3.0), None, Some(2.5)]).unwrap();
    let out = dir.path().join("out.lseg");
    write_checkpoint_rows(
        &out,
        2,
        FieldType::Number,
        &[
            ("a".into(), Some(staged(open(&source), 2))),
            ("b".into(), None),
            ("c".into(), Some(owned(CheckpointValue::Number(-4.0)))),
            ("d".into(), Some(staged(open(&source), 0))),
        ],
    )
    .unwrap();
    let got = open(&out);
    assert_eq!(got.number_at(0), Some(2.5));
    assert_eq!(got.number_at(1), None);
    assert_eq!(got.number_at(2), Some(-4.0));
    assert_eq!(got.number_at(3), Some(-3.0));
    assert_eq!(
        got.number_sorted_bits_at(0)
            .map(SortableF64::from_bits)
            .map(SortableF64::to_f64),
        Some(-4.0)
    );
}

#[test]
fn large_staged_keyword_is_read_one_dictionary_term_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lseg");
    let a = format!("a{}", "x".repeat(64 * 1024));
    let b = format!("b{}", "y".repeat(64 * 1024));
    let c = format!("c{}", "z".repeat(64 * 1024));
    let unused = format!("u{}", "q".repeat(64 * 1024));
    let postings = [
        (a.clone(), [0].into_iter().collect()),
        (b.clone(), [2].into_iter().collect()),
        (c.clone(), [3].into_iter().collect()),
        (unused.clone(), [1].into_iter().collect()),
    ]
    .into_iter()
    .collect();
    write_keyword_segment(
        &source,
        1,
        &[
            Some(a.as_str()),
            Some(unused.as_str()),
            Some(b.as_str()),
            Some(c.as_str()),
        ],
        &postings,
    )
    .unwrap();
    let out = dir.path().join("out.lseg");
    write_checkpoint_rows(
        &out,
        2,
        FieldType::Keyword,
        &[
            ("a".into(), Some(staged(open(&source), 0))),
            ("deleted".into(), None),
            ("b".into(), Some(staged(open(&source), 2))),
            ("c".into(), Some(staged(open(&source), 3))),
        ],
    )
    .unwrap();
    let got = open(&out);
    assert_eq!(got.keyword_at(0).as_deref(), Some(a.as_str()));
    assert_eq!(got.keyword_at(1), None);
    assert_eq!(got.keyword_at(2).as_deref(), Some(b.as_str()));
    assert_eq!(got.keyword_at(3).as_deref(), Some(c.as_str()));
    assert_eq!(
        got.keyword_postings(a.as_str())
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(
        got.keyword_postings(b.as_str())
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        got.keyword_postings(c.as_str())
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(got.keyword_postings(unused.as_str()), None);
}

#[test]
fn wrong_kind_staged_reader_refuses_before_output_exists() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("number.lseg");
    let out = dir.path().join("out.lseg");
    write_number_segment(&source, 1, &[Some(7.0)]).unwrap();
    let error = write_checkpoint_rows(
        &out,
        2,
        FieldType::Keyword,
        &[("a".into(), Some(staged(open(&source), 0)))],
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("staged scalar payload kind does not match field"));
    assert!(!out.exists());
}

#[test]
fn noncanonical_owned_set_refuses_before_output_exists() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.lseg");
    let error = write_checkpoint_rows(
        &out,
        2,
        FieldType::Set,
        &[(
            "a".into(),
            Some(owned(CheckpointValue::Set(vec![
                "dup".into(),
                "dup".into(),
            ]))),
        )],
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("owned Set delta values must be sorted and unique"));
    assert!(!out.exists());
}
