use std::path::Path;

use anyhow::{anyhow, bail, ensure};

use crate::index::application::engine::index::MAX_INDEX_ITEMS;
use crate::index::infrastructure::committed_scalar_files::{
    prepare, prepare_validated_fields, PreparedScalarFile,
};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::FieldType;

fn command(rows: Vec<(&str, FieldValue)>) -> Vec<u8> {
    WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".to_owned(),
        req: IndexRequest {
            request_id: None,
            items: rows
                .into_iter()
                .map(|(id, value)| IndexItem {
                    external_id: id.to_owned(),
                    field: "value".to_owned(),
                    value,
                    version: None,
                })
                .collect(),
        },
    })
    .encode()
    .unwrap()
}
fn staged(bytes: &[u8], rows: &[usize], kind: FieldType, parent: &Path) -> PreparedScalarFile {
    prepare(
        &FastIndexScanner::parse(bytes).unwrap(),
        "value",
        rows,
        kind,
        42,
        parent,
        |_| Ok(()),
    )
    .unwrap()
}
#[test]
fn selected_keyword_rows_keep_sparse_identity_and_only_the_selected_last_value() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![
        ("doc-90000000", FieldValue::String("old".into())),
        ("skip", FieldValue::String("discard".into())),
        ("doc-90000000", FieldValue::String("new".into())),
        ("doc-7", FieldValue::String("".into())),
    ]);
    let output = staged(&bytes, &[3, 2], FieldType::Keyword, root.path());
    assert_eq!(output.external_ids, ["doc-7", "doc-90000000"]);
    assert_eq!(output.item_ordinals, [3, 2]);
    assert_eq!(output.reader.n_docs(), 2);
    assert_eq!(output.reader.keyword_at(0).as_deref(), Some(""));
    assert_eq!(output.reader.keyword_at(1).as_deref(), Some("new"));
    assert_eq!(
        output.reader.keyword_postings("new"),
        Some([1].into_iter().collect())
    );
    assert!(output.reader.keyword_postings("old").is_none());
}
#[test]
fn selected_sets_are_sorted_unique_and_keep_empty_presence() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![
        (
            "one",
            FieldValue::StringList(vec!["z".into(), "a".into(), "z".into(), "".into()]),
        ),
        ("two", FieldValue::StringList(vec![])),
    ]);
    let output = staged(&bytes, &[1, 0], FieldType::Set, root.path());
    assert_eq!(output.reader.set_at(0), Some(vec![]));
    assert_eq!(
        output.reader.set_at(1),
        Some(vec!["".into(), "a".into(), "z".into()])
    );
    assert_eq!(
        output.reader.set_postings("z"),
        Some([1].into_iter().collect())
    );
}
#[test]
fn selected_numbers_keep_row_order_and_numeric_key_order() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![
        ("a", FieldValue::Number(-2.5)),
        ("b", FieldValue::Number(9.0)),
        ("c", FieldValue::Number(0.0)),
    ]);
    let output = staged(&bytes, &[1, 0], FieldType::Number, root.path());
    assert_eq!(output.reader.number_at(0), Some(9.0));
    assert_eq!(output.reader.number_at(1), Some(-2.5));
}
#[test]
fn invalid_selection_and_refused_reservation_create_no_files() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![
        ("same", FieldValue::String("one".into())),
        ("same", FieldValue::String("two".into())),
    ]);
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    for (rows, kind, field) in [
        (vec![0], FieldType::Number, "value"),
        (vec![0, 0], FieldType::Keyword, "value"),
        (vec![2], FieldType::Keyword, "value"),
        (vec![0, 1], FieldType::Keyword, "value"),
        (vec![0], FieldType::Keyword, "other"),
    ] {
        assert!(prepare(&scanner, field, &rows, kind, 1, root.path(), |_| Ok(())).is_err());
    }
    assert!(prepare(
        &scanner,
        "value",
        &[0],
        FieldType::Keyword,
        1,
        root.path(),
        |_| Err(anyhow!("reservation refused"))
    )
    .is_err());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
#[test]
fn reader_reservation_refusal_removes_private_output_before_mmap() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![("one", FieldValue::String("value".into()))]);
    let mut calls = 0;
    SegmentReader::reset_owned_stage_open_calls();
    let outcome = prepare(
        &FastIndexScanner::parse(&bytes).unwrap(),
        "value",
        &[0],
        FieldType::Keyword,
        1,
        root.path(),
        |_| {
            calls += 1;
            if calls == 2 {
                bail!("reader reservation refused")
            }
            Ok(())
        },
    );
    assert!(outcome.is_err());
    assert_eq!(calls, 2);
    assert_eq!(SegmentReader::owned_stage_open_calls(), 0);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
#[test]
fn stage_file_lives_until_last_reader_reference_and_same_size_fields_do_not_collide() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![("one", FieldValue::String("value".into()))]);
    let first = staged(&bytes, &[0], FieldType::Keyword, root.path());
    let second = staged(&bytes, &[0], FieldType::Keyword, root.path());
    let path = first.reader.owned_stage_dir().unwrap().to_owned();
    assert_ne!(Some(path.as_path()), second.reader.owned_stage_dir());
    let held = first.reader.clone();
    drop(first);
    assert!(path.join("field.lseg").exists());
    assert_eq!(held.keyword_at(0).as_deref(), Some("value"));
    drop(held);
    assert!(!path.exists());
    assert!(second.reader.owned_stage_dir().unwrap().exists());
}
#[test]
fn a_real_1000_item_oversized_source_uses_bounded_reservations_and_sparse_files() {
    let root = tempfile::tempdir().unwrap();
    let items = (0..1000)
        .map(|ordinal| IndexItem {
            external_id: format!("doc-{ordinal}"),
            field: "value".into(),
            version: None,
            value: FieldValue::String(format!("{ordinal:04}-{}", "x".repeat(270 * 1024))),
        })
        .collect();
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: None,
            items,
        },
    })
    .encode()
    .unwrap();
    assert!(bytes.len() > crate::ingest::domain::change_budget::HARD_LIMIT);
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let rows: Vec<_> = (0..1000).rev().collect();
    let mut peak = 0;
    let output = prepare(
        &scanner,
        "value",
        &rows,
        FieldType::Keyword,
        42,
        root.path(),
        |required| {
            peak = peak.max(required);
            ensure!(
                required <= crate::ingest::domain::change_budget::HARD_LIMIT,
                "private scalar reservation exceeds 256 MiB"
            );
            Ok(())
        },
    )
    .unwrap();
    assert!(peak > 0);
    assert_eq!(output.reader.n_docs(), 1000);
    for (local, ordinal) in rows.iter().enumerate() {
        assert_eq!(output.external_ids[local], format!("doc-{ordinal}"));
        let expected = format!("{ordinal:04}-{}", "x".repeat(270 * 1024));
        assert_eq!(
            output.reader.keyword_at(local as u32).as_deref(),
            Some(expected.as_str())
        );
    }
}
#[test]
fn existing_full_index_item_limit_can_prepare_small_values_within_the_budget() {
    let root = tempfile::tempdir().unwrap();
    let items = (0..MAX_INDEX_ITEMS)
        .map(|ordinal| IndexItem {
            external_id: format!("doc-{ordinal}"),
            field: "value".into(),
            version: None,
            value: FieldValue::String("shared".into()),
        })
        .collect();
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: None,
            items,
        },
    })
    .encode()
    .unwrap();
    let rows: Vec<_> = (0..MAX_INDEX_ITEMS).collect();
    let output = prepare(
        &FastIndexScanner::parse(&bytes).unwrap(),
        "value",
        &rows,
        FieldType::Keyword,
        42,
        root.path(),
        |required| {
            ensure!(
                required <= crate::ingest::domain::change_budget::HARD_LIMIT,
                "existing full Index item limit must remain reservable for small values"
            );
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(output.reader.n_docs() as usize, MAX_INDEX_ITEMS);
    assert_eq!(
        output.reader.keyword_postings("shared").unwrap().len() as usize,
        MAX_INDEX_ITEMS
    );
}

#[test]
fn validated_replace_fields_can_exceed_index_limit_while_index_prepare_refuses() {
    const REPLACE_DOCUMENTS: usize = 32;
    const FIELDS_PER_DOCUMENT: usize = MAX_INDEX_ITEMS / REPLACE_DOCUMENTS + 1;
    let root = tempfile::tempdir().unwrap();
    let count = REPLACE_DOCUMENTS * FIELDS_PER_DOCUMENT;
    let bytes = WalRecord::new(RaftLogEntry::Index {
        collection_id: "docs".into(),
        req: IndexRequest {
            request_id: None,
            items: (0..count)
                .map(|ordinal| IndexItem {
                    external_id: format!("doc-{}", ordinal / FIELDS_PER_DOCUMENT),
                    field: format!("field-{}", ordinal % FIELDS_PER_DOCUMENT),
                    value: FieldValue::String("small".into()),
                    version: None,
                })
                .collect(),
        },
    })
    .encode()
    .unwrap();
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let rows: Vec<_> = (0..REPLACE_DOCUMENTS)
        .map(|document| document * FIELDS_PER_DOCUMENT)
        .collect();

    let rejected = prepare(
        &scanner,
        "field-0",
        &rows,
        FieldType::Keyword,
        42,
        root.path(),
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(rejected.to_string().contains("Index item limit"));

    let prepared = prepare_validated_fields(
        &scanner,
        "field-0",
        &rows,
        FieldType::Keyword,
        42,
        root.path(),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(prepared.reader.n_docs() as usize, REPLACE_DOCUMENTS);
}

#[test]
fn one_264_mib_keyword_remains_borrowed_through_private_file_preparation() {
    let root = tempfile::tempdir().unwrap();
    let bytes = command(vec![(
        "large",
        FieldValue::String("x".repeat(264 * 1024 * 1024)),
    )]);
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    assert!(bytes.len() > crate::ingest::domain::change_budget::HARD_LIMIT);
    let mut peak = 0;
    let output = prepare(
        &scanner,
        "value",
        &[0],
        FieldType::Keyword,
        9,
        root.path(),
        |need| {
            peak = peak.max(need);
            ensure!(
                need <= crate::ingest::domain::change_budget::HARD_LIMIT,
                "single raw term must not need a whole-term heap reservation"
            );
            Ok(())
        },
    )
    .unwrap();
    let value = output.reader.keyword_at_cow(0).unwrap();
    assert!(
        matches!(value, std::borrow::Cow::Borrowed(_)),
        "giant staged term must remain mmap borrowed"
    );
    assert_eq!(value.len(), 264 * 1024 * 1024);
    assert!(value.as_bytes().iter().all(|&b| b == b'x'));
    assert_eq!(output.reader.keyword_postings(&value).unwrap().len(), 1);
    assert!(peak > 0);
}
