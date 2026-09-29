use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use crate::composed_segment::ComposedSegmentReader;
use crate::persistence::infrastructure::segment::stream::keyword::write_keyword_projection;
use crate::persistence::infrastructure::segment::stream::scalar_projection::ScalarProjectionScratch;
use crate::persistence::infrastructure::segment::stream::set::write_set_projection;
use crate::persistence::infrastructure::segment::stream::tests::{set_source, BorrowedKeywordRows};
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::persistence::infrastructure::segment::*;

#[test]
fn raw_scalar_dictionary_keyword_borrows_empty_and_large_terms() {
    let root = tempfile::tempdir().unwrap();
    let large = "x".repeat(256 * 1024);
    let rows = vec![Some(""), Some(large.as_str()), None, Some("a")];
    let target = root.path().join("raw-keyword.lseg");
    write_keyword_projection(
        &target,
        71,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let reader = SegmentReader::open(&target).unwrap();
    assert_eq!(reader.keyword_at(0).as_deref(), Some(""));
    assert_eq!(reader.keyword_at(1).as_deref(), Some(large.as_str()));
    assert_eq!(reader.keyword_postings(""), Some([0].into_iter().collect()));
    assert!(matches!(
        reader.dict_value(0),
        Some(std::borrow::Cow::Borrowed(""))
    ));
    assert!(matches!(
        reader.dict_value(1),
        Some(std::borrow::Cow::Borrowed(_))
    ));
    assert!(matches!(
        reader.keyword_at_cow(1),
        Some(std::borrow::Cow::Borrowed(_))
    ));
    assert!(
        matches!(
            reader.keyword_term_at_ordinal_cow(1),
            Some(std::borrow::Cow::Borrowed(_))
        ),
        "the ordinal stream must lend the large mmap term"
    );
}

#[test]
fn raw_scalar_dictionary_set_keeps_ordinal_postings() {
    let root = tempfile::tempdir().unwrap();
    let rows = vec![
        Some(vec!["".to_owned(), "b".to_owned()]),
        Some(vec!["a".to_owned()]),
        None,
    ];
    let base = set_source(&root.path().join("base"), &rows);
    let target = root.path().join("raw-set.lseg");
    write_set_projection(
        &target,
        72,
        &ComposedSegmentReader::from_base(base),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let reader = SegmentReader::open(&target).unwrap();
    assert_eq!(reader.set_at(0), Some(vec!["".to_owned(), "b".to_owned()]));
    assert_eq!(reader.set_postings("a"), Some([1].into_iter().collect()));
    assert!(matches!(
        reader.dict_value(0),
        Some(std::borrow::Cow::Borrowed(""))
    ));
    assert!(matches!(
        reader.set_member_at_cow(0, 0),
        Some(std::borrow::Cow::Borrowed(""))
    ));
}

#[test]
fn raw_scalar_dictionary_refuses_bad_offsets_utf8_and_unknown_codec() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("raw-invalid.lseg");
    let rows = vec![Some("a")];
    write_keyword_projection(
        &target,
        73,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let bytes = std::fs::read(&target).unwrap();
    let footer = Footer::from_bytes(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let start = footer.dir_offset as usize;
    let end = start + footer.dir_len as usize;
    let mut dir: Vec<ColumnRef> = ciborium::from_reader(&bytes[start..end]).unwrap();
    let offsets = dir
        .iter()
        .find(|c| c.role == ROLE_DICT_OFFSETS)
        .unwrap()
        .clone();
    let mut file = OpenOptions::new().write(true).open(&target).unwrap();
    file.seek(SeekFrom::Start(offsets.byte_offset + 8)).unwrap();
    file.write_all(&99u64.to_le_bytes()).unwrap();
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("offset"));
    // Restore a valid file, then poison raw bytes without changing its directory.
    write_keyword_projection(
        &target,
        73,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let dict = dir.iter().find(|c| c.role == ROLE_DICT).unwrap().clone();
    let mut file = OpenOptions::new().write(true).open(&target).unwrap();
    file.seek(SeekFrom::Start(dict.byte_offset)).unwrap();
    file.write_all(&[0xff]).unwrap();
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("UTF-8"));
    // Change only the CBOR directory codec byte and refresh its footer CRC.
    write_keyword_projection(
        &target,
        73,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let bytes = std::fs::read(&target).unwrap();
    let footer = Footer::from_bytes(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let start = footer.dir_offset as usize;
    let end = start + footer.dir_len as usize;
    let mut dir: Vec<ColumnRef> = ciborium::from_reader(&bytes[start..end]).unwrap();
    dir.iter_mut().find(|c| c.role == ROLE_DICT).unwrap().codec = 3;
    let mut encoded = Vec::new();
    ciborium::into_writer(&dir, &mut encoded).unwrap();
    assert_eq!(encoded.len(), footer.dir_len as usize);
    let mut changed = bytes;
    changed[start..end].copy_from_slice(&encoded);
    let mut footer = footer;
    footer.crc32 = crc32fast::hash(&encoded);
    let tail = changed.len() - FOOTER_LEN;
    changed[tail..].copy_from_slice(&footer.to_bytes());
    std::fs::write(&target, changed).unwrap();
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("unsupported segment column codec"));
}

fn rewrite_directory(path: &Path, change: impl FnOnce(&mut Vec<ColumnRef>)) {
    let bytes = std::fs::read(path).unwrap();
    let mut footer = Footer::from_bytes(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let start = footer.dir_offset as usize;
    let end = start + footer.dir_len as usize;
    let mut dir: Vec<ColumnRef> = ciborium::from_reader(&bytes[start..end]).unwrap();
    change(&mut dir);
    let mut encoded = Vec::new();
    ciborium::into_writer(&dir, &mut encoded).unwrap();
    footer.dir_len = encoded.len() as u64;
    footer.crc32 = crc32fast::hash(&encoded);
    let mut changed = bytes[..start].to_vec();
    changed.extend_from_slice(&encoded);
    changed.extend_from_slice(&footer.to_bytes());
    std::fs::write(path, changed).unwrap();
}

#[test]
fn raw_scalar_dictionary_refuses_split_codepoint_unsorted_and_duplicate_entries() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("raw-entry-validity.lseg");
    let rows = vec![Some("a"), Some("é"), Some("z")];
    write_keyword_projection(
        &target,
        74,
        &BorrowedKeywordRows(&rows),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let bytes = std::fs::read(&target).unwrap();
    let footer = Footer::from_bytes(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let dir: Vec<ColumnRef> = ciborium::from_reader(
        &bytes[footer.dir_offset as usize..(footer.dir_offset + footer.dir_len) as usize],
    )
    .unwrap();
    let offsets = dir
        .iter()
        .find(|column| column.role == ROLE_DICT_OFFSETS)
        .unwrap();
    let mut file = OpenOptions::new().write(true).open(&target).unwrap();
    // Split the two-byte UTF-8 `é` between two dictionary entries while
    // retaining a monotone table that still ends at the data length.
    file.seek(SeekFrom::Start(offsets.byte_offset + 16))
        .unwrap();
    file.write_all(&3u64.to_le_bytes()).unwrap();
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("entry is not UTF-8"));

    let sorted = vec![Some("a"), Some("b")];
    write_keyword_projection(
        &target,
        74,
        &BorrowedKeywordRows(&sorted),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let bytes = std::fs::read(&target).unwrap();
    let footer = Footer::from_bytes(&bytes[bytes.len() - FOOTER_LEN..]).unwrap();
    let dir: Vec<ColumnRef> = ciborium::from_reader(
        &bytes[footer.dir_offset as usize..(footer.dir_offset + footer.dir_len) as usize],
    )
    .unwrap();
    let dict = dir.iter().find(|column| column.role == ROLE_DICT).unwrap();
    let mut file = OpenOptions::new().write(true).open(&target).unwrap();
    file.seek(SeekFrom::Start(dict.byte_offset)).unwrap();
    file.write_all(b"ba").unwrap();
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("strictly sorted"));

    write_keyword_projection(
        &target,
        74,
        &BorrowedKeywordRows(&sorted),
        ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
    )
    .unwrap();
    let mut file = OpenOptions::new().write(true).open(&target).unwrap();
    file.seek(SeekFrom::Start(dict.byte_offset)).unwrap();
    file.write_all(b"aa").unwrap();
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("strictly sorted"));
}

#[test]
fn raw_scalar_dictionary_refuses_duplicate_and_orphan_offset_roles() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("raw-roles.lseg");
    let rows = vec![Some("a")];
    let write = || {
        write_keyword_projection(
            &target,
            75,
            &BorrowedKeywordRows(&rows),
            ScalarProjectionScratch::new(64).raw_scalar_dictionary(),
        )
        .unwrap()
    };
    write();
    rewrite_directory(&target, |dir| {
        let dict = dir
            .iter()
            .find(|column| column.role == ROLE_DICT)
            .unwrap()
            .clone();
        dir.push(dict);
    });
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("duplicate dictionary"));
    write();
    rewrite_directory(&target, |dir| dir.retain(|column| column.role != ROLE_DICT));
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("orphan raw dictionary offsets"));
    write();
    rewrite_directory(&target, |dir| {
        let offsets = dir
            .iter()
            .find(|column| column.role == ROLE_DICT_OFFSETS)
            .unwrap()
            .clone();
        dir.push(offsets);
    });
    assert!(SegmentReader::open(&target)
        .unwrap_err()
        .to_string()
        .contains("duplicate raw dictionary offsets"));
}
