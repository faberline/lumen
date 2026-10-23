use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::bail;

use crate::index::domain::analysis::tokenize;
use crate::index::infrastructure::staging::staged_text_row::{
    token_stream, StagedTextRow, LAST_STAGE_DIRECTORY,
};
use crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions;
use crate::persistence::infrastructure::segment::SegmentReader;
use crate::shared_kernel::types::schema::Analyzer;

#[cfg(any(feature = "jieba", unix))]
use crate::index::infrastructure::staging::staged_text_row::StageDirectory;

#[cfg(feature = "jieba")]
use crate::index::infrastructure::analysis::jieba_disk_route;

#[cfg(unix)]
use std::fs;

fn staged(analyzer: Analyzer, input: &str) -> StagedTextRow {
    StagedTextRow::stage(
        input,
        analyzer,
        TextRowStageOptions::minimum_scratch_bytes() + 4096,
        |_| Ok(()),
    )
    .unwrap()
}

#[test]
fn ngram_receipt_has_exact_term_frequency_document_length_and_empty_row() {
    let row = staged(Analyzer::Ngram, "ababa");
    assert_eq!(row.input_bytes(), 5);
    assert_eq!(row.doc_len(), 7);
    assert_eq!(row.indexed_bytes("doc"), 22);
    let reader = row.reader();
    assert_eq!(reader.text_doc_len(0), 7);
    assert_eq!(reader.text_postings("ab"), Some((vec![0], vec![2])));
    assert_eq!(reader.text_postings("ba"), Some((vec![0], vec![2])));
    assert_eq!(reader.text_postings("aba"), Some((vec![0], vec![2])));
    assert_eq!(reader.text_postings("bab"), Some((vec![0], vec![1])));

    let empty = staged(Analyzer::Ngram, " ");
    assert_eq!(empty.doc_len(), 0);
    assert_eq!(empty.reader().text_doc_len(0), 0);
    assert_eq!(empty.reader().text_postings("missing"), None);
}

#[cfg(not(feature = "jieba"))]
#[test]
fn fallback_jieba_receipt_matches_shipped_tokens_and_empty_presence() {
    let input = "  lumen 搜尋引擎 ΣΟΣ  ";
    let row = staged(Analyzer::Jieba, input);
    let expected = tokenize::tokenize(input, Analyzer::Jieba);
    let mut frequencies = BTreeMap::<String, u32>::new();
    for token in expected {
        *frequencies.entry(token).or_default() += 1;
    }
    assert_eq!(row.doc_len(), frequencies.values().sum::<u32>());
    for (token, frequency) in frequencies {
        assert_eq!(
            row.reader().text_postings(&token),
            Some((vec![0], vec![frequency]))
        );
    }
    let empty = staged(Analyzer::Jieba, " \u{3000}\t ");
    assert_eq!(empty.doc_len(), 0);
    assert_eq!(empty.reader().text_doc_len(0), 0);
}

#[cfg(feature = "jieba")]
#[test]
fn dictionary_jieba_receipt_matches_existing_cut_and_removes_route_file() {
    let input = format!(
        "{} abc123+_#&.%-XYZ\n\tΣΟΣ İSTANBUL 👪",
        "南京市长江大桥".repeat(1000)
    );
    let row = staged(Analyzer::Jieba, &input);
    let expected = tokenize::tokenize(&input, Analyzer::Jieba);
    assert_eq!(row.doc_len(), expected.len() as u32);
    let mut frequencies = BTreeMap::<String, u32>::new();
    for token in expected {
        *frequencies.entry(token).or_default() += 1;
    }
    for (token, frequency) in frequencies {
        assert_eq!(
            row.reader().text_postings(&token),
            Some((vec![0], vec![frequency])),
            "{token}"
        );
    }
    let directory = LAST_STAGE_DIRECTORY.with(|last| last.borrow().clone().unwrap());
    assert!(directory.join("row.lseg").is_file());
    assert!(!directory.join("jieba-route.tmp").exists());
    drop(row);
    assert!(!directory.exists());
}

#[test]
fn whitespace_lower_receipt_matches_shared_unicode_tokenizer() {
    let input = "  İSTANBUL\tStraße  İSTANBUL ";
    let row = staged(Analyzer::WhitespaceLower, input);
    let expected = tokenize::tokenize(input, Analyzer::WhitespaceLower);
    let mut frequencies = BTreeMap::<String, u32>::new();
    for token in expected {
        *frequencies.entry(token).or_default() += 1;
    }
    assert_eq!(row.doc_len(), frequencies.values().sum::<u32>());
    for (token, frequency) in frequencies {
        assert_eq!(
            row.reader().text_postings(&token),
            Some((vec![0], vec![frequency]))
        );
    }
}

#[test]
fn denied_reader_reservation_removes_the_sealed_private_directory() {
    SegmentReader::reset_owned_stage_open_calls();
    let result = StagedTextRow::stage(
        "reserve after seal",
        Analyzer::WhitespaceLower,
        TextRowStageOptions::minimum_scratch_bytes() + 4096,
        |_| {
            let path = last_stage_dir();
            assert!(path.join("row.lseg").is_file());
            assert_eq!(SegmentReader::owned_stage_open_calls(), 0);
            bail!("deny reader reservation")
        },
    );
    assert!(result.is_err());
    assert!(!last_stage_dir().exists());
}

#[test]
fn reader_arc_keeps_only_its_private_stage_directory_until_last_drop() {
    let row = staged(Analyzer::WhitespaceLower, "kept reader");
    let reader = row.reader().clone();
    let path = reader.owned_stage_dir().unwrap().to_owned();
    assert!(path.is_dir());
    drop(row);
    assert!(path.is_dir());
    drop(reader);
    assert!(!path.exists());
}

#[test]
fn ngram_stream_preserves_retryable_workspace_callback_error() {
    let stream = token_stream(
        "abcd",
        Analyzer::Ngram,
        #[cfg(feature = "jieba")]
        None,
    )
    .unwrap();
    let error = stream(&mut |_| {
        Err(anyhow::Error::new(
            crate::persistence::infrastructure::segment::text_row_stage::RequiredTextRowWorkspace {
                required_bytes: TextRowStageOptions::minimum_scratch_bytes() + 1,
            },
        ))
    })
    .unwrap_err();
    assert!(
        error
            .downcast_ref::<crate::persistence::infrastructure::segment::text_row_stage::RequiredTextRowWorkspace>()
            .is_some(),
        "the caller must be able to retry a bounded Ngram workspace request"
    );
}

#[cfg(feature = "jieba")]
#[test]
fn dictionary_stream_preserves_retryable_workspace_error_and_cleans_route() {
    let directory = StageDirectory::create().unwrap();
    let route = jieba_disk_route::DiskRoute::create(directory.path()).unwrap();
    let stream = token_stream("南京市长江大桥", Analyzer::Jieba, Some(route)).unwrap();
    let error = stream(&mut |_| {
        Err(anyhow::Error::new(
            crate::persistence::infrastructure::segment::text_row_stage::RequiredTextRowWorkspace {
                required_bytes: TextRowStageOptions::minimum_scratch_bytes() + 1,
            },
        ))
    })
    .unwrap_err();
    assert!(error
        .downcast_ref::<crate::persistence::infrastructure::segment::text_row_stage::RequiredTextRowWorkspace>()
        .is_some());
    assert!(!directory.path().join("jieba-route.tmp").exists());
}

#[cfg(unix)]
#[test]
fn stage_directory_is_private_before_any_token_is_written() {
    use std::os::unix::fs::PermissionsExt;
    let directory = StageDirectory::create().unwrap();
    assert_eq!(
        fs::metadata(directory.path()).unwrap().permissions().mode() & 0o777,
        0o700
    );
}

fn last_stage_dir() -> PathBuf {
    LAST_STAGE_DIRECTORY
        .with(|last| last.borrow().clone())
        .unwrap()
}
