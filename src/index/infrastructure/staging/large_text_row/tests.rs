use std::collections::BTreeMap;

use crate::index::infrastructure::staging::large_text_row::stage_large_whitespace_row;
use crate::persistence::infrastructure::segment::text_row_stage::TextRowStageOptions;
use crate::persistence::infrastructure::segment::SegmentReader;

use crate::index::infrastructure::staging::large_text_row::{
    set_token_failure_for_test, TokenFailure,
};

fn oracle(input: &str) -> BTreeMap<String, u32> {
    let mut terms = BTreeMap::new();
    crate::index::domain::analysis::tokenize::for_whitespace_lower_cow(input, |term| {
        *terms.entry(term.into_owned()).or_insert(0) += 1;
    });
    terms
}

#[test]
fn small_threshold_merges_two_long_casefolds_with_a_short_collision() {
    let dir = tempfile::tempdir().unwrap();
    // U+212A lowers to one-byte `k`: both first terms are long at four
    // bytes, while KKK is short, and all three must become `kkk`.
    let input = "!!!KKK!!! KKk KKK AΣ !!!";
    let final_path = dir.path().join("row.lseg");
    let row = stage_large_whitespace_row(
        input,
        &final_path,
        dir.path(),
        TextRowStageOptions {
            scratch_bytes: TextRowStageOptions::minimum_scratch_bytes() + 4096,
        },
        4,
        |_| Ok(()),
    )
    .unwrap();

    assert!(row.final_reader_metadata_bytes > 0);
    let reader = SegmentReader::open(&final_path).unwrap();
    let expected = oracle(input);
    assert_eq!(row.doc_len, 4);
    assert_eq!(reader.text_doc_len(0), 4);
    assert_eq!(reader.text_doc_count(), 1);
    for (term, tf) in expected {
        assert_eq!(reader.text_postings(&term), Some((vec![0], vec![tf])));
    }
    assert!(matches!(
        (0..reader.keyword_ordinal_count().unwrap()).find_map(|ordinal| {
            reader
                .keyword_term_at_ordinal_cow(ordinal)
                .and_then(|term| (term == "kkk").then_some(term))
        }),
        Some(std::borrow::Cow::Borrowed("kkk"))
    ));
    let names: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, vec!["row.lseg"]);
}

#[test]
fn empty_trimmed_input_is_present_with_zero_document_length() {
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("empty.lseg");
    let row = stage_large_whitespace_row(
        " !!\u{2003}... ",
        &final_path,
        dir.path(),
        TextRowStageOptions {
            scratch_bytes: TextRowStageOptions::minimum_scratch_bytes() + 4096,
        },
        4,
        |_| Ok(()),
    )
    .unwrap();
    let reader = SegmentReader::open(&final_path).unwrap();
    assert_eq!(row.doc_len, 0);
    assert!(reader.text_is_present(0));
    assert_eq!(reader.text_doc_len(0), 0);
    assert_eq!(reader.keyword_ordinal_count(), Some(0));
}

#[test]
fn refusing_a_long_handle_reservation_creates_no_private_files() {
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("never.lseg");
    let error = stage_large_whitespace_row(
        "KKK",
        &final_path,
        dir.path(),
        TextRowStageOptions {
            scratch_bytes: TextRowStageOptions::minimum_scratch_bytes() + 4096,
        },
        4,
        |_| anyhow::bail!("refuse"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("refuse"));
    assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
}

#[test]
fn normalization_and_map_failure_remove_the_whole_private_child() {
    for failure in [TokenFailure::Normalize, TokenFailure::Map] {
        let dir = tempfile::tempdir().unwrap();
        let final_path = dir.path().join("never.lseg");
        set_token_failure_for_test(Some(failure));
        let error = stage_large_whitespace_row(
            "KKK",
            &final_path,
            dir.path(),
            TextRowStageOptions {
                scratch_bytes: TextRowStageOptions::minimum_scratch_bytes() + 4096,
            },
            4,
            |_| Ok(()),
        )
        .unwrap_err();
        set_token_failure_for_test(None);
        assert!(error.to_string().contains("injected"));
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }
}
