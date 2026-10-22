use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{bail, Result};

use crate::persistence::infrastructure::segment::text_row_stage::row_var_writer::BoundedBytes;
use crate::persistence::infrastructure::segment::text_row_stage::sorted_run::{
    write_run_record, RunReader,
};
use crate::persistence::infrastructure::segment::text_row_stage::{
    stage_text_row, RequiredTextRowWorkspace, TextRowStageOptions, MIN_CODEC_BYTES, PAYLOAD_SLOTS,
    RUN_RECORD_OVERHEAD,
};
use crate::persistence::infrastructure::segment::*;

fn stage(path: &Path, tokens: &[&str], len: u32, budget: usize) -> Result<()> {
    stage_text_row(
        path,
        41,
        len,
        path.parent().unwrap(),
        TextRowStageOptions {
            scratch_bytes: budget,
        },
        Box::new(|emit| {
            for token in tokens {
                emit(token)?;
            }
            Ok(())
        }),
    )
}

#[test]
fn row_stage_round_trips_sorted_terms_and_duplicate_frequency() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    stage(
        &path,
        &["zebra", "ant", "zebra", "bee", "ant"],
        5,
        TextRowStageOptions::minimum_scratch_bytes() + 128,
    )
    .unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.applied_seq(), 41);
    assert!(reader.text_is_present(0));
    assert_eq!(reader.text_doc_len(0), 5);
    assert_eq!(reader.text_postings("ant"), Some((vec![0], vec![2])));
    assert_eq!(reader.text_postings("bee"), Some((vec![0], vec![1])));
    assert_eq!(reader.text_postings("zebra"), Some((vec![0], vec![2])));
}

#[test]
fn row_stage_merges_duplicate_terms_across_runs() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    stage(
        &path,
        &["same", "b", "same", "a", "same", "b"],
        6,
        TextRowStageOptions::minimum_scratch_bytes() + RUN_RECORD_OVERHEAD + 4,
    )
    .unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.text_postings("same"), Some((vec![0], vec![3])));
    assert_eq!(reader.text_postings("b"), Some((vec![0], vec![2])));
}

#[test]
fn row_stage_keeps_unicode_and_empty_present_row() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    stage(
        &path,
        &["台北", "ß"],
        2,
        TextRowStageOptions::minimum_scratch_bytes() + 128,
    )
    .unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.text_postings("台北"), Some((vec![0], vec![1])));
    let empty = temp.path().join("empty.lseg");
    stage(
        &empty,
        &[],
        0,
        TextRowStageOptions::minimum_scratch_bytes() + 128,
    )
    .unwrap();
    let reader = SegmentReader::open(&empty).unwrap();
    assert!(reader.text_is_present(0));
    assert_eq!(reader.text_doc_len(0), 0);
}

#[test]
fn row_stage_reprices_workspace_and_accepts_one_term_larger_than_a_var_block() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    let budget = TextRowStageOptions::minimum_scratch_bytes() + RUN_RECORD_OVERHEAD;
    let token = "x".repeat(VAR_BLOCK_BYTES + 1);
    let error = stage(&path, &[token.as_str()], 1, budget).unwrap_err();
    let required = error
        .downcast_ref::<RequiredTextRowWorkspace>()
        .unwrap()
        .required_bytes;
    assert!(required > budget);
    assert!(!path.exists());
    assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    stage(&path, &[token.as_str()], 1, required).unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.text_postings(&token), Some((vec![0], vec![1])));
}

#[test]
fn row_stage_rejects_wrong_declared_document_length() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    let error = stage(
        &path,
        &["one", "two"],
        1,
        TextRowStageOptions::minimum_scratch_bytes() + 128,
    )
    .unwrap_err();
    assert!(error.to_string().contains("document length mismatch"));
    assert!(!path.exists());
}

#[test]
fn run_reader_rejects_partial_length_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("truncated.run");
    fs::write(&path, [3u8, 0]).unwrap();
    let error = match RunReader::open(&path, 1024) {
        Ok(_) => panic!("partial prefix accepted"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("truncated text term run length prefix"));
}

#[test]
fn row_stage_rejects_too_small_budget_and_cleans_workspace() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    let error = stage(
        &path,
        &["token"],
        1,
        TextRowStageOptions::minimum_scratch_bytes() - 1,
    )
    .unwrap_err();
    assert!(error.to_string().contains("needs at least"));
    assert!(!path.exists());
    assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
}

#[test]
fn row_stage_removes_output_temp_after_rename_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("occupied");
    fs::create_dir(&path).unwrap();
    let error = stage(
        &path,
        &["term"],
        1,
        TextRowStageOptions::minimum_scratch_bytes() + 64,
    )
    .unwrap_err();
    assert!(error.to_string().contains("rename") || error.to_string().contains("Is a directory"));
    assert!(fs::read_dir(&path).unwrap().next().is_none());
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[test]
fn row_stage_removes_temporary_files_after_stream_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("row.lseg");
    let result = stage_text_row(
        &path,
        1,
        1,
        temp.path(),
        TextRowStageOptions {
            scratch_bytes: TextRowStageOptions::minimum_scratch_bytes() + 64,
        },
        Box::new(|emit| {
            emit("ok")?;
            bail!("injected stream failure")
        }),
    );
    assert!(result.unwrap_err().to_string().contains("injected"));
    assert!(!path.exists());
    assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
}
#[test]
fn disk_spooled_directory_preserves_ordinals_across_many_var_blocks() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("many.lseg");
    let tokens: Vec<_> = (0..1300).map(|i| format!("term-{i:04}")).collect();
    stage_text_row(
        &path,
        17,
        tokens.len() as u32,
        temp.path(),
        TextRowStageOptions {
            scratch_bytes: MIN_CODEC_BYTES + PAYLOAD_SLOTS * 200_000,
        },
        Box::new(|emit| {
            for token in tokens.iter().rev() {
                emit(token)?;
            }
            Ok(())
        }),
    )
    .unwrap();
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.text_doc_len(0), 1300);
    for ordinal in [0, 255, 256, 511, 512, 1024, 1299] {
        assert_eq!(
            reader.text_postings(&tokens[ordinal]),
            Some((vec![0], vec![1]))
        );
    }
    assert_eq!(
        fs::read_dir(temp.path()).unwrap().count(),
        1,
        "skip metadata and all run files are scoped scratch"
    );
}

#[test]
fn row_stage_run_rejects_payload_and_frequency_bit_rot() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("corrupt.run");
    let mut original = Vec::new();
    write_run_record(&mut original, b"term", 3).unwrap();
    // Each mutation remains structurally valid: one changes an ASCII term
    // and the other changes a positive TF. Size checks alone cannot catch it.
    for (offset, context) in [(4, "term payload"), (8, "term frequency")] {
        let mut corrupt = original.clone();
        corrupt[offset] ^= 1;
        fs::write(&path, corrupt).unwrap();
        assert!(
            RunReader::open(&path, 128).is_err(),
            "run readback must reject bit rot in {context} before publishing a Text segment"
        );
    }
}

#[test]
fn row_stage_run_rejects_empty_invalid_utf8_and_zero_frequency() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("invalid.run");
    for (term, count) in [(b"".as_slice(), 1), (&[0xff], 1), (b"valid", 0)] {
        let mut encoded = Vec::new();
        write_run_record(&mut encoded, term, count).unwrap();
        fs::write(&path, encoded).unwrap();
        assert!(
            RunReader::open(&path, 128).is_err(),
            "invalid normalized run record must fail before segment publication"
        );
    }
}

#[test]
fn codec_output_cannot_reallocate_past_its_reserved_capacity() {
    let mut bytes = BoundedBytes(Vec::with_capacity(3));
    let capacity = bytes.0.capacity();
    bytes.write_all(&[1, 2, 3]).unwrap();
    assert!(bytes.write_all(&[4]).is_err());
    assert_eq!(bytes.0.capacity(), capacity);
    assert_eq!(bytes.0, vec![1, 2, 3]);
}
