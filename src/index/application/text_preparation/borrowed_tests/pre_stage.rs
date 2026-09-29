use std::collections::BTreeMap;

use crate::index::application::admission;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation::borrowed_tests::{encode, spec};
use crate::index::application::text_preparation::{
    borrowed_text_metadata_bound, lowercase_token_workspace_bound, TEXT_SCRATCH_BYTES,
};
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::types::document::{FieldValue, IndexItem};
use crate::shared_kernel::types::schema::{Analyzer, CreateCollectionRequest, FieldType};
use crate::storage::staged_text_row;

// Append inside `#[cfg(test)] mod borrowed_tests` in
// src/storage/text_preparation.rs.
// This adds an analyzer-selecting helper; existing test helpers stay unchanged.

fn engine_with_analyzer(analyzer: Analyzer) -> Engine {
    let mut schema = BTreeMap::new();
    schema.insert("body".to_owned(), spec(FieldType::Text, Some(analyzer)));
    let engine = Engine::new();
    engine
        .create_collection("docs", CreateCollectionRequest { fields: schema })
        .unwrap();
    engine
}

fn borrowed_pre_stage_reservation(
    engine: &Engine,
    scanner: &FastIndexScanner<'_>,
) -> admission::record_reservation::RecordReservation {
    // This is the current committed Text admission: two metadata populations
    // plus the fixed row-writer workspace, with no normalization or route
    // workspace. The target guard must reject it before stage creation.
    let metadata = borrowed_text_metadata_bound(scanner)
        .unwrap()
        .checked_mul(2)
        .unwrap();
    let bytes = metadata.checked_add(TEXT_SCRATCH_BYTES).unwrap();
    engine
        .wait_reserve_record_ram(&engine.record_ram_request_from_bound(bytes, 0))
        .unwrap()
}

#[test]
fn borrowed_unicode_single_token_requires_pre_stage_workspace_before_staging() {
    // One token only. Do not turn this into many small tokens: the required
    // transient allocation is the largest normalized token, not whole input.
    let input = "İ".repeat(3_000_000);
    let bytes = encode(
        vec![IndexItem {
            external_id: "one".into(),
            field: "body".into(),
            value: FieldValue::String(input),
            version: None,
        }],
        None,
    );
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let engine = engine_with_analyzer(Analyzer::WhitespaceLower);
    let mut reservation = borrowed_pre_stage_reservation(&engine, &scanner);

    let before_stage = staged_text_row::last_stage_directory_for_test();
    let result = engine.prepare_borrowed_text_rows(&scanner, &mut reservation);
    assert_eq!(
        staged_text_row::last_stage_directory_for_test(),
        before_stage,
        "the pre-stage guard must refuse before StageDirectory::create"
    );
    let error = result
        .err()
        .expect("pre-stage lowercase workspace must be reserved before staging starts");
    assert!(
        error
            .to_string()
            .contains("borrowed Text pre-stage workspace"),
        "the refusal must be the pre-stage reservation guard: {error:#}"
    );
}

#[cfg(feature = "jieba")]
#[test]
fn borrowed_dictionary_jieba_requires_route_cache_before_staging() {
    let bytes = encode(
        vec![IndexItem {
            external_id: "one".into(),
            field: "body".into(),
            value: FieldValue::String("南京市长江大桥".into()),
            version: None,
        }],
        None,
    );
    let scanner = FastIndexScanner::parse(&bytes).unwrap();
    let engine = engine_with_analyzer(Analyzer::Jieba);
    let mut reservation = borrowed_pre_stage_reservation(&engine, &scanner);

    let before_stage = staged_text_row::last_stage_directory_for_test();
    let result = engine.prepare_borrowed_text_rows(&scanner, &mut reservation);
    assert_eq!(
        staged_text_row::last_stage_directory_for_test(),
        before_stage,
        "the pre-stage guard must refuse before StageDirectory::create or DiskRoute::create"
    );
    let error = result
        .err()
        .expect("the two-page Jieba route cache must be reserved before staging starts");
    assert!(
        error
            .to_string()
            .contains("borrowed Text pre-stage workspace"),
        "the refusal must happen before DiskRoute allocation: {error:#}"
    );
}

// Append inside `borrowed_tests` after the implementation candidate.
#[test]
fn lowercase_pre_stage_bound_is_limited_to_the_largest_token() {
    let input = "İ ".repeat(4096);
    let bound = lowercase_token_workspace_bound(&input).unwrap();
    assert!(
        bound < input.len(),
        "many small words must not price whole input"
    );
    assert!(
        bound >= "İ".len() * 3,
        "one Unicode-lowercase token remains priced"
    );
}

// Append inside `#[cfg(test)] mod borrowed_tests` in
// src/storage/text_preparation.rs.
//
// The expected charge uses only the reservation baseline and the stage receipt.
// It deliberately does not duplicate any large-row helper workspace formula.
#[test]
fn repeated_large_rows_release_transient_stage_charge_before_the_next_row() {
    let engine = engine_with_analyzer(Analyzer::WhitespaceLower);
    let baseline = TEXT_SCRATCH_BYTES;
    let mut reservation = engine
        .wait_reserve_record_ram(&engine.record_ram_request_from_bound(baseline, 0))
        .unwrap();
    let starting_bytes = reservation.bytes();
    let input = "İ".repeat(65_537); // one source token, above 64 KiB

    for ordinal in 0..3 {
        let (row, reader_bytes, scratch_growth) = engine
            .stage_text_row(&input, Analyzer::WhitespaceLower, &mut reservation)
            .unwrap();
        let during_stage = starting_bytes
            .checked_add(reader_bytes)
            .and_then(|bytes| bytes.checked_add(scratch_growth))
            .unwrap();
        assert_eq!(
            reservation.bytes(),
            during_stage,
            "row {ordinal} retains only its reader receipt and any explicit scratch retry"
        );
        assert_eq!(row.doc_len(), 1);

        // The caller owns the staged reader until it is no longer needed. Once
        // it drops, only the receipt-reported charges may remain to release.
        drop(row);
        reservation
            .release_preparation_workspace(reader_bytes + scratch_growth)
            .unwrap();
        assert_eq!(
            reservation.bytes(),
            starting_bytes,
            "row {ordinal} must not carry private helper workspace into row {}",
            ordinal + 1
        );
    }
}
