use crate::index::application::admission::tests::engine;
use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};

// A generous test-only workspace margin keeps this admission test independent
// of the private table layout. The available budget still stays below the old
// all-window estimate.
const NGRAM_TEST_WORKSPACE_HEADROOM: usize = 64 * 1024;

#[test]
fn repeated_default_ngram_admits_exact_bound_plus_reserved_workspace() {
    const HARD: usize = 512 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = engine(&budget);
    engine
        .add_field(
            "c",
            "body",
            serde_json::from_str(r#"{"type":"text","analyzer":"ngram"}"#).unwrap(),
        )
        .unwrap();
    let input = "durable search token ".repeat(16);
    let entry = RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "repeated".into(),
                field: "body".into(),
                value: FieldValue::String(input.clone()),
                version: None,
            }],
            request_id: None,
        },
    };
    let old_bound = engine.record_memory_bound(&entry, 0, false).unwrap();

    // Derive the intended Text inputs independently from the shared stream.
    // Do not duplicate tokenizer windows or cost multipliers in this test.
    let mut unique = std::collections::BTreeSet::new();
    crate::index::domain::analysis::ngram_stream::stream_default_ngrams(&input, |token| {
        unique.insert(token.as_bytes().to_vec());
        Ok::<_, ()>(())
    })
    .unwrap();
    let exact_field = crate::ingest::domain::change_memory_cost::FieldCost::Text {
        distinct_terms: unique.len(),
        total_term_bytes: unique.iter().map(Vec::len).sum(),
    };
    let index = crate::ingest::domain::change_memory_cost::estimate_change(
        &crate::ingest::domain::change_memory_cost::Change::Index {
            external_id_bytes: "repeated".len(),
            new_document: true,
            field: exact_field,
            volatile_metadata_bytes: 0,
        },
    )
    .unwrap();
    let direct = crate::ingest::domain::change_memory_cost::estimate_change(
        &crate::ingest::domain::change_memory_cost::Change::Direct {
            // Existing record-cost contract: field bytes plus direct metadata.
            metadata_bytes: "body".len() + 96,
        },
    )
    .unwrap();
    let exact_bound = Engine::record_owned_bytes(&entry).unwrap() + index.total() + direct.total();
    let available = exact_bound + NGRAM_TEST_WORKSPACE_HEADROOM;
    assert!(
        available < old_bound,
        "fixture including workspace must fit below old all-window bound"
    );
    let baseline = budget.snapshot().total;
    assert!(baseline + available < HARD);
    let held = budget
        .owner()
        .try_reserve(HARD - baseline - available)
        .unwrap();

    // Baseline fails here because it reserves `old_bound`. The implemented
    // route must reserve raw+workspace, exact-price under that reservation,
    // then retain `exact_bound` only.
    let reservation = engine.try_reserve_record(&entry, 0);
    assert!(
        reservation.is_ok(),
        "exact Ngram admission must fit below old bound"
    );
    drop(reservation);
    drop(held);
}
fn ngram_entry(engine: &Engine) -> RaftLogEntry {
    engine
        .add_field(
            "c",
            "body",
            serde_json::from_str(r#"{"type":"text","analyzer":"ngram"}"#).unwrap(),
        )
        .unwrap();
    RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "ngram-document".into(),
                field: "body".into(),
                value: FieldValue::String("durable search token ".repeat(16)),
                version: None,
            }],
            request_id: None,
        },
    }
}

#[test]
fn exact_ngram_local_admission_reserves_full_normalized_cost_before_return() {
    const HARD: usize = 512 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = engine(&budget);
    let entry = ngram_entry(&engine);
    let accepted = engine.try_reserve_record(&entry, 0).unwrap();
    let full = accepted.bytes();
    let floor = Engine::record_owned_bytes(&entry).unwrap()
        + crate::ingest::domain::change_record_cost::ngram_distinct_table::DEFAULT_NGRAM_COST_WORKSPACE_BYTES;
    assert!(
        full > floor + 1,
        "raw input and workspace alone cannot cover normalized changes"
    );
    drop(accepted);
    let baseline = budget.snapshot().total;
    let held = budget
        .owner()
        .try_reserve(HARD - baseline - floor - 1)
        .unwrap();
    let before = budget.snapshot();
    let result = engine.try_reserve_record(&entry, 0);
    assert!(
        matches!(
            result,
            Err(RecordAdmissionError::Capacity(AdmissionError::Full { .. }))
        ),
        "local admission must refuse before publication when only raw input and workspace fit"
    );
    assert_eq!(
        budget.snapshot().total,
        before.total,
        "failed admission must release all temporary reservation"
    );
    assert_eq!(budget.snapshot().reserved, before.reserved);
    assert!(engine.state.read().unwrap().collections["c"]
        .interner
        .id("ngram-document")
        .is_none());
    drop(held);
}

#[test]
fn exact_ngram_decoded_local_admission_prices_changes_before_publication() {
    const HARD: usize = 512 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = engine(&budget);
    let entry = ngram_entry(&engine);
    let raw = Engine::record_owned_bytes(&entry).unwrap();
    let workspace = crate::ingest::domain::change_record_cost::ngram_distinct_table::DEFAULT_NGRAM_COST_WORKSPACE_BYTES;
    let accepted = engine.try_reserve_record(&entry, 0).unwrap();
    assert!(accepted.bytes() > raw + workspace + 1);
    drop(accepted);
    let baseline = budget.snapshot().total;
    let request = engine.record_ram_request(&entry, 0).unwrap();
    let mut reserved = engine.try_reserve_record_ram(&request).unwrap();
    let held = budget
        .owner()
        .try_reserve(HARD - budget.snapshot().total - workspace - 1)
        .unwrap();
    let held_bytes = held.bytes();
    let result = engine.price_decoded_record(&entry, &mut reserved, raw, 0, true);
    assert!(matches!(result, Err(RecordAdmissionError::Capacity(AdmissionError::Full { .. }))),
        "decoded local admission must not return success after reserving only decoder and table bytes");
    drop(reserved);
    assert_eq!(budget.snapshot().total, baseline + held_bytes);
    assert!(engine.state.read().unwrap().collections["c"]
        .interner
        .id("ngram-document")
        .is_none());
    drop(held);
}

#[test]
fn exact_ngram_committed_workspace_wait_retains_source_and_releases_scratch_before_apply() {
    const HARD: usize = 512 * 1024;
    let budget = ChangeBudget::with_hard_limit(HARD);
    let engine = engine(&budget);
    let entry = ngram_entry(&engine);
    let raw = Engine::record_owned_bytes(&entry).unwrap();
    let workspace = crate::ingest::domain::change_record_cost::ngram_distinct_table::DEFAULT_NGRAM_COST_WORKSPACE_BYTES;
    let accepted = engine.try_reserve_record(&entry, 0).unwrap();
    let final_cost = accepted.bytes() - workspace;
    drop(accepted);
    let baseline = budget.snapshot().total;
    let request = engine.record_ram_request(&entry, 0).unwrap();
    let reserved = engine.try_reserve_record_ram(&request).unwrap();
    assert_eq!(reserved.bytes(), raw);
    let held = budget
        .owner()
        .try_reserve(HARD - budget.snapshot().total - workspace + 1)
        .unwrap();
    let mut reprice = engine
        .begin_admitted_record(entry, reserved)
        .err()
        .expect("unreserved table must return source ownership before it is constructed");
    assert_eq!(reprice.required, Some(raw + workspace));
    assert_eq!(reprice.reservation.bytes(), raw);
    assert_eq!(reprice.reservation.ngram_cost_workspace_bytes, 0);
    assert!(matches!(
        reprice.error,
        RecordAdmissionError::Capacity(AdmissionError::Full { .. })
    ));
    // A capture can start while the caller owns the returned source. No
    // capacity wait may retain the apply boundary.
    let snapshot = engine.snapshot().unwrap();
    drop(snapshot);
    drop(held);
    reprice
        .reservation
        .try_grow_to(reprice.required.unwrap())
        .unwrap();
    let mut reprice = engine
        .begin_admitted_record(reprice.entry, reprice.reservation)
        .err()
        .expect("raw plus table still needs the whole normalized change price");
    assert_eq!(reprice.required, Some(final_cost + workspace));
    reprice
        .reservation
        .try_grow_to(reprice.required.unwrap())
        .unwrap();
    let mut guard = engine
        .begin_admitted_record(reprice.entry, reprice.reservation)
        .ok()
        .unwrap();
    assert_eq!(
        guard.charge.bytes(),
        final_cost,
        "temporary cost table must not remain in retained changes"
    );
    assert_eq!(budget.snapshot().reserved, 0);
    assert_eq!(budget.snapshot().total, baseline + final_cost);
    engine.apply_prepared_raft_entry(&mut guard).unwrap();
    drop(guard);
    assert!(engine.state.read().unwrap().collections["c"]
        .interner
        .id("ngram-document")
        .is_some());
}
