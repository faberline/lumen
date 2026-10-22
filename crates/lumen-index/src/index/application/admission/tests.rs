use std::collections::BTreeMap;

use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::{AdmissionError, ChangeBudget};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{FieldValue, IndexItem, IndexRequest};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, FieldSpec};

fn engine(budget: &ChangeBudget) -> Engine {
    let mut engine = Engine::new();
    engine.changes.budget = budget.clone();
    engine.changes.owner = budget.owner();
    // These ownership units start from an existing schema baseline. The
    // direct API admission case below exercises the charged write route.
    engine
        .create_collection_inner(
            "c",
            CreateCollectionRequest {
                fields: BTreeMap::from([(
                    "k".into(),
                    serde_json::from_str::<FieldSpec>(r#"{"type":"keyword"}"#).unwrap(),
                )]),
            },
        )
        .unwrap();
    engine
}

fn entry() -> RaftLogEntry {
    RaftLogEntry::Index {
        collection_id: "c".into(),
        req: IndexRequest {
            items: vec![IndexItem {
                external_id: "one".into(),
                field: "k".into(),
                value: FieldValue::String("payload".repeat(100)),
                version: None,
            }],
            request_id: None,
        },
    }
}

fn apply(engine: &Engine, entry: RaftLogEntry) {
    let reservation = engine.try_reserve_record(&entry, 0).unwrap();
    let mut guard = engine
        .begin_admitted_record(entry, reservation)
        .ok()
        .unwrap();
    engine.apply_prepared_raft_entry(&mut guard).unwrap();
}

/// Poison the schema/coverage lock so `estimate_record_cost` and
/// `estimate_record_exact_default_ngram_cost` cannot resolve context and
/// return `RecordEstimate::Retain` (#lumen-pending-change-capacity-door).
fn poison_state(engine: &Engine) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = engine.state.write().unwrap();
        panic!("poison state for test");
    }));
    assert!(result.is_err(), "poisoning thread must panic");
    assert!(engine.state.is_poisoned());
}

/// Door pricing that needs context it cannot resolve (a poisoned lock
/// stands in for "unknown schema/live coverage") must never publish a
/// record with zero reservation. It must reserve at least the
/// raw+transport bound apply already knows how to charge — the same
/// bound `raft_sm::decode_admitted` reserves before it decodes — so a
/// later local `submit()` never reaches the WAL delivery loop's
/// unbounded `wait_reserve_record_ram` with no reservation to grow.
#[test]
fn try_reserve_record_reserves_the_minimal_bound_when_pricing_needs_context() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let engine = engine(&budget);
    poison_state(&engine);

    let entry = entry();
    let raw = Engine::record_owned_bytes(&entry).unwrap();
    let reservation = engine
        .try_reserve_record(&entry, 0)
        .unwrap_or_else(|error| {
            panic!(
                "a record whose exact price needs unavailable context must still be \
                 reserved at its known minimal bound before publication, not left \
                 unreserved: {error:?}"
            )
        });
    assert_eq!(
        reservation.bytes(),
        raw,
        "the minimal reservation must match the raw+transport bound, the same \
             floor apply uses before it can price exactly"
    );
}

/// The same context-missing pricing failure, but with capacity already
/// full even for the minimal raw+transport bound: the door must refuse
/// with a capacity error (#429 upstream) before publication rather than
/// publish the record with no reservation and let it hang later.
#[test]
fn try_reserve_record_refuses_before_publication_when_even_the_minimal_bound_is_full() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let engine = engine(&budget);
    poison_state(&engine);

    let remaining = (1 << 20) - budget.snapshot().total;
    let competing = budget.owner();
    let _held = competing.try_reserve(remaining).unwrap();

    let entry = entry();
    let error = match engine.try_reserve_record(&entry, 0) {
        Ok(_) => panic!("a full budget must not silently publish an unreserved record"),
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            RecordAdmissionError::Capacity(AdmissionError::Full { .. })
        ),
        "a Full minimal bound must classify as capacity, not a bare domain error \
             a caller would otherwise treat as safe to publish unreserved: {error:?}"
    );
}

mod dispatch;

mod ngram;

mod ownership;
