use crate::index::application::admission::tests::{apply, engine, entry};
use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::ChangeBudget;

#[test]
fn real_row_and_frozen_file_ownership_survives_durable_publication() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let engine = engine(&budget);
    apply(&engine, entry());
    let charged = budget.snapshot().total;
    assert!(charged > 700);
    let escaped = {
        let state = engine.state.read().unwrap();
        let frozen = state.collections["c"].change_journal.freeze();
        let value = frozen.rows().next().unwrap().2.value().unwrap().clone();
        value
    };
    let root = tempfile::tempdir().unwrap();
    let store =
        crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(root.path())
            .unwrap();
    let engine = std::sync::Arc::new(engine);
    store.save(&engine, 0).unwrap();
    assert_eq!(
        budget.snapshot().total,
        charged,
        "an escaped row must retain its record charge after CURRENT"
    );
    drop(escaped);
    assert_eq!(
        budget.snapshot().total,
        0,
        "publication and last payload drop must free capacity"
    );
}

#[test]
fn frozen_payload_keeps_capacity_after_metadata_acknowledgement() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let engine = engine(&budget);
    apply(&engine, entry());
    let charged = budget.snapshot().total;
    let frozen = engine.freeze_checkpoint_collections(None).unwrap();
    assert_eq!(budget.snapshot().active, 0);
    assert_eq!(budget.snapshot().frozen, charged);
    engine.acknowledge_record_charges(&frozen.capture).unwrap();
    drop(engine);
    assert_eq!(budget.snapshot().total, charged);
    drop(frozen);
    assert_eq!(budget.snapshot().total, 0);
}

#[test]
fn a_reservation_cannot_be_applied_to_a_second_engine() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let first = engine(&budget);
    let second = engine(&budget);
    let entry = entry();
    let reservation = first.try_reserve_record(&entry, 0).unwrap();
    let charged = budget.snapshot().reserved;
    let rejected = second
        .begin_admitted_record(entry, reservation)
        .err()
        .unwrap();
    assert!(matches!(rejected.error, RecordAdmissionError::WrongEngine));
    assert_eq!(budget.snapshot().reserved, charged);
    assert!(second.state.read().unwrap().collections["c"]
        .interner
        .id("one")
        .is_none());
    drop(rejected);
    assert_eq!(budget.snapshot().reserved, 0);
}
#[test]
fn replacement_adopts_charged_candidate_and_releases_old_live_metadata() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let active = std::sync::Arc::new(engine(&budget));
    let candidate = engine(&budget);
    apply(&active, entry());
    let old = budget.snapshot().total;
    apply(&candidate, entry());
    assert!(budget.snapshot().total > old);
    active.activate_replacement(candidate).unwrap();
    assert_eq!(
        budget.snapshot().total,
        old,
        "candidate remains charged; replaced live state has dropped"
    );
    let root = tempfile::tempdir().unwrap();
    let store =
        crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore::new(root.path())
            .unwrap();
    store.save(&active, 0).unwrap();
    assert_eq!(
        budget.snapshot().total,
        0,
        "live publication releases imported candidate metadata and row handles"
    );
}

#[test]
fn restore_preserves_pending_reservation_and_retired_frozen_payload() {
    let budget = ChangeBudget::with_hard_limit(1 << 20);
    let active = engine(&budget);
    apply(&active, entry());
    let frozen = active.freeze_checkpoint_collections(None).unwrap();
    let old = budget.snapshot().frozen;
    let replacement = engine(&budget).snapshot().unwrap();
    let record = entry();
    let reserved = active.try_reserve_record(&record, 0).unwrap();
    let pending = reserved.bytes();
    active.restore(replacement).unwrap();
    assert_eq!(budget.snapshot().total, old + pending);
    let mut reprice = active
        .begin_admitted_record(record, reserved)
        .err()
        .unwrap();
    let required = reprice
        .required
        .expect("restored row needs a new interner and coverage entry");
    assert!(required > pending);
    assert_eq!(
        budget.snapshot().reserved,
        pending,
        "reprice retains its original reservation"
    );
    reprice.reservation.try_grow_to(required).unwrap();
    let mut apply = active
        .begin_admitted_record(reprice.entry, reprice.reservation)
        .ok()
        .unwrap();
    active.apply_prepared_raft_entry(&mut apply).unwrap();
    drop(apply);
    assert_eq!(budget.snapshot().total, old + required);
    drop(frozen);
    assert_eq!(
        budget.snapshot().total,
        required,
        "old capture releases only its original payload"
    );
}
#[test]
fn raw_delivery_reservation_charges_then_releases_dropped_frame_bytes() {
    let entry = entry();
    let raw = Engine::record_owned_bytes(&entry).unwrap();
    let encoded = 4096;
    let budget = ChangeBudget::with_hard_limit(raw + encoded + 1);
    let engine = engine(&budget);
    let request = engine.record_ram_request(&entry, encoded).unwrap();
    let mut reservation = engine.try_reserve_record_ram(&request).unwrap();

    assert_eq!(
        budget.snapshot().reserved,
        raw + encoded,
        "the decoded entry and encoded frame coexist until replay drops the frame"
    );
    reservation.release_transport_bytes().unwrap();
    assert_eq!(
        budget.snapshot().reserved,
        raw,
        "only the explicitly dropped encoded frame may release capacity"
    );
    drop(reservation);
    assert_eq!(budget.snapshot().reserved, 0);
}
