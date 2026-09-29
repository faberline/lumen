use std::sync::{Arc, Condvar, Mutex, Weak};

use crate::persistence::application::capacity::tests::{
    engine, relay_diagnostic_records, relay_phases, relay_record, sink,
};
use crate::persistence::application::capacity::worker::{relay_cycle, Owner};
use crate::persistence::application::capacity::{Endpoint, PublicationFence, Requests, Work};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

#[test]
fn relay_cycle_checkpoints_merges_then_consumes_request() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut owner = Owner::start(sink(engine.clone(), store), true)
        .unwrap()
        .unwrap();
    engine.request_pending_checkpoint();
    let revision = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    relay_cycle(&engine, &owner.endpoint(), Some(revision)).unwrap();
    assert!(engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .is_none());
    let requests = owner.endpoint.requests.lock().unwrap();
    assert_eq!(requests.requested, 2);
    assert_eq!(requests.completed, 2);
    assert_eq!(requests.operations, [Work::Checkpoint, Work::Merge]);
    drop(requests);
    assert_eq!(
        engine
            .metrics()
            .segment_capacity_relief_checkpoint_merge_seconds_count
            .get(),
        1,
        "one successful relay cycle records one checkpoint-plus-merge interval"
    );
    owner.join().unwrap();
}

#[test]
fn stale_relay_revision_cannot_consume_successor_request() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut owner = Owner::start(sink(engine.clone(), store), true)
        .unwrap()
        .unwrap();
    engine.request_pending_checkpoint();
    let first = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    engine.consume_checkpoint_request(first);
    engine.request_pending_checkpoint();
    let successor = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    assert!(successor > first);
    relay_cycle(&engine, &owner.endpoint(), Some(first)).unwrap();
    assert_eq!(
        engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision),
        Some(successor)
    );
    owner.join().unwrap();
}

#[test]
fn relay_cycle_error_keeps_request_pending() {
    let engine = engine();
    engine.request_pending_checkpoint();
    let revision = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    let endpoint = Endpoint {
        requests: Mutex::new(Requests {
            completed: 1,
            error: Some("terminal checkpoint failure".into()),
            ..Requests::default()
        }),
        changed: Condvar::new(),
        fence: PublicationFence {
            registry: Weak::new(),
            token: 0,
        },
    };
    assert!(relay_cycle(&engine, &endpoint, Some(revision)).is_err());
    assert_eq!(
        engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision),
        Some(revision)
    );
    assert_eq!(
        engine
            .metrics()
            .segment_capacity_relief_checkpoint_merge_seconds_count
            .get(),
        0,
        "a failed checkpoint must not publish a successful-cycle observation"
    );
}

#[test]
fn diagnostic_relay_success_emits_one_ordered_terminal_record() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut owner = Owner::start(sink(engine.clone(), store), true)
        .unwrap()
        .unwrap();
    engine.request_pending_checkpoint();
    let revision = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    let records = relay_diagnostic_records(true, || {
        relay_cycle(&engine, &owner.endpoint(), Some(revision)).unwrap();
    });
    assert_eq!(
        relay_phases(&records),
        [
            "relay_started",
            "checkpoint_completed",
            "merge_completed",
            "terminal"
        ]
    );
    let phase_records: Vec<_> = records
        .iter()
        .filter(|record| record["fields"]["event"] == "segment_capacity_relay_diagnostic")
        .collect();
    let cycle_id = phase_records[0]["fields"]["relay_cycle_id"].clone();
    for record in phase_records {
        let fields = record["fields"].as_object().unwrap();
        assert_eq!(fields["relay_cycle_id"], cycle_id);
        for field in [
            "request_revision",
            "active_bytes",
            "frozen_bytes",
            "pending_delta_bytes",
            "pending_delta_layers",
            "merge_completed_total",
            "relay_elapsed_ns",
        ] {
            assert!(
                fields.contains_key(field),
                "missing relay phase field {field}"
            );
        }
    }
    let record = relay_record(records);
    let fields = record["fields"].as_object().unwrap();
    assert_eq!(fields["start_request_revision"], revision);
    assert_eq!(fields["checkpoint_result"], "ok");
    assert_eq!(fields["merge_result"], "ok");
    assert_eq!(fields["consume_attempted_revision"], revision);
    assert_eq!(fields["consume_result"], "consumed");
    assert_eq!(fields["end_reason"], "complete");
    for field in [
        "relay_cycle_id",
        "checkpoint_ns",
        "merge_ns",
        "consume_ns",
        "end_active_bytes",
        "end_frozen_bytes",
        "end_pending_delta_bytes",
        "end_pending_delta_layers",
        "end_merge_completed_total",
    ] {
        assert!(
            fields.contains_key(field),
            "missing relay diagnostic field {field}"
        );
    }
    owner.join().unwrap();
}

#[test]
fn diagnostic_relay_error_preserves_request_and_emits_terminal_record() {
    let engine = engine();
    engine.request_pending_checkpoint();
    let revision = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    let endpoint = Endpoint {
        requests: Mutex::new(Requests {
            completed: 1,
            error: Some("terminal checkpoint failure".into()),
            ..Requests::default()
        }),
        changed: Condvar::new(),
        fence: PublicationFence {
            registry: Weak::new(),
            token: 0,
        },
    };
    let records = relay_diagnostic_records(true, || {
        assert!(relay_cycle(&engine, &endpoint, Some(revision)).is_err());
    });
    assert_eq!(relay_phases(&records), ["relay_started", "terminal"]);
    let record = relay_record(records);
    let fields = record["fields"].as_object().unwrap();
    assert_eq!(fields["checkpoint_result"], "error");
    assert_eq!(fields["merge_result"], "not_started");
    assert_eq!(fields["consume_result"], "not_attempted");
    assert_eq!(fields["end_reason"], "checkpoint_error");
    assert_eq!(fields["end_capacity_request_revision"], revision);
    assert_eq!(
        engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision),
        Some(revision)
    );
}

#[test]
fn diagnostic_relay_stale_request_preserves_successor_and_records_stale_consume() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut owner = Owner::start(sink(engine.clone(), store), true)
        .unwrap()
        .unwrap();
    engine.request_pending_checkpoint();
    let first = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    engine.consume_checkpoint_request(first);
    engine.request_pending_checkpoint();
    let successor = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    let records = relay_diagnostic_records(true, || {
        relay_cycle(&engine, &owner.endpoint(), Some(first)).unwrap();
    });
    assert_eq!(
        relay_phases(&records),
        [
            "relay_started",
            "checkpoint_completed",
            "merge_completed",
            "terminal"
        ]
    );
    let record = relay_record(records);
    let fields = record["fields"].as_object().unwrap();
    assert_eq!(fields["consume_attempted_revision"], first);
    assert_eq!(fields["consume_result"], "stale");
    assert_eq!(fields["end_capacity_request_revision"], successor);
    assert_eq!(
        engine
            .capacity_owner_state()
            .and_then(|state| state.checkpoint_request_revision),
        Some(successor)
    );
    owner.join().unwrap();
}

#[test]
fn relay_emits_no_diagnostic_record_without_exact_flag() {
    let engine = engine();
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(SegmentRdbStore::new(root.path()).unwrap());
    let mut owner = Owner::start(sink(engine.clone(), store), true)
        .unwrap()
        .unwrap();
    engine.request_pending_checkpoint();
    let revision = engine
        .capacity_owner_state()
        .and_then(|state| state.checkpoint_request_revision)
        .unwrap();
    let records = relay_diagnostic_records(false, || {
        relay_cycle(&engine, &owner.endpoint(), Some(revision)).unwrap();
    });
    assert!(
        !records
            .iter()
            .any(|record| record["fields"]["event"] == "segment_capacity_relay_diagnostic"),
        "ordinary and qualifying relays must not emit a diagnostic record"
    );
    owner.join().unwrap();
}
