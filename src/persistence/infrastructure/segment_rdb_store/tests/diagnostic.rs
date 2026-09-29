use crate::persistence::infrastructure::segment_rdb_store::diagnostic::CheckpointDiagnosticContext;
#[cfg(test)]
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
    send_numeric_event, DiagnosticCapture, NumericCanonicalEvent,
};
use crate::persistence::infrastructure::segment_rdb_store::tests::{
    index_kw, kw_schema, DiagnosticEnvironment, DiagnosticTraceWriter,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;
use anyhow::Result;
use std::sync::Arc;

fn checkpoint_diagnostic_records(
    enabled: bool,
    origin: &'static str,
    attempt_id: Option<u64>,
) -> Vec<serde_json::Value> {
    use tracing_subscriber::prelude::*;

    let _environment = DiagnosticEnvironment::set(enabled);
    let writer = DiagnosticTraceWriter::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(writer.clone()),
    );
    let _guard = tracing::subscriber::set_default(subscriber);
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "one", "trace-value");
    let store = SegmentRdbStore::new(dir.path()).unwrap();

    assert_eq!(
        store
            .save_with_sequence_diagnostic(&engine, 1, origin, attempt_id)
            .unwrap(),
        1
    );
    drop(_guard);
    writer.records()
}

#[test]
fn checkpoint_diagnostic_trace_is_machine_readable() {
    let record = checkpoint_diagnostic_records(true, "periodic", None)
        .into_iter()
        .find(|record| record["fields"]["event"] == "segment_checkpoint_diagnostic")
        .expect("durable checkpoint must emit its diagnostic trace");
    let fields = record["fields"]
        .as_object()
        .expect("diagnostic trace fields must be JSON object");
    assert_eq!(fields["checkpoint_origin"], "periodic");
    assert_eq!(fields["checkpoint_sequence"], 1);
    assert_eq!(fields["capture_to_save_gate_wait_ns"], 0);
    for name in [
        "frozen_cut_bytes",
        "save_gate_wait_ns",
        "save_gate_hold_ns",
        "publish_ns",
        "acknowledge_ns",
        "capacity_request_pending",
        "capacity_request_revision",
        "root_merge_queued",
        "root_merge_running",
        "root_merge_requested",
        "root_merge_published_revision",
    ] {
        assert!(fields.contains_key(name), "missing diagnostic field {name}");
    }
}

#[test]
fn numeric_capture_tokens_isolate_parallel_subscribers_and_expire_on_drop() {
    let first = DiagnosticCapture::new();
    let second = DiagnosticCapture::new();
    assert_ne!(first.token(), second.token());
    let event = NumericCanonicalEvent::Phase {
        attempt_id: 7,
        phase: "terminal",
        pass: 0,
        reused: false,
        frozen_bytes: 0,
    };
    send_numeric_event(first.token(), event);
    assert_eq!(first.drain(), [event]);
    assert!(second.drain().is_empty());
    let old = first.token();
    drop(first);
    send_numeric_event(old, event);
    send_numeric_event(second.token(), event);
    assert_eq!(second.drain(), [event]);
}

#[test]
fn checkpoint_diagnostic_trace_is_absent_without_exact_environment_flag() {
    let records = checkpoint_diagnostic_records(false, "periodic", None);
    assert!(
        !records
            .iter()
            .any(|record| record["fields"]["event"] == "segment_checkpoint_diagnostic"),
        "ordinary checkpoints must not emit a diagnostic trace"
    );
}

#[test]
fn manual_checkpoint_diagnostic_phases_keep_one_attempt_id() {
    let records = checkpoint_diagnostic_records(true, "manual", Some(41));
    let phases: Vec<_> = records
        .iter()
        .filter(|record| record["fields"]["event"] == "segment_checkpoint_diagnostic_phase")
        .collect();
    assert!(phases
        .iter()
        .any(|record| record["fields"]["phase"] == "freeze_completed"));
    assert!(phases
        .iter()
        .any(|record| record["fields"]["phase"] == "publish_completed"));
    assert!(phases
        .iter()
        .all(|record| record["fields"]["checkpoint_attempt_id"] == 41));
}

#[test]
fn periodic_checkpoint_lifecycle_phases_are_ordered_and_share_one_attempt_id() {
    use tracing_subscriber::prelude::*;

    let _environment = DiagnosticEnvironment::set(true);
    let writer = DiagnosticTraceWriter::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .json()
            .with_ansi(false)
            .with_writer(writer.clone()),
    );
    let _guard = tracing::subscriber::set_default(subscriber);
    let context = CheckpointDiagnosticContext::new("periodic", Some(73));
    let pending = crate::ingest::domain::change_budget::Snapshot {
        reserved: 11,
        active: 22,
        frozen: 33,
        total: crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
        work_revision: 1,
        checkpoint_request_revision: None,
    };
    context.trace_scheduler_selected("threshold", "initial_sample", pending);
    context.trace_phase("checkpoint_started");
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new());
    engine.create_collection("u", kw_schema()).unwrap();
    index_kw(&engine, "one", "trace-value");
    SegmentRdbStore::new(dir.path())
        .unwrap()
        .save_with_sequence_diagnostic_context(&engine, 1, Some(context))
        .unwrap();
    let terminal: Result<()> = Ok(());
    context.trace_terminal(&terminal);
    drop(_guard);

    let phases: Vec<_> = writer
        .records()
        .into_iter()
        .filter(|record| record["fields"]["event"] == "segment_checkpoint_diagnostic_phase")
        .collect();
    assert_eq!(
        phases
            .iter()
            .map(|record| record["fields"]["phase"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "scheduler_selected",
            "checkpoint_started",
            "freeze_completed",
            "publish_completed",
            "terminal",
        ]
    );
    assert!(phases
        .iter()
        .all(|record| record["fields"]["checkpoint_attempt_id"] == 73));
    assert!(phases.windows(2).all(|pair| {
        pair[0]["fields"]["elapsed_ns"].as_u64() <= pair[1]["fields"]["elapsed_ns"].as_u64()
    }));
    let selected = &phases[0]["fields"];
    assert_eq!(
        selected["pending_total_bytes"],
        crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER
    );
    assert_eq!(selected["pending_reserved_bytes"], 11);
    assert_eq!(selected["pending_active_bytes"], 22);
    assert_eq!(selected["pending_frozen_bytes"], 33);
    assert_eq!(selected["scheduler_reason"], "threshold");
    assert_eq!(selected["resample_source"], "initial_sample");
    assert!(selected.get("wake_lag_ns").is_none());
    let frozen = &phases[2]["fields"];
    assert_eq!(frozen["checkpoint_pass"], 1);
    assert_eq!(frozen["frozen_cut_reused"], false);
    assert!(frozen["frozen_cut_bytes"].as_u64().is_some());
}
