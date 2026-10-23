use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Map, Value};
use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::{Dispatch, Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

use crate::index::application::engine::{raft_dispatch::ApplyOutcome, Engine};
use crate::index::domain::storage_error::StorageError;
use crate::ingest::application::write_coordinator::errors::{
    RestartRequired, StorageFullError, SubmitStalled,
};
use crate::ingest::application::write_coordinator::tests::{
    admitted_index_entry, keyword_schema, wait_for_reserved, ControlledWal,
};
use crate::ingest::application::write_coordinator::{WriteCoordinator, SUBMIT_TIMEOUT};
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::ingest::domain::wal_log::{SharedWal, WalLog, WalStream};
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::DiagnosticCapture;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;

const SECRET: &str = "write-phase-secret-document-request-and-error";
const NORMAL_PHASES: &[&str] = &[
    "admission_begin",
    "admission_end",
    "publication_begin",
    "publication_end",
    "apply_wait_begin",
    "caller_end",
];
type Fields = Map<String, Value>;

#[derive(Default)]
struct EventFields(Fields);

impl Visit for EventFields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
}

struct CaptureLayer {
    events: Arc<Mutex<Vec<Fields>>>,
    changed: Arc<Notify>,
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = EventFields::default();
        event.record(&mut fields);
        if fields.0.get("event").and_then(Value::as_str) == Some("lumen_write_phase") {
            self.events.lock().unwrap().push(fields.0);
            self.changed.notify_one();
        }
    }
}

struct Capture {
    events: Arc<Mutex<Vec<Fields>>>,
    changed: Arc<Notify>,
    dispatch: Dispatch,
}

impl Capture {
    fn new() -> Self {
        let events = Arc::new(Mutex::new(Vec::new()));
        let changed = Arc::new(Notify::new());
        let dispatch = Dispatch::new(tracing_subscriber::registry().with(CaptureLayer {
            events: events.clone(),
            changed: changed.clone(),
        }));
        Self {
            events,
            changed,
            dispatch,
        }
    }

    fn rows(&self) -> Vec<Fields> {
        self.events.lock().unwrap().clone()
    }

    fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    fn spawn(
        &self,
        coord: Arc<WriteCoordinator>,
        entry: RaftLogEntry,
    ) -> tokio::task::JoinHandle<Result<ApplyOutcome>> {
        tokio::spawn(
            async move { coord.submit(entry).await }.with_subscriber(self.dispatch.clone()),
        )
    }

    fn attempt(&self, attempt_id: u64) -> Vec<Fields> {
        self.rows()
            .into_iter()
            .filter(|row| row["attempt_id"] == attempt_id)
            .collect()
    }

    async fn phase_after(&self, cursor: usize, phase: &str) -> u64 {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let changed = self.changed.notified();
                if let Some(row) = self.rows()[cursor..]
                    .iter()
                    .find(|row| row["phase"] == phase)
                {
                    return row["attempt_id"].as_u64().unwrap();
                }
                changed.await;
            }
        })
        .await
        .expect("held real submit must emit its phase before the client boundary")
    }
}

fn setup(
    wal: SharedWal,
) -> (
    Arc<WriteCoordinator>,
    Arc<Engine>,
    ChangeBudget,
    DiagnosticCapture,
) {
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    engine.create_collection("u", keyword_schema()).unwrap();
    let coord = WriteCoordinator::start(wal, engine.clone());
    let enable = DiagnosticCapture::new();
    coord.set_diagnostic_capture(enable.token());
    (coord, engine, budget, enable)
}

fn secret_entry() -> RaftLogEntry {
    let mut entry = admitted_index_entry();
    if let RaftLogEntry::Index { req, .. } = &mut entry {
        req.request_id = Some(SECRET.into());
        req.items[0].external_id = SECRET.into();
        req.items[0].value = FieldValue::String(SECRET.into());
    }
    entry
}

fn phases(rows: &[Fields]) -> Vec<&str> {
    rows.iter()
        .map(|row| row["phase"].as_str().unwrap())
        .collect()
}

fn terminal(capture: &Capture, cursor: usize, expected: &[&str], result: &str, sequence: bool) {
    let rows = &capture.rows()[cursor..];
    assert_eq!(phases(rows), expected);
    let id = rows[0]["attempt_id"].as_u64().unwrap();
    assert!(rows.iter().all(|row| row["attempt_id"] == id));
    let end = rows.last().unwrap();
    assert_eq!(end["result"], result);
    assert_eq!(end["wal_sequence_present"], sequence);
    assert_eq!(end["wal_sequence"], if sequence { 1 } else { 0 });
}

async fn held_fence(coord: &WriteCoordinator) {
    assert!(
        tokio::time::timeout(Duration::from_millis(20), coord.fence_mutations())
            .await
            .is_err(),
        "a published or publishing record must keep its shared restore permit"
    );
}

async fn applied(coord: &WriteCoordinator) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while coord.applied_seq() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delivery after caller completion must still apply");
}

async fn joined(task: tokio::task::JoinHandle<Result<ApplyOutcome>>) -> Result<ApplyOutcome> {
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
}

struct ErrorWal;

#[async_trait::async_trait]
impl WalLog for ErrorWal {
    async fn publish(&self, _: WalRecord) -> Result<u64> {
        Err(anyhow::Error::new(StorageFullError(SECRET.into())))
    }
    async fn subscribe(&self, _: u64) -> Result<WalStream> {
        Ok(Box::pin(futures::stream::pending()))
    }
    async fn latest_seq(&self) -> Result<u64> {
        Ok(7)
    }
}

#[tokio::test]
async fn write_phase_diagnostics_are_truthful_and_request_scoped() {
    assert_ne!(
        std::env::var("LUMEN_PERF_DIAGNOSTIC").as_deref(),
        Ok("1"),
        "this proof uses only local capture enables"
    );
    let capture = Capture::new();
    let first_wal =
        ControlledWal::paused(ControlledWal::PAUSE_PUBLISH | ControlledWal::PAUSE_DELIVERY);
    let second_wal = ControlledWal::paused(ControlledWal::PAUSE_DELIVERY);
    let (first, first_engine, first_budget, _first_enable) = setup(first_wal.clone());
    let (second, second_engine, second_budget, _second_enable) = setup(second_wal.clone());
    let second_fence = second.fence_mutations().await.unwrap();
    let early_started = Instant::now();
    let first_caller = capture.spawn(first.clone(), secret_entry());
    let second_caller = capture.spawn(second.clone(), secret_entry());
    tokio::time::timeout(
        Duration::from_secs(2),
        first_wal.observed_publish.notified(),
    )
    .await
    .unwrap();
    wait_for_reserved(&second_budget).await;
    assert_eq!(first_wal.latest_seq().await.unwrap(), 1);
    assert_eq!(second_wal.latest_seq().await.unwrap(), 0);
    let early = capture.rows();
    let ids: BTreeSet<_> = early
        .iter()
        .map(|row| row["attempt_id"].as_u64().unwrap())
        .collect();
    assert_eq!(
        ids.len(),
        2,
        "missing diagnostic phase events while publication and mutation gate are held"
    );
    let first_id = *ids
        .iter()
        .find(|&&id| phases(&capture.attempt(id)) == NORMAL_PHASES[..3])
        .unwrap();
    let second_id = *ids
        .iter()
        .find(|&&id| phases(&capture.attempt(id)) == NORMAL_PHASES[..1])
        .unwrap();
    assert_ne!(first_id, second_id);
    assert!(early
        .iter()
        .all(|row| row["wal_sequence_present"] == false && row["wal_sequence"] == 0));
    assert!(early_started.elapsed() < Duration::from_secs(5));
    println!("checked early held prefixes: publication=3, mutation gate=1; distinct attempt IDs");

    first_caller.abort();
    assert!(first_caller.await.unwrap_err().is_cancelled());
    let detached = capture.attempt(first_id);
    assert_eq!(detached.last().unwrap()["result"], "detached_unknown");
    assert_eq!(detached.last().unwrap()["wal_sequence_present"], false);
    assert!(first_budget.snapshot().reserved > 0);
    held_fence(&first).await;
    first_wal.release_publish.notify_one();
    assert_eq!(capture.phase_after(0, "publication_end").await, first_id);
    let late = capture.attempt(first_id);
    assert_eq!(
        phases(&late),
        [
            "admission_begin",
            "admission_end",
            "publication_begin",
            "caller_end",
            "publication_end"
        ]
    );
    assert_eq!(late.last().unwrap()["result"], "sequence_returned");
    assert_eq!(late.last().unwrap()["wal_sequence_present"], true);
    assert_eq!(late.last().unwrap()["wal_sequence"], 1);
    assert!(late.last().unwrap()["phase_elapsed_us"].as_u64().unwrap() >= 10_000);
    assert!(first_budget.snapshot().reserved > 0);
    held_fence(&first).await;
    first_wal.release_delivery.notify_one();
    applied(&first).await;
    assert_eq!(first_engine.stats("u").unwrap().documents_indexed, 1);
    assert_eq!(first_budget.snapshot().reserved, 0);
    println!("checked detached caller, late publisher with the same ID, retained permit/reservation, and late apply");

    drop(second_fence);
    assert_eq!(capture.phase_after(0, "apply_wait_begin").await, second_id);
    assert_eq!(second_wal.latest_seq().await.unwrap(), 1);
    assert_eq!(phases(&capture.attempt(second_id)), NORMAL_PHASES[..5]);
    assert!(second_budget.snapshot().reserved > 0);
    held_fence(&second).await;
    let error = tokio::time::timeout(SUBMIT_TIMEOUT + Duration::from_secs(2), second_caller)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.downcast_ref::<SubmitStalled>().is_some());
    assert!(error
        .to_string()
        .contains("30s total waiting for sequence 1"));
    let timed_out = capture.attempt(second_id);
    assert_eq!(phases(&timed_out), NORMAL_PHASES);
    let end = timed_out.last().unwrap();
    assert_eq!(end["result"], "timeout_unknown");
    assert_eq!(end["wal_sequence_present"], true);
    assert_eq!(end["wal_sequence"], 1);
    assert!(
        end["total_elapsed_us"].as_u64().unwrap()
            > end["phase_elapsed_us"].as_u64().unwrap() + 10_000
    );
    assert_eq!(second.applied_seq(), 0);
    assert!(second_budget.snapshot().reserved > 0);
    assert!(second.completions.lock().unwrap().waiters.contains_key(&1));
    held_fence(&second).await;
    second_wal.release_delivery.notify_one();
    applied(&second).await;
    assert_eq!(second_engine.stats("u").unwrap().documents_indexed, 1);
    assert_eq!(second_budget.snapshot().reserved, 0);
    assert_eq!(capture.attempt(second_id).len(), 6);
    println!("checked unchanged absolute 30-second deadline, sequence-known unknown timeout, and eventual apply");

    let cursor = capture.len();
    let (coord, _, _, _enable) = setup(Arc::new(MemWal::new()));
    assert!(matches!(
        joined(capture.spawn(coord, secret_entry())).await.unwrap(),
        ApplyOutcome::Indexed(_)
    ));
    terminal(&capture, cursor, NORMAL_PHASES, "applied", true);

    let cursor = capture.len();
    let wal = ControlledWal::paused(ControlledWal::PAUSE_DELIVERY);
    let (coord, _, budget, _enable) = setup(wal.clone());
    let fence = coord.fence_mutations().await.unwrap();
    let caller = capture.spawn(coord.clone(), secret_entry());
    wait_for_reserved(&budget).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    terminal(
        &capture,
        cursor,
        &["admission_begin", "caller_end"],
        "detached_not_started",
        false,
    );
    assert_eq!(wal.latest_seq().await.unwrap(), 0);
    assert_eq!(budget.snapshot().reserved, 0);
    drop(fence);

    let cursor = capture.len();
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget.clone()));
    let wal = Arc::new(MemWal::new());
    let coord = WriteCoordinator::start(wal.clone(), engine);
    let enable = DiagnosticCapture::new();
    coord.set_diagnostic_capture(enable.token());
    let foreign = budget.owner();
    let _held = foreign.try_reserve(1024 * 1024).unwrap();
    let before = coord.engine.metrics().segment_backpressure_total.get();
    let error = joined(capture.spawn(
        coord.clone(),
        RaftLogEntry::CreateCollection {
            collection_id: SECRET.into(),
            req: keyword_schema(),
        },
    ))
    .await
    .unwrap_err();
    assert!(error.downcast_ref::<PendingChangeCapacity>().is_some());
    assert_eq!(
        coord.engine.metrics().segment_backpressure_total.get(),
        before + 1
    );
    assert_eq!(wal.latest_seq().await.unwrap(), 0);
    terminal(
        &capture,
        cursor,
        &["admission_begin", "admission_end", "caller_end"],
        "refused_not_started",
        false,
    );

    let cursor = capture.len();
    let (coord, _, _, _enable) = setup(Arc::new(ErrorWal));
    assert_eq!(coord.wal.latest_seq().await.unwrap(), 7);
    let error = joined(capture.spawn(coord, secret_entry()))
        .await
        .unwrap_err();
    assert!(error.downcast_ref::<StorageFullError>().is_some());
    terminal(
        &capture,
        cursor,
        &[
            "admission_begin",
            "admission_end",
            "publication_begin",
            "publication_end",
            "caller_end",
        ],
        "publication_unknown",
        false,
    );

    let cursor = capture.len();
    let wal = ControlledWal::paused(ControlledWal::PAUSE_DELIVERY);
    let (coord, _, budget, _enable) = setup(wal.clone());
    let prior = coord
        .register_waiter(1, coord.mutation_gate.shared().await.unwrap())
        .unwrap();
    let error = joined(capture.spawn(coord.clone(), secret_entry()))
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("duplicate waiter for sequence 1"));
    terminal(
        &capture,
        cursor,
        &[
            "admission_begin",
            "admission_end",
            "publication_begin",
            "publication_end",
            "caller_end",
        ],
        "registration_unknown",
        true,
    );
    let rows = &capture.rows()[cursor..];
    assert_eq!(rows[3]["result"], "sequence_returned_unknown");
    assert!(budget.snapshot().reserved > 0);
    held_fence(&coord).await;
    wal.release_delivery.notify_one();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), prior)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        ApplyOutcome::Indexed(_)
    ));
    applied(&coord).await;

    for result in [
        "stale_unknown",
        "restart_unknown",
        "receiver_closed_unknown",
    ] {
        let cursor = capture.len();
        let wal = ControlledWal::paused(ControlledWal::PAUSE_DELIVERY);
        let (coord, _, budget, _enable) = setup(wal.clone());
        let caller = capture.spawn(coord.clone(), secret_entry());
        capture.phase_after(cursor, "apply_wait_begin").await;
        match result {
            "stale_unknown" => coord.complete_stale(1),
            "restart_unknown" => coord.fail_unresolved(
                1,
                Err(anyhow::Error::new(RestartRequired(SECRET.into()))),
                None,
            ),
            _ => {
                drop(coord.completions.lock().unwrap().waiters.remove(&1));
            }
        }
        let error = joined(caller).await.unwrap_err();
        match result {
            "stale_unknown" => assert!(error.downcast_ref::<SubmitStalled>().is_some()),
            "restart_unknown" => assert!(error.downcast_ref::<RestartRequired>().is_some()),
            _ => assert!(error
                .to_string()
                .contains("apply loop stopped before sequence 1")),
        }
        terminal(&capture, cursor, NORMAL_PHASES, result, true);
        assert_eq!(coord.applied_seq(), 0);
        assert!(budget.snapshot().reserved > 0);
        if result != "stale_unknown" {
            assert!(coord
                .completions
                .lock()
                .unwrap()
                .mutation_permits
                .contains_key(&1));
            held_fence(&coord).await;
        }
        wal.release_delivery.notify_one();
        applied(&coord).await;
    }

    let cursor = capture.len();
    let (coord, _, _, _enable) = setup(Arc::new(MemWal::new()));
    let mut missing = secret_entry();
    if let RaftLogEntry::Index { collection_id, .. } = &mut missing {
        *collection_id = SECRET.into();
    }
    let error = joined(capture.spawn(coord, missing)).await.unwrap_err();
    assert!(matches!(
        error.downcast_ref::<StorageError>(),
        Some(StorageError::CollectionNotFound(_))
    ));
    terminal(&capture, cursor, NORMAL_PHASES, "apply_error_unknown", true);
    println!("checked normal success, admission cancellation/refusal, WAL error, registration error, and typed stale/restart/apply/closure rows");

    let cursor = capture.len();
    let budget = ChangeBudget::with_hard_limit(1024 * 1024);
    let engine = Arc::new(Engine::with_change_budget(budget));
    engine.create_collection("u", keyword_schema()).unwrap();
    let ordinary = WriteCoordinator::start(Arc::new(MemWal::new()), engine);
    joined(capture.spawn(ordinary, secret_entry()))
        .await
        .unwrap();
    assert_eq!(
        capture.len(),
        cursor,
        "ordinary writes must emit no diagnostic phases"
    );

    let allowed: BTreeSet<_> = [
        "event",
        "attempt_id",
        "kind",
        "phase",
        "result",
        "wal_sequence_present",
        "wal_sequence",
        "total_elapsed_us",
        "phase_elapsed_us",
    ]
    .into_iter()
    .collect();
    let results = [
        "started",
        "admitted",
        "refused",
        "error",
        "sequence_returned",
        "unknown",
        "sequence_returned_unknown",
        "waiting",
        "applied",
        "refused_not_started",
        "admission_error_not_started",
        "publication_unknown",
        "registration_unknown",
        "receiver_closed_unknown",
        "timeout_unknown",
        "stale_unknown",
        "restart_unknown",
        "apply_error_unknown",
        "detached_not_started",
        "detached_unknown",
        "detached_sequence_returned",
    ];
    let mut groups: BTreeMap<u64, Vec<Fields>> = BTreeMap::new();
    for row in capture.rows() {
        assert!(row.keys().all(|key| allowed.contains(key.as_str())));
        assert!(!serde_json::to_string(&row).unwrap().contains(SECRET));
        assert!(NORMAL_PHASES.contains(&row["phase"].as_str().unwrap()));
        assert!(results.contains(&row["result"].as_str().unwrap()));
        assert!(matches!(
            row["kind"].as_str(),
            Some("index" | "create_collection")
        ));
        assert!(
            row["total_elapsed_us"].as_u64().unwrap() >= row["phase_elapsed_us"].as_u64().unwrap()
        );
        let id = row["attempt_id"].as_u64().unwrap();
        assert!(id > 0);
        groups.entry(id).or_default().push(row);
    }
    assert_eq!(groups.len(), 11);
    for rows in groups.values() {
        assert!(rows.len() <= 6);
        assert_eq!(
            rows.iter()
                .filter(|row| row["phase"] == "caller_end")
                .count(),
            1
        );
        assert!(rows
            .windows(2)
            .all(|pair| pair[0]["total_elapsed_us"].as_u64().unwrap()
                <= pair[1]["total_elapsed_us"].as_u64().unwrap()));
    }
    println!("checked 11 isolated attempts: at most six real events, safe fields only, no secret, and disabled ordinary writes");
}

#[derive(Clone, Default)]
struct HttpPhaseWriter(Arc<Mutex<Vec<u8>>>);

struct HttpPhaseWriterGuard(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for HttpPhaseWriterGuard {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for HttpPhaseWriter {
    type Writer = HttpPhaseWriterGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        HttpPhaseWriterGuard(self.0.clone())
    }
}

impl HttpPhaseWriter {
    fn records(&self) -> Vec<Value> {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes)
            .unwrap()
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("real formatter must emit JSON"))
            .collect()
    }

    fn attempt(&self, attempt_id: u64) -> Vec<Value> {
        self.records()
            .into_iter()
            .filter(|row| {
                row["event"] == "lumen_write_phase"
                    && row["attributes"]["attempt_id"] == attempt_id
            })
            .collect()
    }
}

fn http_phase_capture() -> (Capture, HttpPhaseWriter) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let changed = Arc::new(Notify::new());
    let writer = HttpPhaseWriter::default();
    let identity = service_observability::ServiceIdentity::new("lumen", "test").unwrap();
    let formatter = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(writer.clone())
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_line_number(false)
        .json()
        .with_current_span(true)
        .event_format(service_observability::ServiceJsonFormatter::new(identity));
    let dispatch = Dispatch::new(
        tracing_subscriber::registry()
            .with(CaptureLayer {
                events: events.clone(),
                changed: changed.clone(),
            })
            .with(formatter),
    );
    (
        Capture {
            events,
            changed,
            dispatch,
        },
        writer,
    )
}

fn http_index_request(trace_id: &str) -> axum::http::Request<axum::body::Body> {
    let RaftLogEntry::Index { req, .. } = secret_entry() else {
        unreachable!();
    };
    let parent_span_id = rand::random::<u64>() | 1;
    axum::http::Request::builder()
        .method("POST")
        .uri("/collections/u/index")
        .header("content-type", "application/json")
        .header(
            "traceparent",
            format!("00-{trace_id}-{parent_span_id:016x}-01"),
        )
        .header("cookie", SECRET)
        .header("baggage", SECRET)
        .body(axum::body::Body::from(serde_json::to_vec(&req).unwrap()))
        .unwrap()
}

fn assert_http_phase_trace(rows: &[Value], trace_id: &str) {
    assert!(
        !rows.is_empty(),
        "the actual request must emit write phases"
    );
    for row in rows {
        assert_eq!(
            row["trace_id"].as_str(),
            Some(trace_id),
            "actual HTTP write phase {} must retain its supplied request trace ID",
            row["attributes"]["phase"]
        );
        assert!(!serde_json::to_string(row).unwrap().contains(SECRET));
    }
}

#[tokio::test]
async fn write_phase_keeps_http_trace_context_after_handoff_and_caller_drop() {
    use tower::ServiceExt as _;

    use crate::access::application::auth_config::AuthConfig;
    use crate::app::http::{app_state::AppState, router::router};

    assert_ne!(
        std::env::var("LUMEN_PERF_DIAGNOSTIC").as_deref(),
        Ok("1"),
        "this proof uses only the existing local capture enable"
    );
    let (capture, json) = http_phase_capture();
    let normal_trace_id = format!("{:032x}", rand::random::<u128>() | 1);
    let cancelled_trace_id = format!("{:032x}", rand::random::<u128>() | 1);
    assert_ne!(normal_trace_id, cancelled_trace_id);

    let normal_wal =
        ControlledWal::paused(ControlledWal::PAUSE_PUBLISH | ControlledWal::PAUSE_DELIVERY);
    let (normal, normal_engine, normal_budget, _normal_enable) = setup(normal_wal.clone());
    let normal_app = router(AppState::with_components(
        normal_engine.clone(),
        Arc::new(AuthConfig::open()),
        normal.clone(),
    ));
    let normal_request = http_index_request(&normal_trace_id);
    let normal_caller = tokio::spawn(
        async move { normal_app.oneshot(normal_request).await }
            .with_subscriber(capture.dispatch.clone()),
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        normal_wal.observed_publish.notified(),
    )
    .await
    .expect("actual HTTP request must reach held publication");
    let normal_id = capture.phase_after(0, "publication_begin").await;
    let normal_held = capture.attempt(normal_id);
    assert_eq!(phases(&normal_held), NORMAL_PHASES[..3]);
    assert!(normal_held
        .iter()
        .all(|row| row["wal_sequence_present"] == false && row["wal_sequence"] == 0));
    assert_http_phase_trace(&json.attempt(normal_id), &normal_trace_id);
    assert!(normal_budget.snapshot().reserved > 0);

    normal_wal.release_publish.notify_one();
    assert_eq!(capture.phase_after(0, "apply_wait_begin").await, normal_id);
    let normal_waiting = capture.attempt(normal_id);
    assert_eq!(phases(&normal_waiting), NORMAL_PHASES[..5]);
    assert_eq!(normal_waiting[3]["result"], "sequence_returned");
    assert_eq!(normal_waiting[3]["wal_sequence_present"], true);
    assert_eq!(normal_waiting[3]["wal_sequence"], 1);
    assert_eq!(normal.applied_seq(), 0);
    normal_wal.release_delivery.notify_one();
    let response = tokio::time::timeout(Duration::from_secs(2), normal_caller)
        .await
        .expect("released actual HTTP request must complete")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let response_json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response_json["indexed"], 1);
    terminal(&capture, 0, NORMAL_PHASES, "applied", true);
    assert_eq!(normal_engine.stats("u").unwrap().documents_indexed, 1);
    assert_eq!(normal_budget.snapshot().reserved, 0);
    let normal_json = json.attempt(normal_id);
    assert_eq!(normal_json.len(), NORMAL_PHASES.len());
    println!(
        "normal supplied trace_id={normal_trace_id}; actual formatter phases={}",
        serde_json::to_string(&normal_json).unwrap()
    );

    let cancel_cursor = capture.len();
    let cancelled_wal =
        ControlledWal::paused(ControlledWal::PAUSE_PUBLISH | ControlledWal::PAUSE_DELIVERY);
    let (cancelled, cancelled_engine, cancelled_budget, _cancelled_enable) =
        setup(cancelled_wal.clone());
    let cancelled_app = router(AppState::with_components(
        cancelled_engine.clone(),
        Arc::new(AuthConfig::open()),
        cancelled.clone(),
    ));
    let cancelled_request = http_index_request(&cancelled_trace_id);
    let cancelled_caller = tokio::spawn(
        async move { cancelled_app.oneshot(cancelled_request).await }
            .with_subscriber(capture.dispatch.clone()),
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        cancelled_wal.observed_publish.notified(),
    )
    .await
    .expect("second actual HTTP request must reach held publication");
    let cancelled_id = capture
        .phase_after(cancel_cursor, "publication_begin")
        .await;
    assert_ne!(normal_id, cancelled_id);
    assert_eq!(phases(&capture.attempt(cancelled_id)), NORMAL_PHASES[..3]);
    assert_http_phase_trace(&json.attempt(cancelled_id), &cancelled_trace_id);
    assert_eq!(cancelled.applied_seq(), 0);
    assert!(cancelled_budget.snapshot().reserved > 0);

    // This cancels the held router future. It does not model a network disconnect.
    cancelled_caller.abort();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), cancelled_caller)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    terminal(
        &capture,
        cancel_cursor,
        &[
            "admission_begin",
            "admission_end",
            "publication_begin",
            "caller_end",
        ],
        "detached_unknown",
        false,
    );
    assert_eq!(cancelled_engine.stats("u").unwrap().documents_indexed, 0);
    assert!(cancelled_budget.snapshot().reserved > 0);

    cancelled_wal.release_publish.notify_one();
    assert_eq!(
        capture.phase_after(cancel_cursor, "publication_end").await,
        cancelled_id
    );
    let cancelled_rows = capture.attempt(cancelled_id);
    assert_eq!(
        phases(&cancelled_rows),
        [
            "admission_begin",
            "admission_end",
            "publication_begin",
            "caller_end",
            "publication_end",
        ]
    );
    let late = cancelled_rows.last().unwrap();
    assert_eq!(late["result"], "sequence_returned");
    assert_eq!(late["wal_sequence_present"], true);
    assert_eq!(late["wal_sequence"], 1);
    assert_eq!(cancelled.applied_seq(), 0);
    assert!(cancelled_budget.snapshot().reserved > 0);
    cancelled_wal.release_delivery.notify_one();
    applied(&cancelled).await;
    assert_eq!(cancelled_engine.stats("u").unwrap().documents_indexed, 1);
    assert_eq!(cancelled_budget.snapshot().reserved, 0);
    let cancelled_json = json.attempt(cancelled_id);
    assert_eq!(cancelled_json.len(), 5);
    println!(
        "cancelled supplied trace_id={cancelled_trace_id}; actual formatter phases={}",
        serde_json::to_string(&cancelled_json).unwrap()
    );

    // Check all final JSON events after both controlled flows have completed.
    assert_http_phase_trace(&normal_json, &normal_trace_id);
    assert_http_phase_trace(&cancelled_json, &cancelled_trace_id);
    let access = json.records();
    let normal_access = access
        .iter()
        .find(|row| {
            row["attributes"]["target"] == "http.access" && row["trace_id"] == normal_trace_id
        })
        .expect("normal HTTP access log must retain the same supplied trace ID");
    assert_eq!(normal_access["attributes"]["subject"], "anonymous");
    println!("checked two fresh strict traceparents: normal HTTP completion, held router-future cancellation, unknown caller, and late exact WAL sequence 1");
}
