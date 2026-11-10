//! Controlled capacity schedules reuse every original two-writer assertion.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::Duration;

use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

static GATE: OnceLock<Arc<Gate>> = OnceLock::new();

struct Gate {
    force_six_seed_cut: bool,
    first_start: AtomicBool,
    indexes: AtomicUsize,
    release_start: mpsc::Sender<()>,
    start: Mutex<mpsc::Receiver<()>>,
    frozen: mpsc::Sender<()>,
    froze: Mutex<mpsc::Receiver<()>>,
    first_terminal: AtomicBool,
    resumed_writes: AtomicUsize,
    gated_metrics: AtomicBool,
    release: mpsc::Sender<()>,
    resume: Mutex<mpsc::Receiver<()>>,
    terminal_count: AtomicUsize,
    second: mpsc::Sender<()>,
    completed: Mutex<mpsc::Receiver<()>>,
}

impl Gate {
    fn new(force_six_seed_cut: bool) -> Self {
        let (release_start, start) = mpsc::channel();
        let (frozen, froze) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let (second, completed) = mpsc::channel();
        Self {
            force_six_seed_cut,
            first_start: AtomicBool::new(false),
            indexes: AtomicUsize::new(0),
            release_start,
            start: Mutex::new(start),
            frozen,
            froze: Mutex::new(froze),
            first_terminal: AtomicBool::new(false),
            resumed_writes: AtomicUsize::new(0),
            gated_metrics: AtomicBool::new(false),
            release,
            resume: Mutex::new(resume),
            terminal_count: AtomicUsize::new(0),
            second,
            completed: Mutex::new(completed),
        }
    }
}

#[derive(Clone)]
struct GateWriter;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for GateWriter {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

impl std::io::Write for GateWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
            let gate = GATE.get().expect("isolated schedule installed");
            let fields = &value["fields"];
            if value["target"] == "http.access"
                && fields["status"] == 200
                && value["span"]["uri"]
                    .as_str()
                    .is_some_and(|uri| uri.ends_with("/index"))
            {
                if gate.first_terminal.load(Ordering::Acquire) {
                    gate.resumed_writes.fetch_add(1, Ordering::AcqRel);
                }
                if gate.force_six_seed_cut && gate.indexes.fetch_add(1, Ordering::AcqRel) + 1 == 7 {
                    gate.release_start
                        .send(())
                        .expect("permit cut after six large seeds");
                    gate.froze
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(30))
                        .expect("capture six seeds before the next response");
                }
            }
            if fields["event"] == "segment_checkpoint_diagnostic_phase"
                && fields["checkpoint_origin"] == "periodic"
            {
                if fields["phase"] == "checkpoint_started"
                    && !gate.first_start.swap(true, Ordering::AcqRel)
                    && gate.force_six_seed_cut
                {
                    gate.start
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(30))
                        .expect("six-seed capture start");
                }
                if fields["phase"] == "freeze_completed" {
                    let _ = gate.frozen.send(());
                }
                if fields["phase"] == "terminal" {
                    if gate.terminal_count.fetch_add(1, Ordering::AcqRel) == 0 {
                        gate.first_terminal.store(true, Ordering::Release);
                        gate.resume
                            .lock()
                            .unwrap()
                            .recv_timeout(Duration::from_secs(30))
                            .expect("resume after both waiting writes and before metrics");
                    } else {
                        let _ = gate.second.send(());
                    }
                }
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct RequestFields {
    method: String,
    uri: String,
}

impl Visit for RequestFields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}").trim_matches('"').to_owned();
        match field.name() {
            "method" => self.method = value,
            "uri" => self.uri = value,
            _ => {}
        }
    }
}

struct GateLayer;

impl<S: Subscriber> Layer<S> for GateLayer {
    fn on_new_span(&self, attributes: &Attributes<'_>, _: &Id, _: Context<'_, S>) {
        if attributes.metadata().name() != "request" {
            return;
        }
        let mut fields = RequestFields::default();
        attributes.record(&mut fields);
        let gate = GATE.get().expect("isolated schedule installed");
        if fields.method == "GET"
            && fields.uri.ends_with("/metrics")
            && gate.first_terminal.load(Ordering::Acquire)
            && gate.resumed_writes.load(Ordering::Acquire) == 2
            && !gate.gated_metrics.swap(true, Ordering::AcqRel)
        {
            gate.release
                .send(())
                .expect("release the completed publication");
            // Delay this read, not the original five-second admission bound.
            // A duplicate completes first on the broken path; the unchanged
            // original exact counter then rejects it.
            let _ = gate
                .completed
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(2));
        }
    }
}

async fn run(case: &'static str, name: &'static str, force_six_seed_cut: bool) {
    if !super::capacity_child_enters(case) {
        super::run_isolated_capacity_case(case, name).await;
        return;
    }
    let gate = Arc::new(Gate::new(force_six_seed_cut));
    assert!(GATE.set(gate.clone()).is_ok());
    std::env::set_var("LUMEN_PERF_DIAGNOSTIC", "1");
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(GateLayer).with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(GateWriter),
        ),
    )
    .expect("install isolated capacity schedule");
    super::two_local_full_requests_wait_for_one_checkpoint_then_commit_once_body().await;
    assert!(gate.first_start.load(Ordering::Acquire));
    assert!(gate.first_terminal.load(Ordering::Acquire));
    assert_eq!(gate.resumed_writes.load(Ordering::Acquire), 2);
    assert!(
        gate.gated_metrics.load(Ordering::Acquire),
        "the metrics handler must enter the controlled post-publication schedule"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_publication_resume_keeps_one_relief_checkpoint() {
    run("post-publication-capacity-resume", "capacity_http_contract::interleavings::post_publication_resume_keeps_one_relief_checkpoint", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_capture_does_not_treat_relieved_writers_as_fresh_work() {
    run("drained-capacity-cohort", "capacity_http_contract::interleavings::drained_capture_does_not_treat_relieved_writers_as_fresh_work", true).await;
}
