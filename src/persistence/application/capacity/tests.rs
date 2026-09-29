use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};

use storage_durable::{CommitStep, FailureInjector, FailurePoint};

use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::storage::Engine;

struct DiagnosticEnvironment {
    _lock: std::sync::MutexGuard<'static, ()>,
    previous: Option<std::ffi::OsString>,
}

impl DiagnosticEnvironment {
    fn set(enabled: bool) -> Self {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = std::env::var_os("LUMEN_PERF_DIAGNOSTIC");
        if enabled {
            std::env::set_var("LUMEN_PERF_DIAGNOSTIC", "1");
        } else {
            std::env::remove_var("LUMEN_PERF_DIAGNOSTIC");
        }
        Self {
            _lock: lock,
            previous,
        }
    }
}

impl Drop for DiagnosticEnvironment {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            std::env::set_var("LUMEN_PERF_DIAGNOSTIC", previous);
        } else {
            std::env::remove_var("LUMEN_PERF_DIAGNOSTIC");
        }
    }
}

#[derive(Clone, Default)]
struct DiagnosticTraceWriter(Arc<Mutex<Vec<u8>>>);

struct DiagnosticTraceWriterGuard(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for DiagnosticTraceWriterGuard {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for DiagnosticTraceWriter {
    type Writer = DiagnosticTraceWriterGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        DiagnosticTraceWriterGuard(self.0.clone())
    }
}

impl DiagnosticTraceWriter {
    fn records(&self) -> Vec<serde_json::Value> {
        String::from_utf8(self.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

fn relay_diagnostic_records(enabled: bool, run: impl FnOnce()) -> Vec<serde_json::Value> {
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
    run();
    drop(_guard);
    writer.records()
}

fn relay_record(records: Vec<serde_json::Value>) -> serde_json::Value {
    records
        .into_iter()
        .find(|record| {
            record["fields"]["event"] == "segment_capacity_relay_diagnostic"
                && record["fields"]["relay_phase"] == "terminal"
        })
        .expect("relay must emit one terminal diagnostic event")
}

fn relay_phases(records: &[serde_json::Value]) -> Vec<&str> {
    records
        .iter()
        .filter(|record| record["fields"]["event"] == "segment_capacity_relay_diagnostic")
        .map(|record| {
            record["fields"]["relay_phase"]
                .as_str()
                .expect("relay diagnostic event must include phase")
        })
        .collect()
}

fn engine() -> Arc<Engine> {
    let engine = Arc::new(Engine::new());
    engine
        .create_collection(
            "docs",
            serde_json::from_value(serde_json::json!({
                "fields": {"kw":{"type":"keyword"}}
            }))
            .unwrap(),
        )
        .unwrap();
    engine
        .index(
            "docs",
            serde_json::from_value(serde_json::json!({
                "items":[{"external_id":"one","field":"kw","value":"retained"}]
            }))
            .unwrap(),
        )
        .unwrap();
    engine
}

fn sink(engine: Arc<Engine>, store: Arc<SegmentRdbStore>) -> Arc<SegmentCheckpointSink> {
    Arc::new(SegmentCheckpointSink {
        writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
        engine,
        store,
        aof: None,
    })
}

struct FailSync(AtomicBool);

impl FailureInjector for FailSync {
    fn check(&self, point: &FailurePoint) -> std::io::Result<()> {
        if point.step == CommitStep::SyncFile && self.0.swap(false, Ordering::AcqRel) {
            return Err(std::io::Error::other("capacity frozen retry test"));
        }
        Ok(())
    }
}

struct HoldSync {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl FailureInjector for HoldSync {
    fn check(&self, point: &FailurePoint) -> std::io::Result<()> {
        if point.step == CommitStep::SyncFile {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
        }
        Ok(())
    }
}

struct Release(Option<mpsc::Sender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

mod capacity_requests;

mod owner;

mod relay;
