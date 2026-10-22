//! Checkpoint diagnostics: the per-attempt trace a checkpoint emits when its
//! diagnostic environment flag is set, the save-gate and capacity-wait trace
//! points, and the test-only capture of numeric canonical events.

use crate::persistence::application::background_merge::CapacityWait;
use crate::persistence::infrastructure::segment_rdb_store::save_gate::{SaveGate, SavePermit};
use anyhow::Result;
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Identifies one diagnostic checkpoint attempt. This context is trace-only
/// and never participates in save selection or ownership.
#[derive(Clone, Copy)]
pub(crate) struct CheckpointDiagnosticContext {
    pub(super) origin: &'static str,
    pub(super) attempt_id: Option<u64>,
    started: Instant,
    #[cfg(test)]
    capture: Option<DiagnosticCaptureToken>,
}

/// A copyable test address. The registry owns the sender, so production
/// checkpoint contexts remain small and copyable across blocking tasks.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DiagnosticCaptureToken(u64);

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NumericCanonicalEvent {
    Phase {
        attempt_id: u64,
        phase: &'static str,
        pass: u8,
        reused: bool,
        frozen_bytes: u64,
    },
    Selected {
        attempt_id: u64,
        reason: &'static str,
        source: &'static str,
        pending_total: usize,
    },
    Refusal {
        revision: u64,
        present: bool,
        requested: usize,
        used: usize,
        hard_limit: usize,
    },
}

#[cfg(test)]
type CaptureSenders =
    Mutex<HashMap<DiagnosticCaptureToken, std::sync::mpsc::SyncSender<NumericCanonicalEvent>>>;

#[cfg(test)]
fn capture_senders() -> &'static CaptureSenders {
    static SENDERS: OnceLock<CaptureSenders> = OnceLock::new();
    SENDERS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(crate) fn send_numeric_event(token: DiagnosticCaptureToken, event: NumericCanonicalEvent) {
    // Never hold the registry lock while delivering. A full or closed test
    // channel must not change the checkpoint or refusal path.
    let sender = capture_senders()
        .lock()
        .ok()
        .and_then(|senders| senders.get(&token).cloned());
    if let Some(sender) = sender {
        let _ = sender.try_send(event);
    }
}

#[cfg(test)]
pub(crate) struct DiagnosticCapture {
    token: DiagnosticCaptureToken,
    receiver: std::sync::mpsc::Receiver<NumericCanonicalEvent>,
}

#[cfg(test)]
impl DiagnosticCapture {
    pub(crate) fn new() -> Self {
        static NEXT_TOKEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let token = DiagnosticCaptureToken(
            NEXT_TOKEN
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |next| next.checked_add(1),
                )
                .expect("diagnostic capture tokens exhausted"),
        );
        let (sender, receiver) = std::sync::mpsc::sync_channel(64);
        capture_senders().lock().unwrap().insert(token, sender);
        Self { token, receiver }
    }

    pub(crate) fn token(&self) -> DiagnosticCaptureToken {
        self.token
    }

    pub(crate) fn drain(&self) -> Vec<NumericCanonicalEvent> {
        self.receiver.try_iter().collect()
    }
}

#[cfg(test)]
impl Drop for DiagnosticCapture {
    fn drop(&mut self) {
        if let Ok(mut senders) = capture_senders().lock() {
            senders.remove(&self.token);
        }
    }
}

impl CheckpointDiagnosticContext {
    pub(crate) fn new(origin: &'static str, attempt_id: Option<u64>) -> Self {
        Self {
            origin,
            attempt_id,
            started: Instant::now(),
            #[cfg(test)]
            capture: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_capture(mut self, token: DiagnosticCaptureToken) -> Self {
        self.capture = Some(token);
        self
    }

    #[cfg(test)]
    fn capture_event(self, event: NumericCanonicalEvent) {
        if let Some(token) = self.capture {
            send_numeric_event(token, event);
        }
    }

    fn attempt_id(self) -> Option<u64> {
        self.attempt_id
    }

    /// Emit one bounded, machine-readable lifecycle phase. A missing attempt
    /// id is intentionally silent, so ordinary saves keep their existing path.
    pub(crate) fn trace_phase(self, phase: &'static str) {
        let Some(checkpoint_attempt_id) = self.attempt_id() else {
            return;
        };
        #[cfg(test)]
        self.capture_event(NumericCanonicalEvent::Phase {
            attempt_id: checkpoint_attempt_id,
            phase,
            pass: 0,
            reused: false,
            frozen_bytes: 0,
        });
        tracing::info!(
            event = "segment_checkpoint_diagnostic_phase",
            phase,
            checkpoint_attempt_id,
            checkpoint_origin = self.origin,
            elapsed_ns = duration_ns(self.started.elapsed()),
            "segment checkpoint diagnostic phase"
        );
    }

    /// Record the scheduler branch that actually selected this checkpoint.
    /// This has no wake-lag field because a coalesced channel notice cannot
    /// provide a precise wake timestamp.
    pub(crate) fn trace_scheduler_selected(
        self,
        reason: &'static str,
        resample_source: &'static str,
        pending: crate::ingest::domain::change_budget::Snapshot,
    ) {
        let Some(checkpoint_attempt_id) = self.attempt_id() else {
            return;
        };
        #[cfg(test)]
        self.capture_event(NumericCanonicalEvent::Selected {
            attempt_id: checkpoint_attempt_id,
            reason,
            source: resample_source,
            pending_total: pending.total,
        });
        tracing::info!(
            event = "segment_checkpoint_diagnostic_phase",
            phase = "scheduler_selected",
            checkpoint_attempt_id,
            checkpoint_origin = self.origin,
            scheduler_reason = reason,
            resample_source,
            elapsed_ns = duration_ns(self.started.elapsed()),
            pending_total_bytes = pending.total,
            pending_reserved_bytes = pending.reserved,
            pending_active_bytes = pending.active,
            pending_frozen_bytes = pending.frozen,
            checkpoint_trigger_bytes = crate::ingest::domain::change_budget::CHECKPOINT_TRIGGER,
            "segment checkpoint diagnostic phase"
        );
    }

    pub(crate) fn trace_freeze_completed(
        self,
        checkpoint_pass: u8,
        frozen_cut_reused: bool,
        frozen_cut_bytes: u64,
    ) {
        let Some(checkpoint_attempt_id) = self.attempt_id() else {
            return;
        };
        #[cfg(test)]
        self.capture_event(NumericCanonicalEvent::Phase {
            attempt_id: checkpoint_attempt_id,
            phase: "freeze_completed",
            pass: checkpoint_pass,
            reused: frozen_cut_reused,
            frozen_bytes: frozen_cut_bytes,
        });
        tracing::info!(
            event = "segment_checkpoint_diagnostic_phase",
            phase = "freeze_completed",
            checkpoint_attempt_id,
            checkpoint_origin = self.origin,
            checkpoint_pass,
            frozen_cut_reused,
            frozen_cut_bytes,
            elapsed_ns = duration_ns(self.started.elapsed()),
            "segment checkpoint diagnostic phase"
        );
    }

    pub(crate) fn trace_publish_completed(self, checkpoint_pass: u8) {
        let Some(checkpoint_attempt_id) = self.attempt_id() else {
            return;
        };
        #[cfg(test)]
        self.capture_event(NumericCanonicalEvent::Phase {
            attempt_id: checkpoint_attempt_id,
            phase: "publish_completed",
            pass: checkpoint_pass,
            reused: false,
            frozen_bytes: 0,
        });
        tracing::info!(
            event = "segment_checkpoint_diagnostic_phase",
            phase = "publish_completed",
            checkpoint_attempt_id,
            checkpoint_origin = self.origin,
            checkpoint_pass,
            elapsed_ns = duration_ns(self.started.elapsed()),
            "segment checkpoint diagnostic phase"
        );
    }

    pub(crate) fn trace_terminal(self, result: &Result<()>) {
        let Some(checkpoint_attempt_id) = self.attempt_id() else {
            return;
        };
        #[cfg(test)]
        self.capture_event(NumericCanonicalEvent::Phase {
            attempt_id: checkpoint_attempt_id,
            phase: "terminal",
            pass: 0,
            reused: false,
            frozen_bytes: 0,
        });
        tracing::info!(
            event = "segment_checkpoint_diagnostic_phase",
            phase = "terminal",
            checkpoint_attempt_id,
            checkpoint_origin = self.origin,
            elapsed_ns = duration_ns(self.started.elapsed()),
            terminal_result = if result.is_ok() { "ok" } else { "error" },
            "segment checkpoint diagnostic phase"
        );
    }
}

/// Timing that starts before a root save permit is requested. A checkpoint
/// always takes this permit before it freezes a cut, so capture-to-gate wait is
/// structurally zero and is emitted as such in the diagnostic event.
#[derive(Clone, Copy)]
pub(super) struct SaveGateTrace {
    pub(super) wait_ns: u64,
    pub(super) acquired_at: Instant,
}

impl SaveGateTrace {
    pub(super) fn acquire(gate: &Arc<SaveGate>) -> (SavePermit, Self) {
        let started = Instant::now();
        let permit = gate.lock_owned();
        (
            permit,
            Self {
                wait_ns: duration_ns(started.elapsed()),
                acquired_at: Instant::now(),
            },
        )
    }
}

pub(super) fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn checkpoint_diagnostic_enabled() -> bool {
    std::env::var("LUMEN_PERF_DIAGNOSTIC").as_deref() == Ok("1")
}

pub(super) fn trace_save_gate_acquired(
    context: Option<CheckpointDiagnosticContext>,
    trace: SaveGateTrace,
) {
    // Gate timing remains in the one bounded completion record. It is not a
    // lifecycle phase, so one checkpoint never emits duplicate phase paths.
    let _ = (context, trace);
}

pub(super) fn trace_capacity_wait_begin(
    context: Option<CheckpointDiagnosticContext>,
    revision: u64,
) {
    let _ = (context, revision);
}

pub(super) fn trace_capacity_wait_end(
    context: Option<CheckpointDiagnosticContext>,
    revision: u64,
    duration_ns: Option<u64>,
    result: std::result::Result<&CapacityWait, &anyhow::Error>,
) {
    let _ = (context, revision, duration_ns, result);
}

pub(super) fn trace_durable_save_end(
    context: Option<CheckpointDiagnosticContext>,
    sequence: u64,
    revision: u64,
    duration_ns: u64,
) {
    let _ = (context, sequence, revision, duration_ns);
}
