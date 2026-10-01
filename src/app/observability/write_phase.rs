//! Opt-in local submit facts. A WAL return is not a durability guarantee, and
//! an unknown caller outcome never proves rollback or a safe retry.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::app::observability::metrics::labels::ApplyKind;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::ingest::application::write_coordinator::errors::{RestartRequired, SubmitStalled};
use crate::ingest::domain::change_admission::PendingChangeCapacity;

enum Phase {
    AdmissionBegin,
    AdmissionEnd,
    PublicationBegin,
    PublicationEnd,
    ApplyWaitBegin,
    CallerEnd,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::AdmissionBegin => "admission_begin",
            Self::AdmissionEnd => "admission_end",
            Self::PublicationBegin => "publication_begin",
            Self::PublicationEnd => "publication_end",
            Self::ApplyWaitBegin => "apply_wait_begin",
            Self::CallerEnd => "caller_end",
        }
    }
}

enum ResultLabel {
    Started,
    Admitted,
    Refused,
    Error,
    SequenceReturned,
    Unknown,
    SequenceReturnedUnknown,
    Waiting,
    Applied,
    RefusedNotStarted,
    AdmissionErrorNotStarted,
    PublicationUnknown,
    RegistrationUnknown,
    ReceiverClosedUnknown,
    TimeoutUnknown,
    StaleUnknown,
    RestartUnknown,
    ApplyErrorUnknown,
    DetachedNotStarted,
    DetachedUnknown,
    DetachedSequenceReturned,
}

impl ResultLabel {
    fn label(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Admitted => "admitted",
            Self::Refused => "refused",
            Self::Error => "error",
            Self::SequenceReturned => "sequence_returned",
            Self::Unknown => "unknown",
            Self::SequenceReturnedUnknown => "sequence_returned_unknown",
            Self::Waiting => "waiting",
            Self::Applied => "applied",
            Self::RefusedNotStarted => "refused_not_started",
            Self::AdmissionErrorNotStarted => "admission_error_not_started",
            Self::PublicationUnknown => "publication_unknown",
            Self::RegistrationUnknown => "registration_unknown",
            Self::ReceiverClosedUnknown => "receiver_closed_unknown",
            Self::TimeoutUnknown => "timeout_unknown",
            Self::StaleUnknown => "stale_unknown",
            Self::RestartUnknown => "restart_unknown",
            Self::ApplyErrorUnknown => "apply_error_unknown",
            Self::DetachedNotStarted => "detached_not_started",
            Self::DetachedUnknown => "detached_unknown",
            Self::DetachedSequenceReturned => "detached_sequence_returned",
        }
    }
}

struct Context {
    attempt_id: u64,
    kind: ApplyKind,
    started: Instant,
    handed_off: AtomicBool,
    sequence_present: AtomicBool,
    sequence: AtomicU64,
    dispatch: tracing::Dispatch,
}

impl Context {
    fn sequence(&self) -> (bool, u64) {
        let present = self.sequence_present.load(Ordering::Acquire);
        (
            present,
            if present {
                self.sequence.load(Ordering::Relaxed)
            } else {
                0
            },
        )
    }

    fn emit(&self, phase: Phase, result: ResultLabel, elapsed: Duration) {
        self.emit_snapshot(phase, result, elapsed, self.sequence());
    }

    fn emit_snapshot(
        &self,
        phase: Phase,
        result: ResultLabel,
        elapsed: Duration,
        (wal_sequence_present, wal_sequence): (bool, u64),
    ) {
        tracing::dispatcher::with_default(&self.dispatch, || {
            tracing::info!(
                target: "lumen_write_phase",
                event = "lumen_write_phase",
                attempt_id = self.attempt_id,
                kind = self.kind.label(),
                phase = phase.label(),
                result = result.label(),
                wal_sequence_present,
                wal_sequence,
                total_elapsed_us = micros(self.started.elapsed()),
                phase_elapsed_us = micros(elapsed),
            );
        });
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

struct Caller {
    context: Arc<Context>,
    phase_started: Instant,
    finished: bool,
}

impl Caller {
    fn finish(&mut self, result: ResultLabel, sequence: (bool, u64)) {
        self.finished = true;
        self.context.emit_snapshot(
            Phase::CallerEnd,
            result,
            self.phase_started.elapsed(),
            sequence,
        );
    }
}

impl Drop for Caller {
    fn drop(&mut self) {
        if !self.finished {
            let sequence = self.context.sequence();
            let result = if sequence.0 {
                ResultLabel::DetachedSequenceReturned
            } else if self.context.handed_off.load(Ordering::Acquire) {
                ResultLabel::DetachedUnknown
            } else {
                ResultLabel::DetachedNotStarted
            };
            self.finish(result, sequence);
        }
    }
}

/// Disabled submits have no diagnostic ID, clock, Arc, or field formatting.
/// The environment enable is cached once; INFO must also be enabled.
pub(crate) struct WritePhase(Option<Caller>);

impl WritePhase {
    pub(crate) fn begin(kind: ApplyKind, #[cfg(test)] capture_enabled: bool) -> Self {
        static ENABLED: OnceLock<bool> = OnceLock::new();
        let enabled =
            *ENABLED.get_or_init(|| std::env::var("LUMEN_PERF_DIAGNOSTIC").as_deref() == Ok("1"));
        #[cfg(test)]
        let enabled = enabled || capture_enabled;
        if !enabled || !tracing::enabled!(target: "lumen_write_phase", tracing::Level::INFO) {
            return Self(None);
        }
        static NEXT_ATTEMPT: AtomicU64 = AtomicU64::new(1);
        let Ok(attempt_id) =
            NEXT_ATTEMPT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
        else {
            // Exhaustion disables only diagnostics. IDs never wrap or repeat.
            return Self(None);
        };
        let started = Instant::now();
        let context = Arc::new(Context {
            attempt_id,
            kind,
            started,
            handed_off: AtomicBool::new(false),
            sequence_present: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
        });
        context.emit(Phase::AdmissionBegin, ResultLabel::Started, Duration::ZERO);
        Self(Some(Caller {
            context,
            phase_started: started,
            finished: false,
        }))
    }

    pub(crate) fn admission_end(&self, error: Option<&anyhow::Error>) {
        if let Some(caller) = &self.0 {
            let result = match error {
                None => ResultLabel::Admitted,
                Some(error) if error.downcast_ref::<PendingChangeCapacity>().is_some() => {
                    ResultLabel::Refused
                }
                Some(_) => ResultLabel::Error,
            };
            caller
                .context
                .emit(Phase::AdmissionEnd, result, caller.phase_started.elapsed());
        }
    }

    pub(crate) fn prepublication_error(&mut self, error: &anyhow::Error) {
        if let Some(caller) = &mut self.0 {
            let result = if error.downcast_ref::<PendingChangeCapacity>().is_some() {
                ResultLabel::RefusedNotStarted
            } else {
                ResultLabel::AdmissionErrorNotStarted
            };
            caller.finish(result, caller.context.sequence());
        }
    }

    pub(crate) fn publication_begin(&mut self) -> Publication {
        Publication(self.0.as_mut().map(|caller| {
            caller.phase_started = Instant::now();
            caller.context.handed_off.store(true, Ordering::Release);
            caller.context.emit(
                Phase::PublicationBegin,
                ResultLabel::Started,
                Duration::ZERO,
            );
            (caller.context.clone(), caller.phase_started)
        }))
    }

    pub(crate) fn publication_error(&mut self, receiver_closed: bool) {
        if let Some(caller) = &mut self.0 {
            let sequence = caller.context.sequence();
            let result = if sequence.0 && !receiver_closed {
                ResultLabel::RegistrationUnknown
            } else {
                ResultLabel::PublicationUnknown
            };
            caller.finish(result, sequence);
        }
    }

    pub(crate) fn apply_wait_begin(&mut self) {
        if let Some(caller) = &mut self.0 {
            caller.phase_started = Instant::now();
            caller
                .context
                .emit(Phase::ApplyWaitBegin, ResultLabel::Waiting, Duration::ZERO);
        }
    }

    pub(crate) fn outcome(&mut self, outcome: &Result<ApplyOutcome>) {
        if let Some(caller) = &mut self.0 {
            let result = match outcome {
                Ok(_) => ResultLabel::Applied,
                Err(error) if error.downcast_ref::<SubmitStalled>().is_some() => {
                    ResultLabel::StaleUnknown
                }
                Err(error) if error.downcast_ref::<RestartRequired>().is_some() => {
                    ResultLabel::RestartUnknown
                }
                Err(_) => ResultLabel::ApplyErrorUnknown,
            };
            caller.finish(result, caller.context.sequence());
        }
    }

    pub(crate) fn receiver_closed(&mut self) {
        if let Some(caller) = &mut self.0 {
            caller.finish(
                ResultLabel::ReceiverClosedUnknown,
                caller.context.sequence(),
            );
        }
    }

    pub(crate) fn timeout(&mut self) {
        if let Some(caller) = &mut self.0 {
            caller.finish(ResultLabel::TimeoutUnknown, caller.context.sequence());
        }
    }
}

/// Owned only by the existing publisher, never by a per-sequence ledger.
pub(crate) struct Publication(Option<(Arc<Context>, Instant)>);

impl Publication {
    pub(crate) fn sequence_returned(&self, seq: u64) {
        if let Some((context, _)) = &self.0 {
            context.sequence.store(seq, Ordering::Relaxed);
            context.sequence_present.store(true, Ordering::Release);
        }
    }

    pub(crate) fn end(&self, succeeded: bool) {
        if let Some((context, started)) = &self.0 {
            let result = if succeeded {
                ResultLabel::SequenceReturned
            } else if context.sequence().0 {
                ResultLabel::SequenceReturnedUnknown
            } else {
                ResultLabel::Unknown
            };
            context.emit(Phase::PublicationEnd, result, started.elapsed());
        }
    }
}
