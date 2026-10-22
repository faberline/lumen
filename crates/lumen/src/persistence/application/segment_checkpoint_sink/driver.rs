//! The periodic checkpoint driver: a native thread that turns change-budget
//! wakes into notices, and a Tokio task that samples the budget, asks the
//! checkpoint schedule whether to start, and checkpoints under the capacity
//! owner's publication fence.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::ingest::domain::change_budget::{BudgetWake, ChangeBudget, Snapshot};
use crate::persistence::application::segment_checkpoint_sink::{
    CheckpointTraceOrigin, SegmentCheckpointSink,
};
use crate::persistence::domain::checkpoint_schedule::CheckpointSchedule;
use crate::persistence::infrastructure::checkpoint_process_state::NEXT_CHECKPOINT_ATTEMPT_ID;
#[cfg(test)]
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::DiagnosticCaptureToken;
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
    checkpoint_diagnostic_enabled, CheckpointDiagnosticContext,
};

const WAITER_SHUTDOWN_POLL: Duration = Duration::from_millis(50);

/// Owns one native budget waiter and one Tokio checkpoint task.
#[doc(hidden)]
pub struct SegmentCheckpointDriver {
    pub(super) stop: Arc<AtomicBool>,
    waiter: Option<std::thread::JoinHandle<()>>,
    checkpoint_task: Option<tokio::task::JoinHandle<()>>,
    shutdown: Option<oneshot::Sender<()>>,
    capacity_owner: Option<crate::persistence::application::capacity::worker::Owner>,
}

impl Drop for SegmentCheckpointDriver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.checkpoint_task.take() {
            task.abort();
        }
        if let Some(waiter) = self.waiter.take() {
            let _ = waiter.join();
        }
    }
}

impl SegmentCheckpointDriver {
    /// Stop new scheduling without waiting for an already-owned save. Shutdown
    /// cache IO subsequently joins the same store gate within its own deadline.
    #[doc(hidden)]
    pub fn request_stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(owner) = &mut self.capacity_owner {
            owner.stop();
        }
    }

    /// Stop scheduling and wait for a started blocking save before another
    /// driver uses the same checkpoint root. `Drop` remains emergency cleanup.
    #[doc(hidden)]
    pub async fn shutdown(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        // Await through a mutable field borrow. Cancellation leaves the task
        // handle in self, so a later shutdown can still wait for the same save.
        let result = match self.checkpoint_task.as_mut() {
            Some(task) => task
                .await
                .map_err(|error| anyhow::anyhow!("checkpoint task failed: {error}")),
            None => Ok(()),
        };
        self.checkpoint_task.take();
        result?;
        if let Some(mut owner) = self.capacity_owner.take() {
            tokio::task::spawn_blocking(move || owner.join())
                .await
                .map_err(|error| anyhow::anyhow!("capacity shutdown task failed: {error}"))??;
        }
        // The task has now observed stop after any started blocking save. The
        // waiter exits within WAITER_SHUTDOWN_POLL, so this bounded join has no
        // await point that could lose its ownership to cancellation.
        if let Some(waiter) = self.waiter.take() {
            waiter
                .join()
                .map_err(|_| anyhow::anyhow!("checkpoint waiter panicked"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CheckpointResampleSource {
    InitialSample,
    BudgetNotice,
    DeadlineTick,
    PostCheckpoint,
}

impl CheckpointResampleSource {
    fn label(self) -> &'static str {
        match self {
            Self::InitialSample => "initial_sample",
            Self::BudgetNotice => "budget_notice",
            Self::DeadlineTick => "deadline_tick",
            Self::PostCheckpoint => "post_checkpoint",
        }
    }
}

impl SegmentCheckpointSink {
    pub(super) fn complete_periodic_checkpoint(
        schedule: &mut CheckpointSchedule,
        after: Snapshot,
        owner_before: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
        owner_after: Option<crate::ingest::domain::change_budget::OwnerCapacityState>,
    ) {
        schedule.completed_success(Instant::now(), after, owner_before, owner_after);
    }

    /// Start the periodic form of checkpoint_now. The budget only supplies
    /// wake hints; the same sink performs manual and periodic saves.
    #[doc(hidden)]
    pub fn spawn_periodic_driver(self: Arc<Self>, period: Duration) -> SegmentCheckpointDriver {
        self.spawn_driver_with_budget(period, ChangeBudget::process_shared())
    }

    pub(super) fn spawn_driver_with_budget(
        self: Arc<Self>,
        period: Duration,
        budget: ChangeBudget,
    ) -> SegmentCheckpointDriver {
        self.spawn_driver_with_owner_kind(period, budget, true)
    }

    pub(super) fn spawn_driver_with_owner_kind(
        self: Arc<Self>,
        period: Duration,
        budget: ChangeBudget,
        configured: bool,
    ) -> SegmentCheckpointDriver {
        #[cfg(test)]
        {
            self.spawn_driver_inner(period, budget, configured, None)
        }
        #[cfg(not(test))]
        {
            self.spawn_driver_inner(period, budget, configured)
        }
    }

    #[cfg(test)]
    pub(super) fn spawn_driver_with_capture(
        self: Arc<Self>,
        period: Duration,
        budget: ChangeBudget,
        token: DiagnosticCaptureToken,
    ) -> (SegmentCheckpointDriver, oneshot::Receiver<()>) {
        let (idle_tx, idle_rx) = oneshot::channel();
        (
            self.spawn_driver_inner(period, budget, true, Some((token, idle_tx))),
            idle_rx,
        )
    }

    fn spawn_driver_inner(
        self: Arc<Self>,
        period: Duration,
        budget: ChangeBudget,
        configured: bool,
        #[cfg(test)] diagnostic: Option<(DiagnosticCaptureToken, oneshot::Sender<()>)>,
    ) -> SegmentCheckpointDriver {
        let capacity_owner = crate::persistence::application::capacity::worker::Owner::start(
            self.clone(),
            configured,
        )
        .expect("start native layer capacity owner");
        let periodic_fence = capacity_owner.as_ref().map(|owner| owner.fence());
        if capacity_owner.is_none() && !configured {
            return SegmentCheckpointDriver {
                stop: Arc::new(AtomicBool::new(false)),
                waiter: None,
                checkpoint_task: None,
                shutdown: None,
                capacity_owner: None,
            };
        }
        let wake = budget.checkpoint_wake();
        let (notices, mut notices_rx) = mpsc::channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let observed = wake.epoch();
        let waiter = spawn_budget_waiter(wake, notices, stop.clone(), observed);
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let task_stop = stop.clone();
        #[cfg(test)]
        let (capture_token, mut idle_signal) = match diagnostic {
            Some((token, signal)) => (Some(token), Some(signal)),
            None => (None, None),
        };
        let checkpoint_task = tokio::spawn(async move {
            let mut schedule = CheckpointSchedule::new(period, Instant::now());
            let mut resample_source = CheckpointResampleSource::InitialSample;
            loop {
                if task_stop.load(Ordering::Acquire) {
                    return;
                }
                // This pre-sleep sample makes initially high work visible even
                // if it predates creation of the waiter.
                let now = Instant::now();
                let pending = budget.snapshot();
                let owner_before = self.engine.capacity_owner_state();
                if let Some(reason) = schedule.select_attempt(now, pending, owner_before) {
                    #[cfg(not(test))]
                    let diagnostic_context = checkpoint_diagnostic_enabled().then(|| {
                        CheckpointDiagnosticContext::new(
                            CheckpointTraceOrigin::Periodic.label(),
                            Some(NEXT_CHECKPOINT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)),
                        )
                    });
                    #[cfg(test)]
                    let diagnostic_context =
                        (capture_token.is_some() || checkpoint_diagnostic_enabled()).then(|| {
                            let context = CheckpointDiagnosticContext::new(
                                CheckpointTraceOrigin::Periodic.label(),
                                Some(NEXT_CHECKPOINT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)),
                            );
                            if let Some(token) = capture_token {
                                context.with_capture(token)
                            } else {
                                context
                            }
                        });
                    if let Some(context) = diagnostic_context {
                        context.trace_scheduler_selected(
                            reason.label(),
                            resample_source.label(),
                            pending,
                        );
                    }
                    match self
                        .checkpoint_with_fence(
                            periodic_fence.clone(),
                            CheckpointTraceOrigin::Periodic,
                            diagnostic_context,
                        )
                        .await
                    {
                        Ok(_) => {
                            let after = budget.snapshot();
                            let owner_after = self.engine.capacity_owner_state();
                            Self::complete_periodic_checkpoint(
                                &mut schedule,
                                after,
                                owner_before,
                                owner_after,
                            );
                        }
                        Err(error) => {
                            if crate::ingest::application::write_coordinator::errors::is_storage_full(&error) {
                                self.engine.metrics().mark_storage_degraded();
                            }
                            tracing::warn!(error = %format!("{error:#}"), "periodic segment checkpoint failed");
                            // Failed checkpoints retain the normal period
                            // backoff and leave the request pending.
                            schedule.completed(Instant::now(), pending);
                        }
                    }
                    let mut saw_budget_notice = false;
                    while notices_rx.try_recv().is_ok() {
                        saw_budget_notice = true;
                    }
                    resample_source = if saw_budget_notice {
                        CheckpointResampleSource::BudgetNotice
                    } else {
                        // This immediate loop is a fresh sample after the
                        // completed checkpoint, not a deadline wake.
                        CheckpointResampleSource::PostCheckpoint
                    };
                    continue;
                }
                #[cfg(test)]
                if let Some(signal) = idle_signal.take() {
                    let _ = signal.send(());
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(schedule.next_deadline) => {
                        resample_source = CheckpointResampleSource::DeadlineTick;
                    }
                    _ = &mut shutdown_rx => return,
                    notice = notices_rx.recv() => {
                        match notice {
                            Some(_) => resample_source = CheckpointResampleSource::BudgetNotice,
                            None => return,
                        }
                    }
                }
            }
        });
        SegmentCheckpointDriver {
            stop,
            waiter: Some(waiter),
            checkpoint_task: Some(checkpoint_task),
            shutdown: Some(shutdown),
            capacity_owner,
        }
    }
}

pub(super) fn spawn_budget_waiter(
    wake: Arc<BudgetWake>,
    notices: mpsc::Sender<Instant>,
    stop: Arc<AtomicBool>,
    mut observed: u64,
) -> std::thread::JoinHandle<()> {
    // Subscribe before the async task can sample pending work. Starting the
    // thread later must not swallow a change between that sample and sleep.
    std::thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            let _ = wake.wait_for_change_timeout(observed, WAITER_SHUTDOWN_POLL);
            let current = wake.epoch();
            if current == observed {
                continue;
            }
            observed = current;
            match notices.try_send(Instant::now()) {
                Ok(()) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Closed(_)) => return,
            }
        }
    })
}
