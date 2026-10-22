//! CommittedApplyTelemetry: one state-writing apply's lock and HNSW timings,
//! kept on the stack while the engine state write lock is held and published to
//! Metrics once it drops.

use std::time::Duration;

use crate::app::observability::metrics::histogram::{
    observe_apply_duration_observations, ApplyDurationObservations,
};
use crate::app::observability::metrics::Metrics;

/// One state-writing apply telemetry scope. It stores measurements locally while
/// the engine state write lock is held, then publishes them only after that
/// guard drops. This keeps metrics atomics out of the state-lock interval.
pub(crate) struct CommittedApplyTelemetry<'a> {
    metrics: &'a Metrics,
    state_write_lock_wait: Option<Duration>,
    state_write_lock_held_started: Option<std::time::Instant>,
    hnsw_adds: ApplyDurationObservations,
    hnsw_graph_rebuilds: ApplyDurationObservations,
    hnsw_write_lock_waits: ApplyDurationObservations,
    hnsw_write_lock_holds: ApplyDurationObservations,
}

impl CommittedApplyTelemetry<'_> {
    /// Store the state-write wait without touching global metrics.
    pub(crate) fn record_state_write_lock_wait(&mut self, elapsed: Duration) {
        debug_assert!(self.state_write_lock_wait.is_none());
        self.state_write_lock_wait = Some(elapsed);
    }

    /// Begin the engine writer hold interval after the guard is acquired.
    pub(crate) fn start_state_write_lock_hold(&mut self) {
        debug_assert!(self.state_write_lock_held_started.is_none());
        self.state_write_lock_held_started = Some(std::time::Instant::now());
    }

    /// Finish the writer interval immediately after dropping the state guard.
    /// On an early return, [`Drop`] runs after that guard and finishes it.
    pub(crate) fn finish_state_write_lock_hold(&mut self) {
        if let Some(started) = self.state_write_lock_held_started.take() {
            self.metrics
                .observe_engine_state_write_lock_hold(started.elapsed());
        }
    }

    /// Record one HNSW graph-add duration without touching global metrics.
    pub(crate) fn record_hnsw_add(&mut self, elapsed: Duration) {
        self.hnsw_adds.record(elapsed);
    }

    /// Store a rare HNSW rebuild interval without publishing atomics under the
    /// Engine writer lock.
    pub(crate) fn record_hnsw_graph_rebuild(&mut self, elapsed: Duration) {
        self.hnsw_graph_rebuilds.record(elapsed);
    }

    /// Store an HNSW lock split without publishing atomics under Engine lock.
    pub(crate) fn record_hnsw_write_lock(&mut self, wait: Duration, held: Duration) {
        self.hnsw_write_lock_waits.record(wait);
        self.hnsw_write_lock_holds.record(held);
    }
}

impl Drop for CommittedApplyTelemetry<'_> {
    fn drop(&mut self) {
        if let Some(elapsed) = self.state_write_lock_wait {
            self.metrics.observe_engine_state_write_lock_wait(elapsed);
        }
        self.finish_state_write_lock_hold();
        self.metrics.observe_hnsw_add_observations(&self.hnsw_adds);
        self.metrics
            .observe_hnsw_graph_rebuild_observations(&self.hnsw_graph_rebuilds);
        self.metrics
            .observe_hnsw_write_lock_wait_observations(&self.hnsw_write_lock_waits);
        self.metrics
            .observe_hnsw_write_lock_hold_observations(&self.hnsw_write_lock_holds);
    }
}

impl Metrics {
    /// Start a state-writing apply telemetry scope. Its [`Drop`] publishes only
    /// after the state write guard declared after it has dropped.
    pub(crate) fn apply_telemetry(&self) -> CommittedApplyTelemetry<'_> {
        CommittedApplyTelemetry {
            metrics: self,
            state_write_lock_wait: None,
            state_write_lock_held_started: None,
            hnsw_adds: ApplyDurationObservations::default(),
            hnsw_graph_rebuilds: ApplyDurationObservations::default(),
            hnsw_write_lock_waits: ApplyDurationObservations::default(),
            hnsw_write_lock_holds: ApplyDurationObservations::default(),
        }
    }

    /// Start a committed-apply telemetry scope.
    pub(crate) fn committed_apply_telemetry(&self) -> CommittedApplyTelemetry<'_> {
        self.apply_telemetry()
    }

    pub(super) fn observe_hnsw_add_observations(&self, observations: &ApplyDurationObservations) {
        observe_apply_duration_observations(
            &self.hnsw_add_seconds_buckets,
            &self.hnsw_add_seconds_us_sum,
            &self.hnsw_add_seconds_count,
            observations,
        );
    }

    fn observe_hnsw_write_lock_wait_observations(&self, observations: &ApplyDurationObservations) {
        observe_apply_duration_observations(
            &self.hnsw_write_lock_wait_seconds_buckets,
            &self.hnsw_write_lock_wait_seconds_us_sum,
            &self.hnsw_write_lock_wait_seconds_count,
            observations,
        );
    }

    pub(super) fn observe_hnsw_graph_rebuild_observations(
        &self,
        observations: &ApplyDurationObservations,
    ) {
        observe_apply_duration_observations(
            &self.hnsw_graph_rebuild_seconds_buckets,
            &self.hnsw_graph_rebuild_seconds_us_sum,
            &self.hnsw_graph_rebuild_seconds_count,
            observations,
        );
    }

    fn observe_hnsw_write_lock_hold_observations(&self, observations: &ApplyDurationObservations) {
        observe_apply_duration_observations(
            &self.hnsw_write_lock_held_seconds_buckets,
            &self.hnsw_write_lock_held_seconds_us_sum,
            &self.hnsw_write_lock_held_seconds_count,
            observations,
        );
    }
}
