//! The budget relay's opt-in diagnostic trace (`LUMEN_PERF_DIAGNOSTIC=1`): the
//! capacity state a relay cycle starts from, and the timing and result of its
//! checkpoint, merge and consume phases.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::index::application::engine::Engine;

pub(super) fn relay_diagnostic_enabled() -> bool {
    std::env::var("LUMEN_PERF_DIAGNOSTIC").as_deref() == Ok("1")
}

#[derive(Clone, Copy)]
struct RelayCapacityState {
    active_bytes: usize,
    frozen_bytes: usize,
    request_revision: Option<u64>,
    work_revision: u64,
    pending_delta_bytes: u64,
    pending_delta_layers: u64,
    merge_completed_total: u64,
}

impl RelayCapacityState {
    fn read(engine: &Engine) -> Self {
        let owner = engine.capacity_owner_state();
        Self {
            active_bytes: owner.map_or(0, |state| state.active),
            frozen_bytes: owner.map_or(0, |state| state.frozen),
            request_revision: owner.and_then(|state| state.checkpoint_request_revision),
            work_revision: owner.map_or(0, |state| state.work_revision),
            pending_delta_bytes: engine.metrics().segment_pending_delta_bytes.get(),
            pending_delta_layers: engine.metrics().segment_pending_delta_layers.get(),
            // This counter is the safely available root merge progress view.
            // It never claims scheduler queue or ownership state.
            merge_completed_total: engine.metrics().segment_merge_completed_total.get(),
        }
    }
}

pub(super) struct RelayTrace {
    cycle_id: u64,
    start_request_revision: Option<u64>,
    start: RelayCapacityState,
    started: Instant,
    pub(super) checkpoint_ns: Option<u64>,
    pub(super) checkpoint_result: &'static str,
    pub(super) merge_ns: Option<u64>,
    pub(super) merge_result: &'static str,
    pub(super) consume_ns: Option<u64>,
    pub(super) consume_result: &'static str,
}

impl RelayTrace {
    pub(super) fn start(engine: &Engine, request_revision: Option<u64>) -> Self {
        static NEXT_RELAY_CYCLE_ID: AtomicU64 = AtomicU64::new(1);
        Self {
            cycle_id: NEXT_RELAY_CYCLE_ID.fetch_add(1, Ordering::Relaxed),
            start_request_revision: request_revision,
            start: RelayCapacityState::read(engine),
            started: Instant::now(),
            checkpoint_ns: None,
            checkpoint_result: "not_started",
            merge_ns: None,
            merge_result: "not_started",
            consume_ns: None,
            consume_result: "not_attempted",
        }
    }

    pub(super) fn phase(&self, engine: &Engine, phase: &'static str) {
        let state = RelayCapacityState::read(engine);
        tracing::info!(
            event = "segment_capacity_relay_diagnostic",
            relay_cycle_id = self.cycle_id,
            relay_phase = phase,
            request_revision = self.start_request_revision.unwrap_or_default(),
            request_pending = self.start_request_revision.is_some(),
            capacity_request_revision = state.request_revision.unwrap_or_default(),
            capacity_request_pending = state.request_revision.is_some(),
            capacity_work_revision = state.work_revision,
            active_bytes = state.active_bytes,
            frozen_bytes = state.frozen_bytes,
            pending_delta_bytes = state.pending_delta_bytes,
            pending_delta_layers = state.pending_delta_layers,
            merge_completed_total = state.merge_completed_total,
            relay_elapsed_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            "segment capacity relay diagnostic phase"
        );
    }

    pub(super) fn terminal(
        self,
        engine: &Engine,
        reason: &'static str,
        error: Option<&anyhow::Error>,
    ) {
        let end = RelayCapacityState::read(engine);
        let error = error.map_or_else(String::new, |error| format!("{error:#}"));
        tracing::info!(
            event = "segment_capacity_relay_diagnostic",
            relay_cycle_id = self.cycle_id,
            relay_phase = "terminal",
            request_revision = self.start_request_revision.unwrap_or_default(),
            request_pending = self.start_request_revision.is_some(),
            capacity_request_revision = end.request_revision.unwrap_or_default(),
            capacity_request_pending = end.request_revision.is_some(),
            capacity_work_revision = end.work_revision,
            active_bytes = end.active_bytes,
            frozen_bytes = end.frozen_bytes,
            pending_delta_bytes = end.pending_delta_bytes,
            pending_delta_layers = end.pending_delta_layers,
            merge_completed_total = end.merge_completed_total,
            relay_elapsed_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            start_request_revision = self.start_request_revision.unwrap_or_default(),
            start_request_pending = self.start_request_revision.is_some(),
            start_capacity_request_revision = self.start.request_revision.unwrap_or_default(),
            start_capacity_request_pending = self.start.request_revision.is_some(),
            start_capacity_work_revision = self.start.work_revision,
            start_active_bytes = self.start.active_bytes,
            start_frozen_bytes = self.start.frozen_bytes,
            checkpoint_result = self.checkpoint_result,
            checkpoint_ns = self.checkpoint_ns.unwrap_or_default(),
            merge_result = self.merge_result,
            merge_ns = self.merge_ns.unwrap_or_default(),
            consume_attempted_revision = self.start_request_revision.unwrap_or_default(),
            consume_attempted = self.start_request_revision.is_some(),
            consume_result = self.consume_result,
            consume_ns = self.consume_ns.unwrap_or_default(),
            end_reason = reason,
            end_capacity_request_revision = end.request_revision.unwrap_or_default(),
            end_capacity_request_pending = end.request_revision.is_some(),
            end_capacity_work_revision = end.work_revision,
            end_active_bytes = end.active_bytes,
            end_frozen_bytes = end.frozen_bytes,
            end_pending_delta_bytes = end.pending_delta_bytes,
            end_pending_delta_layers = end.pending_delta_layers,
            end_merge_completed_total = end.merge_completed_total,
            relay_total_ns = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            error = %error,
            "segment capacity relay diagnostic"
        );
    }
}
