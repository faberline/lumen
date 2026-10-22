//! Slow Engine state-lock holds.
//!
//! Every holder of `state` blocks writers, and a waiting writer blocks new
//! readers, so one long hold stalls search and apply across the shard. A hold
//! or wait past [`SLOW_STATE_HOLD`] is logged with the path that took it.

use std::time::{Duration, Instant};

pub(super) const SLOW_STATE_HOLD: Duration = Duration::from_secs(1);

/// Declare before taking the lock, so it drops after the guard.
pub(super) struct StateHoldTrace<'a> {
    path: &'static str,
    collection: &'a str,
    detail: &'static str,
    started: Instant,
    acquired: Option<Instant>,
}

impl<'a> StateHoldTrace<'a> {
    pub(super) fn begin(path: &'static str, collection: &'a str, detail: &'static str) -> Self {
        Self {
            path,
            collection,
            detail,
            started: Instant::now(),
            acquired: None,
        }
    }

    pub(super) fn acquired(&mut self) {
        self.acquired = Some(Instant::now());
    }
}

impl Drop for StateHoldTrace<'_> {
    fn drop(&mut self) {
        let Some(acquired) = self.acquired else {
            return;
        };
        let waited = acquired.saturating_duration_since(self.started);
        let held = acquired.elapsed();
        if waited < SLOW_STATE_HOLD && held < SLOW_STATE_HOLD {
            return;
        }
        tracing::warn!(
            path = self.path,
            collection = self.collection,
            detail = self.detail,
            waited_ms = waited.as_millis() as u64,
            held_ms = held.as_millis() as u64,
            "slow engine state lock hold"
        );
    }
}
