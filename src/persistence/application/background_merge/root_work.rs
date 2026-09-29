//! The root-local merge scheduler: the pins that keep a generation's files
//! alive while a reader or a merge still owns them, retired-generation reclaim,
//! the capacity-wait handshake with a blocked checkpoint, and job completion.

use crate::persistence::application::background_merge::{
    CapacityWait, MergeOutcome, RootWork, TraceState, WorkState,
};
use crate::storage::Engine;
use anyhow::{anyhow, bail, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};

impl WorkState {
    // Called under the root mutex, with the save permit still held. An idle
    // observation expires when a later publication changes this revision.
    pub(super) fn validate_capacity_retry(
        &self,
        idle_revision: Option<u64>,
        deadline: Option<Instant>,
    ) -> Result<()> {
        if self.publication_revision_overflowed {
            bail!("background segment merge publication revision overflowed");
        }
        if (idle_revision.is_some() || deadline.is_some()) && !self.queued && !self.running {
            if let Some(error) = &self.error {
                bail!("background segment merge failed: {error}");
            }
        }
        // A ready predicate may wake after its deadline. Preflight is allowed
        // to use the freed capacity, but a still-full field cannot request more
        // work after that same deadline.
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            bail!("background segment merge capacity wait timed out");
        }
        if idle_revision
            .is_some_and(|revision| self.published_revision <= revision && !self.retryable_stale)
        {
            bail!("background segment merge made no capacity progress");
        }
        Ok(())
    }
}

impl RootWork {
    pub(in crate::persistence) fn trace_state(&self) -> TraceState {
        let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        TraceState {
            queued: state.queued,
            running: state.running,
            requested: state.requested,
            published_revision: state.published_revision,
        }
    }

    pub(in crate::persistence) fn has_live_ownership(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .readers
            .retain(|(_, readers)| readers.iter().any(|reader| reader.strong_count() != 0));
        state.queued
            || state.running
            || !state.protected.is_empty()
            || !state.retired.is_empty()
            || !state.readers.is_empty()
    }
    pub(in crate::persistence) fn defer_reclaim(&self, name: &str) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retired
            .insert(name.to_owned());
    }
    pub(in crate::persistence) fn known_retired(&self, name: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retired
            .contains(name)
    }
    pub(in crate::persistence) fn reclaimed(&self, name: &str) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retired
            .remove(name);
    }
    pub(in crate::persistence) fn pin_loaded_engine(
        &self,
        generation: String,
        engine: &Engine,
    ) -> Result<()> {
        let _capture = engine
            .capture_barrier
            .capture(0)
            .map_err(anyhow::Error::msg)?;
        let expected = engine
            .checkpoint_dirty_fields()?
            .into_iter()
            .map(|(name, (generation, schema_version, _))| {
                (
                    name,
                    crate::storage::CheckpointCollectionIdentity {
                        generation,
                        schema_version,
                        data_version: 0,
                    },
                )
            })
            .collect();
        let capture = engine.capture_background_merge(expected)?;
        let readers = capture
            .live_base_inputs
            .values()
            .flat_map(|fields| fields.values())
            .chain(
                capture
                    .live_delta_inputs
                    .values()
                    .flat_map(|fields| fields.values())
                    .flatten(),
            )
            .map(Arc::downgrade)
            .collect::<Vec<_>>();
        if !readers.is_empty() {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .readers
                .push((generation, readers));
        }
        Ok(())
    }
    pub(in crate::persistence) fn protects(&self, name: &str) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .readers
            .retain(|(_, readers)| readers.iter().any(|reader| reader.strong_count() != 0));
        state.protected.contains_key(name)
            || state
                .readers
                .iter()
                .any(|(generation, _)| generation == name)
    }

    pub(in crate::persistence) fn pin(&self, name: String) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        *state.protected.entry(name).or_default() += 1;
    }

    pub(in crate::persistence) fn unpin(&self, name: &str) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let Some(refs) = state.protected.get_mut(name) else {
            return;
        };
        *refs -= 1;
        if *refs == 0 {
            state.protected.remove(name);
        }
    }

    pub(super) fn pin_readers(
        &self,
        generation: String,
        capture: &crate::storage::CheckpointCapture,
    ) {
        let readers = capture
            .prepared_compactions
            .values()
            .flatten()
            .flat_map(|field| {
                field
                    .base
                    .iter()
                    .chain(field.inputs.iter())
                    .map(Arc::downgrade)
            })
            .collect::<Vec<_>>();
        if !readers.is_empty() {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .readers
                .push((generation, readers));
        }
    }

    pub(super) fn wait(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(test)]
        {
            state.wait_entries = state
                .wait_entries
                .checked_add(1)
                .expect("test merge-wait entry counter overflowed");
            self.changed.notify_all();
        }
        while state.queued || state.running {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("background segment merge wait timed out");
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        if let Some(error) = &state.error {
            bail!("background segment merge failed: {error}");
        }
        Ok(())
    }

    pub(in crate::persistence) fn wait_for_capacity_progress_after(
        &self,
        revision: u64,
        deadline: Instant,
    ) -> Result<CapacityWait> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        #[cfg(test)]
        {
            state.wait_entries = state
                .wait_entries
                .checked_add(1)
                .expect("test merge-wait entry counter overflowed");
            self.changed.notify_all();
        }
        loop {
            if !state.queued && !state.running {
                if let Some(error) = &state.error {
                    bail!("background segment merge failed: {error}");
                }
            }
            if state.published_revision > revision {
                return Ok(CapacityWait::Published);
            }
            if !state.queued && !state.running {
                return Ok(CapacityWait::Idle);
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("background segment merge capacity wait timed out");
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
    }

    pub(in crate::persistence) fn finish_job(
        &self,
        result: &mut Result<MergeOutcome>,
        engine_alive: bool,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.running = false;
        if result
            .as_ref()
            .is_ok_and(|outcome| *outcome == MergeOutcome::Published)
        {
            match state.published_revision.checked_add(1) {
                Some(revision) => state.published_revision = revision,
                None => {
                    state.publication_revision_overflowed = true;
                    *result = Err(anyhow!(
                        "background segment merge publication revision overflowed"
                    ));
                }
            }
        }
        if state.publication_revision_overflowed {
            state.requested = false;
            state.queued = false;
            state.error = Some("background segment merge publication revision overflowed".into());
            self.changed.notify_all();
            return false;
        }
        let retryable = result.as_ref().is_err()
            || result
                .as_ref()
                .is_ok_and(|outcome| *outcome == MergeOutcome::RetryableStale);
        if retryable && state.ordinary_retry_permits > 0 {
            state.ordinary_retry_permits -= 1;
            state.requested = true;
        } else if result.as_ref().is_ok_and(|outcome| {
            matches!(
                outcome,
                MergeOutcome::Published | MergeOutcome::NoEligibleWork
            )
        }) {
            state.ordinary_retry_permits = 0;
        }
        // A successful pair merge must not immediately schedule another pass
        // against the same immutable catalog. The scheduler will request the
        // next pass when a later checkpoint creates another delta layer, or
        // when a request arrived while this job was running. This preserves
        // the selected two-layer window instead of draining a four-layer
        // stack through automatic follow-up jobs.
        // A failed job leaves its source and pins intact.  Keep that state
        // retryable so a later durable checkpoint can submit one fresh job
        // after the worker has become idle.
        state.retryable_stale = retryable;
        let again = state.requested;
        state.error = result.as_ref().err().map(|error| format!("{error:#}"));
        // Keep the token logically queued across re-enqueue so a waiter cannot
        // observe a false idle gap between two ready fields.
        state.queued = again && engine_alive;
        self.changed.notify_all();
        again
    }

    #[cfg(test)]
    pub(super) fn test_wait_entries(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .wait_entries
    }

    #[cfg(test)]
    pub(super) fn wait_for_test_wait_entry_after(
        &self,
        baseline: u64,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        while state.wait_entries <= baseline {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("target checkpoint did not enter RootWork::wait");
            }
            state = self
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(|p| p.into_inner())
                .0;
        }
        Ok(())
    }
}
