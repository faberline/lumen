//! The save entry points: sequencing a save against the active generation,
//! taking the root's save permit, and matching a retry to the frozen checkpoint
//! it left pending.

use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::CapacityWait;
use crate::persistence::domain::generation_manifest::SegmentGenerationManifest;
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
    checkpoint_diagnostic_enabled, duration_ns, trace_capacity_wait_begin, trace_capacity_wait_end,
    trace_save_gate_acquired, CheckpointDiagnosticContext, SaveGateTrace,
};
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::read_generation_manifest;
use crate::persistence::infrastructure::segment_rdb_store::records::{
    parse_legacy_name, parse_revision_name,
};
use crate::persistence::infrastructure::segment_rdb_store::save_gate::SavePermit;
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, PendingFrozenLease, PendingPredecessor, SaveAttempt, SaveIntent,
    SegmentArchivePin, SegmentRdbStore, StagingSelection,
};
use anyhow::{anyhow, bail, Context, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage_durable::GenerationName;

impl SegmentRdbStore {
    /// Checkpoint `engine` through a new immutable generation.
    ///
    /// Same-sequence saves remain meaningful because reshard operations can
    /// change state without advancing `applied_seq`. A lower sequence can only
    /// be a stale background caller, so it returns without moving `CURRENT`.
    pub fn save(&self, engine: &Arc<Engine>, up_to_seq: u64) -> Result<()> {
        self.save_inner(engine, up_to_seq, false, StagingSelection::CurrentIfDurable)
            .map(|_| ())
    }

    /// Return the durable cut selected by this save. A caller may have sampled
    /// its watermark before an in-flight apply record finished.
    pub fn save_with_sequence(&self, engine: &Arc<Engine>, up_to_seq: u64) -> Result<u64> {
        let name = self.save_inner(engine, up_to_seq, false, StagingSelection::CurrentIfDurable)?;
        parse_revision_name(name.as_str())
            .map(|(sequence, _)| sequence)
            .or_else(|| parse_legacy_name(name.as_str()))
            .ok_or_else(|| anyhow!("saved checkpoint has an invalid generation name"))
    }

    /// Save with one trace-only scheduler label. The label does not affect
    /// checkpoint selection, bytes, locking, publication, or acknowledgement.
    pub(crate) fn save_with_sequence_diagnostic(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        origin: &'static str,
        attempt_id: Option<u64>,
    ) -> Result<u64> {
        let trace_context = checkpoint_diagnostic_enabled()
            .then(|| CheckpointDiagnosticContext::new(origin, attempt_id));
        self.save_with_sequence_diagnostic_context(engine, up_to_seq, trace_context)
    }

    pub(crate) fn save_with_sequence_diagnostic_context(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        trace_context: Option<CheckpointDiagnosticContext>,
    ) -> Result<u64> {
        let name = self.save_inner_traced(
            engine,
            up_to_seq,
            false,
            StagingSelection::CurrentIfDurable,
            trace_context,
        )?;
        parse_revision_name(name.as_str())
            .map(|(sequence, _)| sequence)
            .or_else(|| parse_legacy_name(name.as_str()))
            .ok_or_else(|| anyhow!("saved checkpoint has an invalid generation name"))
    }

    /// Save a generation for a restore operation.
    ///
    /// Unlike [`Self::save`], this never silently ignores a stale sequence and
    /// always creates a new revision, including when the sequence is unchanged.
    pub fn save_required(&self, engine: &Arc<Engine>, up_to_seq: u64) -> Result<GenerationName> {
        self.save_inner(engine, up_to_seq, true, StagingSelection::Generic)
    }

    pub(super) fn save_inner(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        required: bool,
        selection: StagingSelection,
    ) -> Result<GenerationName> {
        self.save_inner_traced(engine, up_to_seq, required, selection, None)
    }

    fn save_inner_traced(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        required: bool,
        selection: StagingSelection,
        trace_context: Option<CheckpointDiagnosticContext>,
    ) -> Result<GenerationName> {
        let (permit, gate_trace) = if trace_context.is_some() {
            let (permit, trace) = SaveGateTrace::acquire(&self.save_gate);
            trace_save_gate_acquired(trace_context, trace);
            (permit, Some(trace))
        } else {
            (self.save_gate.lock_owned(), None)
        };
        self.save_inner_permitted_selected(
            engine,
            up_to_seq,
            required,
            permit,
            SaveIntent::Ordinary,
            None,
            selection,
            trace_context,
            gate_trace,
        )
    }

    pub(super) fn save_inner_permitted(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        required: bool,
        permit: SavePermit,
        intent: SaveIntent,
        archive_pin: Option<&mut Option<SegmentArchivePin>>,
    ) -> Result<GenerationName> {
        self.save_inner_permitted_selected(
            engine,
            up_to_seq,
            required,
            permit,
            intent,
            archive_pin,
            StagingSelection::Generic,
            None,
            None,
        )
    }

    fn save_inner_permitted_selected(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        required: bool,
        permit: SavePermit,
        intent: SaveIntent,
        mut archive_pin: Option<&mut Option<SegmentArchivePin>>,
        selection: StagingSelection,
        trace_context: Option<CheckpointDiagnosticContext>,
        gate_trace: Option<SaveGateTrace>,
    ) -> Result<GenerationName> {
        let mut capacity_deadline = None;
        let mut permit = Some(permit);
        let mut gate_trace = gate_trace;
        let mut idle_revision = None;
        let mut checkpoint_pass = 1u8;
        loop {
            let guard = permit
                .take()
                .expect("each checkpoint attempt must own the save permit");
            let attempt_gate_trace = gate_trace.take();
            match self.save_inner_permitted_attempt(
                engine,
                up_to_seq,
                required,
                guard,
                intent,
                archive_pin.as_deref_mut(),
                idle_revision,
                capacity_deadline,
                selection,
                trace_context,
                attempt_gate_trace,
                checkpoint_pass,
            )? {
                SaveAttempt::Complete(name) => return Ok(name),
                SaveAttempt::FreshCapture => {
                    // The pending cut was published. Its fresh successor can
                    // request work, while retaining any existing wait deadline.
                    idle_revision = None;
                    checkpoint_pass = 2;
                    if trace_context.is_some() {
                        let (next_permit, next_trace) = SaveGateTrace::acquire(&self.save_gate);
                        trace_save_gate_acquired(trace_context, next_trace);
                        permit = Some(next_permit);
                        gate_trace = Some(next_trace);
                    } else {
                        permit = Some(self.save_gate.lock_owned());
                    }
                }
                SaveAttempt::CapacityWait(revision) => {
                    let deadline = *capacity_deadline
                        .get_or_insert_with(|| Instant::now() + Duration::from_secs(60));
                    let capacity_wait_started = trace_context.map(|_| Instant::now());
                    trace_capacity_wait_begin(trace_context, revision);
                    let capacity_wait = self
                        .background
                        .wait_for_capacity_progress_after(revision, deadline);
                    trace_capacity_wait_end(
                        trace_context,
                        revision,
                        capacity_wait_started.map(|started| duration_ns(started.elapsed())),
                        capacity_wait.as_ref(),
                    );
                    match capacity_wait? {
                        CapacityWait::Published => idle_revision = None,
                        CapacityWait::Idle => idle_revision = Some(revision),
                    }
                    if trace_context.is_some() {
                        let (next_permit, next_trace) = SaveGateTrace::acquire(&self.save_gate);
                        trace_save_gate_acquired(trace_context, next_trace);
                        permit = Some(next_permit);
                        gate_trace = Some(next_trace);
                    } else {
                        permit = Some(self.save_gate.lock_owned());
                    }
                }
            }
        }
    }

    pub(in crate::persistence) fn verify_predecessor_catalog(
        &self,
        record: &GenerationRecord,
        manifest: &SegmentGenerationManifest,
    ) -> Result<()> {
        let encoded = serde_json::to_vec(manifest)?;
        let mut verified = self
            .verified_catalog
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some((name, bytes)) = verified.as_ref() {
            if name == &record.name {
                if bytes != &encoded {
                    bail!("CURRENT manifest changed after verification");
                }
                return Ok(());
            }
        }
        // This runs under the store serialization lock, before CaptureBarrier.
        // It validates files directly and never builds another Engine.
        validate_generation_layout(record).context("verify predecessor generation")?;
        if serde_json::to_vec(&read_generation_manifest(&record.path)?)? != encoded {
            bail!("CURRENT manifest changed during verification");
        }
        *verified = Some((record.name.clone(), encoded));
        Ok(())
    }

    pub(super) fn take_matching_pending(
        &self,
        engine: &Arc<Engine>,
        predecessor: &PendingPredecessor,
    ) -> Result<Option<PendingFrozenLease>> {
        let mut slot = self
            .pending_frozen
            .lock()
            .map_err(|_| anyhow!("pending frozen checkpoint lock poisoned"))?;
        let Some(pending) = slot.take() else {
            return Ok(None);
        };
        let Some(owner) = pending.engine.upgrade() else {
            return Ok(None);
        };
        if owner.capture_barrier.is_uncertain() {
            *slot = Some(pending);
            bail!("checkpoint retry refused: durability is uncertain; restart required");
        }
        if !Arc::ptr_eq(&owner, engine) {
            if owner.capture_barrier.epoch() != pending.stamp.epoch {
                // A restore candidate replaced the old owner's epoch. Its
                // detached cut is stale, so release it and let the candidate
                // capture its own state.
                return Ok(None);
            }
            *slot = Some(pending);
            bail!("checkpoint root has pending frozen work for another Engine");
        }
        if owner.capture_barrier.epoch() != pending.stamp.epoch {
            return Ok(None);
        }
        if &pending.predecessor != predecessor {
            // Another durable generation became CURRENT. This cut cannot be
            // rebased without risking a downgrade, so discard only this stale
            // detached work and let this caller capture from the new CURRENT.
            return Ok(None);
        }
        let mut lease = PendingFrozenLease {
            slot: self.pending_frozen.clone(),
            pending: Some(pending),
        };
        drop(slot);
        let validation = engine
            .capture_barrier
            .capture(lease.pending().sequence)
            .map_err(anyhow::Error::msg)?;
        if validation.validate_publish(lease.pending().stamp).is_err() {
            // Restore replaced the Engine epoch. The actual old payload cannot
            // publish into replacement state and is intentionally released.
            lease.disarm();
            return Ok(None);
        }
        drop(validation);
        Ok(Some(lease))
    }

    #[cfg(test)]
    pub(super) fn pending_frozen_identity(&self) -> Option<usize> {
        self.pending_frozen
            .lock()
            .ok()?
            .as_ref()
            .map(|pending| std::ptr::from_ref(&pending.frozen) as usize)
    }
}
