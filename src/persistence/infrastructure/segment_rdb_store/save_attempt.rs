//! One save attempt: capture the engine, write the changed collections into a
//! staged generation, validate it and publish it through `CURRENT`.

use crate::index::application::engine::Engine;
use crate::persistence::application::background_merge::needs_merge;
use crate::persistence::domain::generation_manifest::{SegmentGenerationManifest, SegmentKind};
use crate::persistence::infrastructure::segment_rdb_store::catalog::catalog_collections;
use crate::persistence::infrastructure::segment_rdb_store::delta_integrity::base_payload_sha256;
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
    duration_ns, trace_durable_save_end, CheckpointDiagnosticContext, SaveGateTrace,
};
use crate::persistence::infrastructure::segment_rdb_store::field_deltas::{
    prepare_live_delta_readers, write_field_deltas,
};
use crate::persistence::infrastructure::segment_rdb_store::flat_layout::materialize_flat_reopen_tree;
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout_with_prior;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, register_checkpoint_inherited_files, write_generation_manifest,
};
use crate::persistence::infrastructure::segment_rdb_store::save_gate::SavePermit;
use crate::persistence::infrastructure::segment_rdb_store::telemetry::{
    manifest_bytes, new_file_bytes, pending_deltas, MeasuredCapture,
};
use crate::persistence::infrastructure::segment_rdb_store::{
    GenerationRecord, GenerationStaging, PendingFrozenCheckpoint, PendingFrozenLease,
    PendingPredecessor, SaveAttempt, SaveIntent, SegmentArchivePin, SegmentRdbStore,
    StagingSelection, GENERATION_MANIFEST_SCHEMA_VERSION, GENERATION_MANIFEST_V3,
};
use anyhow::{anyhow, bail, Context, Result};
use std::sync::Arc;
use std::time::Instant;

impl SegmentRdbStore {
    pub(super) fn save_inner_permitted_attempt(
        &self,
        engine: &Arc<Engine>,
        up_to_seq: u64,
        required: bool,
        _guard: SavePermit,
        intent: SaveIntent,
        archive_pin: Option<&mut Option<SegmentArchivePin>>,
        idle_revision: Option<u64>,
        capacity_deadline: Option<Instant>,
        selection: StagingSelection,
        trace_context: Option<CheckpointDiagnosticContext>,
        gate_trace: Option<SaveGateTrace>,
        checkpoint_pass: u8,
    ) -> Result<SaveAttempt> {
        let requested_sequence = up_to_seq;
        let started = std::time::Instant::now();
        let capture_hold_ns = std::sync::atomic::AtomicU64::new(0);
        self.inventory_root()?;
        self.sweep_abandoned_staging()?;

        let current = self.current_record()?;
        let prior_manifest = current
            .as_ref()
            .filter(|record| !record.legacy)
            .map(|record| read_generation_manifest(&record.path))
            .transpose()?;
        if let Some((record, manifest)) = current.as_ref().zip(prior_manifest.as_ref()) {
            self.verify_predecessor_catalog(record, manifest)?;
        }
        let floor = prior_manifest
            .as_ref()
            .map_or(1, |manifest| manifest.next_collection_generation);
        let predecessor = PendingPredecessor::from_current(current.as_ref());
        let (mut pending, capture_stamp, up_to_seq, retried_pending, frozen_cut_bytes) =
            if let Some(pending) = self
                .take_matching_pending(engine, &predecessor)
                .with_context(|| "take matching pending checkpoint".to_owned())?
            {
                let stamp = pending.pending().stamp;
                let sequence = pending.pending().sequence;
                (pending, stamp, sequence, true, 0)
            } else {
                if intent == SaveIntent::ExactRaft {
                    bail!("exact Raft checkpoint lost its captured epoch; cannot recapture");
                }
                let capture_lease = engine
                    .capture_barrier
                    .capture(up_to_seq)
                    .map_err(|error| anyhow!(error))?;
                let capture_lease = MeasuredCapture::new(capture_lease, &capture_hold_ns);
                // Check while apply is excluded, before taking any journal ownership.
                // The worker cannot advance a predecessor while frozen work waits.
                if let Some(manifest) = &prior_manifest {
                    if self.needs_delta_capacity(
                        engine,
                        manifest,
                        &current.as_ref().expect("manifest has CURRENT").path,
                    )? {
                        let revision = self.request_merge_for_capacity_retry(
                            engine,
                            idle_revision,
                            capacity_deadline,
                        )?;
                        drop(capture_lease);
                        drop(_guard);
                        return Ok(SaveAttempt::CapacityWait(revision));
                    }
                }
                let capture_stamp = capture_lease.stamp();
                // Callers may label prepared/imported snapshots with an explicit cut.
                // Never label live data below a record completed while capture waited.
                let sequence = up_to_seq.max(capture_stamp.sequence);
                if let Some(current) = &current {
                    if sequence < current.sequence && intent != SaveIntent::RaftRestore {
                        if required {
                            bail!(
                                "required segment generation sequence {sequence} is below CURRENT sequence {}",
                                current.sequence
                            );
                        }
                        return Ok(SaveAttempt::Complete(current.name.clone()));
                    }
                }
                engine.prepare_checkpoint_namespace(&self.root, floor)?;
                let frozen_before = trace_context
                    .and_then(|_| engine.capacity_owner_state().map(|state| state.frozen));
                let frozen = engine.freeze_checkpoint_collections(
                    current.as_ref().map(|record| record.path.as_path()),
                )?;
                let frozen_cut_bytes = frozen_before
                    .zip(
                        trace_context
                            .and_then(|_| engine.capacity_owner_state().map(|state| state.frozen)),
                    )
                    .map_or(0, |(before, after)| after.saturating_sub(before));
                let layer_window = engine.layer_maintenance.freeze();
                drop(capture_lease);
                (
                    PendingFrozenLease {
                        slot: self.pending_frozen.clone(),
                        pending: Some(PendingFrozenCheckpoint {
                            engine: Arc::downgrade(engine),
                            stamp: capture_stamp,
                            sequence,
                            predecessor,
                            frozen,
                            detached_capture_ns: 0,
                            _layer_window: layer_window,
                        }),
                    },
                    capture_stamp,
                    sequence,
                    false,
                    u64::try_from(frozen_cut_bytes).unwrap_or(u64::MAX),
                )
            };
        if let Some(context) = trace_context {
            context.trace_freeze_completed(checkpoint_pass, retried_pending, frozen_cut_bytes);
        }
        capture_hold_ns.fetch_add(
            pending.pending().detached_capture_ns,
            std::sync::atomic::Ordering::Relaxed,
        );
        if let Some(current) = &current {
            if up_to_seq < current.sequence && intent != SaveIntent::RaftRestore {
                if required {
                    bail!(
                        "required segment generation sequence {up_to_seq} is below CURRENT sequence {}",
                        current.sequence
                    );
                }
                return Ok(SaveAttempt::Complete(current.name.clone()));
            }
        }
        let staging_selection = if current.as_ref().is_some_and(|record| !record.legacy) {
            selection
        } else {
            StagingSelection::Generic
        };
        let (revision, mut staged) =
            self.begin_next_generation_selected(up_to_seq, staging_selection)?;
        let staging_path = staged.path().to_path_buf();
        // A v3 predecessor stores payloads under `payload/`, while the
        // capture writer consumes collection-directory paths.  Materialize
        // temporary hard-link aliases only for this migration input.  The
        // aliases are removed before the v2 generation is validated and
        // published, so the new writer remains collection-directory based.
        let compatibility_tree = current
            .as_ref()
            .zip(prior_manifest.as_ref())
            .filter(|(_, manifest)| manifest.schema_version == GENERATION_MANIFEST_V3)
            .map(|(record, manifest)| materialize_flat_reopen_tree(&record.path, manifest))
            .transpose()?;
        let frozen_result = pending.pending().frozen.write(&staging_path, up_to_seq);
        if let Some(tree) = compatibility_tree {
            for path in tree {
                let _ = std::fs::remove_dir_all(path);
            }
        }
        let mut capture = match frozen_result {
            Ok(capture) => capture,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging_path);
                return Err(error).context("write frozen checkpoint collections");
            }
        };

        let previous = current
            .as_ref()
            .filter(|record| record.sequence <= up_to_seq)
            .map(|record| record.name.clone());
        let mut collections = match catalog_collections(&staging_path, up_to_seq) {
            Ok(collections) => collections,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging_path);
                return Err(error).context("catalog staged segment checkpoint");
            }
        };
        for collection in &mut collections {
            let identity = capture
                .collections
                .get(&collection.collection_id)
                .ok_or_else(|| anyhow!("staged collection missing capture identity"))?;
            collection.collection_generation = identity.generation;
            collection.data_version = identity.data_version;
            if capture.reused.contains(&collection.collection_id) {
                if let Some(prior) = prior_manifest.as_ref().and_then(|manifest| {
                    manifest
                        .collections
                        .iter()
                        .find(|old| old.collection_id == collection.collection_id)
                }) {
                    if prior.collection_generation != identity.generation
                        || prior.schema_version != identity.schema_version
                        || prior.schema != collection.schema
                    {
                        bail!("reused collection catalog identity changed");
                    }
                    if prior
                        .segments
                        .iter()
                        .any(|segment| segment.payload_sha256.is_none())
                    {
                        bail!("v2 predecessor is missing payload checksum");
                    }
                    collection.segments = prior.segments.clone();
                }
            }
            for segment in &mut collection.segments {
                if matches!(segment.kind, SegmentKind::Base) && segment.payload_sha256.is_none() {
                    segment.payload_sha256 =
                        Some(base_payload_sha256(&staging_path.join(&segment.path))?);
                }
            }
            if let Some(fields) = capture.field_deltas.get(&collection.collection_id) {
                write_field_deltas(&staging_path, up_to_seq, collection, fields).with_context(
                    || {
                        format!(
                            "write v2 field deltas collection={} staging={}",
                            collection.collection_id,
                            staging_path.display()
                        )
                    },
                )?;
            }
        }
        // Open fresh layers before compaction replaces the last input's path.
        // The live index must first acknowledge the exact captured delta, then
        // replace its immutable inputs with the compacted view.
        prepare_live_delta_readers(&staging_path, &collections, &mut capture)?;
        // Construct and validate every scalar catalog replacement while the
        // staged files are still private. CURRENT must never expose a catalog
        // whose live view has not retained its post-capture private suffix.
        engine.prepare_scalar_checkpoint_publications(&mut capture)?;
        let checkpoint_payload_bytes = new_file_bytes(
            &staging_path,
            current.as_ref().map(|record| record.path.as_path()),
        )?;
        let manifest = SegmentGenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            checkpoint_sequence: up_to_seq,
            revision,
            previous: previous.as_ref().map(|name| name.as_str().to_owned()),
            next_collection_generation: capture.next_generation,
            collections,
        };
        if let Err(error) = write_generation_manifest(&staging_path, &manifest) {
            let _ = std::fs::remove_dir_all(&staging_path);
            return Err(error);
        }
        let written_bytes = checkpoint_payload_bytes
            .checked_add(manifest_bytes(&staging_path)?)
            .ok_or_else(|| anyhow!("checkpoint byte count overflow"))?;

        let staged_record = GenerationRecord {
            name: staged.generation().clone(),
            path: staging_path.clone(),
            sequence: up_to_seq,
            revision,
            legacy: false,
            previous,
        };
        // Validate the catalog and physical segment envelopes. Reopening an
        // Engine here would reconstruct every interner and HNSW graph merely
        // to publish unchanged hard links. Actual cold-open validation belongs
        // to the recovery path, before it installs data into a caller engine.
        let inherited = current
            .as_ref()
            .zip(prior_manifest.as_ref())
            .map(|(record, manifest)| (record.path.as_path(), manifest));
        if let Err(error) = validate_generation_layout_with_prior(&staged_record, inherited) {
            let _ = std::fs::remove_dir_all(&staging_path);
            return Err(error).context("validate staged segment generation");
        }
        let (pending_bytes, pending_layers) = pending_deltas(&staging_path, &manifest.collections)?;
        if let GenerationStaging::Current(current_stage) = &mut staged {
            register_checkpoint_inherited_files(
                current_stage,
                &manifest.collections,
                prior_manifest.as_ref(),
                &capture,
            )?;
        }
        // Retain owner exclusion through durable publication AND live binding.
        // A replacement owner never observes a half-installed catalog.
        let publish_started = trace_context.map(|_| Instant::now());
        let mut owner_publication = None;
        let commit = staged.commit_with_publication_guard(&self.generations, || {
            owner_publication = self
                .publication_fence
                .as_ref()
                .map(|fence| fence.acquire())
                .transpose()
                .map_err(std::io::Error::other)?;
            let publication = engine
                .capture_barrier
                .capture(up_to_seq)
                .map_err(std::io::Error::other)?;
            let publication = MeasuredCapture::new(publication, &capture_hold_ns);
            let pin = publication
                .publication_pin(capture_stamp)
                .map_err(std::io::Error::other)?;
            drop(publication);
            Ok(pin)
        });
        if let Err(error) = commit {
            if error.class() == storage_durable::CommitFailureClass::CommitUncertain {
                engine.capture_barrier.apply().mark_uncertain();
                // A durable pointer may have advanced. Keep the actual payload
                // for restart diagnosis; the uncertainty latch makes every
                // in-process retry refuse it rather than reporting success.
            }
            return Err(anyhow::Error::new(error)).with_context(|| {
                format!("activate segment generation seq {up_to_seq} revision {revision}")
            });
        }
        if let Some(context) = trace_context {
            context.trace_publish_completed(checkpoint_pass);
        }
        let publish_ns = publish_started.map(|started| duration_ns(started.elapsed()));
        *self
            .verified_catalog
            .lock()
            .unwrap_or_else(|p| p.into_inner()) =
            Some((staged_record.name.clone(), serde_json::to_vec(&manifest)?));
        // Publication is durable. Bind only collections whose captured tuple
        // still matches; concurrent mutations retain their dirty state.
        let acknowledge_started = trace_context.map(|_| Instant::now());
        let binding = engine
            .capture_barrier
            .capture(up_to_seq)
            .map_err(anyhow::Error::msg)?;
        let binding = MeasuredCapture::new(binding, &capture_hold_ns);
        binding
            .validate_publish(capture_stamp)
            .map_err(anyhow::Error::msg)?;
        self.retain_root_for(engine);
        engine
            .bind_checkpoint_origins(&self.root.join(staged_record.name.as_str()), &mut capture)
            .with_context(|| {
                format!(
                    "bind v2 checkpoint origins generation={}",
                    self.root.join(staged_record.name.as_str()).display()
                )
            })?;
        engine.acknowledge_record_charges(&capture)?;
        drop(binding);
        let acknowledge_ns = acknowledge_started.map(|started| duration_ns(started.elapsed()));
        engine.metrics().observe_segment_checkpoint(
            written_bytes,
            started.elapsed(),
            std::time::Duration::from_nanos(
                capture_hold_ns.load(std::sync::atomic::Ordering::Relaxed),
            ),
        );
        engine
            .metrics()
            .set_segment_pending_delta(pending_bytes, pending_layers);
        match self.disk_bytes() {
            Ok(bytes) => engine.metrics().set_segment_disk_bytes(bytes),
            Err(error) => {
                tracing::warn!(%error, "segment disk metric unavailable after durable checkpoint")
            }
        }
        let name = staged_record.name.clone();
        if let Some(slot) = archive_pin {
            *slot = Some(self.pin_published_generation(&name)?);
        }
        let follow_with_fresh_capture = intent == SaveIntent::Ordinary
            && retried_pending
            && (required || requested_sequence > up_to_seq);
        pending.disarm();
        drop(owner_publication);
        drop(_guard);
        if needs_merge(&manifest) {
            if let Err(error) = self.request_merge(engine) {
                tracing::warn!(%error, "could not request segment merge after durable checkpoint");
            }
        }
        trace_durable_save_end(
            trace_context,
            up_to_seq,
            revision,
            duration_ns(started.elapsed()),
        );
        if let Some(trace_context) = trace_context {
            let gate_trace = gate_trace.expect("diagnostic checkpoints time the save gate");
            let save_gate_hold_ns = duration_ns(gate_trace.acquired_at.elapsed());
            let capacity_request_revision = engine
                .capacity_owner_state()
                .and_then(|state| state.checkpoint_request_revision);
            let root_merge = self.background.trace_state();
            tracing::info!(
                event = "segment_checkpoint_diagnostic",
                checkpoint_origin = trace_context.origin,
                checkpoint_sequence = up_to_seq,
                checkpoint_revision = revision,
                checkpoint_pass,
                frozen_cut_bytes,
                frozen_cut_reused = retried_pending,
                // The root gate is deliberately acquired before the capture
                // barrier. Keep this explicit zero in the trace so operators
                // do not infer that a frozen cut waits behind another save.
                capture_to_save_gate_wait_ns = 0u64,
                save_gate_wait_ns = gate_trace.wait_ns,
                save_gate_hold_ns,
                publish_ns = publish_ns.expect("diagnostic checkpoints time publication"),
                acknowledge_ns =
                    acknowledge_ns.expect("diagnostic checkpoints time acknowledgement"),
                capacity_request_pending = capacity_request_revision.is_some(),
                capacity_request_revision = capacity_request_revision.unwrap_or_default(),
                root_merge_queued = root_merge.queued,
                root_merge_running = root_merge.running,
                root_merge_requested = root_merge.requested,
                root_merge_published_revision = root_merge.published_revision,
                "segment checkpoint diagnostic"
            );
        }
        if follow_with_fresh_capture {
            return Ok(SaveAttempt::FreshCapture);
        }
        Ok(SaveAttempt::Complete(name))
    }
}
