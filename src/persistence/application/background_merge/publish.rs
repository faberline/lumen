//! One background merge job: select a delta window from the newest complete
//! generation, compact it into scratch outside the checkpoint lock, then rebase
//! the verified output onto the latest catalog and publish it.

use crate::persistence::application::background_merge::link::{
    background_merge_supports_manifest, link_collection, link_collections_with_paths,
    MergeStepCosts,
};
use crate::persistence::application::background_merge::{identities, needs_merge, MergeOutcome};
use crate::persistence::domain::generation_manifest::{SegmentReference, SegmentRole};
use crate::persistence::domain::merge_rebase;
use crate::persistence::infrastructure::segment_rdb_store::compacted_fields::compact_staged_delta_windows;
use crate::persistence::infrastructure::segment_rdb_store::compaction::confined;
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout_with_prior;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, write_generation_manifest,
};
use crate::persistence::infrastructure::segment_rdb_store::merge_selection::select_staged_delta_window;
use crate::persistence::infrastructure::segment_rdb_store::{
    telemetry, GenerationRecord, GenerationStaging, MergePhase, SegmentRdbStore, StagingSelection,
};
use crate::storage::Engine;
use anyhow::{anyhow, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

impl SegmentRdbStore {
    pub(in crate::persistence) fn merge_one(&self, engine: &Arc<Engine>) -> Result<MergeOutcome> {
        let job_started = Instant::now();
        let mut costs = MergeStepCosts::default();
        let mut save_gate_held = Duration::ZERO;
        let mut compacted_fields = 0u64;
        if self
            .publication_fence
            .as_ref()
            .is_some_and(|fence| fence.acquire().is_err())
        {
            return Ok(MergeOutcome::RetryableStale);
        }
        let guard = self.save_gate.lock_owned();
        // A failed checkpoint owns an older cut. It must publish before a
        // background rewrite can change that cut's predecessor.
        if self
            .pending_frozen
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            return Ok(MergeOutcome::RetryableStale);
        }
        let Some(source) = self.current_record()? else {
            return Ok(MergeOutcome::NoEligibleWork);
        };
        if source.legacy {
            return Ok(MergeOutcome::NoEligibleWork);
        }
        let prior = read_generation_manifest(&source.path)?;
        // v3 is a legacy flat layout that the normal reader can reopen, but
        // background merge must never copy it into a newly published
        // generation. The checkpoint writer is the only migration path to v2.
        if !background_merge_supports_manifest(prior.schema_version) {
            return Ok(MergeOutcome::NoEligibleWork);
        }
        if !needs_merge(&prior) {
            return Ok(MergeOutcome::NoEligibleWork);
        }
        self.verify_predecessor_catalog(&source, &prior)?;
        let stamp = engine
            .capture_barrier
            .capture(source.sequence)
            .map_err(anyhow::Error::msg)?;
        let captured_stamp = stamp.stamp();
        let capture_before_started = Instant::now();
        let mut capture = engine.capture_background_merge(identities(&prior))?;
        costs.record(
            crate::metrics::MergeStep::CaptureBefore,
            capture_before_started.elapsed(),
            0,
        );
        drop(stamp);
        let scratch = self.begin_background_merge_stage(source.sequence)?;
        let scratch_path = scratch.path().to_path_buf();
        let scratch_name = scratch_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        self.background.pin(source.name.as_str().to_owned());
        self.background.pin(scratch_name.clone());
        drop(guard);

        let result = (|| -> Result<MergeOutcome> {
            let mut collections = prior.collections.clone();
            // Selection reads only sizes, so it runs against the read-only
            // source generation. Knowing the collection first is what keeps
            // the scratch link below proportional to the job's payload: every
            // candidate returned shares that one collection, so one link
            // pass covers every field this job will compact.
            let candidates = select_staged_delta_window(&source.path, &collections)?;
            if candidates.is_empty() {
                return Ok(MergeOutcome::NoEligibleWork);
            }
            let collection_id = candidates[0].collection_id.clone();
            let selected_fields: Vec<String> = candidates
                .iter()
                .map(|candidate| candidate.field.clone())
                .collect();
            let selected_inputs: BTreeMap<String, (Vec<SegmentReference>, SegmentReference, bool)> =
                candidates
                    .iter()
                    .map(|candidate| {
                        (
                            candidate.field.clone(),
                            (
                                candidate.inputs.clone(),
                                candidate.base.clone(),
                                candidate.includes_base,
                            ),
                        )
                    })
                    .collect();
            let link_scratch_started = Instant::now();
            let scratch_links =
                link_collection(&source.path, &scratch_path, collection_id.as_str())?;
            costs.record(
                crate::metrics::MergeStep::LinkScratch,
                link_scratch_started.elapsed(),
                scratch_links as u64,
            );
            let compact_started = Instant::now();
            // The selector chooses the collection and the tied field set.
            // Each admitted field is then drained once from its complete
            // captured stack. This keeps one publication while avoiding
            // repeated scratch rounds and repeated encode setup.
            let pending_candidates = candidates;
            let mut scratch_delta_readers = capture
                .live_delta_inputs
                .get(collection_id.as_str())
                .cloned()
                .unwrap_or_default();
            let mut final_outputs = BTreeMap::new();
            let compacted_input_count = pending_candidates
                .iter()
                .map(|candidate| candidate.inputs.len() as u64)
                .sum::<u64>();
            let outputs = compact_staged_delta_windows(
                &scratch_path,
                source.sequence,
                &mut collections,
                &mut capture,
                &mut scratch_delta_readers,
                self.merge_observer.as_ref(),
                pending_candidates,
            )?;
            for output in outputs {
                final_outputs.insert(output.output.field.clone().unwrap_or_default(), output);
            }
            costs.record(
                crate::metrics::MergeStep::Compact,
                compact_started.elapsed(),
                compacted_input_count,
            );
            compacted_fields = final_outputs.len() as u64;
            if final_outputs.is_empty() {
                return Ok(MergeOutcome::NoEligibleWork);
            }
            // One job publishes the selected field window. The fold is kept
            // as a sequence so the output identity check stays per-output.
            let mut merges = Vec::with_capacity(final_outputs.len());
            for field in &selected_fields {
                let mut output = final_outputs
                    .remove(field)
                    .ok_or_else(|| anyhow!("bounded merge produced no final output"))?;
                let (selected_inputs, selected_base, includes_base) = selected_inputs
                    .get(field)
                    .ok_or_else(|| anyhow!("bounded merge lost original inputs"))?;
                let old_collection = prior
                    .collections
                    .iter()
                    .find(|collection| collection.segments.contains(&selected_inputs[0]))
                    .ok_or_else(|| anyhow!("merge inputs have no source collection"))?;
                // The scratch output was produced from one window per round.
                // Publication must carry the complete original durable
                // identity set so the rebase compares against the captured
                // live layer, rather than the shortened scratch catalog.
                output.inputs = selected_inputs.clone();
                let base = includes_base.then(|| selected_base.clone());
                let selection = merge_rebase::MergeSelection {
                    collection_id: old_collection.collection_id.clone(),
                    collection_generation: old_collection.collection_generation,
                    schema_version: old_collection.schema_version,
                    schema: old_collection.schema.clone(),
                    field: field.clone(),
                    inputs: selected_inputs.clone(),
                    vector_sidecar: base.as_ref().and_then(|base| {
                        old_collection
                            .segments
                            .iter()
                            .find(|segment| {
                                segment.role == SegmentRole::VectorEids
                                    && segment.field == base.field
                            })
                            .cloned()
                    }),
                    base,
                };
                merges.push((selection, output));
            }
            engine.prepare_checkpoint_compactions(&mut capture)?;
            self.background
                .pin_readers(source.name.as_str().to_owned(), &capture);
            self.merge_observer.observe(MergePhase::BeforePublish)?;
            let guard = self.save_gate.lock_owned();
            let save_gate_acquired = Instant::now();
            if self
                .pending_frozen
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_some()
            {
                return Ok(MergeOutcome::RetryableStale);
            }
            if engine.capture_barrier.epoch() != captured_stamp.epoch {
                return Ok(MergeOutcome::RetryableStale);
            }
            let Some(latest) = self.current_record()? else {
                return Ok(MergeOutcome::RetryableStale);
            };
            if latest.legacy {
                return Ok(MergeOutcome::RetryableStale);
            }
            let latest_manifest = read_generation_manifest(&latest.path)?;
            // CURRENT may have changed while compaction ran. Refuse a v3
            // predecessor before staging or publishing any merge output.
            if !background_merge_supports_manifest(latest_manifest.schema_version) {
                return Ok(MergeOutcome::RetryableStale);
            }
            // Each rebase refuses unless its exact inputs still occur in the
            // catalog it is applied to, so folding the outputs in order keeps
            // every field's identity check as strict as a single-field job.
            let mut manifest = latest_manifest.clone();
            for (selection, output) in &merges {
                manifest = match merge_rebase::rebase_manifest(
                    &manifest,
                    selection,
                    merge_rebase::VerifiedOutput {
                        field: output.output.clone(),
                        vector_sidecar: output.vector_eids.clone(),
                    },
                ) {
                    Ok(manifest) => manifest,
                    Err(_) => return Ok(MergeOutcome::RetryableStale),
                };
            }
            let (revision, mut staged) = self.begin_next_generation_selected(
                latest.sequence,
                StagingSelection::CurrentIfDurable,
            )?;
            let path = staged.path().to_path_buf();
            let link_generation_started = Instant::now();
            let mut inherited_current_files =
                link_collections_with_paths(&latest.path, &path, &latest_manifest)?;
            costs.record(
                crate::metrics::MergeStep::LinkGeneration,
                link_generation_started.elapsed(),
                inherited_current_files.len() as u64,
            );
            for (_, output) in &merges {
                for reference in std::iter::once(&output.output).chain(output.vector_eids.iter()) {
                    for relative in std::iter::once(&reference.path)
                        .chain(reference.local_rows.iter().map(|rows| &rows.path))
                    {
                        let destination = confined(&path, relative)?;
                        if destination.exists() {
                            std::fs::remove_file(&destination)?;
                        }
                        std::fs::hard_link(confined(&scratch_path, relative)?, destination)?;
                        inherited_current_files.remove(relative);
                    }
                }
            }
            // Remove replaced input links which no longer occur in the full catalog.
            let retained: BTreeSet<_> = manifest
                .collections
                .iter()
                .flat_map(|collection| collection.segments.iter())
                .flat_map(|segment| {
                    std::iter::once(segment.path.as_str())
                        .chain(segment.local_rows.iter().map(|rows| rows.path.as_str()))
                })
                .collect();
            for (_, output) in &merges {
                for reference in &output.inputs {
                    for relative in std::iter::once(&reference.path)
                        .chain(reference.local_rows.iter().map(|rows| &rows.path))
                    {
                        if !retained.contains(relative.as_str()) {
                            std::fs::remove_file(confined(&path, relative)?)?;
                            inherited_current_files.remove(relative);
                        }
                    }
                }
            }
            manifest.revision = revision;
            manifest.previous = Some(latest.name.as_str().to_owned());
            let manifest_write_started = Instant::now();
            write_generation_manifest(&path, &manifest)?;
            costs.record(
                crate::metrics::MergeStep::ManifestWrite,
                manifest_write_started.elapsed(),
                1,
            );
            let record = GenerationRecord {
                name: staged.generation().clone(),
                path: path.clone(),
                sequence: latest.sequence,
                revision,
                legacy: false,
                previous: Some(latest.name.clone()),
            };
            let validate_started = Instant::now();
            validate_generation_layout_with_prior(&record, Some((&latest.path, &latest_manifest)))?;
            costs.record(
                crate::metrics::MergeStep::ValidateLayout,
                validate_started.elapsed(),
                0,
            );
            let inherit_started = Instant::now();
            let mut inherited = 0u64;
            if let GenerationStaging::Current(current_stage) = &mut staged {
                for relative in &inherited_current_files {
                    current_stage.inherit_current_file(relative)?;
                    inherited += 1;
                }
            }
            costs.record(
                crate::metrics::MergeStep::InheritFiles,
                inherit_started.elapsed(),
                inherited,
            );
            let pending_started = Instant::now();
            let pending = telemetry::pending_deltas(&path, &manifest.collections)?;
            costs.record(
                crate::metrics::MergeStep::PendingDeltas,
                pending_started.elapsed(),
                0,
            );
            capture.collections = identities(&manifest);
            let mut owner_publication = None;
            let mut capture_publish_elapsed = Duration::ZERO;
            let commit = staged.commit_with_publication_guard(&self.generations, || {
                owner_publication = self
                    .publication_fence
                    .as_ref()
                    .map(|fence| fence.acquire())
                    .transpose()
                    .map_err(std::io::Error::other)?;
                let publication = engine
                    .capture_barrier
                    .capture(latest.sequence)
                    .map_err(std::io::Error::other)?;
                // The catalog can lag a live schema/drop mutation that has not
                // checkpointed yet. Do not publish a merge of that old identity.
                let capture_publish_started = Instant::now();
                engine
                    .capture_background_merge(identities(&manifest))
                    .map_err(std::io::Error::other)?;
                capture_publish_elapsed += capture_publish_started.elapsed();
                let pin = publication
                    .publication_pin(captured_stamp)
                    .map_err(std::io::Error::other)?;
                drop(publication);
                Ok(pin)
            });
            if let Err(error) = commit {
                if error.class() == storage_durable::CommitFailureClass::CommitUncertain {
                    engine.capture_barrier.apply().mark_uncertain();
                }
                return Err(anyhow::Error::new(error)).context("publish rebased background merge");
            }
            *self
                .verified_catalog
                .lock()
                .unwrap_or_else(|p| p.into_inner()) =
                Some((record.name.clone(), serde_json::to_vec(&manifest)?));
            let binding = engine
                .capture_barrier
                .capture(latest.sequence)
                .map_err(anyhow::Error::msg)?;
            binding
                .validate_publish(captured_stamp)
                .map_err(anyhow::Error::msg)?;
            engine.bind_background_merge(&self.root.join(record.name.as_str()), &mut capture)?;
            drop(binding);
            drop(owner_publication);
            for (_, output) in &merges {
                engine
                    .metrics()
                    .observe_segment_merge(output.logical_read_bytes, output.logical_write_bytes);
            }
            engine
                .metrics()
                .set_segment_pending_delta(pending.0, pending.1);
            costs.record(
                crate::metrics::MergeStep::CapturePublish,
                capture_publish_elapsed,
                0,
            );
            save_gate_held = save_gate_acquired.elapsed();
            drop(guard);
            self.merge_observer.observe(MergePhase::AfterPublish)?;
            Ok(MergeOutcome::Published)
        })();
        // Source files and scratch stay protected through all detached I/O.
        // On panic this cleanup is not reached and the worker retains the pins.
        self.cleanup_owned_merge_staging(
            &scratch_path,
            &scratch_name,
            source.name.as_str(),
            scratch,
        )?;
        if matches!(result, Ok(MergeOutcome::Published)) {
            costs.publish(
                engine.metrics(),
                job_started.elapsed(),
                save_gate_held,
                compacted_fields,
            );
        }
        result
    }
}
