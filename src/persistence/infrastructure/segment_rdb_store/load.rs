//! Loading the generation `CURRENT` names into an engine, and the HNSW graph
//! caches saved beside it.

use crate::index::application::{engine::Engine, recovery_profile::RecoveryPhase};
use crate::persistence::infrastructure::segment_rdb_store::startup::{
    SegmentStartupDecision, SegmentStartupOutcome, StartupBootstrap,
};
use crate::persistence::infrastructure::segment_rdb_store::telemetry::generation_disk_bytes;
use crate::persistence::infrastructure::segment_rdb_store::{
    LoadedSegmentGeneration, SegmentRdbStore, HNSW_GRAPH_CACHE_DIR,
};
use anyhow::{bail, Context, Result};
use std::sync::Arc;
use std::time::Instant;
use storage_durable::{CurrentReadErrorKind, CurrentTarget};

impl SegmentRdbStore {
    /// Allocated generation bytes, counted once per file identity across hard links.
    pub fn disk_bytes(&self) -> Result<u64> {
        generation_disk_bytes(&self.root)
    }

    /// Optional acceleration only. The caller holds the mutation fence; the
    /// save gate keeps physical publication and cache IO serialized.
    pub(crate) fn save_hnsw_graph_caches(&self, engine: &Engine) -> Result<usize> {
        let _guard = self.save_gate.lock_owned();
        engine.save_hnsw_graph_caches(&self.root.join(HNSW_GRAPH_CACHE_DIR))
    }

    pub(crate) fn has_hnsw_graph_cache(&self) -> bool {
        std::fs::symlink_metadata(self.root.join(HNSW_GRAPH_CACHE_DIR))
            .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
    }

    /// Finish standalone recovery only after the authoritative AOF tail has
    /// been applied. The optional graph must match those final vector contents.
    #[doc(hidden)]
    pub fn finish_aof_graph_restore(&self, engine: &Engine) -> Result<()> {
        let _guard = self.save_gate.lock_owned();
        let started = self.recovery_profile.enabled().then(Instant::now);
        self.recovery_profile
            .phase_start(RecoveryPhase::CheckpointHnswGraph, || {
                engine.finish_checkpoint_vectors_with_graph_cache(Some(
                    &self.root.join(HNSW_GRAPH_CACHE_DIR),
                ))
            })?;
        if let Some(started) = started {
            self.add_recovery_timing(|timings| {
                timings.vector_finish_ms = started.elapsed().as_millis() as u64;
            });
        }
        self.emit_recovery_profile();
        Ok(())
    }

    /// Reopen the exact active checkpoint into a fresh engine.
    pub fn load_latest(&self) -> Result<Option<(Arc<Engine>, u64)>> {
        let engine = Arc::new(Engine::new());
        match self.reopen_into(&engine)? {
            Some(seq) => Ok(Some((engine, seq))),
            None => Ok(None),
        }
    }

    /// Load exactly the generation named by `CURRENT`.
    ///
    /// This method never performs legacy adoption and never searches for a
    /// higher, unpointed generation.
    pub fn load_current_generation(&self) -> Result<Option<LoadedSegmentGeneration>> {
        let _guard = self.save_gate.lock_owned();
        let CurrentTarget::Generation(name) = self
            .generations
            .read_current()
            .map_err(|error| anyhow::Error::new(error).context("read CURRENT"))?
        else {
            return Ok(None);
        };
        let record = self.record_for_name(name.clone())?;
        let engine = Arc::new(Engine::new());
        self.reopen_record(&engine, &record)?;
        Ok(Some(LoadedSegmentGeneration {
            name,
            sequence: record.sequence,
            engine,
        }))
    }

    /// Reopen the cold-start checkpoint and return the decision that selected
    /// it. A missing `CURRENT` can adopt only an exact 0.4.28 generation that
    /// the open-time inventory already accepted. It never selects an unpointed
    /// revision or falls back from a corrupt highest legacy generation.
    pub fn reopen_into_with_outcome(&self, engine: &Arc<Engine>) -> Result<SegmentStartupOutcome> {
        self.reopen_into_with_graph_policy(engine, false)
    }

    /// Standalone startup only. A true second result requires calling
    /// `finish_aof_graph_restore` after AOF replay and before serving queries.
    /// Ordinary checkpoint readers and Raft keep the eager restore path.
    #[doc(hidden)]
    pub fn reopen_for_aof_replay(
        &self,
        engine: &Arc<Engine>,
    ) -> Result<(SegmentStartupOutcome, bool)> {
        let deferred = self.has_hnsw_graph_cache();
        Ok((
            self.reopen_into_with_graph_policy(engine, deferred)?,
            deferred,
        ))
    }

    fn reopen_into_with_graph_policy(
        &self,
        engine: &Arc<Engine>,
        defer_until_aof: bool,
    ) -> Result<SegmentStartupOutcome> {
        let _guard = self.save_gate.lock_owned();
        match self.generations.read_current() {
            Ok(CurrentTarget::Empty) => match self.bootstrap {
                StartupBootstrap::InitializedEmpty {
                    recovered_uncommitted,
                    staging_cleaned,
                } => Ok(SegmentStartupOutcome {
                    decision: if recovered_uncommitted {
                        SegmentStartupDecision::RecoveredUncommittedEmpty
                    } else {
                        SegmentStartupDecision::InitializedEmptyRoot
                    },
                    checkpoint_sequence: None,
                    generation: None,
                    recovered_legacy_aside: false,
                    staging_cleaned,
                }),
                StartupBootstrap::ExistingCurrent { staging_cleaned } => {
                    Ok(SegmentStartupOutcome {
                        decision: SegmentStartupDecision::RestoredCurrentEmpty,
                        checkpoint_sequence: None,
                        generation: None,
                        recovered_legacy_aside: false,
                        staging_cleaned,
                    })
                }
                StartupBootstrap::Legacy { .. } => {
                    bail!("CURRENT became empty before legacy segment generation adoption")
                }
            },
            Ok(CurrentTarget::Generation(name)) => {
                let record = self.record_for_name(name)?;
                let seq = self.reopen_record_with_graph_policy(engine, &record, defer_until_aof)?;
                let staging_cleaned = match self.bootstrap {
                    StartupBootstrap::ExistingCurrent { staging_cleaned } => staging_cleaned,
                    StartupBootstrap::InitializedEmpty {
                        staging_cleaned, ..
                    }
                    | StartupBootstrap::Legacy {
                        staging_cleaned, ..
                    } => staging_cleaned,
                };
                Ok(SegmentStartupOutcome {
                    decision: SegmentStartupDecision::RestoredCurrentGeneration,
                    checkpoint_sequence: Some(seq),
                    generation: Some(record.name),
                    recovered_legacy_aside: false,
                    staging_cleaned,
                })
            }
            Err(error) if error.kind == CurrentReadErrorKind::Missing => {
                let StartupBootstrap::Legacy {
                    recovered_legacy_aside,
                    staging_cleaned,
                } = self.bootstrap
                else {
                    bail!("CURRENT disappeared after segment root initialization");
                };
                let inventory = self.inventory_root()?;
                if let Some(name) = inventory.revision_generations.first() {
                    bail!(
                        "CURRENT is missing but root contains unpointed revision generation `{name}`; refusing to select or initialize it"
                    );
                }
                let Some(record) = self.legacy_records()?.into_iter().next_back() else {
                    bail!("CURRENT is missing and no exact 0.4.28 generation can be adopted");
                };
                let seq = self.reopen_record_with_graph_policy(engine, &record, defer_until_aof)?;
                self.generations
                    .adopt_legacy(record.name.clone())
                    .map_err(anyhow::Error::new)
                    .with_context(|| format!("adopt legacy segment generation {}", record.name))?;
                Ok(SegmentStartupOutcome {
                    decision: SegmentStartupDecision::AdoptedLegacy0428,
                    checkpoint_sequence: Some(seq),
                    generation: Some(record.name),
                    recovered_legacy_aside,
                    staging_cleaned,
                })
            }
            Err(error) => Err(anyhow::Error::new(error).context("read CURRENT")),
        }
    }

    /// Reopen the cold-start checkpoint without exposing the startup decision.
    pub fn reopen_into(&self, engine: &Arc<Engine>) -> Result<Option<u64>> {
        Ok(self.reopen_into_with_outcome(engine)?.checkpoint_sequence)
    }
}
