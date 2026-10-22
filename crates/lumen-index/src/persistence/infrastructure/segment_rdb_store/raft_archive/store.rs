//! The store's Raft snapshot entry points: validate an archive stream, restore
//! it as the exact generation `CURRENT` selects, and restore a legacy Raft
//! snapshot through an ordinary save.

use crate::index::application::engine::Engine;
use crate::persistence::infrastructure::segment_rdb_store::generation_validation::validate_generation_layout;
use crate::persistence::infrastructure::segment_rdb_store::manifest_io::{
    read_generation_manifest, write_generation_manifest,
};
use crate::persistence::infrastructure::segment_rdb_store::raft_archive::{
    read_header, stage_archive, StageCleanup,
};
use crate::persistence::infrastructure::segment_rdb_store::{SaveIntent, SegmentRdbStore};
use anyhow::{bail, Context, Result};
use std::io::Read;
use std::sync::Arc;

impl SegmentRdbStore {
    pub(crate) fn validate_raft_archive(&self, input: &mut dyn Read) -> Result<()> {
        let _permit = self.save_gate.lock_owned();
        let (sequence, count) = read_header(input)?;
        let (_, staged) = self.begin_next_generation(sequence)?;
        let _cleanup = StageCleanup(staged.path().to_owned());
        let candidate = stage_archive(input, staged.path(), sequence, count)?;
        // Receiver validation may build the backend; snapshot encoding never does.
        let engine = Engine::new();
        self.reopen_once(&engine, &candidate.record, candidate.collections)?;
        Ok(())
    }

    pub(crate) fn restore_raft_archive(
        &self,
        live: &Arc<Engine>,
        input: &mut dyn Read,
        activated: impl FnOnce(u64),
    ) -> Result<u64> {
        let _permit = self.save_gate.lock_owned();
        let (sequence, count) = read_header(input)?;
        let (revision, staged) = self.begin_next_generation(sequence)?;
        let _cleanup = StageCleanup(staged.path().to_owned());
        let mut candidate = stage_archive(input, staged.path(), sequence, count)?;
        let mut manifest = read_generation_manifest(staged.path())?;
        let previous = self
            .current_record()?
            .filter(|old| old.sequence <= sequence)
            .map(|old| old.name);
        manifest.revision = revision;
        manifest.previous = previous.as_ref().map(|name| name.as_str().to_owned());
        write_generation_manifest(staged.path(), &manifest)?;
        candidate.record.name = staged.generation().clone();
        candidate.record.revision = revision;
        candidate.record.previous = previous;
        validate_generation_layout(&candidate.record)?;
        let fresh = Engine::new();
        self.reopen_once(&fresh, &candidate.record, candidate.collections)?;
        let final_path = self.generations.generation_path(staged.generation());
        fresh.bind_legacy_checkpoint_origin(&final_path)?;
        self.retain_root_for(&fresh);
        self.background
            .pin_loaded_engine(staged.generation().as_str().to_owned(), &fresh)?;
        let encoded_manifest = serde_json::to_vec(&manifest)?;
        let inhibition = live
            .capture_barrier
            .apply()
            .inhibit_checkpoints_for_restore();
        let name = match self.generations.commit(staged) {
            Ok(name) => name,
            Err(error) => {
                if error.class() == storage_durable::CommitFailureClass::CommitUncertain {
                    inhibition.mark_uncertain();
                }
                return Err(error.into());
            }
        };
        // There are no more file reads or backend reconstruction after CURRENT.
        let activation = inhibition.activation_apply();
        if let Err(error) = live.activate_replacement(fresh) {
            inhibition.mark_uncertain();
            return Err(error).context("Raft snapshot published; live activation requires restart");
        }
        activation.initialize_sequence(sequence);
        activated(sequence);
        *self
            .verified_catalog
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some((name, encoded_manifest));
        Ok(sequence)
    }
}

impl SegmentRdbStore {
    pub(crate) fn restore_legacy_raft_snapshot(
        &self,
        live: &Arc<Engine>,
        rdb: crate::persistence::infrastructure::rdb::RdbSnapshot,
        activated: impl FnOnce(u64),
    ) -> Result<u64> {
        let sequence = rdb.up_to_seq;
        let fresh = Arc::new(Engine::new());
        fresh.restore(rdb.snapshot)?;
        fresh.capture_barrier.apply().initialize_sequence(sequence);
        let inhibition = live
            .capture_barrier
            .apply()
            .inhibit_checkpoints_for_restore();
        let permit = self.save_gate.lock_owned();
        if let Err(error) = self.save_inner_permitted(
            &fresh,
            sequence,
            true,
            permit,
            SaveIntent::RaftRestore,
            None,
        ) {
            if error.chain().any(|cause| {
                cause
                    .downcast_ref::<storage_durable::CommitError>()
                    .is_some_and(|commit| {
                        commit.class() == storage_durable::CommitFailureClass::CommitUncertain
                    })
            }) {
                inhibition.mark_uncertain();
            }
            return Err(error);
        }
        let fresh = match Arc::try_unwrap(fresh) {
            Ok(fresh) => fresh,
            Err(_) => {
                inhibition.mark_uncertain();
                bail!("legacy Raft restore published; unexpected candidate owner requires restart");
            }
        };
        let activation = inhibition.activation_apply();
        if let Err(error) = live.activate_replacement(fresh) {
            inhibition.mark_uncertain();
            return Err(error)
                .context("legacy Raft restore published; activation requires restart");
        }
        activation.initialize_sequence(sequence);
        activated(sequence);
        Ok(sequence)
    }
}
