//! Sealing every live collection into a segment checkpoint, one collection at a
//! time under the state lock, with each collection's schema sidecar and its
//! dirty values captured as deltas.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Result};

use crate::index::application::checkpoint_capture::{
    CheckpointCapture, CheckpointCollectionIdentity, CheckpointDeltas,
};
use crate::index::application::engine::Engine;
use crate::index::domain::collection::{Collection, FieldDirtySnapshot};
use crate::index::infrastructure::checkpoint_fs::{
    checkpoint_write_boundary, collection_dir_name, hard_link_checkpoint_tree, CheckpointLayout,
    CheckpointSchema, CHECKPOINT_SCHEMA_FILE,
};

fn capture_dirty_values(coll: &Collection, dirty: &FieldDirtySnapshot) -> Result<CheckpointDeltas> {
    dirty
        .iter()
        .map(|(field, ids)| {
            let rows = ids
                .keys()
                .map(|eid| {
                    Ok((
                        eid.clone(),
                        coll.checkpoint_value(field, eid)?.map(|value| {
                            crate::ingest::domain::change_journal::SharedValue::new(
                                std::sync::Arc::new(value),
                                None,
                            )
                        }),
                    ))
                })
                .collect::<Result<_>>()?;
            Ok((field.clone(), rows))
        })
        .collect()
}

impl Engine {
    /// PRODUCTION checkpoint (Phase 2f-2): seal EVERY live collection into a
    /// segment checkpoint under `dir` — one subdir `dir/<collection>/` per
    /// collection holding its `<field>.lseg` segments, EID column, vector
    /// sidecars, and a `_schema.json` sidecar tagged with `up_to_seq`. Each
    /// collection is sealed under the state WRITE lock (so a concurrent index does
    /// not race the seal), via the re-seal-capable `Collection::seal_to_segments`
    /// (base docs are gathered through the segment-aware dispatch, so a checkpoint
    /// AFTER a prior seal+drop re-materializes correctly). Soft-deleted collections
    /// are skipped. The flush is idempotent and repeatable: flush → index more →
    /// flush again → reopen yields every doc identical.
    ///
    /// Atomicity is the caller's: `dir` is expected to be a staging directory that
    /// the store atomically renames into place, so this never half-replaces a good
    /// checkpoint. The state lock is taken and released PER collection (not held
    /// across the whole flush), so reads/writes to other collections proceed.
    pub fn flush_to_segments(&self, dir: &std::path::Path, up_to_seq: u64) -> Result<()> {
        self.flush_checkpoint_collections(dir, up_to_seq, None, CheckpointLayout::Legacy)
            .map(|_| ())
    }

    fn flush_checkpoint_collections(
        &self,
        dir: &std::path::Path,
        up_to_seq: u64,
        reuse_root: Option<&std::path::Path>,
        layout: CheckpointLayout,
    ) -> Result<CheckpointCapture> {
        let mut captured = BTreeMap::new();
        let mut field_dirty = BTreeMap::new();
        let mut reused = BTreeSet::new();
        let mut field_deltas = BTreeMap::new();
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow!("create checkpoint dir {}: {e}", dir.display()))?;
        // Snapshot the live collection names under a read lock, then seal each one
        // under its own short write-lock window.
        let names: Vec<String> = {
            let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
            state.collections.keys().cloned().collect()
        };
        for name in names {
            let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
            let Some(coll) = state.collections.get_mut(&name) else {
                continue; // dropped between snapshot and seal
            };
            if coll.deleted_at.is_some() {
                continue; // soft-deleted: not part of the checkpoint
            }
            let coll_dir = dir.join(collection_dir_name(&name));
            std::fs::create_dir_all(&coll_dir)
                .map_err(|e| anyhow!("create checkpoint subdir {}: {e}", coll_dir.display()))?;
            captured.insert(
                name.clone(),
                CheckpointCollectionIdentity {
                    generation: coll.collection_generation,
                    data_version: coll.data_version,
                    schema_version: coll.version,
                },
            );
            let dirty = coll.field_dirty_snapshot();
            field_dirty.insert(name.clone(), dirty.clone());
            let reusable = reuse_root.map(|root| root.join(collection_dir_name(&name)));
            let origin = coll
                .checkpoint_lineage
                .as_ref()
                .filter(|origin| {
                    Some(*origin) == reusable.as_ref()
                        && coll.checkpoint_lineage_schema == Some(coll.version)
                        && !coll.requires_full_checkpoint
                })
                .cloned();
            if let Some(origin) = origin {
                let fields = capture_dirty_values(coll, &dirty)?;
                reused.insert(name.clone());
                field_deltas.insert(name.clone(), fields);
                drop(state);
                hard_link_checkpoint_tree(&origin, &coll_dir)?;
                continue;
            }
            // Persist the schema sidecar BEFORE the segments so a reopen that finds
            // the segments always finds the schema too (the store's atomic rename
            // makes the whole subdir visible at once regardless of write order).
            let sidecar = CheckpointSchema {
                version: coll.version,
                applied_seq: up_to_seq,
                fields: coll.schema.clone(),
                segment_layout: layout,
            };
            let schema_path = coll_dir.join(CHECKPOINT_SCHEMA_FILE);
            checkpoint_write_boundary();
            let json = serde_json::to_vec_pretty(&sidecar)
                .map_err(|e| anyhow!("encode checkpoint schema for `{name}`: {e}"))?;
            std::fs::write(&schema_path, &json)
                .map_err(|e| anyhow!("write checkpoint schema {}: {e}", schema_path.display()))?;
            coll.seal_to_segments_with_layout(&coll_dir, up_to_seq, layout)?;
        }
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        Ok(CheckpointCapture {
            prepared: BTreeMap::new(),
            prepared_deltas: BTreeMap::new(),
            prepared_compactions: BTreeMap::new(),
            scalar_cuts: BTreeMap::new(),
            scalar_publications: BTreeMap::new(),
            scalar_retire: BTreeMap::new(),
            live_delta_inputs: BTreeMap::new(),
            live_base_inputs: BTreeMap::new(),
            collections: captured,
            next_generation: state.next_collection_generation.max(1),
            field_dirty,
            frozen_changes: BTreeMap::new(),
            reused,
            initial_sparse: BTreeSet::new(),
            field_deltas: std::sync::Arc::new(field_deltas),
            record_cut: None,
        })
    }
}

#[cfg(test)]
mod tests;
