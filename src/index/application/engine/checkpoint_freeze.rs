//! The capture side of a checkpoint: the namespace its segments go under, the
//! background merge bound to a capture, the dirty fields a capture may reuse,
//! and the collections frozen under the capture barrier.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Result};

use crate::index::application::checkpoint_capture::{
    CheckpointCapture, CheckpointCollectionIdentity,
};
use crate::index::application::engine::Engine;
use crate::index::application::frozen_checkpoint::{
    FrozenCheckpoint, FrozenCollectionFiles, FrozenField,
};
use crate::index::application::live_delta::{
    live_base_reader, live_delta_readers, replace_live_checkpoint_deltas,
};
use crate::index::domain::field_index::FieldIndex;
use crate::index::infrastructure::checkpoint_fs::collection_dir_name;
use crate::persistence::infrastructure::composed_segment::ScalarCheckpointCut;

impl Engine {
    pub(crate) fn prepare_checkpoint_namespace(
        &self,
        root: &std::path::Path,
        floor: u64,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        if state.checkpoint_namespace.as_deref() != Some(root) {
            state.next_collection_generation = state.next_collection_generation.max(floor);
            let names: Vec<_> = state.collections.keys().cloned().collect();
            for name in names {
                let generation = state.allocate_collection_generation()?;
                let coll = state.collections.get_mut(&name).unwrap();
                coll.collection_generation = generation;
                coll.checkpoint_origin = None;
                coll.checkpoint_lineage = None;
                coll.checkpoint_lineage_schema = None;
            }
            state.checkpoint_namespace = Some(root.to_path_buf());
        }
        Ok(())
    }

    /// Called only inside CaptureBarrier. Capture has no filesystem work.
    pub(crate) fn capture_background_merge(
        &self,
        expected: BTreeMap<String, CheckpointCollectionIdentity>,
    ) -> Result<CheckpointCapture> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let mut bases = BTreeMap::new();
        let mut deltas = BTreeMap::new();
        for (name, identity) in &expected {
            let coll = state
                .collections
                .get(name)
                .ok_or_else(|| anyhow!("background merge collection is absent"))?;
            if coll.deleted_at.is_some()
                || coll.collection_generation != identity.generation
                || coll.version != identity.schema_version
            {
                bail!("background merge collection identity changed");
            }
            bases.insert(
                name.clone(),
                coll.fields
                    .iter()
                    .filter_map(|(field, index)| {
                        live_base_reader(index).map(|reader| (field.clone(), reader))
                    })
                    .collect(),
            );
            deltas.insert(
                name.clone(),
                coll.fields
                    .iter()
                    .map(|(field, index)| (field.clone(), live_delta_readers(index)))
                    .collect(),
            );
        }
        Ok(CheckpointCapture {
            prepared: BTreeMap::new(),
            prepared_deltas: BTreeMap::new(),
            prepared_compactions: BTreeMap::new(),
            scalar_cuts: BTreeMap::new(),
            scalar_publications: BTreeMap::new(),
            scalar_retire: BTreeMap::new(),
            live_delta_inputs: deltas,
            live_base_inputs: bases,
            collections: expected,
            next_generation: state.next_collection_generation.max(1),
            field_dirty: BTreeMap::new(),
            frozen_changes: BTreeMap::new(),
            reused: BTreeSet::new(),
            initial_sparse: BTreeSet::new(),
            field_deltas: std::sync::Arc::new(BTreeMap::new()),
            record_cut: None,
        })
    }

    pub(crate) fn checkpoint_dirty_fields(
        &self,
    ) -> Result<BTreeMap<String, (u64, u32, BTreeSet<String>)>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        Ok(state
            .collections
            .iter()
            .filter(|(_, coll)| coll.deleted_at.is_none())
            .map(|(name, coll)| {
                (
                    name.clone(),
                    (
                        coll.collection_generation,
                        coll.version,
                        coll.field_dirty
                            .iter()
                            .filter(|(_, rows)| !rows.is_empty())
                            .map(|(field, _)| field.clone())
                            .collect(),
                    ),
                )
            })
            .collect())
    }

    pub(crate) fn bind_background_merge(
        &self,
        root: &std::path::Path,
        capture: &mut CheckpointCapture,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for (name, identity) in &capture.collections {
            let Some(coll) = state.collections.get_mut(name) else {
                continue;
            };
            if coll.deleted_at.is_some()
                || coll.collection_generation != identity.generation
                || coll.version != identity.schema_version
            {
                continue;
            }
            coll.checkpoint_lineage = Some(root.join(collection_dir_name(name)));
            coll.checkpoint_lineage_schema = Some(coll.version);
            if coll.data_version == identity.data_version && !coll.requires_full_checkpoint {
                coll.checkpoint_origin = coll.checkpoint_lineage.clone();
            }
            if let Some(compactions) = capture.prepared_compactions.remove(name) {
                for compacted in compactions {
                    replace_live_checkpoint_deltas(coll, compacted)?;
                }
            }
        }
        self.publish_storage_bytes(&state);
        Ok(())
    }

    pub(crate) fn checkpoint_reusable_dirty_fields(
        &self,
        root: &std::path::Path,
    ) -> Result<BTreeMap<String, (u64, u32, BTreeSet<String>)>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        Ok(state
            .collections
            .iter()
            .filter(|(name, coll)| {
                coll.deleted_at.is_none()
                    && !coll.requires_full_checkpoint
                    && coll.checkpoint_lineage_schema == Some(coll.version)
                    && coll.checkpoint_lineage.as_ref()
                        == Some(&root.join(collection_dir_name(name)))
            })
            .map(|(name, coll)| {
                (
                    name.clone(),
                    (
                        coll.collection_generation,
                        coll.version,
                        coll.field_dirty
                            .iter()
                            .filter(|(_, rows)| !rows.is_empty())
                            .map(|(field, _)| field.clone())
                            .collect(),
                    ),
                )
            })
            .collect())
    }

    pub(crate) fn freeze_checkpoint_collections(
        &self,
        reuse_root: Option<&std::path::Path>,
    ) -> Result<FrozenCheckpoint> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let mut capture = CheckpointCapture {
            prepared: BTreeMap::new(),
            prepared_deltas: BTreeMap::new(),
            prepared_compactions: BTreeMap::new(),
            scalar_cuts: BTreeMap::new(),
            scalar_publications: BTreeMap::new(),
            scalar_retire: BTreeMap::new(),
            live_delta_inputs: BTreeMap::new(),
            live_base_inputs: BTreeMap::new(),
            collections: BTreeMap::new(),
            next_generation: state.next_collection_generation.max(1),
            field_dirty: BTreeMap::new(),
            frozen_changes: BTreeMap::new(),
            reused: BTreeSet::new(),
            initial_sparse: BTreeSet::new(),
            field_deltas: std::sync::Arc::new(BTreeMap::new()),
            record_cut: None,
        };
        let mut files = Vec::new();
        for (name, coll) in &state.collections {
            if coll.deleted_at.is_some() {
                continue;
            }
            capture.collections.insert(
                name.clone(),
                CheckpointCollectionIdentity {
                    generation: coll.collection_generation,
                    data_version: coll.data_version,
                    schema_version: coll.version,
                },
            );
            // Pin every scalar field, including a field that has not yet acquired
            // a composed reader. The empty cut is later replaced by the real empty
            // base plus its first sparse catalog delta.
            let mut cuts = BTreeMap::new();
            for (field, index) in &coll.fields {
                let segment = match index {
                    FieldIndex::Keyword(index) => index.segment.as_ref(),
                    FieldIndex::Number(index) => index.segment.as_ref(),
                    FieldIndex::Set(index) => index.segment.as_ref(),
                    _ => continue,
                };
                cuts.insert(
                    field.clone(),
                    match segment {
                        Some(segment) => segment.checkpoint_cut()?,
                        None => ScalarCheckpointCut::empty(),
                    },
                );
            }
            capture.scalar_cuts.insert(name.clone(), cuts);
            let reusable = reuse_root.map(|root| root.join(collection_dir_name(name)));
            let origin = coll.checkpoint_lineage.as_ref().filter(|origin| {
                Some(*origin) == reusable.as_ref()
                    && coll.checkpoint_lineage_schema == Some(coll.version)
                    && !coll.requires_full_checkpoint
            });
            let work = if let Some(origin) = origin {
                capture.live_base_inputs.insert(
                    name.clone(),
                    coll.fields
                        .iter()
                        .filter_map(|(field, index)| {
                            live_base_reader(index).map(|reader| (field.clone(), reader))
                        })
                        .collect(),
                );
                capture.live_delta_inputs.insert(
                    name.clone(),
                    coll.fields
                        .iter()
                        .map(|(field, index)| (field.clone(), live_delta_readers(index)))
                        .collect(),
                );
                capture.reused.insert(name.clone());
                FrozenCollectionFiles::Linked(origin.clone())
            } else if coll.journal_complete_since_empty && !coll.requires_full_checkpoint {
                capture.initial_sparse.insert(name.clone());
                FrozenCollectionFiles::EmptyBase {
                    schema: coll.schema.clone(),
                    version: coll.version,
                }
            } else {
                let fields = coll
                    .fields
                    .iter()
                    .map(|(field, index)| {
                        Ok((field.clone(), FrozenField::capture(index, coll, field)?))
                    })
                    .collect::<Result<_>>()?;
                FrozenCollectionFiles::Base {
                    schema: coll.schema.clone(),
                    version: coll.version,
                    eids: coll.interner.to_eid.clone(),
                    coverage: coll.eid_fields.clone(),
                    fields,
                }
            };
            files.push((name.clone(), work));
        }
        // Finish every fallible base capture before changing journal ownership.
        // Each freeze moves a BTreeMap root and shares it through Arc. It does
        // not traverse changed rows or clone their payloads.
        capture.record_cut = Some(self.freeze_record_charges()?);
        for name in capture.collections.keys() {
            capture.frozen_changes.insert(
                name.clone(),
                state.collections[name].change_journal.freeze(),
            );
        }
        Ok(FrozenCheckpoint { capture, files })
    }
}

#[cfg(test)]
mod tests;
