//! Publishing a checkpoint: each collection bound to the generation that now
//! holds it, its scalar layers and live deltas swapped in by the captured cut,
//! and the collection identities recovery hydrates.

use std::collections::BTreeSet;

use anyhow::{anyhow, Result};

use crate::index::application::checkpoint_capture::CheckpointCapture;
use crate::index::application::engine::Engine;
use crate::index::application::live_base::{
    checkpoint_scalar_retire_matches, install_concurrent_prepared_base,
    install_scalar_checkpoint_publication,
};
use crate::index::application::live_delta::{
    attach_live_checkpoint_delta, replace_live_checkpoint_deltas, retire_live_delta_overlay,
};
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::MISSING_SORTABLE_F64_BITS;
use crate::storage::collection_dir_name;

impl Engine {
    pub(crate) fn bind_checkpoint_origins(
        &self,
        root: &std::path::Path,
        capture: &mut CheckpointCapture,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for (name, identity) in &capture.collections {
            if let Some(coll) = state.collections.get_mut(name) {
                if coll.collection_generation == identity.generation
                    && coll.version == identity.schema_version
                    && coll.deleted_at.is_none()
                {
                    coll.checkpoint_lineage = Some(root.join(collection_dir_name(name)));
                    coll.checkpoint_lineage_schema = Some(coll.version);
                    // A durable base with this schema is reusable even when newer
                    // row mutations remain above its captured data version.
                    coll.requires_full_checkpoint = false;
                    coll.journal_complete_since_empty = false;
                    // Publish catalog scalar layers by the captured cut.  This
                    // replaces only the prefix present at freeze and retains any
                    // private layers attached while files were written.
                    let scalar_fields: BTreeSet<_> = capture
                        .scalar_publications
                        .get(name)
                        .into_iter()
                        .flat_map(|fields| fields.keys().cloned())
                        .collect();
                    let scalar_retire = capture.scalar_retire.remove(name).unwrap_or_default();
                    if let Some(publications) = capture.scalar_publications.remove(name) {
                        for (field, publication) in publications {
                            let index = coll.fields.get_mut(&field).ok_or_else(|| {
                                anyhow!("checkpoint scalar field is absent from live collection")
                            })?;
                            install_scalar_checkpoint_publication(index, &publication)?;
                            // An ordinary mutation made before the first reader
                            // existed could not mark that reader's tombstones.
                            // Mask the newly published catalog row now. A newer
                            // private winner already masks it and must stay visible.
                            for (eid, revision) in
                                coll.field_dirty.get(&field).into_iter().flatten()
                            {
                                if capture
                                    .field_dirty
                                    .get(name)
                                    .and_then(|fields| fields.get(&field))
                                    .and_then(|rows| rows.get(eid))
                                    == Some(revision)
                                {
                                    continue;
                                }
                                let Some(id) = coll.interner.id(eid) else {
                                    continue;
                                };
                                match index {
                                    FieldIndex::Keyword(k) => {
                                        let overlay = k
                                            .dense_forward
                                            .get(id as usize)
                                            .is_some_and(Option::is_some)
                                            || k.forward.contains_key(&id);
                                        if overlay
                                            || !k
                                                .segment
                                                .as_ref()
                                                .is_some_and(|view| view.has_private_winner(id))
                                        {
                                            k.tombstones.insert(id);
                                        }
                                    }
                                    FieldIndex::Number(n) => {
                                        let overlay =
                                            n.dense_forward.get(id as usize).is_some_and(|value| {
                                                *value != MISSING_SORTABLE_F64_BITS
                                            }) || n.forward.contains_key(&id);
                                        if overlay
                                            || !n
                                                .segment
                                                .as_ref()
                                                .is_some_and(|view| view.has_private_winner(id))
                                        {
                                            n.tombstones.insert(id);
                                        }
                                    }
                                    FieldIndex::Set(s) => {
                                        if s.forward.contains_key(&id)
                                            || !s
                                                .segment
                                                .as_ref()
                                                .is_some_and(|view| view.has_private_winner(id))
                                        {
                                            s.tombstones.insert(id);
                                        }
                                    }
                                    _ => unreachable!("prepared scalar kind"),
                                }
                            }
                            for (id, eid, captured_revision) in
                                scalar_retire.get(&field).into_iter().flatten()
                            {
                                if checkpoint_scalar_retire_matches(
                                    coll.field_dirty.get(&field).and_then(|rows| rows.get(eid)),
                                    *captured_revision,
                                ) {
                                    retire_live_delta_overlay(index, *id);
                                }
                            }
                        }
                    }
                    if coll.data_version == identity.data_version {
                        coll.checkpoint_origin = coll.checkpoint_lineage.clone();
                        coll.requires_full_checkpoint = false;
                        // Prepared mmap fields describe exactly the captured version.
                        // Never overwrite mutations made while file I/O was running.
                        if let Some(fields) = capture.prepared.remove(name) {
                            let dirty = capture.field_dirty.get(name).cloned().unwrap_or_default();
                            for field in fields {
                                if scalar_fields.contains(&field.name) {
                                    continue;
                                }
                                if field.vector_base.is_some()
                                    || capture.initial_sparse.contains(name)
                                {
                                    install_concurrent_prepared_base(coll, field, &dirty)?;
                                } else {
                                    coll.fields.insert(field.name, field.index);
                                }
                            }
                        }
                    } else if let Some(fields) = capture.prepared.remove(name) {
                        let dirty = capture.field_dirty.get(name).cloned().unwrap_or_default();
                        for field in fields {
                            if scalar_fields.contains(&field.name) {
                                continue;
                            }
                            install_concurrent_prepared_base(coll, field, &dirty)?;
                        }
                    }
                    if let Some(deltas) = capture.prepared_deltas.remove(name) {
                        let dirty = capture.field_dirty.get(name).cloned().unwrap_or_default();
                        for delta in deltas {
                            if scalar_fields.contains(&delta.field) {
                                continue;
                            }
                            attach_live_checkpoint_delta(coll, delta, &dirty)?;
                        }
                    }
                    if let Some(compactions) = capture.prepared_compactions.remove(name) {
                        for compacted in compactions {
                            replace_live_checkpoint_deltas(coll, compacted)?;
                        }
                    }
                    if let Some(dirty) = capture.field_dirty.get(name) {
                        coll.acknowledge_field_dirty(dirty);
                    }
                    if let Some(frozen) = capture.frozen_changes.get(name) {
                        coll.change_journal.acknowledge_through(frozen);
                    }
                }
            }
        }
        self.publish_storage_bytes(&state);
        Ok(())
    }

    pub(crate) fn bind_legacy_checkpoint_origin(&self, root: &std::path::Path) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for (name, coll) in &mut state.collections {
            coll.checkpoint_origin = Some(root.join(collection_dir_name(name)));
            coll.checkpoint_lineage = coll.checkpoint_origin.clone();
            coll.checkpoint_lineage_schema = Some(coll.version);
            coll.journal_complete_since_empty = false;
        }
        state.checkpoint_namespace = root.parent().map(std::path::Path::to_path_buf);
        Ok(())
    }

    pub(crate) fn hydrate_checkpoint_identities(
        &self,
        root: &std::path::Path,
        capture: &CheckpointCapture,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        for (name, identity) in &capture.collections {
            let coll = state
                .collections
                .get_mut(name)
                .ok_or_else(|| anyhow!("missing reopened collection"))?;
            coll.collection_generation = identity.generation;
            coll.data_version = identity.data_version;
            coll.checkpoint_origin = Some(root.join(collection_dir_name(name)));
            coll.checkpoint_lineage = coll.checkpoint_origin.clone();
            coll.checkpoint_lineage_schema = Some(coll.version);
            coll.journal_complete_since_empty = false;
        }
        state.next_collection_generation = capture.next_generation;
        state.checkpoint_namespace = root.parent().map(std::path::Path::to_path_buf);
        Ok(())
    }
}
