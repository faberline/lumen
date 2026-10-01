//! Deleting documents: by external id, a bounded group's complete rows, and a
//! whole collection's documents replaced by an empty state whose old generation
//! retires in the background.

use anyhow::{anyhow, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::storage_error::StorageError;
use crate::index::infrastructure::collection_retirement::{
    collection_retirement_worker, retire_collection_with, CollectionRetirementWorker,
};
use crate::shared_kernel::types::document::{
    validate_batch_unindex_docs_request, BatchUnindexDocsRequest,
};

impl Engine {
    pub(super) fn delete_inner(
        &self,
        collection_id: &str,
        external_id: &str,
        field: Option<&str>,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
    ) -> Result<()> {
        let _apply = self.capture_barrier.apply();
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;

        // Unknown external_id → never interned → nothing to delete.
        let Some(id) = coll.interner.id(external_id) else {
            return Ok(());
        };
        coll.clear_search_cache();
        coll.clear_number_filter_caches();
        match field {
            Some(f) => {
                if let Some(fi) = coll.fields.get_mut(f) {
                    fi.drop_eid(id, external_id);
                }
                coll.mark_field_dirty_charged(f, external_id, charge)?;
                if let Some(fields) = coll.eid_fields.get_mut(&id) {
                    fields.remove(f);
                    if fields.is_empty() {
                        coll.eid_fields.remove(&id);
                    }
                }
            }
            None => {
                let fields: Vec<String> = coll
                    .eid_fields
                    .get(&id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                for f in fields {
                    if let Some(fi) = coll.fields.get_mut(&f) {
                        fi.drop_eid(id, external_id);
                    }
                    coll.mark_field_dirty_charged(&f, external_id, charge)?;
                }
                coll.eid_fields.remove(&id);
            }
        }
        self.publish_storage_bytes(&state);
        Ok(())
    }

    /// Remove complete indexed rows for a bounded group of external ids.
    ///
    /// Validation runs before the state lock.  Under the lock we resolve every
    /// known id and its recorded fields before changing caches or postings.
    /// Once that preparation succeeds, the remainder only removes owned map
    /// entries and postings, so one durable command is atomic within this
    /// physical shard.  Unknown ids deliberately never enter the interner and
    /// are successful no-ops.
    pub(super) fn unindex_docs_inner(
        &self,
        collection_id: &str,
        req: BatchUnindexDocsRequest,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
    ) -> Result<()> {
        let _apply = self.capture_barrier.apply();
        validate_batch_unindex_docs_request(&req)?;

        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;

        // Do all allocation and lookup work before cache invalidation.  A
        // field listed in `eid_fields` was admitted by index/replace, so the
        // later removal loop contains no fallible validation or I/O.
        let known: Vec<(u32, String, Vec<String>)> = req
            .external_ids
            .iter()
            .filter_map(|external_id| {
                let id = coll.interner.id(external_id)?;
                let fields = coll
                    .eid_fields
                    .get(&id)
                    .map(|coverage| coverage.iter().cloned().collect())
                    .unwrap_or_default();
                // The interner is append-only, so an id can be known with no
                // live fields.  Keep it in the plan anyway: it may still own
                // an old LWW/checksum side entry that must not become a
                // delete tombstone.
                Some((id, external_id.clone(), fields))
            })
            .collect();

        if known.is_empty() {
            return Ok(());
        }

        // These derived caches are collection-wide.  Clear each once rather
        // than once per selected row, before any posting is removed.
        coll.clear_search_cache();
        coll.clear_text_rank_caches();
        coll.clear_number_filter_caches();

        for (id, external_id, fields) in known {
            for field in fields {
                if let Some(index) = coll.fields.get_mut(&field) {
                    index.drop_eid(id, &external_id);
                }
                coll.mark_field_dirty_charged(&field, &external_id, charge)?;
            }
            coll.eid_fields.remove(&id);
            // A batch unindex has no tombstone.  Future writes to this id
            // start with no cell/doc version ceiling or replace checksum.
            coll.cell_versions.remove(&id);
            coll.doc_versions.remove(&id);
            coll.field_checksums.remove(&id);
        }

        self.publish_storage_bytes(&state);
        Ok(())
    }

    /// Replace one live collection's document state with a fresh empty state.
    ///
    /// The map-slot replacement is the shard-local linearization point.  It
    /// costs only the declared schema, not the number of existing documents:
    /// old postings, interned ids, HNSW state, and mmap handles move to the
    /// process-wide reclaimer after the new state is visible.  This is not a
    /// loop over [`Self::delete`].
    pub(super) fn truncate_docs_inner(&self, collection_id: &str) -> Result<()> {
        let _apply = self.capture_barrier.apply();
        self.truncate_docs_with_retirement(collection_id, collection_retirement_worker())
    }

    fn truncate_docs_with_retirement(
        &self,
        collection_id: &str,
        retirement_worker: &CollectionRetirementWorker,
    ) -> Result<()> {
        let old = {
            let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
            let existing = state
                .collections
                .get(collection_id)
                .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
            existing.check_live(collection_id)?;

            let mut replacement = Collection::new(existing.schema.clone())?;
            // `Collection::new` starts a newly-created collection at version
            // one.  Truncate retains the declaration exactly, including its
            // add/drop-field version.
            replacement.version = existing.version;
            replacement.collection_generation = state.allocate_collection_generation()?;
            let old = state
                .collections
                .insert(collection_id.to_string(), replacement)
                .expect("collection was checked while holding the state write lock");
            self.publish_storage_bytes(&state);
            old
        };
        retire_collection_with(retirement_worker, old);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
