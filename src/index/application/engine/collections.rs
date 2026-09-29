//! Collection and field DDL: create or merge a schema, drop a collection softly
//! or physically and sweep the soft-deleted ones, add and drop fields, and list
//! the collections.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::schema_validation::validate_schema;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::schema::{
    CreateCollectionRequest, CreateCollectionResponse, FieldSpec,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropOutcome {
    /// The collection did not exist.
    NotFound,
    /// `force=false` — soft-deleted, awaiting sweep.
    Marked,
    /// `force=false` but already marked previously.
    AlreadyMarked,
    /// `force=true` — physically removed.
    Physical,
}

impl Engine {
    /// Create-or-merge a collection schema.
    ///
    /// * If the collection does not exist, it is created with the
    ///   declared fields at version 1.
    /// * If it exists but is soft-deleted, the tombstone is superseded
    ///   (#3953): the result is a fresh, empty collection holding only the
    ///   declared fields — but its version CONTINUES the tombstone's rather
    ///   than restarting at 1, so a caller watching the id cannot mistake a
    ///   supersede for a stale read. This holds across a *soft* delete only:
    ///   `force` and `sweep_deleted` drop the entry outright, and the next
    ///   create answers 1 again — that id has no history left to continue, and
    ///   `tests/it/collection_version_never_moves_backwards.rs` pins the reset. A
    ///   caller that keys a cache on `version` must therefore treat a
    ///   *decrease* as "different collection", not as a stale response.
    /// * If it exists, fields **missing** from the existing schema are
    ///   appended online (version bumps once per call regardless of how
    ///   many fields were added). Re-declaring an existing field with a
    ///   **different** type / analyzer / multi flag is rejected — type
    ///   changes are an offline op (collection version bump + reindex)
    ///   not covered by this surface in v1.
    ///
    /// `collection_id` must not contain `:` — that character is reserved for
    /// custom-method routes such as `POST /collections:search`, so keeping
    /// it out of collection ids means the `:search` verb syntax can never be
    /// ambiguous with a collection id.
    pub(crate) fn create_collection_inner(
        &self,
        collection_id: &str,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        let _apply = self.capture_barrier.apply();
        if collection_id.contains(':') {
            return Err(StorageError::InvalidCollectionName(collection_id.to_string()).into());
        }
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let schema: BTreeMap<String, FieldSpec> = req
            .fields
            .into_iter()
            .map(|(k, v)| (k, v.normalize()))
            .collect();
        validate_schema(&schema)?;

        // #3953: a soft-deleted entry (`deleted_at.is_some()`) is a
        // tombstone, not a live collection to merge schema into. A `PUT`
        // against a tombstoned id is explicit caller intent to reclaim the
        // name — it supersedes the tombstone immediately with a fresh,
        // empty collection rather than waiting for `sweep_deleted`'s grace
        // window (which stays owned by the "deleted and never recreated"
        // case; see that fn's doc comment). Only a genuinely live entry
        // takes the additive-merge path below.
        let is_live = state
            .collections
            .get(collection_id)
            .is_some_and(|coll| coll.deleted_at.is_none());

        if is_live {
            let coll = state
                .collections
                .get_mut(collection_id)
                .expect("checked live above under the same state lock");
            let mut added = 0u32;
            for (name, spec) in schema {
                match coll.schema.get(&name) {
                    Some(existing) if existing != &spec => {
                        bail!(
                            "field `{name}` already declared in collection `{collection_id}` \
                             with a different spec — type changes need an offline reindex"
                        );
                    }
                    Some(_) => continue,
                    None => {
                        coll.clear_search_cache();
                        coll.requires_full_checkpoint = true;
                        coll.journal_complete_since_empty = false;
                        coll.schema.insert(name.clone(), spec.clone());
                        let idx = FieldIndex::from_spec(&spec)?;
                        idx.add_field();
                        coll.fields.insert(name, idx);
                        added += 1;
                    }
                }
            }
            if added > 0 {
                coll.clear_search_cache();
                coll.version += 1;
            }
            let resp = CreateCollectionResponse {
                collection_id: collection_id.to_string(),
                version: coll.version,
                fields_count: coll.fields_count(),
            };
            drop(state);
            return Ok(resp);
        }

        // Either the id has never been used, or it names a tombstone being
        // superseded: both start from a brand-new `Collection` built from
        // the request's own schema alone (never merged with whatever a
        // deleted predecessor declared) and with no inherited docs. `insert`
        // overwrites any tombstoned entry in place.
        //
        // The one thing that DOES cross the tombstone is `version`. It is the
        // number `PUT` hands the caller to key cached schema state on, so it
        // has to be monotonic for the life of the id: an id that climbed to 7
        // through online field additions and is then deleted and re-`PUT`
        // must not answer 1, or a caller comparing against its cached 7
        // reads the supersede as a stale response and keeps serving a schema
        // whose documents are all gone. A supersede continues from the
        // tombstone's last version; an id with no entry at all — never used,
        // or physically removed by `force`/`sweep_deleted` — is genuinely new
        // and starts at 1.
        let mut coll = Collection::new(schema)?;
        if let Some(tombstone) = state.collections.get(collection_id) {
            debug_assert!(
                tombstone.deleted_at.is_some(),
                "a live entry took the additive-merge path above"
            );
            coll.version = tombstone.version.saturating_add(1);
        }
        coll.collection_generation = state.allocate_collection_generation()?;
        let version = coll.version;
        let fields_count = coll.fields_count();
        state.collections.insert(collection_id.to_string(), coll);
        drop(state);
        self.metrics.incr_collection_created(fields_count as u64);
        Ok(CreateCollectionResponse {
            collection_id: collection_id.to_string(),
            version,
            fields_count,
        })
    }

    /// Drop a collection.
    ///
    /// - `force = true`: physically remove now.
    /// - `force = false`: soft-delete — mark `deleted_at = now()` so
    ///   reads/writes start returning 410 Gone and the periodic
    ///   `sweep_deleted` task removes the data after the grace window.
    pub(super) fn drop_collection_inner(
        &self,
        collection_id: &str,
        force: bool,
    ) -> Result<DropOutcome> {
        let _apply = self.capture_barrier.apply();
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let Some(coll) = state.collections.get_mut(collection_id) else {
            return Ok(DropOutcome::NotFound);
        };
        if force {
            state.collections.remove(collection_id);
            self.publish_storage_bytes(&state);
            return Ok(DropOutcome::Physical);
        }
        if coll.deleted_at.is_some() {
            return Ok(DropOutcome::AlreadyMarked);
        }
        coll.clear_search_cache();
        coll.deleted_at = Some(Instant::now());
        self.publish_storage_bytes(&state);
        Ok(DropOutcome::Marked)
    }

    /// Drop a field from an existing collection. Online — postings are
    /// freed immediately and the schema version bumps. Returns the new
    /// collection version.
    pub(super) fn drop_field_inner(&self, collection_id: &str, field_name: &str) -> Result<u32> {
        let _apply = self.capture_barrier.apply();
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;
        if !coll.schema.contains_key(field_name) {
            return Err(StorageError::UnknownField {
                collection: collection_id.to_string(),
                field: field_name.to_string(),
            }
            .into());
        }
        coll.clear_search_cache();
        coll.schema.remove(field_name);
        coll.fields.remove(field_name);
        coll.journal_complete_since_empty = false;
        coll.requires_full_checkpoint = true;
        // Scrub the eid→fields back-references so deletions don't try
        // to drop nonexistent index entries.
        for fields in coll.eid_fields.values_mut() {
            fields.remove(field_name);
        }
        coll.eid_fields.retain(|_, fs| !fs.is_empty());
        coll.version += 1;
        let new_version = coll.version;
        self.publish_storage_bytes(&state);
        Ok(new_version)
    }

    /// Physically remove every collection whose `deleted_at` is older
    /// than `grace`. Returns the number of physically removed entries.
    pub fn sweep_deleted(&self, grace: Duration) -> Result<usize> {
        let _apply = self.capture_barrier.apply();
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let now = Instant::now();
        let to_remove: Vec<String> = state
            .collections
            .iter()
            .filter_map(|(id, c)| {
                let ts = c.deleted_at?;
                if now.duration_since(ts) >= grace {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        let n = to_remove.len();
        for id in &to_remove {
            state.collections.remove(id);
        }
        if n > 0 {
            self.publish_storage_bytes(&state);
        }
        Ok(n)
    }

    /// Append a new field to an existing collection. Online — existing
    /// documents simply have no postings on the new field until they are
    /// re-indexed. Returns the new collection version.
    pub(crate) fn add_field_inner(
        &self,
        collection_id: &str,
        field_name: &str,
        spec: FieldSpec,
    ) -> Result<u32> {
        let _apply = self.capture_barrier.apply();
        let spec = spec.normalize();
        if field_name.is_empty() {
            bail!("field name cannot be empty");
        }
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;
        if coll.schema.contains_key(field_name) {
            bail!("field `{field_name}` already exists in collection `{collection_id}`");
        }
        let mut new_schema = coll.schema.clone();
        new_schema.insert(field_name.to_string(), spec.clone());
        validate_schema(&new_schema)?;
        coll.clear_search_cache();
        coll.schema = new_schema;
        coll.journal_complete_since_empty = false;
        coll.requires_full_checkpoint = true;
        let idx = FieldIndex::from_spec(&spec)?;
        idx.add_field();
        coll.fields.insert(field_name.to_string(), idx);
        coll.version += 1;
        let new_version = coll.version;
        Ok(new_version)
    }

    pub fn list_collections(&self) -> Result<Vec<String>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        Ok(state
            .collections
            .iter()
            .filter(|(_, c)| c.deleted_at.is_none())
            .map(|(id, _)| id.clone())
            .collect())
    }
}
