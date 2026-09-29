//! Indexing a batch of documents into one collection: each item's fields
//! applied under the state lock, its coverage recorded, and the bytes written
//! per field counted.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{anyhow, Result};

use crate::index::application::apply::apply_prepared_value;
use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::storage_error::StorageError;
use crate::metrics::{CommittedApplyTelemetry, Metrics};
use crate::shared_kernel::types::document::{IndexRequest, IndexResponse};
use crate::shared_kernel::types::schema::FieldType;

/// Maximum items in a single `POST /index` request (README §1 v1 limit).
pub const MAX_INDEX_ITEMS: usize = 10_000;

fn add_bytes_written(bytes_by_field: &mut Vec<(String, u64)>, field: &str, bytes: u64) {
    if let Some((_, total)) = bytes_by_field.iter_mut().find(|(seen, _)| seen == field) {
        *total += bytes;
        return;
    }
    bytes_by_field.push((field.to_string(), bytes));
}

fn flush_group_coverage(
    eid_fields: &mut FastHashMap<u32, FieldCoverage>,
    id: u32,
    new_doc_in_request: bool,
    fields: &mut FieldCoverage,
) {
    if !new_doc_in_request {
        return;
    }
    if fields.is_empty() {
        return;
    }
    eid_fields.insert(id, std::mem::take(fields));
}

impl Engine {
    pub(crate) fn index_inner(
        &self,
        collection_id: &str,
        req: IndexRequest,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
        prepared_text: Option<&crate::index::application::text_preparation::PreparedTextRows>,
    ) -> Result<IndexResponse> {
        let _apply = self.capture_barrier.apply();
        let mut telemetry = self.metrics.apply_telemetry();
        let outcome = {
            let state_write_wait_started = Instant::now();
            let state_write = self.state.write();
            telemetry.record_state_write_lock_wait(state_write_wait_started.elapsed());
            let mut state = state_write.map_err(|_| anyhow!("state poisoned"))?;
            let outcome = {
                let coll = state
                    .collections
                    .get_mut(collection_id)
                    .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
                Self::index_collection(
                    &self.metrics,
                    collection_id,
                    coll,
                    req,
                    charge,
                    prepared_text,
                    &mut telemetry,
                )
            };
            // Keep the live gauge correct even when a malformed batch partially
            // applied before returning its error: it must describe the real local
            // index, not merely successfully acknowledged writes.
            self.publish_storage_bytes(&state);
            outcome
        };
        drop(telemetry);
        outcome
    }

    fn index_collection(
        metrics: &Metrics,
        collection_id: &str,
        coll: &mut Collection,
        req: IndexRequest,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
        prepared_text: Option<&crate::index::application::text_preparation::PreparedTextRows>,
        telemetry: &mut CommittedApplyTelemetry<'_>,
    ) -> Result<IndexResponse> {
        if req.items.len() > MAX_INDEX_ITEMS {
            return Err(StorageError::BulkLimit {
                got: req.items.len(),
                max: MAX_INDEX_ITEMS,
            }
            .into());
        }
        coll.check_live(collection_id)?;

        if coll.check_request_id(req.request_id.as_deref()) {
            return Ok(IndexResponse {
                indexed: 0,
                bytes_written: BTreeMap::new(),
                shard_lag_ms: 0,
            });
        }
        if !req.items.is_empty() {
            coll.clear_search_cache();
        }

        let mut bytes_by_field: Vec<(String, u64)> = Vec::new();
        let mut cleared_text_rank_caches = false;
        let mut cleared_number_filter_caches = false;
        let mut indexed = 0u32;

        let mut items = req.items;
        let mut cursor = 0usize;
        while cursor < items.len() {
            let group_end = {
                let external_id = items[cursor].external_id.as_str();
                let mut end = cursor + 1;
                while end < items.len() && items[end].external_id == external_id {
                    end += 1;
                }
                end
            };
            let external_id = std::mem::take(&mut items[cursor].external_id);
            let (id, new_doc_in_request) = coll.interner.intern_owned_with_status(external_id);
            let mut new_doc_fields = FieldCoverage::default();

            for pos in cursor..group_end {
                let field_name = std::mem::take(&mut items[pos].field);
                let field = field_name.as_str();
                // #184: external-version LWW — drop a strictly-older versioned
                // write for this (external_id, field) cell. Absent version means
                // arrival order (no check, today's behavior).
                let item_version = items[pos].version;
                if let Some(v) = item_version {
                    let stale = coll
                        .cell_versions
                        .get(&id)
                        .and_then(|m| m.get(field))
                        .is_some_and(|stored| *stored >= v);
                    if stale {
                        continue;
                    }
                }
                if !cleared_text_rank_caches || !cleared_number_filter_caches {
                    let field_type = match coll.fields.get(field) {
                        Some(fi) => fi.field_type(),
                        None => {
                            flush_group_coverage(
                                &mut coll.eid_fields,
                                id,
                                new_doc_in_request,
                                &mut new_doc_fields,
                            );
                            return Err(StorageError::UnknownField {
                                collection: collection_id.to_string(),
                                field: field.to_string(),
                            }
                            .into());
                        }
                    };
                    if matches!(field_type, FieldType::Text) && !cleared_text_rank_caches {
                        coll.clear_text_rank_caches();
                        cleared_text_rank_caches = true;
                    }
                    if matches!(field_type, FieldType::Keyword | FieldType::Number)
                        && !cleared_number_filter_caches
                    {
                        coll.clear_number_filter_caches();
                        cleared_number_filter_caches = true;
                    }
                }
                let field_already_indexed = if new_doc_in_request {
                    new_doc_fields.contains(field)
                } else {
                    coll.eid_fields
                        .get(&id)
                        .is_some_and(|fields| fields.contains(field))
                };
                let bytes = {
                    let eid = coll.interner.resolve(id);
                    let fi = coll.fields.get_mut(field).ok_or_else(|| {
                        flush_group_coverage(
                            &mut coll.eid_fields,
                            id,
                            new_doc_in_request,
                            &mut new_doc_fields,
                        );
                        StorageError::UnknownField {
                            collection: collection_id.to_string(),
                            field: field.to_string(),
                        }
                    })?;
                    // Drop any existing posting for (eid, field) before reapply
                    // — re-indexing is a full replacement at field granularity. Pure
                    // append batches skip this path: there is nothing to remove, and
                    // `drop_eid` is intentionally expensive because it must handle sealed
                    // segment tombstones and old forward values.
                    if field_already_indexed {
                        fi.drop_eid(id, eid);
                    }
                    match apply_prepared_value(
                        fi,
                        id,
                        eid,
                        &items[pos].value,
                        field,
                        prepared_text.and_then(|rows| rows.get(pos, field)),
                        Some(&mut *telemetry),
                    ) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            // `drop_eid` above already made the old value
                            // absent. Journal that actual post-error value so
                            // a checkpoint cannot revive a sealed base row.
                            let stable_id = coll.interner.resolve(id).to_owned();
                            if let Err(journal_error) =
                                coll.mark_field_dirty_charged(field, &stable_id, charge)
                            {
                                flush_group_coverage(
                                    &mut coll.eid_fields,
                                    id,
                                    new_doc_in_request,
                                    &mut new_doc_fields,
                                );
                                return Err(journal_error.context(format!(
                                    "record dropped value after apply error: {e}"
                                )));
                            }
                            flush_group_coverage(
                                &mut coll.eid_fields,
                                id,
                                new_doc_in_request,
                                &mut new_doc_fields,
                            );
                            return Err(e);
                        }
                    }
                };
                add_bytes_written(&mut bytes_by_field, field, bytes);
                let stable_id = coll.interner.resolve(id).to_owned();
                coll.mark_field_dirty_charged(field, &stable_id, charge)?;
                // #184: record the highest applied version for this cell so a
                // later strictly-older write is dropped above.
                if let Some(v) = item_version {
                    coll.cell_versions
                        .entry(id)
                        .or_default()
                        .insert(field.to_string(), v);
                }
                if !field_already_indexed {
                    if new_doc_in_request {
                        new_doc_fields.insert_absent(field_name);
                    } else {
                        coll.eid_fields
                            .entry(id)
                            .or_default()
                            .insert_absent(field_name);
                    }
                } else if new_doc_in_request {
                    debug_assert!(new_doc_fields.contains(field));
                }
                indexed += 1;
            }
            flush_group_coverage(
                &mut coll.eid_fields,
                id,
                new_doc_in_request,
                &mut new_doc_fields,
            );
            cursor = group_end;
        }

        let total_bytes: u64 = bytes_by_field.iter().map(|(_, bytes)| *bytes).sum();
        let bytes_written: BTreeMap<String, u64> = bytes_by_field.into_iter().collect();
        if indexed > 0 {
            coll.last_indexed_at = Some(std::time::SystemTime::now());
        }
        metrics.incr_index(indexed as u64, total_bytes);
        let resp = IndexResponse {
            indexed,
            bytes_written,
            shard_lag_ms: 0,
        };
        Ok(resp)
    }
}
