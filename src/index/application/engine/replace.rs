//! Full-replacement writes: each item's fields become the document's whole
//! indexed state, and a text or vector value whose checksum has not changed is
//! not rewritten.

mod one_doc;

use std::collections::BTreeSet;
use std::hash::Hasher;
use std::time::Instant;

use anyhow::{anyhow, Result};
use rustc_hash::FxHasher;

use crate::app::observability::metrics::{apply_telemetry::CommittedApplyTelemetry, Metrics};
use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::document::{
    FieldValue, ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse, MAX_BATCH_REPLACE_SIZE,
};

impl Engine {
    /// `PUT /collections/{id}/docs:replace`: each item's `fields` becomes
    /// the doc's entire indexed state, implicitly deleting any declared
    /// schema field the doc has today but that is absent from `fields`.
    ///
    /// Batch-level result stays `Ok` (HTTP 200) unless the batch itself is
    /// malformed or over [`MAX_BATCH_REPLACE_SIZE`] — a single bad item
    /// (unknown field, type mismatch, stale version) is reported per-item
    /// in [`ReplaceDocResult`] and never fails its siblings.
    pub(in crate::index::application) fn replace_docs_inner(
        &self,
        collection_id: &str,
        req: ReplaceDocsRequest,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
        prepared_text: Option<&crate::index::application::text_preparation::PreparedTextRows>,
    ) -> Result<ReplaceDocsResponse> {
        let _apply = self.capture_barrier.apply();
        let mut telemetry = self.metrics.apply_telemetry();
        let outcome = {
            let state_write_wait_started = Instant::now();
            let state_write = self.state.write();
            telemetry.record_state_write_lock_wait(state_write_wait_started.elapsed());
            let mut state = state_write.map_err(|_| anyhow!("state poisoned"))?;
            telemetry.start_state_write_lock_hold();
            let outcome = {
                let coll = state
                    .collections
                    .get_mut(collection_id)
                    .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
                Self::replace_docs_collection(
                    &self.metrics,
                    collection_id,
                    coll,
                    req,
                    charge,
                    prepared_text,
                    &mut telemetry,
                )
            };
            self.publish_storage_bytes(&state);
            drop(state);
            telemetry.finish_state_write_lock_hold();
            outcome
        };
        drop(telemetry);
        outcome
    }

    fn replace_docs_collection(
        metrics: &Metrics,
        collection_id: &str,
        coll: &mut Collection,
        req: ReplaceDocsRequest,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
        prepared_text: Option<&crate::index::application::text_preparation::PreparedTextRows>,
        telemetry: &mut CommittedApplyTelemetry<'_>,
    ) -> Result<ReplaceDocsResponse> {
        if req.docs.len() > MAX_BATCH_REPLACE_SIZE {
            return Err(StorageError::BulkLimit {
                got: req.docs.len(),
                max: MAX_BATCH_REPLACE_SIZE,
            }
            .into());
        }
        coll.check_live(collection_id)?;
        if !req.docs.is_empty() {
            coll.clear_search_cache();
            coll.clear_text_rank_caches();
            coll.clear_number_filter_caches();
        }

        let mut results = Vec::with_capacity(req.docs.len());
        let mut total_fields_written = 0u64;
        let mut total_fields_skipped = 0u64;
        let mut total_bytes = 0u64;
        let mut any_written = false;
        for (ordinal, item) in req.docs.into_iter().enumerate() {
            let (result, bytes) = Self::replace_one_doc(
                collection_id,
                coll,
                item,
                ordinal,
                charge,
                prepared_text,
                telemetry,
            );
            if let ReplaceDocResult::Ok {
                fields_written,
                fields_skipped,
            } = &result
            {
                any_written = true;
                total_fields_written += *fields_written as u64;
                total_fields_skipped += *fields_skipped as u64;
                total_bytes += bytes;
            }
            results.push(result);
        }
        if any_written {
            coll.last_indexed_at = Some(std::time::SystemTime::now());
        }
        metrics.incr_index(total_fields_written, total_bytes);
        metrics.incr_replace_skipped(total_fields_skipped);
        Ok(ReplaceDocsResponse { results })
    }

    /// Read-only per-(doc,field) equality check against the currently
    /// indexed state — the no-op suppression decision for `docs:replace`
    /// (#1293). Only called for a field the doc already had (`old_fields`);
    /// a brand-new field is never "unchanged".
    ///
    /// - `keyword`/`number`/`set`/`hash` compare the exact indexed value:
    ///   each backend already keeps a forward accessor (`keyword_at`,
    ///   `number_at`, `set_members`, `hash_at`) for predicate/delete use, so
    ///   equality is a real value compare, no extra storage.
    /// - `text` has no raw forward store (only tokenized postings — the
    ///   original string isn't recoverable), and `vector` deliberately
    ///   avoids a full f32 compare (the point is to skip the expensive
    ///   HNSW tombstone/reinsert, not pay an equivalent cost checking it).
    ///   Both instead compare a stored FxHash checksum of the last
    ///   *replaced* value, kept in `Collection::field_checksums` — a
    ///   replace-path-only side cache, never persisted and never touched
    ///   by the `/index` merge path (out of scope; #1293 Scope). A missing
    ///   checksum (field never replaced before, or last written via
    ///   `/index`) is always treated as "changed": the safe default is to
    ///   write, never to silently skip.
    fn replace_value_unchanged(
        coll: &Collection,
        id: u32,
        field_name: &str,
        value: &FieldValue,
    ) -> bool {
        let Some(fi) = coll.fields.get(field_name) else {
            return false;
        };
        match (fi, value) {
            (FieldIndex::Keyword(k), FieldValue::String(s)) => {
                k.keyword_at(id).as_deref() == Some(s.as_str())
            }
            (FieldIndex::Number(n), FieldValue::Number(x)) => {
                SortableF64::new(*x).is_ok_and(|key| n.number_at(id) == Some(key))
            }
            (FieldIndex::Set(s), FieldValue::StringList(elems)) => {
                let want: BTreeSet<String> = elems.iter().cloned().collect();
                s.set_members(id) == Some(want)
            }
            (FieldIndex::Hash(h), FieldValue::String(s)) => parse_hash(s).ok() == h.hash_at(id),
            (FieldIndex::Text { .. }, FieldValue::String(s)) => coll
                .field_checksums
                .get(&id)
                .and_then(|m| m.get(field_name))
                .is_some_and(|&cs| cs == checksum_bytes(s.as_bytes())),
            (FieldIndex::Vector { .. }, FieldValue::Vector(v)) => coll
                .field_checksums
                .get(&id)
                .and_then(|m| m.get(field_name))
                .is_some_and(|&cs| cs == checksum_f32(v)),
            _ => false,
        }
    }

    /// After actually writing a `text`/`vector` field on the replace path,
    /// remember its content checksum for the next `replace_value_unchanged`
    /// comparison (see that function for why only these two types need a
    /// side cache). No-op for every other field type.
    fn record_replace_checksum(
        coll: &mut Collection,
        id: u32,
        field_name: &str,
        value: &FieldValue,
    ) {
        let checksum = match (coll.fields.get(field_name), value) {
            (Some(FieldIndex::Text { .. }), FieldValue::String(s)) => checksum_bytes(s.as_bytes()),
            (Some(FieldIndex::Vector { .. }), FieldValue::Vector(v)) => checksum_f32(v),
            _ => return,
        };
        coll.field_checksums
            .entry(id)
            .or_default()
            .insert(field_name.to_string(), checksum);
    }
}

/// FxHash content checksum of a `text` field's raw string, used only by
/// `docs:replace`'s no-op suppression (`Engine::replace_value_unchanged` /
/// `Engine::record_replace_checksum`) — see those for why `text` compares a
/// checksum instead of the raw value.
pub(in crate::index::application) fn checksum_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(bytes);
    hasher.finish()
}

/// FxHash content checksum of a `vector` field's raw `f32` values (hashed
/// via their bit patterns, not their text form), used only by
/// `docs:replace`'s no-op suppression — the whole point is avoiding a full
/// f32 compare (or an HNSW round trip) just to detect "unchanged".
fn checksum_f32(v: &[f32]) -> u64 {
    let mut hasher = FxHasher::default();
    for x in v {
        hasher.write_u32(x.to_bits());
    }
    hasher.finish()
}
