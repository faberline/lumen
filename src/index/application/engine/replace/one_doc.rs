//! One document's full replacement: every field type-checked before any
//! mutation, absent fields dropped, and each present field rewritten unless it
//! is unchanged.

use std::collections::BTreeSet;

use crate::index::application::apply::apply_prepared_value;
use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::schema_validation::validate_value;
use crate::index::domain::storage_error::StorageError;
use crate::metrics::CommittedApplyTelemetry;
use crate::shared_kernel::types::document::{ReplaceDocItem, ReplaceDocResult};

impl Engine {
    /// Apply one [`ReplaceDocItem`], full-replacement at doc granularity.
    /// Fields absent from `item.fields` but present on the doc today are
    /// dropped; fields present in both are compared against the currently
    /// indexed state (see [`Engine::replace_value_unchanged`]) and, when
    /// equal, skipped entirely — no `drop_eid`, no `apply_value`, no
    /// posting-list rewrite or HNSW tombstone/reinsert (#1293); fields new
    /// to the doc, or whose value changed, are re-applied (drop-then-
    /// reapply, the same field-granularity replacement `index_collection`
    /// already uses).
    ///
    /// Every field is type-checked against the schema *before* any
    /// mutation happens, so a per-item error (unknown field, type
    /// mismatch) leaves the doc's prior state completely untouched —
    /// unlike `index_collection`'s partial-apply-on-error, docs:replace is
    /// framed as an atomic full replacement and a partially-applied doc
    /// would break that guarantee.
    ///
    /// Returns the per-item result plus the total bytes actually written
    /// (0 for a fully-skipped item, `Dropped`, or `Error`), which the
    /// caller feeds into `Metrics::incr_index` so a byte-identical resend
    /// leaves `lumen_index_bytes_total` unmoved.
    pub(super) fn replace_one_doc(
        collection_id: &str,
        coll: &mut Collection,
        item: ReplaceDocItem,
        ordinal: usize,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
        prepared_text: Option<&crate::storage::text_preparation::PreparedTextRows>,
        telemetry: &mut CommittedApplyTelemetry<'_>,
    ) -> (ReplaceDocResult, u64) {
        let (id, new_doc_in_request) = coll
            .interner
            .intern_owned_with_status(item.external_id.clone());

        // Doc-level LWW: a strictly-older version arriving later drops the
        // *entire* item, reported as its own `Dropped` variant (not `Ok`
        // and not `Error`) so callers can tell "a newer write already won"
        // apart from both success and failure.
        if let Some(v) = item.version {
            if let Some(stored) = coll.doc_versions.get(&id).copied() {
                if stored >= v {
                    return (
                        ReplaceDocResult::Dropped {
                            current_version: stored,
                        },
                        0,
                    );
                }
            }
        }

        for (field_name, value) in &item.fields {
            let Some(fi) = coll.fields.get(field_name.as_str()) else {
                return (
                    ReplaceDocResult::Error {
                        code: "unknown_field".to_string(),
                        message: StorageError::UnknownField {
                            collection: collection_id.to_string(),
                            field: field_name.clone(),
                        }
                        .to_string(),
                    },
                    0,
                );
            };
            if let Err(e) = validate_value(fi, value, field_name) {
                return (
                    ReplaceDocResult::Error {
                        code: "type_mismatch".to_string(),
                        message: e.to_string(),
                    },
                    0,
                );
            }
        }

        let old_fields: BTreeSet<String> = if new_doc_in_request {
            BTreeSet::new()
        } else {
            coll.eid_fields
                .get(&id)
                .map(FieldCoverage::to_btree_set)
                .unwrap_or_default()
        };
        let eid = coll.interner.resolve(id).to_string();

        for f in &old_fields {
            if !item.fields.contains_key(f) {
                if let Some(fi) = coll.fields.get_mut(f.as_str()) {
                    fi.drop_eid(id, &eid);
                    if matches!(
                        &*fi,
                        FieldIndex::Vector { spec, .. }
                            if matches!(spec.backend, crate::shared_kernel::types::schema::VectorBackend::HnswCpu)
                    ) {
                        let FieldIndex::Vector { idx, .. } = fi else {
                            unreachable!("matched HNSW Vector field")
                        };
                        if let Some((wait, held)) = idx.take_hnsw_write_lock_timing() {
                            telemetry.record_hnsw_write_lock(wait, held);
                        }
                    }
                }
                if let Err(error) = coll.mark_field_dirty_charged(f, &eid, charge) {
                    return (
                        ReplaceDocResult::Error {
                            code: "apply_failed".to_owned(),
                            message: error.to_string(),
                        },
                        0,
                    );
                }
                // The field is gone; drop its stale replace-path checksum
                // (see `replace_value_unchanged`) so it can never be
                // mistakenly compared against if the field is re-added
                // later without a fresh write in between.
                if let Some(sub) = coll.field_checksums.get_mut(&id) {
                    sub.remove(f);
                }
            }
        }

        let mut fields_written = 0u32;
        let mut fields_skipped = 0u32;
        let mut bytes_written = 0u64;
        for (field_name, value) in &item.fields {
            let is_delta = old_fields.contains(field_name);
            if is_delta && Self::replace_value_unchanged(coll, id, field_name, value) {
                // Server-side no-op suppression (#1293): the incoming value
                // is byte-identical to what's already indexed — skip the
                // drop/reapply entirely so unchanged fields never rewrite a
                // posting list or tombstone+reinsert an HNSW vector.
                fields_skipped += 1;
                continue;
            }
            let fi = coll
                .fields
                .get_mut(field_name.as_str())
                .expect("field presence validated above");
            if is_delta {
                fi.drop_eid(id, &eid);
                if matches!(
                    &*fi,
                    FieldIndex::Vector { spec, .. }
                        if matches!(spec.backend, crate::shared_kernel::types::schema::VectorBackend::HnswCpu)
                ) {
                    let FieldIndex::Vector { idx, .. } = fi else {
                        unreachable!("matched HNSW Vector field")
                    };
                    if let Some((wait, held)) = idx.take_hnsw_write_lock_timing() {
                        telemetry.record_hnsw_write_lock(wait, held);
                    }
                }
            }
            let bytes = match apply_prepared_value(
                fi,
                id,
                &eid,
                value,
                field_name,
                prepared_text.and_then(|rows| rows.get(ordinal, field_name)),
                Some(&mut *telemetry),
            ) {
                Ok(bytes) => bytes,
                Err(e) => {
                    // Validation normally makes this unreachable, but an
                    // apply-time error after `drop_eid` still must journal the
                    // resulting absence before it reports the original error.
                    if is_delta {
                        if let Err(journal_error) =
                            coll.mark_field_dirty_charged(field_name, &eid, charge)
                        {
                            return (
                                ReplaceDocResult::Error {
                                    code: "apply_failed".to_string(),
                                    message: format!(
                                        "{journal_error}; record dropped value after apply error: {e}"
                                    ),
                                },
                                0,
                            );
                        }
                    }
                    return (
                        ReplaceDocResult::Error {
                            code: "apply_failed".to_string(),
                            message: e.to_string(),
                        },
                        0,
                    );
                }
            };
            if let Err(error) = coll.mark_field_dirty_charged(field_name, &eid, charge) {
                return (
                    ReplaceDocResult::Error {
                        code: "apply_failed".to_owned(),
                        message: error.to_string(),
                    },
                    0,
                );
            }
            Self::record_replace_checksum(coll, id, field_name, value);
            fields_written += 1;
            bytes_written += bytes;
        }

        if item.fields.is_empty() {
            coll.eid_fields.remove(&id);
            coll.field_checksums.remove(&id);
        } else {
            coll.eid_fields.insert(
                id,
                FieldCoverage::from_btree_set(item.fields.keys().cloned().collect()),
            );
        }
        if let Some(v) = item.version {
            coll.doc_versions.insert(id, v);
        }

        (
            ReplaceDocResult::Ok {
                fields_written,
                fields_skipped,
            },
            bytes_written,
        )
    }
}
