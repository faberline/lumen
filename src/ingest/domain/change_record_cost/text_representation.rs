//! The walk that prices a committed record item by item, and charges the prefix
//! a failed batch already applied.

use crate::ingest::domain::change_memory_cost::{Change, FieldCost, VectorBackendCost};
use crate::ingest::domain::change_record_cost::ngram_distinct_table::NgramDistinctTable;
use crate::ingest::domain::change_record_cost::text_upper_bound::{
    text_upper_bound, AnalyzerKind, NormalizeError, TextUpperBound,
};
use crate::ingest::domain::change_record_cost::{CostContext, RecordCost};
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::{Analyzer, FieldSpec, FieldType, VectorBackend};

#[derive(Clone, Copy)]
pub(super) enum TextRepresentation {
    Normalized,
    PreparedRow,
}

pub(super) fn estimate_record_with_text_representation(
    entry: &RaftLogEntry,
    ctx: &impl CostContext,
    text_representation: TextRepresentation,
    mut ngram_table: Option<&mut NgramDistinctTable>,
) -> Result<RecordCost, NormalizeError> {
    match entry {
        RaftLogEntry::CreateCollection { collection_id, req } => {
            let bytes = collection_id
                .len()
                .checked_add(req.fields.iter().try_fold(0usize, |n, (name, _)| {
                    n.checked_add(name.len() + 64)
                        .ok_or(NormalizeError::Overflow)
                })?)
                .ok_or(NormalizeError::Overflow)?;
            one(Change::Schema {
                metadata_bytes: bytes,
            })
        }
        RaftLogEntry::Index { collection_id, req } => {
            // The storage path rejects a missing collection before it interns
            // an ID or records request metadata.
            if !ctx.collection_exists(collection_id) {
                return Ok(RecordCost::default());
            }
            if ctx.request_is_deduplicated(collection_id, req.request_id.as_deref()) {
                return Ok(RecordCost::default());
            }
            let mut out = RecordCost::default();
            for item in &req.items {
                if ctx.index_cell_is_stale(
                    collection_id,
                    &item.external_id,
                    &item.field,
                    item.version,
                ) {
                    continue;
                }
                let Some(spec) = ctx.field_spec(collection_id, &item.field) else {
                    charge_index_error_prefix(&mut out, ctx, collection_id, item, false)?;
                    out.add_request_id(req.request_id.as_deref())?;
                    return Ok(out);
                };
                let field = match field_cost(
                    spec,
                    &item.value,
                    text_representation,
                    ngram_table.as_deref_mut(),
                ) {
                    Ok(field) => field,
                    Err(FieldCostError::Invalid) => {
                        charge_index_error_prefix(&mut out, ctx, collection_id, item, true)?;
                        out.add_request_id(req.request_id.as_deref())?;
                        return Ok(out);
                    }
                    Err(FieldCostError::Overflow) => return Err(NormalizeError::Overflow),
                };
                out.add_change(&Change::Index {
                    external_id_bytes: item.external_id.len(),
                    new_document: !ctx.known_external_id(collection_id, &item.external_id),
                    field,
                    volatile_metadata_bytes: item.version.map(|_| 96).unwrap_or(0),
                })?;
                out.add_change(&Change::Direct {
                    metadata_bytes: field_metadata_bytes(&item.field)?,
                })?;
            }
            out.add_request_id(req.request_id.as_deref())?;
            Ok(out)
        }
        RaftLogEntry::ReplaceDocs { collection_id, req } => {
            // replace_docs rejects a missing collection before it touches any
            // document. A malformed document is per-item, so earlier valid
            // document replacements remain applied and must stay charged.
            if !ctx.collection_exists(collection_id) {
                return Ok(RecordCost::default());
            }
            let mut out = RecordCost::default();
            for doc in &req.docs {
                let mut fields = Vec::with_capacity(doc.fields.len());
                let mut sidecars: usize = doc.version.map(|_| 96).unwrap_or(0);
                let mut invalid = false;
                for (name, value) in &doc.fields {
                    let Some(spec) = ctx.field_spec(collection_id, name) else {
                        invalid = true;
                        break;
                    };
                    if matches!(spec.field_type, FieldType::Text | FieldType::Vector) {
                        sidecars = sidecars
                            .checked_add(name.len() + 96)
                            .ok_or(NormalizeError::Overflow)?;
                    }
                    match field_cost(spec, value, text_representation, ngram_table.as_deref_mut()) {
                        Ok(field) => fields.push(field),
                        // replace_one_doc validates its complete field map
                        // before it deletes or writes this document. The
                        // accumulated earlier documents are still a real
                        // prefix, but this document is not.
                        Err(FieldCostError::Invalid) => {
                            invalid = true;
                            break;
                        }
                        Err(FieldCostError::Overflow) => return Err(NormalizeError::Overflow),
                    }
                }
                if invalid {
                    charge_replace_invalid_prefix(&mut out, ctx, collection_id, doc)?;
                    continue;
                }
                out.add_change(&Change::Replace {
                    external_id_bytes: doc.external_id.len(),
                    new_document: !ctx.known_external_id(collection_id, &doc.external_id),
                    fields,
                    volatile_metadata_bytes: sidecars,
                })?;
                for name in doc.fields.keys() {
                    out.add_change(&Change::Direct {
                        metadata_bytes: field_metadata_bytes(name)?,
                    })?;
                }
                // Omitted declared-and-present fields become dirty tombstones.
                let mut omitted = 0usize;
                ctx.visit_coverage(collection_id, &doc.external_id, &mut |name| {
                    if !doc.fields.contains_key(name) {
                        omitted = omitted.saturating_add(1);
                    }
                });
                if omitted != 0 {
                    out.add_change(&Change::Unindex {
                        external_id_bytes: doc.external_id.len(),
                        field_count: omitted,
                    })?;
                }
            }
            Ok(out)
        }
        RaftLogEntry::UnindexDocs { collection_id, req } => {
            let mut out = RecordCost::default();
            for external_id in &req.external_ids {
                let mut fields = 0usize;
                ctx.visit_coverage(collection_id, external_id, &mut |_| {
                    fields = fields.saturating_add(1);
                });
                if fields != 0 {
                    out.add_change(&Change::Unindex {
                        external_id_bytes: external_id.len(),
                        field_count: fields,
                    })?;
                }
            }
            Ok(out)
        }
        RaftLogEntry::Delete {
            collection_id,
            external_id,
            field,
        } => {
            let mut fields = 0usize;
            if ctx.known_external_id(collection_id, external_id) {
                match field {
                    Some(_) => fields = 1, // current delete marks this field dirty
                    None => ctx.visit_coverage(collection_id, external_id, &mut |_| {
                        fields = fields.saturating_add(1);
                    }),
                }
            }
            if fields == 0 {
                Ok(RecordCost::default())
            } else {
                one(Change::Unindex {
                    external_id_bytes: external_id.len(),
                    field_count: fields,
                })
            }
        }
        // Retired old state is already charged by the reclaimer owner.
        // No negative capacity credit is created here.
        RaftLogEntry::TruncateDocs { .. } | RaftLogEntry::DropCollection { .. } => {
            Ok(RecordCost::default())
        }
        RaftLogEntry::AddField {
            collection_id,
            field_name,
            spec: _,
        }
        | RaftLogEntry::DropField {
            collection_id,
            field_name,
        } => one(Change::Schema {
            metadata_bytes: collection_id.len() + field_name.len() + 64,
        }),
    }
}

/// The index path interns the group external ID before field lookup and may
/// drop an existing field before its value validation fails. Charge that
/// concrete prefix without guessing a field payload that never became live.
fn charge_index_error_prefix(
    out: &mut RecordCost,
    ctx: &impl CostContext,
    collection_id: &str,
    item: &crate::shared_kernel::types::document::IndexItem,
    declared_field: bool,
) -> Result<(), NormalizeError> {
    if !ctx.known_external_id(collection_id, &item.external_id) {
        out.add_change(&Change::Direct {
            metadata_bytes: item
                .external_id
                .len()
                .checked_add(item.field.len())
                .and_then(|bytes| bytes.checked_add(128))
                .ok_or(NormalizeError::Overflow)?,
        })?;
        return Ok(());
    }
    if declared_field {
        // apply_value errors call mark_field_dirty even when coverage did not
        // yet contain this declared field.
        out.add_change(&Change::Unindex {
            external_id_bytes: item.external_id.len(),
            field_count: 1,
        })?;
        out.add_change(&Change::Direct {
            metadata_bytes: field_metadata_bytes(&item.field)?,
        })?;
    }
    Ok(())
}

fn charge_replace_invalid_prefix(
    out: &mut RecordCost,
    ctx: &impl CostContext,
    collection_id: &str,
    doc: &crate::shared_kernel::types::document::ReplaceDocItem,
) -> Result<(), NormalizeError> {
    if !ctx.known_external_id(collection_id, &doc.external_id) {
        out.add_change(&Change::Direct {
            metadata_bytes: doc
                .external_id
                .len()
                .checked_add(128)
                .ok_or(NormalizeError::Overflow)?,
        })?;
    }
    Ok(())
}

fn field_metadata_bytes(field: &str) -> Result<usize, NormalizeError> {
    field.len().checked_add(96).ok_or(NormalizeError::Overflow)
}

fn one(change: Change) -> Result<RecordCost, NormalizeError> {
    let mut out = RecordCost::default();
    out.add_change(&change)?;
    Ok(out)
}

enum FieldCostError {
    Overflow,
    Invalid,
}

fn field_cost(
    spec: &FieldSpec,
    value: &FieldValue,
    text_representation: TextRepresentation,
    ngram_table: Option<&mut NgramDistinctTable>,
) -> Result<FieldCost, FieldCostError> {
    match (&spec.field_type, value) {
        (FieldType::Keyword, FieldValue::String(value)) => Ok(FieldCost::Keyword {
            value_bytes: value.len(),
        }),
        (FieldType::Number, FieldValue::Number(_)) => Ok(FieldCost::Number),
        (FieldType::Set, FieldValue::StringList(values)) => Ok(FieldCost::Set {
            members: values.len(),
            member_bytes: values.iter().try_fold(0usize, |n, value| {
                n.checked_add(value.len()).ok_or(FieldCostError::Overflow)
            })?,
        }),
        (FieldType::Hash, FieldValue::String(_)) => Ok(FieldCost::Hash),
        (FieldType::Text, FieldValue::String(value)) => {
            if matches!(text_representation, TextRepresentation::PreparedRow) {
                return Ok(FieldCost::Text {
                    distinct_terms: 0,
                    total_term_bytes: 0,
                });
            }
            let analyzer = match spec.analyzer.unwrap_or(Analyzer::WhitespaceLower) {
                Analyzer::WhitespaceLower => AnalyzerKind::WhitespaceLower,
                Analyzer::Jieba => AnalyzerKind::Jieba,
                Analyzer::Ngram => AnalyzerKind::Ngram,
            };
            let TextUpperBound {
                terms,
                total_utf8_bytes,
            } = match (analyzer, ngram_table) {
                (AnalyzerKind::Ngram, Some(table)) => table.bound(value),
                _ => text_upper_bound(
                    value,
                    analyzer,
                    crate::tokenize::DEFAULT_NGRAM_MIN,
                    crate::tokenize::DEFAULT_NGRAM_MAX,
                ),
            }
            .map_err(|_| FieldCostError::Overflow)?;
            Ok(FieldCost::Text {
                distinct_terms: terms,
                total_term_bytes: total_utf8_bytes,
            })
        }
        (FieldType::Vector, FieldValue::Vector(value)) => {
            let dim = usize::try_from(spec.dim.ok_or(FieldCostError::Invalid)?)
                .map_err(|_| FieldCostError::Overflow)?;
            if value.len() != dim {
                return Err(FieldCostError::Invalid);
            }
            let backend = match spec.backend.unwrap_or(VectorBackend::HnswCpu) {
                VectorBackend::FlatCpu => VectorBackendCost::Flat,
                VectorBackend::HnswCpu => VectorBackendCost::Hnsw,
            };
            Ok(FieldCost::Vector {
                dim,
                backend,
                quantized_sq: spec.quantize.is_some(),
            })
        }
        _ => Err(FieldCostError::Invalid),
    }
}
