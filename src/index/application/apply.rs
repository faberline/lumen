//! Applying one value to one field index: the prepared path, which installs a
//! staged text row, and the plain path, which checks the value against the
//! field's type and indexes it.

mod committed_index_apply;
mod committed_index_plan;
mod committed_replace_apply;
mod committed_replace_plan;
mod committed_replace_view;
mod committed_text_apply;
mod record_apply;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::analysis::{ngram_stream, tokenize};
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::postings::Postings;
use crate::index::domain::schema_validation::value_kind;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::token_set::TokenSet;
use crate::metrics::CommittedApplyTelemetry;
use crate::shared_kernel::types::document::FieldValue;
use crate::shared_kernel::types::schema::{Analyzer, FieldType};

pub(super) fn apply_prepared_value(
    fi: &mut FieldIndex,
    id: u32,
    eid: &str,
    value: &FieldValue,
    field: &str,
    prepared: Option<&Arc<crate::index::infrastructure::staging::staged_text_row::StagedTextRow>>,
    telemetry: Option<&mut CommittedApplyTelemetry<'_>>,
) -> Result<u64> {
    let Some(row) = prepared else {
        return apply_value(fi, id, eid, value, field, telemetry);
    };
    let FieldIndex::Text { idx, .. } = fi else {
        bail!("prepared Text field changed after validation");
    };
    let bytes = row.indexed_bytes(eid);
    idx.staged_rows.insert(id, row.clone());
    idx.doc_count += 1;
    idx.total_doc_len += u64::from(row.doc_len());
    idx.bytes += bytes;
    Ok(bytes)
}

fn apply_value(
    fi: &mut FieldIndex,
    id: u32,
    eid: &str,
    value: &FieldValue,
    field_name: &str,
    telemetry: Option<&mut CommittedApplyTelemetry<'_>>,
) -> Result<u64> {
    match (fi, value) {
        (FieldIndex::Text { analyzer, idx }, FieldValue::String(s)) => {
            let mut bytes = 0u64;
            let mut distinct = TokenSet::default();
            let mut apply_token = |idx: &mut TextIndex, token: &str| {
                if distinct.insert_str(token) {
                    bytes += (token.len() + eid.len()) as u64;
                }
                if let Some(posting) = idx.tokens.get_mut(token) {
                    posting.upsert_add(id, 1);
                } else {
                    let mut posting = Postings::default();
                    posting.upsert(id, 1);
                    idx.tokens.insert(token.to_string(), posting);
                }
            };
            let doc_len = match analyzer {
                Analyzer::WhitespaceLower => tokenize::for_whitespace_lower_cow(&s, |tok| {
                    apply_token(idx, tok.as_ref());
                }),
                Analyzer::Ngram => ngram_stream::stream_default_ngrams(
                    &s,
                    |token| -> Result<(), std::convert::Infallible> {
                        apply_token(idx, token);
                        Ok(())
                    },
                )?,
                Analyzer::Jieba => {
                    let tokens = tokenize::tokenize(&s, *analyzer);
                    let doc_len = tokens.len() as u32;
                    for tok in &tokens {
                        apply_token(idx, tok);
                    }
                    doc_len
                }
            };
            idx.delta_docs.insert(id, (doc_len, distinct));
            idx.doc_count += 1;
            idx.total_doc_len += doc_len as u64;
            idx.bytes += bytes;
            Ok(bytes)
        }
        (FieldIndex::Keyword(k), FieldValue::String(s)) => {
            let bytes = (s.len() + eid.len()) as u64;
            if let Some(posting) = k.terms.get_mut(s) {
                posting.insert(id);
                if posting.len() == 2 {
                    k.dup_values.insert(s.clone());
                }
            } else {
                let mut posting = RoaringBitmap::new();
                posting.insert(id);
                k.terms.insert(s.clone(), posting);
            }
            k.set_keyword(id, s.clone());
            k.bytes += bytes;
            Ok(bytes)
        }
        (FieldIndex::Number(n), FieldValue::Number(x)) => {
            let key =
                SortableF64::new(*x).map_err(|e| StorageError::InvalidNumber(e.to_string()))?;
            let bytes = (8 + eid.len()) as u64;
            let posting = n.values.entry(key).or_default();
            posting.insert(id);
            if posting.len() == 2 {
                n.dup_values.insert(key);
            }
            n.set_number(id, key);
            n.bytes += bytes;
            Ok(bytes)
        }
        (FieldIndex::Set(s), FieldValue::StringList(elems)) => {
            let mut bytes = 0u64;
            let mut seen = BTreeSet::new();
            for el in elems {
                if seen.insert(el.clone()) {
                    bytes += (el.len() + eid.len()) as u64;
                    if let Some(posting) = s.elements.get_mut(el) {
                        posting.insert(id);
                        if posting.len() == 2 {
                            s.dup_values.insert(el.clone());
                        }
                    } else {
                        let mut posting = RoaringBitmap::new();
                        posting.insert(id);
                        s.elements.insert(el.clone(), posting);
                    }
                }
            }
            s.forward.insert(id, seen);
            s.bytes += bytes;
            Ok(bytes)
        }
        // Permitted coercions: set field accepts a single string as a
        // 1-element set.
        (FieldIndex::Set(_), FieldValue::String(_)) => Err(StorageError::TypeMismatch {
            field: field_name.to_string(),
            expected: FieldType::Set,
            got: "string (expected array of strings)",
        }
        .into()),
        (FieldIndex::Vector { spec, idx, bytes }, FieldValue::Vector(v)) => {
            if v.len() as u32 != spec.dim {
                bail!(
                    "vector field `{field_name}` declared dim={} but got vector of length {}",
                    spec.dim,
                    v.len()
                );
            }
            if matches!(
                spec.backend,
                crate::shared_kernel::types::schema::VectorBackend::HnswCpu
            ) {
                let hnsw_add_started = Instant::now();
                let add = idx.add(eid, v);
                let hnsw_write_lock_timing = idx.take_hnsw_write_lock_timing();
                let hnsw_graph_rebuild_timing = idx.take_hnsw_graph_rebuild_timing();
                if let Some(telemetry) = telemetry {
                    if let Some((wait, held)) = hnsw_write_lock_timing {
                        telemetry.record_hnsw_write_lock(wait, held);
                    }
                    if let Some(elapsed) = hnsw_graph_rebuild_timing {
                        telemetry.record_hnsw_graph_rebuild(elapsed);
                    }
                    telemetry.record_hnsw_add(hnsw_add_started.elapsed());
                }
                add?;
            } else {
                idx.add(eid, v)?;
            }
            let approx = (spec.dim as u64) * 4 + eid.len() as u64;
            *bytes += approx;
            Ok(approx)
        }
        (FieldIndex::Hash(h), FieldValue::String(s)) => {
            let hash = parse_hash(s)?;
            let bytes = 12u64; // u32 docid + u64 hash
            h.forward.insert(id, hash);
            h.bytes += bytes;
            Ok(bytes)
        }
        (fi, v) => Err(StorageError::TypeMismatch {
            field: field_name.to_string(),
            expected: fi.field_type(),
            got: value_kind(v),
        }
        .into()),
    }
}
