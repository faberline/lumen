//! Recovery's delta replay: a checkpoint delta's rows applied to a reopened
//! field, or its segment attached as the field's live delta reader.

use std::collections::BTreeSet;

use anyhow::{anyhow, bail, Result};

use crate::index::application::checkpoint_capture::CheckpointValue;
use crate::index::application::engine::Engine;
use crate::index::application::live_delta::attach_delta_reader;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::token_set::TokenSet;

impl Engine {
    pub(crate) fn apply_checkpoint_delta(
        &self,
        collection_id: &str,
        field: &str,
        rows: Vec<(String, Option<CheckpointValue>)>,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("missing delta collection"))?;
        for (eid, value) in rows {
            let id = coll.interner.intern(&eid);
            let index = coll
                .fields
                .get_mut(field)
                .ok_or_else(|| anyhow!("missing delta field"))?;
            index.drop_eid(id, &eid);
            if let Some(value) = value {
                match (index, value) {
                    (FieldIndex::Keyword(index), CheckpointValue::Keyword(value)) => {
                        index.bytes += (value.len() + eid.len()) as u64;
                        let posting = index.terms.entry(value.clone()).or_default();
                        posting.insert(id);
                        if posting.len() >= 2 {
                            index.dup_values.insert(value.clone());
                        }
                        index.forward.insert(id, value);
                    }
                    (FieldIndex::Number(index), CheckpointValue::Number(value)) => {
                        let key = SortableF64::new(value)?;
                        index.bytes += (8 + eid.len()) as u64;
                        let posting = index.values.entry(key).or_default();
                        posting.insert(id);
                        if posting.len() >= 2 {
                            index.dup_values.insert(key);
                        }
                        index.forward.insert(id, key);
                        index.clear_keyword_range_cache();
                    }
                    (FieldIndex::Set(index), CheckpointValue::Set(values)) => {
                        let members: BTreeSet<_> = values.into_iter().collect();
                        for value in &members {
                            index.bytes += (value.len() + eid.len()) as u64;
                            let posting = index.elements.entry(value.clone()).or_default();
                            posting.insert(id);
                            if posting.len() >= 2 {
                                index.dup_values.insert(value.clone());
                            }
                        }
                        index.forward.insert(id, members);
                    }
                    (FieldIndex::Hash(index), CheckpointValue::Hash(value)) => {
                        index.bytes += 12;
                        index.forward.insert(id, value);
                    }
                    (FieldIndex::Text { idx, .. }, CheckpointValue::Text { doc_len, tokens }) => {
                        let mut distinct = TokenSet::default();
                        for (token, tf) in tokens {
                            idx.tokens.entry(token.clone()).or_default().upsert(id, tf);
                            idx.bytes += (token.len() + eid.len()) as u64;
                            distinct.insert_str(&token);
                        }
                        idx.doc_count += 1;
                        idx.total_doc_len += doc_len as u64;
                        idx.delta_docs.insert(id, (doc_len, distinct));
                        idx.clear_match_rank_cache();
                    }
                    (
                        FieldIndex::Vector { idx, bytes, .. },
                        CheckpointValue::StagedVector(value),
                    ) => {
                        idx.restore_checkpoint_vector(&eid, value.as_f32_slice())?;
                        *bytes += (value.dim() * 4 + eid.len()) as u64;
                    }
                    (FieldIndex::Vector { idx, bytes, .. }, CheckpointValue::Vector(value)) => {
                        idx.restore_checkpoint_vector(&eid, &value)?;
                        *bytes += (value.len() * 4 + eid.len()) as u64;
                    }
                    _ => bail!("delta field type does not match schema"),
                }
                coll.eid_fields
                    .entry(id)
                    .or_default()
                    .insert(field.to_owned());
            } else if let Some(coverage) = coll.eid_fields.get_mut(&id) {
                coverage.remove(field);
                if coverage.is_empty() {
                    coll.eid_fields.remove(&id);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn attach_checkpoint_delta_reader(
        &self,
        collection_id: &str,
        field: &str,
        reader: std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        external_ids: Vec<String>,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("missing delta collection"))?;
        let ids: Vec<_> = external_ids
            .iter()
            .map(|eid| coll.interner.intern(eid))
            .collect();
        let index = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("missing delta field"))?;
        if let FieldIndex::Text { idx, .. } = index {
            for (row, id) in ids.iter().enumerate() {
                let row = row as u32;
                let old_present = idx
                    .segment
                    .as_ref()
                    .is_some_and(|segment| segment.text_is_present(*id));
                let old_len = old_present
                    .then(|| {
                        idx.segment
                            .as_ref()
                            .map(|segment| segment.text_doc_len(*id))
                            .unwrap_or(0)
                    })
                    .unwrap_or(0);
                let new_present = reader.text_is_present(row);
                let new_len = new_present.then(|| reader.text_doc_len(row)).unwrap_or(0);
                if old_present {
                    idx.doc_count = idx.doc_count.saturating_sub(1);
                    idx.total_doc_len = idx.total_doc_len.saturating_sub(old_len as u64);
                }
                if new_present {
                    idx.doc_count += 1;
                    idx.total_doc_len += new_len as u64;
                }
            }
            idx.clear_match_rank_cache();
        }
        if let FieldIndex::Vector { idx, .. } = index {
            idx.attach_checkpoint_delta(
                reader.clone(),
                &external_ids,
                &vec![true; external_ids.len()],
            )?;
        } else {
            attach_delta_reader(index, reader.clone(), ids)?;
        }
        for (row, eid) in external_ids.iter().enumerate() {
            let present = match index {
                FieldIndex::Keyword(_) => reader.keyword_at(row as u32).is_some(),
                FieldIndex::Number(_) => reader.number_at(row as u32).is_some(),
                FieldIndex::Set(_) => reader.set_at(row as u32).is_some(),
                FieldIndex::Hash(_) => reader.hash_at(row as u32).is_some(),
                FieldIndex::Text { .. } => reader.text_is_present(row as u32),
                FieldIndex::Vector { spec, .. } => {
                    reader.vector_at(row as u32, spec.dim as usize).is_some()
                }
            };
            let id = coll.interner.id(eid).expect("interned delta ID");
            if present {
                coll.eid_fields
                    .entry(id)
                    .or_default()
                    .insert(field.to_owned());
            } else if let Some(fields) = coll.eid_fields.get_mut(&id) {
                fields.remove(field);
                if fields.is_empty() {
                    coll.eid_fields.remove(&id);
                }
            }
        }
        Ok(())
    }
}
