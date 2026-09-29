//! What the next checkpoint must write: each write marks the field and document
//! it changed with a revision and journals the value, and a finished checkpoint
//! acknowledges the revisions it captured.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::storage::CheckpointValue;

impl Collection {
    pub(crate) fn mark_field_dirty(&mut self, field: &str, external_id: &str) -> Result<()> {
        self.mark_field_dirty_charged(field, external_id, None)
    }

    pub(crate) fn mark_field_dirty_charged(
        &mut self,
        field: &str,
        external_id: &str,
        charge: Option<&crate::ingest::domain::change_budget::RetainedCharge>,
    ) -> Result<()> {
        if !matches!(
            self.fields.get(field),
            Some(
                FieldIndex::Keyword(_)
                    | FieldIndex::Number(_)
                    | FieldIndex::Set(_)
                    | FieldIndex::Hash(_)
                    | FieldIndex::Text { .. }
                    | FieldIndex::Vector { .. }
            )
        ) {
            self.requires_full_checkpoint = true;
            return Ok(());
        }
        self.next_field_dirty_revision = self.next_field_dirty_revision.saturating_add(1);
        self.field_dirty
            .entry(field.to_owned())
            .or_default()
            .insert(external_id.to_owned(), self.next_field_dirty_revision);
        let value = self.checkpoint_value(field, external_id).map_err(|error| {
            // The live mutation already happened. A missing journal value must
            // never authorize reusing older files as if this row were saved.
            self.requires_full_checkpoint = true;
            error
        })?;
        self.change_journal.record_charged(
            field.to_owned(),
            external_id.to_owned(),
            self.next_field_dirty_revision,
            value.map(std::sync::Arc::new),
            charge.cloned(),
        );
        Ok(())
    }

    pub(crate) fn checkpoint_value(
        &self,
        field: &str,
        external_id: &str,
    ) -> Result<Option<CheckpointValue>> {
        let Some(id) = self.interner.id(external_id) else {
            return Ok(None);
        };
        let index = self
            .fields
            .get(field)
            .ok_or_else(|| anyhow!("capture field missing"))?;
        match index {
            FieldIndex::Keyword(index) => Ok(index.keyword_at(id).map(CheckpointValue::Keyword)),
            FieldIndex::Number(index) => Ok(index
                .number_at(id)
                .map(|n| CheckpointValue::Number(n.to_f64()))),
            FieldIndex::Set(index) => Ok(index
                .set_members(id)
                .map(|set| CheckpointValue::Set(set.into_iter().collect()))),
            FieldIndex::Hash(index) => Ok(index.hash_at(id).map(CheckpointValue::Hash)),
            FieldIndex::Text { idx, .. } => {
                if let Some(row) = idx.staged_rows.get(&id) {
                    return Ok(Some(CheckpointValue::StagedText(row.clone())));
                }
                let Some(distinct) = idx.distinct_at(id) else {
                    return Ok(None);
                };
                let tokens = distinct
                    .iter()
                    .map(|token| {
                        idx.tokens
                            .get(token)
                            .and_then(|posting| posting.tf(id))
                            .map(|tf| Ok((token.clone(), tf)))
                            .unwrap_or_else(|| Err(anyhow!("text overlay is missing its posting")))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                Ok(Some(CheckpointValue::Text {
                    doc_len: idx.doc_len(id),
                    tokens,
                }))
            }
            FieldIndex::Vector { idx, .. } => Ok(idx
                .checkpoint_vector(external_id)?
                .map(CheckpointValue::Vector)),
        }
    }

    pub(crate) fn field_dirty_snapshot(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        self.field_dirty.clone()
    }

    pub(crate) fn acknowledge_field_dirty(
        &mut self,
        captured: &BTreeMap<String, BTreeMap<String, u64>>,
    ) {
        for (field, rows) in captured {
            let Some(current) = self.field_dirty.get_mut(field) else {
                continue;
            };
            current.retain(|id, revision| rows.get(id) != Some(revision));
            if current.is_empty() {
                self.field_dirty.remove(field);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn field_dirty_len(&self, field: &str) -> usize {
        self.field_dirty.get(field).map_or(0, BTreeMap::len)
    }

    #[cfg(test)]
    pub(crate) fn requires_full_checkpoint(&self) -> bool {
        self.requires_full_checkpoint
    }
}
