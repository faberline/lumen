//! Single-value reads that skip the general search path: a document's number
//! value, and the native wire's single string-term query.

use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::query::page_cursor::make_cursor;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::search::{SearchHit, SearchResponse};

impl Engine {
    pub fn number_value_for_external_id(
        &self,
        collection_id: &str,
        external_id: &str,
        field: &str,
    ) -> Result<Option<f64>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;
        let Some(id) = coll.interner.id(external_id) else {
            return Ok(None);
        };
        let fi = coll
            .fields
            .get(field)
            .ok_or_else(|| StorageError::UnknownField {
                collection: collection_id.to_string(),
                field: field.to_string(),
            })?;
        let FieldIndex::Number(nidx) = fi else {
            return Ok(None);
        };
        Ok(nidx.live_number_at(id).map(SortableF64::to_f64))
    }

    pub(crate) fn search_fast_string_term(
        &self,
        collection_id: &str,
        field: &str,
        value: &str,
        limit: u32,
    ) -> Result<SearchResponse> {
        let start = Instant::now();
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;

        let posting: Option<std::borrow::Cow<RoaringBitmap>> = match coll.fields.get(field) {
            Some(FieldIndex::Keyword(k)) => k.term_postings(value),
            Some(FieldIndex::Set(s)) => s.element_postings(value),
            Some(_) => bail!("term query type mismatch on field `{field}`"),
            None => {
                return Err(StorageError::UnknownField {
                    collection: collection_id.to_string(),
                    field: field.to_string(),
                }
                .into());
            }
        };
        let limit = limit as usize;
        let total = posting.as_deref().map(|p| p.len()).unwrap_or(0);
        let hits: Vec<SearchHit> = posting
            .as_deref()
            .into_iter()
            .flat_map(|p| p.iter())
            .take(limit)
            .map(|id| SearchHit {
                external_id: coll.interner.resolve(id).to_string(),
                score: 1.0,
            })
            .collect();
        let cursor = if hits.len() < total as usize {
            Some(make_cursor(hits.len()))
        } else {
            None
        };

        let el = start.elapsed();
        let took_ms = el.as_millis() as u64;
        self.metrics.observe_search(el);
        Ok(SearchResponse {
            hits,
            total,
            cursor,
            took_ms,
            took_us: el.as_micros() as u64,
        })
    }
}
