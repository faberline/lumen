//! Per-collection and per-field stats, and the test-only count of staged text
//! rows a field has not absorbed.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::stats::{CacheStats, FieldStats, StatsResponse, StorageStats};

#[cfg(test)]
use crate::index::domain::field_index::FieldIndex;
#[cfg(test)]
use crate::storage::STATS_THREAD;

impl Engine {
    pub fn stats(&self, collection_id: &str) -> Result<StatsResponse> {
        #[cfg(test)]
        {
            *STATS_THREAD.lock().expect("stats thread record") = Some(std::thread::current().id());
        }
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;

        let fields: BTreeMap<String, FieldStats> = coll
            .fields
            .iter()
            .map(|(name, fi)| {
                let stats = FieldStats {
                    field_type: fi.field_type(),
                    unique_terms: fi.unique_terms(),
                    bytes: fi.bytes(),
                    avg_doc_len: fi.avg_doc_len(),
                };
                (name.clone(), stats)
            })
            .collect();

        let total_bytes: u64 = fields.values().map(|s| s.bytes).sum();
        // #1397 R2: the response reports this collection's own total, but the
        // gauge is engine-wide — summing only this collection here is the
        // same last-writer-wins defect `evict_not_owned` had (whichever
        // collection was `/stats`-ed last "wins" the gauge), and the reshard
        // trigger reads this gauge expecting an engine-wide figure.
        self.publish_storage_bytes(&state);

        let last_indexed_at = coll.last_indexed_at.map(|t| {
            let dt: chrono::DateTime<chrono::Utc> = t.into();
            dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        });

        Ok(StatsResponse {
            documents_indexed: coll.eid_fields.len() as u64,
            fields,
            storage: StorageStats { total_bytes },
            cache: CacheStats {
                // In-memory engine has no posting-list cache layer;
                // when the LSM backend is attached this will report
                // the moka byte-weighted hit ratio.
                posting_hit_ratio: 1.0,
            },
            last_indexed_at,
        })
    }

    /// Test-only count of un-absorbed staged Text rows for one field. It reads
    /// the same live state `/stats` reads and mutates nothing.
    #[cfg(test)]
    pub(crate) fn staged_text_row_count(&self, collection_id: &str, field: &str) -> usize {
        let state = self.state.read().expect("state poisoned");
        let Some(coll) = state.collections.get(collection_id) else {
            return 0;
        };
        match coll.fields.get(field) {
            Some(FieldIndex::Text { idx, .. }) => idx.staged_rows.len(),
            _ => 0,
        }
    }
}
