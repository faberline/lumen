//! Duplicate groups: the documents that share a value in one field, largest
//! groups first, with external ids resolved for the requested page only.

use std::time::Instant;

use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;

use crate::index::application::engine::Engine;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::sortable_f64::SortableF64;
use crate::index::domain::storage_error::StorageError;
use crate::shared_kernel::types::search::{DuplicateGroup, DuplicatesRequest, DuplicatesResponse};

impl Engine {
    pub fn duplicates(
        &self,
        collection_id: &str,
        req: DuplicatesRequest,
    ) -> Result<DuplicatesResponse> {
        let start = Instant::now();
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| StorageError::CollectionNotFound(collection_id.to_string()))?;
        coll.check_live(collection_id)?;
        let fi = coll
            .fields
            .get(&req.field)
            .ok_or_else(|| StorageError::UnknownField {
                collection: collection_id.to_string(),
                field: req.field.clone(),
            })?;

        let min = req.min_group_size.max(2) as usize;
        let interner = &coll.interner;
        // Phase 1: collect candidate groups as (value, posting) WITHOUT
        // materializing external ids. At high duplicate density the id strings
        // dominate the cost, and only one `limit` page of them is returned —
        // so resolution is deferred until after sort + paging (phase 2).
        use std::borrow::Cow;
        let mut cands: Vec<(serde_json::Value, Cow<'_, RoaringBitmap>)> = match fi {
            FieldIndex::Text { .. } => {
                return Err(StorageError::DuplicatesOnText(req.field.clone()).into());
            }
            FieldIndex::Vector { .. } => {
                bail!(
                    "duplicates is not supported on vector field `{}` — use knn instead",
                    req.field
                );
            }
            FieldIndex::Hash(_) => {
                bail!(
                    "duplicates is not supported on hash field `{}` — use a hamming query instead",
                    req.field
                );
            }
            FieldIndex::Keyword(k) => {
                // Phase 2h-1 FIX: a SEALED Keyword field dropped its in-RAM
                // `terms` driver, so iterating it directly would miss every
                // on-disk term (and report deleted docs for the tail-only
                // case). Drive from the segment-aware `live_terms` — segment
                // dict minus tombstones + live tail. Segment OFF: the
                // `dup_values` side-index already names every term with >= 2
                // docs, so only candidate groups are visited — not every
                // distinct term, and no whole-index clone.
                if k.segment.is_some() {
                    k.live_terms()
                        .into_iter()
                        .filter(|(_, set)| (set.len() as usize) >= min)
                        .map(|(v, set)| (serde_json::Value::String(v), Cow::Owned(set)))
                        .collect()
                } else {
                    k.dup_values
                        .iter()
                        .filter_map(|v| k.terms.get(v).map(|set| (v, set)))
                        .filter(|(_, set)| (set.len() as usize) >= min)
                        .map(|(v, set)| (serde_json::Value::String(v.clone()), Cow::Borrowed(set)))
                        .collect()
                }
            }
            FieldIndex::Number(n) => {
                // Phase 2h-3 FIX: a SEALED Number field dropped its in-RAM
                // `values` driver, so iterating it directly would miss every
                // on-disk value (and report deleted docs for the tail-only
                // case). Drive from the segment-aware `live_values` — segment
                // sorted-value column minus tombstones + live tail. Segment
                // OFF: the `dup_values` side-index names every value with
                // >= 2 docs — no full scan, no whole-index clone.
                let num = |v: SortableF64| {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(v.to_f64())
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    )
                };
                if n.segment.is_some() {
                    n.live_values()
                        .into_iter()
                        .filter(|(_, set)| (set.len() as usize) >= min)
                        .map(|(v, set)| (num(v), Cow::Owned(set)))
                        .collect()
                } else {
                    n.dup_values
                        .iter()
                        .filter_map(|v| n.values.get(v).map(|set| (*v, set)))
                        .filter(|(_, set)| (set.len() as usize) >= min)
                        .map(|(v, set)| (num(v), Cow::Borrowed(set)))
                        .collect()
                }
            }
            FieldIndex::Set(s) => {
                // Phase 2h-2 FIX: a SEALED Set field dropped its in-RAM `elements`
                // driver, so iterating it directly would miss every on-disk element
                // (and report deleted docs for the tail-only case). Drive from the
                // segment-aware `live_elements` — segment dict minus tombstones +
                // live tail. Segment OFF: the `dup_values` side-index names every
                // element with >= 2 docs — no full scan, no whole-index clone.
                if s.segment.is_some() {
                    s.live_elements()
                        .into_iter()
                        .filter(|(_, set)| (set.len() as usize) >= min)
                        .map(|(v, set)| (serde_json::Value::String(v), Cow::Owned(set)))
                        .collect()
                } else {
                    s.dup_values
                        .iter()
                        .filter_map(|v| s.elements.get(v).map(|set| (v, set)))
                        .filter(|(_, set)| (set.len() as usize) >= min)
                        .map(|(v, set)| (serde_json::Value::String(v.clone()), Cow::Borrowed(set)))
                        .collect()
                }
            }
        };
        // Stable: largest groups first, ties broken by value (JSON form) — the
        // same order the materialized sort produced before paging moved here.
        cands.sort_by_cached_key(|(v, set)| (std::cmp::Reverse(set.len()), v.to_string()));

        let offset = req.offset as usize;
        let limit = req.limit.max(1) as usize;
        let total = cands.len();
        // Phase 2: resolve external ids for the requested page ONLY.
        let page: Vec<DuplicateGroup> = cands
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(|(value, set)| DuplicateGroup {
                value,
                external_ids: set
                    .iter()
                    .map(|id| interner.resolve(id).to_string())
                    .collect(),
            })
            .collect();
        let truncated = offset + page.len() < total;

        self.metrics.incr_duplicates();
        Ok(DuplicatesResponse {
            groups: page,
            truncated,
            took_ms: start.elapsed().as_millis() as u64,
        })
    }
}
