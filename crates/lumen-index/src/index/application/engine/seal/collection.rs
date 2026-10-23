//! The whole-collection half of the seam: seal a collection to segments, read
//! its schema, reopen it from its directory, and probe a field's forward
//! payload.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::index::application::engine::Engine;
use crate::index::domain::collection::Collection;
use crate::index::domain::field_index::FieldIndex;
use crate::shared_kernel::types::schema::FieldSpec;

#[cfg(test)]
impl Engine {
    /// TEST HELPER (Phase 2f-1): run the PRODUCTION collection-level
    /// `seal_to_segments` on a collection in place (seal every field, write the
    /// EID column, drop the forward payload). Used by the triple-path diff test
    /// to materialize PATH B (engine after seal-and-drop).
    pub(super) fn __seal_collection_to_segments(
        &self,
        collection_id: &str,
        dir: &std::path::Path,
        applied_seq: u64,
    ) -> Result<()> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        coll.seal_to_segments(dir, applied_seq)
    }

    /// TEST HELPER (Phase 2f-1): the collection's field-spec schema (for driving
    /// `Collection::open_from_segments`, which needs the field types out-of-band).
    pub(super) fn __collection_schema(
        &self,
        collection_id: &str,
    ) -> Result<BTreeMap<String, FieldSpec>> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        Ok(coll.schema.clone())
    }

    /// TEST HELPER (Phase 2f-1): build a fresh Engine whose single collection
    /// `collection_id` is reopened from the segments under `dir` via the
    /// PRODUCTION `Collection::open_from_segments` — NO CBOR snapshot, NO
    /// whole-collection load. Materializes PATH C of the triple-path diff test.
    pub(super) fn __open_collection_from_segments(
        collection_id: &str,
        dir: &std::path::Path,
        schema: BTreeMap<String, FieldSpec>,
        version: u32,
    ) -> Result<std::sync::Arc<Engine>> {
        let coll = Collection::open_from_segments(dir, schema, version)?;
        let engine = Engine::new();
        {
            let mut state = engine
                .state
                .write()
                .map_err(|_| anyhow!("state poisoned"))?;
            state.collections.insert(collection_id.to_string(), coll);
        }
        Ok(std::sync::Arc::new(engine))
    }

    /// TEST HELPER (Phase 2f-1): a direct probe that a field's forward payload
    /// left RAM after a seal-and-drop — the "drop really frees RAM" assertion.
    /// Returns `(forward_len, tokens_len, has_segment)` for the named field.
    pub(super) fn __field_forward_probe(
        &self,
        collection_id: &str,
        field: &str,
    ) -> Result<(usize, usize, bool)> {
        let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let fi = coll
            .fields
            .get(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        Ok(match fi {
            FieldIndex::Number(n) => (n.forward_len(), 0, n.segment.is_some()),
            FieldIndex::Hash(h) => (h.forward.len(), 0, h.segment.is_some()),
            FieldIndex::Keyword(k) => (k.forward_len(), 0, k.segment.is_some()),
            FieldIndex::Set(s) => (s.forward.len(), 0, s.segment.is_some()),
            FieldIndex::Text { idx, .. } => (0, idx.tokens.len(), idx.segment.is_some()),
            FieldIndex::Vector { .. } => (0, 0, true),
        })
    }
}
