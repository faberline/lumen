//! The Engine's disk-tier test seam: seal one field's in-RAM state to a
//! columnar mmap segment and attach it, so a test reads the segment for the
//! sealed id range and the live tail for the rest.

mod collection;
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Result};
use roaring::RoaringBitmap;

use crate::index::application::engine::Engine;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_index::FieldIndex;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;

// ---------------------------------------------------------------------------
// Test seam: seal a Number field to a disk segment (disk tier)
// ---------------------------------------------------------------------------

#[cfg(test)]
impl Engine {
    /// TEST SEAM (Stage 2 Phase 2c): seal the current in-RAM state of a Number
    /// field into a columnar mmap segment under `dir`, then attach it so per-doc
    /// PREDICATE point lookups read the segment for the sealed id range. Mirrors
    /// what a real flush would do: dumps `forward` in dense docid order
    /// `[0..n_docs)` (absent docs → `None`) via [`crate::persistence::infrastructure::segment::number_writer::write_number_segment`],
    /// opens a [`crate::persistence::infrastructure::segment::SegmentReader`], and sets `NumberIndex::segment`.
    ///
    /// `n_docs` is the interner's dense id count, so any doc indexed AFTER
    /// sealing (id >= n_docs) is NOT covered by the segment and stays served
    /// from the live `forward` tail — exactly the live/sealed split the runtime
    /// will use. Returns the sealed doc count.
    pub(super) fn __seal_number_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        // Dense doc-id space is `[0..interner.to_eid.len())`.
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Number(n) = fi else {
            bail!("field `{field}` is not a Number field");
        };
        // Column in dense docid order: each sealed id's live value (or None).
        let values: Vec<Option<f64>> = (0..n_docs as u32)
            .map(|id| n.live_number_at(id).map(|s| s.to_f64()))
            .collect();

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::number_writer::write_number_segment(
            &path,
            n_docs as u64,
            &values,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        n.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Phase 2h-3: mirror PRODUCTION `seal_to_segment` — drop BOTH the in-RAM
        // `forward` tail AND the inverted/range `values` driver (the sorted-value
        // column + per-value postings are on disk now). Queries drive from the
        // mmap; a doc indexed AFTER sealing (id >= n_docs) re-populates the live
        // `values`/`forward` tail, which the unified accessors compose with the
        // segment base.
        n.forward = FastHashMap::default();
        n.dense_forward = Vec::new();
        n.values = BTreeMap::new();
        n.dup_values = BTreeSet::new();
        n.clear_keyword_range_cache();
        // First-time seal from the live `forward`: no prior tombstone exists, but
        // reset for symmetry with the production re-seal path (Phase 2h-3).
        n.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2e-A, extended 2h-1): seal a Keyword field's
    /// in-RAM state into a columnar mmap segment under `dir`, then attach it so
    /// per-doc Keyword PREDICATE lookups (`keyword_at`) AND the inverted
    /// Term/Terms/boolean driver (`term_postings`/`term_df`) serve the sealed id
    /// range from the segment. Dumps `forward` in dense docid order
    /// `[0..n_docs)` (absent docs → `None`) plus the INVERTED postings (folded
    /// from `forward`) via [`crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment`] — a sorted
    /// prefix-compressed string DICT + a fixed `u32[n_docs]` dict-id forward
    /// column + a parallel per-term [`ROLE_KEYWORD_POSTINGS`] posting column.
    ///
    /// Phase 2h-1: this seam now mirrors PRODUCTION `seal_to_segment` — after
    /// attaching the reader it DROPS BOTH the in-RAM `forward` tail AND the
    /// inverted `terms` index (the RAM win). Queries then drive entirely from
    /// the mmap. A doc indexed AFTER sealing (id >= n_docs) re-populates the
    /// live `terms`/`forward` tail, which `term_postings`/`keyword_at` compose
    /// with the segment base. Returns the sealed doc count.
    pub(crate) fn __seal_keyword_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Keyword(k) = fi else {
            bail!("field `{field}` is not a Keyword field");
        };
        // Column in dense docid order: each sealed id's live keyword (or None).
        let owned: Vec<Option<String>> = (0..n_docs as u32).map(|id| k.keyword_at(id)).collect();
        let values: Vec<Option<&str>> = owned.iter().map(|o| o.as_deref()).collect();
        // INVERTED postings to seal: fold the live values (== the live `terms`
        // index restricted to the sealed id range) into a fresh BTreeMap.
        let mut terms: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        for (id, v) in values.iter().enumerate() {
            if let Some(s) = v {
                terms.entry((*s).to_string()).or_default().insert(id as u32);
            }
        }

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
            &path,
            n_docs as u64,
            &values,
            &terms,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        k.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Drop the RAM index — the whole [0..n_docs) inverted+forward state is
        // on disk now (Phase 2h-1). Queries drive from the mmap segment.
        k.forward = FastHashMap::default();
        k.dense_forward = Vec::new();
        k.terms = BTreeMap::new();
        k.dup_values = BTreeSet::new();
        // First-time seal from the live `forward`: no prior tombstone exists, but
        // reset for symmetry with the production re-seal path (Phase 2h-1 FIX).
        k.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2e-A, extended 2h-2): seal a Set field's in-RAM
    /// state into a columnar mmap segment under `dir`, then attach it so per-doc
    /// Set membership PREDICATE lookups (`set_contains` / `set_contains_any`)
    /// AND the inverted membership / Terms / boolean driver (`element_postings`
    /// / `element_df`) serve the sealed id range from the segment. Dumps
    /// `forward` in dense docid order `[0..n_docs)` (absent docs → `None`,
    /// present-empty → `Some(&[])`) plus the INVERTED postings (folded from
    /// `forward`) via [`crate::persistence::infrastructure::segment::set_writer::write_set_segment`] — a shared sorted
    /// string DICT + a fixed `u32[n_docs + 1]` CSR offsets column + a fixed
    /// packed dict-id column + a parallel per-element [`ROLE_SET_POSTINGS`]
    /// posting column.
    ///
    /// Phase 2h-2: this seam now mirrors PRODUCTION `seal_to_segment` — after
    /// attaching the reader it DROPS BOTH the in-RAM `forward` tail AND the
    /// inverted `elements` index (the RAM win). Queries then drive entirely from
    /// the mmap. A doc indexed AFTER sealing (id >= n_docs) re-populates the live
    /// `elements`/`forward` tail, which `element_postings`/`set_contains` compose
    /// with the segment base. Returns the sealed doc count.
    fn __seal_set_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Set(s) = fi else {
            bail!("field `{field}` is not a Set field");
        };
        // Materialize each sealed doc's members as an ascending Vec<String>
        // (BTreeSet iterates sorted), or None for a doc with no set value. Owned
        // because the writer borrows the slices; keep them alive in `owned`.
        let owned: Vec<Option<Vec<String>>> = (0..n_docs as u32)
            .map(|id| s.forward.get(&id).map(|set| set.iter().cloned().collect()))
            .collect();
        let values: Vec<Option<&[String]>> = owned.iter().map(|o| o.as_deref()).collect();
        // INVERTED postings to seal: fold the live members (== the live `elements`
        // index restricted to the sealed id range) into a fresh BTreeMap.
        let mut elements: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        for (id, v) in values.iter().enumerate() {
            if let Some(members) = v {
                for m in members.iter() {
                    elements.entry(m.clone()).or_default().insert(id as u32);
                }
            }
        }

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::set_writer::write_set_segment(
            &path,
            n_docs as u64,
            &values,
            &elements,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        s.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Drop the RAM index — the whole [0..n_docs) inverted+forward state is on
        // disk now (Phase 2h-2). Queries drive from the mmap segment.
        s.forward = FastHashMap::default();
        s.elements = BTreeMap::new();
        s.dup_values = BTreeSet::new();
        // First-time seal from the live `forward`: no prior tombstone exists, but
        // reset for symmetry with the production re-seal path (Phase 2h-2).
        s.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2e-B): seal a Text field's WHOLE in-RAM inverted
    /// index into a columnar mmap segment under `dir`, then attach it so the
    /// BM25 scan (`eval_match` / `match_doc_score`) and `estimate_selectivity`
    /// read from the sealed base plus any live overlays. Text term-frequency is
    /// NOT rebuildable,
    /// so unlike the Keyword/Set seams the inverted postings ARE stored: a sorted
    /// token DICT + a parallel per-token STORED posting block + a fixed
    /// `u32[n_docs]` DocLen column + the BM25 corpus scalars in the header (see
    /// [`crate::persistence::infrastructure::segment::text_writer::write_text_segment`]).
    ///
    /// This slice seals the whole field for ids `[0..n_docs)`. Phase 2h-4: after
    /// attaching, the bulky sealed-base `tokens` postings AND `distinct` AND
    /// `lens` are DROPPED (no RAM rebuild); later writes stay as live overlays,
    /// and `drop_eid` tombstones a sealed base id. `doc_len()` reads the explicit
    /// overlay before the segment DocLen column. Mirrors PRODUCTION
    /// `seal_to_segment`. Returns the sealed doc count.
    pub(crate) fn __seal_text_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Text { idx, .. } = fi else {
            bail!("field `{field}` is not a Text field");
        };
        // DocLen column in dense docid order `[0..n_docs)`: each id's stored
        // length (0 for an absent doc), reproducing `TextIndex::doc_len`.
        let lens: Vec<u32> = (0..n_docs as u32).map(|id| idx.doc_len(id)).collect();
        let present = idx.present_for_seal(n_docs as u32, &|_| true);
        let tokens = idx.tokens_for_seal(&|_| true);

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::text_writer::write_text_segment(
            &path,
            n_docs as u64,
            &tokens,
            &lens,
            &present,
            idx.doc_count,
            idx.total_doc_len,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);

        idx.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        // Phase 2h-4: mirror PRODUCTION `seal_to_segment` — DROP the bulky `tokens`
        // postings AND `distinct` AND `lens` to disk (no rebuild). `drop_eid`
        // tombstones a sealed base id instead of consuming `distinct`; `doc_len()`
        // reads the segment DocLen column. First-time seal: no prior tombstone, but
        // reset for symmetry with the production re-seal path.
        idx.tokens = BTreeMap::new();
        idx.delta_docs.clear();
        idx.distinct = Vec::new();
        idx.lens = Vec::new();
        idx.clear_match_rank_cache();
        idx.tombstones = RoaringBitmap::new();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2d): seal a Hash field's in-RAM state into a
    /// columnar mmap segment under `dir`, then attach it so the per-doc Hamming
    /// hash read (`hash_at`) serves the sealed id range from the segment. Dumps
    /// `forward` in dense docid order `[0..n_docs)` (absent docs → `None`) via
    /// [`crate::persistence::infrastructure::segment::hash_writer::write_hash_segment`]. Mirrors the Number seam. Returns
    /// the sealed doc count.
    pub(crate) fn __seal_hash_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let n_docs = coll.interner.to_eid.len();
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Hash(h) = fi else {
            bail!("field `{field}` is not a Hash field");
        };
        let values: Vec<Option<u64>> = (0..n_docs as u32)
            .map(|id| h.forward.get(&id).copied())
            .collect();

        let path = dir.join(format!("{field}.lseg"));
        crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
            &path,
            n_docs as u64,
            &values,
        )?;
        let reader = crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
        debug_assert_eq!(reader.n_docs() as usize, n_docs);
        h.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
            std::sync::Arc::new(reader),
        )));
        h.tombstones.clear();
        Ok(n_docs as u32)
    }

    /// TEST SEAM (Stage 2 Phase 2d): seal a Vector field's exact-CPU
    /// (`flat-cpu`) corpus into a columnar mmap vector segment under `dir`,
    /// then attach it so the flat kNN scan reads each vector zero-copy off the
    /// page. Delegates to [`VectorIndex::__seal_flat_to_segment`]; returns the
    /// sealed vector count, or an error if the field is not a `flat-cpu` Vector
    /// (HNSW is out of scope for this slice).
    ///
    /// [`VectorIndex::__seal_flat_to_segment`]: crate::index::domain::vector::VectorIndex::__seal_flat_to_segment
    fn __seal_vector_field_to_segment(
        &self,
        collection_id: &str,
        field: &str,
        dir: &std::path::Path,
    ) -> Result<u32> {
        let mut state = self.state.write().map_err(|_| anyhow!("state poisoned"))?;
        let coll = state
            .collections
            .get_mut(collection_id)
            .ok_or_else(|| anyhow!("unknown collection `{collection_id}`"))?;
        let fi = coll
            .fields
            .get_mut(field)
            .ok_or_else(|| anyhow!("unknown field `{field}`"))?;
        let FieldIndex::Vector { idx, .. } = fi else {
            bail!("field `{field}` is not a Vector field");
        };
        let path = dir.join(format!("{field}.lseg"));
        idx.__seal_flat_to_segment(&path)?
            .ok_or_else(|| anyhow!("field `{field}` is not a flat-cpu vector backend"))
    }
}
