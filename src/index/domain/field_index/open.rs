//! A field index reopened from its segment with no snapshot: the forward
//! payload stays on the mmap, and only the driver a query needs is rebuilt in
//! RAM.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, RwLock};

use anyhow::{anyhow, Result};
use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::HashIndex;
use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::set_index::SetIndex;
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::domain::vector::hnsw_cpu_index::HnswCpuIndex;
use crate::index::domain::vector::VectorIndex;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::shared_kernel::types::schema::{Analyzer, FieldSpec, FieldType};

#[cfg_attr(not(test), allow(dead_code))]
impl FieldIndex {
    /// Reopen this field from its `<field>.lseg` segment with NO snapshot: mmap
    /// the forward column, attach the reader, and REBUILD the inverted driver in
    /// RAM by scanning the forward column. The forward payload stays on the mmap
    /// (demand-paged); only the driver is in RAM. `spec` supplies the field type
    /// (and the Vector sub-spec); `vec_row_eids` is the persisted vector row→eid
    /// mapping (only consulted for a Vector field).
    pub(in crate::index) fn open_from_segment(
        spec: &FieldSpec,
        dir: &std::path::Path,
        field_name: &str,
        vec_row_eids: Option<Vec<String>>,
        defer_hnsw: bool,
    ) -> Result<FieldIndex> {
        let path = dir.join(format!("{field_name}.lseg"));
        let reader = std::sync::Arc::new(
            crate::persistence::infrastructure::segment::SegmentReader::open(&path)?,
        );
        // The per-arm reopen reads coverage straight off the reader (e.g. the Text
        // arm via `text_doc_count`/`text_total_doc_len`); no whole-`n_docs` rebuild.
        let _n_docs = reader.n_docs();
        match spec.field_type {
            FieldType::Number => {
                // Phase 2h-3: the inverted/range `values` index is ON DISK (the
                // ROLE_NUMBER_SORTED sorted-value column + ROLE_NUMBER_POSTINGS).
                // Reopen does NOT rebuild it in RAM — `values` stays EMPTY and
                // range / exact / boolean queries drive from the mmap segment via
                // `range_postings` / `value_postings` (binary-search on the sorted
                // column, SELECTIVE — NOT the old O(n_docs) forward scan that
                // rebuilt `values`). RAM after reopen is O(live tail), not
                // O(distinct numeric values). The forward column stays demand-paged
                // on the mmap for per-doc predicate reads (`number_at`).
                Ok(FieldIndex::Number(NumberIndex {
                    values: BTreeMap::new(),
                    dup_values: BTreeSet::new(),
                    forward: FastHashMap::default(), // payload stays on the mmap
                    dense_forward: Vec::new(),
                    keyword_range_cache: RwLock::new(FastHashMap::default()),
                    keyword_range_bitmap_cache: RwLock::new(FastHashMap::default()),
                    range_stats: RwLock::new(None),
                    bytes: 0,
                    segment: Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                        reader,
                    ))),
                    // Reopen starts with NO pending deletes — the on-disk segment
                    // already reflects every deletion baked in at its seal.
                    tombstones: RoaringBitmap::new(),
                }))
            }
            FieldType::Hash => Ok(FieldIndex::Hash(HashIndex {
                tombstones: RoaringBitmap::new(),
                forward: FastHashMap::default(),
                bytes: 0,
                segment: Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                    reader,
                ))),
            })),
            FieldType::Keyword => {
                // Phase 2h-1: the inverted `terms` index is ON DISK (the
                // ROLE_KEYWORD_POSTINGS column). Reopen does NOT rebuild it in
                // RAM — `terms` stays EMPTY and Term/Terms/boolean queries drive
                // from the mmap segment via `term_postings`. RAM after reopen is
                // O(live tail), not O(distinct keyword values). The old fold-the-
                // forward-column rebuild loop is gone; the forward dict-id column
                // stays demand-paged on the mmap for per-doc predicate reads.
                Ok(FieldIndex::Keyword(KeywordIndex {
                    terms: BTreeMap::new(),
                    dup_values: BTreeSet::new(),
                    dense_forward: Vec::new(),
                    forward: FastHashMap::default(),
                    bytes: 0,
                    segment: Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                        reader,
                    ))),
                    // Reopen starts with NO pending deletes — the on-disk segment
                    // already reflects every deletion baked in at its seal.
                    tombstones: RoaringBitmap::new(),
                }))
            }
            FieldType::Set => {
                // Phase 2h-2: the inverted `elements` index is ON DISK (the
                // ROLE_SET_POSTINGS column). Reopen does NOT rebuild it in RAM —
                // `elements` stays EMPTY and membership / Terms / boolean queries
                // drive from the mmap segment via `element_postings`. RAM after
                // reopen is O(live tail), not O(distinct set elements). The old
                // fold-the-forward-column rebuild loop is gone; the CSR forward
                // columns stay demand-paged on the mmap for per-doc predicate reads.
                Ok(FieldIndex::Set(SetIndex {
                    elements: BTreeMap::new(),
                    dup_values: BTreeSet::new(),
                    forward: FastHashMap::default(),
                    bytes: 0,
                    segment: Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                        reader,
                    ))),
                    // Reopen starts with NO pending deletes — the on-disk segment
                    // already reflects every deletion baked in at its seal.
                    tombstones: RoaringBitmap::new(),
                }))
            }
            FieldType::Text => {
                // Phase 2h-4: the inverted `tokens` postings (text tf is STORED on
                // disk — the ROLE_TEXT_POSTINGS blocks) and the corpus-deriving
                // `distinct` map stay EMPTY on reopen — NO RAM rebuild. The old loop
                // decoded EVERY posting to re-materialize the sealed base, making
                // RAM O(total tokens); it is deleted. The BM25 scan / term lookup
                // drive from the mmap via `tok_postings` (decodes a single token's
                // base posting on demand) plus live overlays and `doc_len`, so RAM
                // after reopen is O(live tail), not O(corpus).
                //
                // `lens` stays EMPTY too: `doc_len()` prefers the segment DocLen
                // column for a sealed id, and any post-reopen tail doc extends `lens`
                // through `set_doc_len`. The LIVE corpus scalars are INITIALIZED from
                // the seal-time header (`text_doc_count`/`text_total_doc_len`); from
                // here the index path increments and `drop_eid` decrements them, so
                // `bm25_corpus` (which now reads them, NOT the header) stays current
                // across deletes and the post-seal tail. Tombstones start empty — the
                // segment already reflects every deletion baked in at its seal.
                let doc_count = reader.text_doc_count();
                let total_doc_len = reader.text_total_doc_len();
                let analyzer = spec.analyzer.unwrap_or(Analyzer::WhitespaceLower);
                Ok(FieldIndex::Text {
                    analyzer,
                    idx: TextIndex {
                        staged_rows: BTreeMap::new(),
                        live_term_cache: Mutex::new(None),
                        tokens: BTreeMap::new(),
                        lens: Vec::new(),
                        distinct: Vec::new(),
                        delta_docs: FastHashMap::default(),
                        doc_count,
                        total_doc_len,
                        bytes: 0,
                        segment: Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                            reader,
                        ))),
                        match_rank_cache: RwLock::new(FastHashMap::default()),
                        tombstones: RoaringBitmap::new(),
                    },
                })
            }
            FieldType::Vector => {
                let vs = spec.vector_spec()?.ok_or_else(|| {
                    anyhow!("vector field `{field_name}` is missing its sub-spec")
                })?;
                let row_eids = vec_row_eids.ok_or_else(|| {
                    anyhow!("vector field `{field_name}` reopen needs its row→eid mapping")
                })?;
                // Reopen with the backend the SCHEMA declares, not with
                // whichever one is cheapest to rebuild. Every backend seals
                // into the same columnar segment, so the segment does not
                // record which one wrote it — `vs.backend` is the only record,
                // and honouring it is what keeps a restarted node answering
                // kNN the same way its unrestarted peers do.
                let (idx, bytes): (Box<dyn VectorIndex>, u64) = match vs.backend {
                    crate::shared_kernel::types::schema::VectorBackend::HnswCpu => {
                        // The graph needs its vectors in RAM, so the field's
                        // footprint comes back too. Account it exactly as the
                        // live index path does (`dim * 4 + eid.len()` per row),
                        // so a reopened field reports the same figure the
                        // pre-checkpoint field reported.
                        let bytes = row_eids
                            .iter()
                            .map(|eid| (vs.dim as u64) * 4 + eid.len() as u64)
                            .sum();
                        let idx: Box<dyn VectorIndex> = if defer_hnsw {
                            let mut staging_spec = vs;
                            staging_spec.quantize = None;
                            Box::new(FlatCpuIndex::open_from_segment(
                                staging_spec,
                                reader,
                                row_eids,
                            )?)
                        } else {
                            Box::new(HnswCpuIndex::open_from_segment(vs, reader, row_eids)?)
                        };
                        (idx, bytes)
                    }
                    crate::shared_kernel::types::schema::VectorBackend::FlatCpu => {
                        // The rows stay on the mmap and are read demand-paged,
                        // so nothing is resident to report.
                        let idx = FlatCpuIndex::open_from_segment(vs, reader, row_eids)?;
                        (Box::new(idx), 0)
                    }
                };
                Ok(FieldIndex::Vector {
                    spec: vs,
                    idx,
                    bytes,
                })
            }
        }
    }
}
