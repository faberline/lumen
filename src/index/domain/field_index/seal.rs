//! A field index written to its columnar segment: from a frozen checkpoint
//! without touching it, or sealed in place, dropping the payload the segment
//! now holds.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Result};
use roaring::RoaringBitmap;

use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_index::FieldIndex;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::storage::text_projection;

#[cfg_attr(not(test), allow(dead_code))]
impl FieldIndex {
    /// Write a segment from an immutable detached checkpoint field.  Unlike
    /// `seal_to_segment`, this preserves the source maps and postings so the
    /// same frozen checkpoint can be written again after a pre-publication
    /// failure.  The caller opens a separate small mmap-backed prepared index
    /// for publication.
    pub(crate) fn write_segment_borrowed(
        &self,
        field_name: &str,
        dir: &std::path::Path,
        n_docs: u32,
        applied_seq: u64,
        live: &dyn Fn(u32) -> bool,
    ) -> Result<()> {
        let path = dir.join(format!("{field_name}.lseg"));
        match self {
            FieldIndex::Number(n) => {
                let values: Vec<Option<f64>> = (0..n_docs)
                    .map(|id| {
                        live(id)
                            .then(|| n.number_at(id).map(|s| s.to_f64()))
                            .flatten()
                    })
                    .collect();
                crate::persistence::infrastructure::segment::number_writer::write_number_segment(
                    &path,
                    applied_seq,
                    &values,
                )
            }
            FieldIndex::Hash(h) => {
                let values: Vec<Option<u64>> = (0..n_docs)
                    .map(|id| if live(id) { h.hash_at(id) } else { None })
                    .collect();
                crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
                    &path,
                    applied_seq,
                    &values,
                )
            }
            FieldIndex::Keyword(k) => {
                let owned: Vec<Option<String>> = (0..n_docs)
                    .map(|id| if live(id) { k.keyword_at(id) } else { None })
                    .collect();
                let values: Vec<Option<&str>> =
                    owned.iter().map(|value| value.as_deref()).collect();
                let mut terms = BTreeMap::new();
                for (id, value) in values.iter().enumerate() {
                    if let Some(value) = value {
                        terms
                            .entry((*value).to_owned())
                            .or_insert_with(RoaringBitmap::new)
                            .insert(id as u32);
                    }
                }
                crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
                    &path,
                    applied_seq,
                    &values,
                    &terms,
                )
            }
            FieldIndex::Set(s) => {
                let owned: Vec<Option<Vec<String>>> = (0..n_docs)
                    .map(|id| {
                        if live(id) {
                            s.set_members(id)
                                .map(|members| members.into_iter().collect())
                        } else {
                            None
                        }
                    })
                    .collect();
                let values: Vec<Option<&[String]>> =
                    owned.iter().map(|value| value.as_deref()).collect();
                let mut elements = BTreeMap::new();
                for (id, value) in values.iter().enumerate() {
                    if let Some(members) = value {
                        for member in *members {
                            elements
                                .entry(member.clone())
                                .or_insert_with(RoaringBitmap::new)
                                .insert(id as u32);
                        }
                    }
                }
                crate::persistence::infrastructure::segment::set_writer::write_set_segment(
                    &path,
                    applied_seq,
                    &values,
                    &elements,
                )
            }
            FieldIndex::Text { idx, .. } => {
                text_projection::write_live(&path, applied_seq, idx, n_docs, live)
            }
            // Vectors are captured as `FrozenField::Vectors`, which owns their
            // rows and is already written through immutable slices below.
            FieldIndex::Vector { .. } => bail!("vector field captured as a column"),
        }
    }

    /// Seal this field's forward payload into `<field>.lseg` under `dir`, attach
    /// the reader, and DROP the in-RAM forward payload. Keeps the inverted
    /// driver in RAM. `n_docs` is the collection's dense docid count; `applied_seq`
    /// is the WAL position this seal is current as of. Vector fields return their
    /// row→eid mapping (the vector segment stores only f32 rows), which the caller
    /// persists alongside; other field types return `None`. Idempotent fields
    /// (no value for an id) seal as absent rows.
    ///
    /// TOMBSTONE GC (Phase 2g-A): `live(id)` is the authoritative liveness
    /// predicate for THIS field — `true` iff `eid_fields[id]` still contains the
    /// field (i.e. the doc has NOT been deleted from it). The re-seal gather reads
    /// each base id's value through the segment-aware accessor, which still returns
    /// the IMMUTABLE prior-segment value for a deleted base doc; gating on `live`
    /// writes `None` for a non-live id so the new segment's present-bitset excludes
    /// it, and reopen's `record_field_coverage` never re-adds it — the delete is
    /// GC'd instead of resurrected. A first-time seal of an undeleted corpus has
    /// `live(id) == true` for every value-bearing id, so the gather is unchanged.
    pub(crate) fn seal_to_segment(
        &mut self,
        field_name: &str,
        dir: &std::path::Path,
        n_docs: u32,
        applied_seq: u64,
        live: &dyn Fn(u32) -> bool,
    ) -> Result<Option<Vec<String>>> {
        let path = dir.join(format!("{field_name}.lseg"));
        match self {
            FieldIndex::Number(n) => {
                // RE-SEAL-CAPABLE gather (Phase 2f-2): read each base doc's value
                // through the SEGMENT-AWARE accessor (`number_at`), NOT the raw
                // `forward` map. After a prior seal+drop the forward map is empty
                // for base ids; `number_at` re-materializes them from the prior
                // segment, while the live tail (ids >= prior n_docs) comes from
                // the in-RAM `forward`. A first-time seal reads entirely from
                // `forward` (no segment yet) — byte-identical to the old gather.
                let values: Vec<Option<f64>> = (0..n_docs)
                    .map(|id| {
                        if !live(id) {
                            return None; // deleted base doc → GC'd, not re-sealed
                        }
                        n.number_at(id).map(|s| s.to_f64())
                    })
                    .collect();
                crate::persistence::infrastructure::segment::number_writer::write_number_segment(
                    &path,
                    applied_seq,
                    &values,
                )?;
                let reader =
                    crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
                // Attach the NEW reader (dropping any prior segment Arc) and free
                // BOTH the forward tail AND the inverted/range `values` driver —
                // the whole [0..n_docs) index (sorted-value column + per-value
                // postings) is now on disk; reopen/queries drive from the mmap.
                // The RAM win Phase 2h-3 targets: RAM after reopen is O(live tail),
                // not O(distinct numeric values).
                n.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                    std::sync::Arc::new(reader),
                )));
                n.forward = FastHashMap::default();
                n.dense_forward = Vec::new();
                n.values = BTreeMap::new();
                n.dup_values = BTreeSet::new();
                n.clear_keyword_range_cache();
                // The new segment's live(id) gather already EXCLUDED every
                // tombstoned base docid (deleted-since-last-seal), so the deletions
                // are baked in as absent — reset the query-time tombstone to empty
                // (Phase 2h-3, mirroring Keyword 2h-1 / Set 2h-2). The gather above
                // reads `number_at`, which serves the PRIOR segment ignorant of the
                // tombstone, so the `live(id)` predicate is what actually drops the
                // deleted ids; this reset just retires the now-stale set.
                n.tombstones = RoaringBitmap::new();
                Ok(None)
            }
            FieldIndex::Hash(h) => {
                let values: Vec<Option<u64>> = (0..n_docs)
                    .map(|id| if live(id) { h.hash_at(id) } else { None })
                    .collect();
                crate::persistence::infrastructure::segment::hash_writer::write_hash_segment(
                    &path,
                    applied_seq,
                    &values,
                )?;
                let reader =
                    crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
                h.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                    std::sync::Arc::new(reader),
                )));
                h.tombstones.clear();
                h.forward = FastHashMap::default();
                Ok(None)
            }
            FieldIndex::Keyword(k) => {
                // RE-SEAL-CAPABLE gather (Phase 2h-1): read each base doc's value
                // through the SEGMENT-AWARE accessor (`keyword_at`), NOT the raw
                // `forward` map — after a prior seal+drop `forward` is empty for
                // base ids and `keyword_at` re-materializes them from the prior
                // segment. `keyword_at` yields owned Strings (the segment can only
                // own); gather them first, then borrow for the writer.
                let owned: Vec<Option<String>> = (0..n_docs)
                    .map(|id| if live(id) { k.keyword_at(id) } else { None })
                    .collect();
                let values: Vec<Option<&str>> = owned.iter().map(|o| o.as_deref()).collect();
                // INVERTED postings to seal: fold the just-gathered live values
                // into a fresh `terms` index. This is the SEGMENT-AWARE union
                // (prior segment base for deleted-excluded live ids + live tail)
                // already collapsed into `owned`, so deleted docids are dropped
                // and the new segment's postings are exactly the live inverted
                // index. A first-time seal folds the live values verbatim.
                let mut terms: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
                for (id, v) in values.iter().enumerate() {
                    if let Some(s) = v {
                        terms.entry((*s).to_string()).or_default().insert(id as u32);
                    }
                }
                crate::persistence::infrastructure::segment::keyword_writer::write_keyword_segment(
                    &path,
                    applied_seq,
                    &values,
                    &terms,
                )?;
                let reader =
                    crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
                // Attach the NEW reader (dropping any prior segment Arc) and free
                // BOTH the forward tail AND the inverted `terms` driver — the
                // whole [0..n_docs) index is now on disk; reopen/queries drive
                // from the mmap. The RAM win Phase 2h-1 targets.
                k.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                    std::sync::Arc::new(reader),
                )));
                k.forward = FastHashMap::default();
                k.dense_forward = Vec::new();
                k.terms = BTreeMap::new();
                k.dup_values = BTreeSet::new();
                // The new segment's live(id) gather already EXCLUDED every
                // tombstoned base docid (deleted-since-last-seal), so the
                // deletions are now baked in as absent — reset the query-time
                // tombstone to empty (Phase 2h-1 FIX). The gather above reads
                // `keyword_at`, which serves the PRIOR segment ignorant of the
                // tombstone, so the `live(id)` predicate is what actually drops
                // the deleted ids; this reset just retires the now-stale set.
                k.tombstones = RoaringBitmap::new();
                Ok(None)
            }
            FieldIndex::Set(s) => {
                // RE-SEAL-CAPABLE gather (Phase 2h-2): read each base doc's
                // members through the SEGMENT-AWARE accessor (`set_members`), NOT
                // the raw `forward` map — after a prior seal+drop `forward` is
                // empty for base ids and `set_members` re-materializes them from
                // the prior segment. The deleted-excluded live ids + live tail
                // collapse into `owned`, so the new segment is exactly the live state.
                let owned: Vec<Option<Vec<String>>> = (0..n_docs)
                    .map(|id| {
                        if !live(id) {
                            return None; // deleted base doc → GC'd, not re-sealed
                        }
                        s.set_members(id).map(|set| set.into_iter().collect())
                    })
                    .collect();
                let values: Vec<Option<&[String]>> = owned.iter().map(|o| o.as_deref()).collect();
                // INVERTED postings to seal (Phase 2h-2): fold the just-gathered
                // live members into a fresh `elements` index. Deleted docids are
                // already dropped (gather wrote `None`), so the new segment's
                // postings are exactly the live inverted index.
                let mut elements: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
                for (id, v) in values.iter().enumerate() {
                    if let Some(members) = v {
                        for m in members.iter() {
                            elements.entry(m.clone()).or_default().insert(id as u32);
                        }
                    }
                }
                crate::persistence::infrastructure::segment::set_writer::write_set_segment(
                    &path,
                    applied_seq,
                    &values,
                    &elements,
                )?;
                let reader =
                    crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
                // Attach the NEW reader (dropping any prior segment Arc) and free
                // BOTH the forward tail AND the inverted `elements` driver — the
                // whole [0..n_docs) index is now on disk. The RAM win 2h-2 targets.
                s.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                    std::sync::Arc::new(reader),
                )));
                s.forward = FastHashMap::default();
                s.elements = BTreeMap::new();
                s.dup_values = BTreeSet::new();
                // The new segment's live(id) gather already EXCLUDED every
                // tombstoned base docid, so deletions are baked in — reset the
                // query-time tombstone to empty (Phase 2h-2).
                s.tombstones = RoaringBitmap::new();
                Ok(None)
            }
            FieldIndex::Text { idx, .. } => {
                text_projection::write_live(&path, applied_seq, idx, n_docs, live)?;
                let reader =
                    crate::persistence::infrastructure::segment::SegmentReader::open(&path)?;
                // Phase 2h-4: DROP the bulky `tokens` postings AND `distinct` to
                // disk — neither is rebuilt. `drop_eid` no longer consumes
                // `distinct` for a sealed base id (it records the id in
                // `tombstones` instead — the base postings are immutable on disk),
                // so the eager sealed-base `distinct` invert is gone (post-seal
                // RAM stays bounded while explicit live overlays remain). `lens`
                // is dropped too — `doc_len()` reads an overlay first and then the
                // segment DocLen column for a base id. A re-seal CLEARS
                // `tombstones` after baking deletes via `tokens_for_seal`.
                idx.segment = Some(std::sync::Arc::new(ComposedSegmentReader::from_base(
                    std::sync::Arc::new(reader),
                )));
                idx.tokens = BTreeMap::new(); // bulky postings now on disk
                idx.delta_docs.clear();
                idx.staged_rows.clear();
                idx.distinct = Vec::new(); // drop_eid uses tombstones for base ids
                idx.lens = Vec::new(); // doc_len reads the segment DocLen column
                idx.clear_match_rank_cache();
                idx.tombstones = RoaringBitmap::new(); // re-seal baked deletes in
                Ok(None)
            }
            FieldIndex::Vector { idx, bytes, .. } => {
                // Every backend seals its corpus into the SAME columnar
                // vector-segment format and hands back the row→eid mapping;
                // the collection persists that as the `<field>.eids.lseg`
                // sidecar (issue #3951: `open_from_segments` requires it
                // unconditionally for any `Vector` field, whatever backend
                // sealed it, and reconstructs the field with the backend its
                // schema declares — see `FieldIndex::open_from_segment`).
                //
                // Whether the seal also freed RAM is the INDEX's answer, not
                // the schema's: ask `seal_releases_ram` rather than matching
                // on `spec.backend`, so a backend added later reports its own
                // footprint instead of silently inheriting whichever branch
                // this match happened to have. A flat index hands its whole
                // contiguous buffer to the mmap and keeps nothing; HNSW keeps
                // its graph and raw vectors resident, which is exactly what
                // the declared backend is asking for.
                let row_eids = idx.seal_to_segment_prod_at(&path, applied_seq)?;
                if row_eids.is_some() && idx.seal_releases_ram() {
                    *bytes = 0;
                }
                Ok(row_eids)
            }
        }
    }
}
