//! A collection sealed to and reopened from its columnar segments: every field
//! and the external-id column written under one directory, and the collection
//! rebuilt from them with no snapshot.

use std::collections::{BTreeMap, VecDeque};
use std::sync::RwLock;
use std::time::Instant;

use anyhow::{anyhow, Result};

use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::interner::Interner;
use crate::persistence::infrastructure::composed_segment::ComposedSegmentReader;
use crate::shared_kernel::types::schema::{FieldSpec, FieldType};
#[cfg(test)]
use crate::storage::CHECKPOINT_COLLECTION_OPENS;
use crate::storage::{CheckpointLayout, RecoveryProfile};

// ---------------------------------------------------------------------------
// Production seal + reopen (Stage 2 Phase 2f-1): the RAM=hot / disk=all keystone
// ---------------------------------------------------------------------------
//
// `Collection::seal_to_segments` promotes the per-field test-seam logic into one
// collection-level call: for every field it writes `<field>.lseg` (reusing the
// `write_*_segment` writers), writes the collection EID column
// (`<collection>.lmeta.lseg`), attaches each `SegmentReader`, then DROPS the
// now-on-disk forward payload from RAM. For the INVERTED/RANGE-driver fields the
// driver index is ALSO dropped — the inverted postings live on disk: Keyword
// `terms` (2h-1, `ROLE_KEYWORD_POSTINGS`), Set `elements` (2h-2,
// `ROLE_SET_POSTINGS`), Number `values` (2h-3, the `ROLE_NUMBER_SORTED`
// sorted-value column + `ROLE_NUMBER_POSTINGS`), Text `tokens` (the
// not-rebuildable tf postings). After the drop, every forward read (predicate,
// value retrieval, drop_eid) routes through the segment-aware accessors
// (`number_at`/`keyword_at`/`set_at`/`hash_at`/`tok_postings`) and every inverted
// read through the unified posting accessors (`value_postings`/`range_postings`/
// `term_postings`/`element_postings`), so no code reads a dropped map for a base
// docid. A per-field query-time `tombstones` bitmap absorbs base deletes between
// seals (the on-disk postings are immutable).
//
// `Collection::open_from_segments` reopens a collection from those segments with
// NO CBOR snapshot and NO whole-collection load: it mmaps the EID column to
// rebuild the `Interner`, then mmaps each field segment. The inverted/range
// drivers are NOT rebuilt in RAM — Keyword `terms`, Set `elements`, and Number
// `values` stay EMPTY and queries drive from the on-disk posting / sorted-value
// columns (Text rebuilds `tokens` only because tf is not reconstructable from a
// forward column; Vector rebuilds its graph from the f32 column). The forward
// PAYLOAD stays on the mmap (demand-paged). RAM after reopen is O(live tail),
// not O(distinct values) — the 2h RAM win.

// The production seal/open are driven by the triple-path test today and by the
// runtime segment-persistence path (`--persistence=segment`); in a non-segment
// build they are reachable only from that runtime path, so this block silences
// dead-code in the default (CBOR) configuration rather than carry
// premature plumbing — mirroring `segment.rs`'s `#![cfg_attr(not(test), …)]`.
// The segment READ paths that serve live queries are fully exercised and are
// NOT covered by this allow.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const EID_META_FILE: &str = "_collection.lmeta.lseg";

#[cfg_attr(not(test), allow(dead_code))]
impl Collection {
    /// PRODUCTION seal (Phase 2f-1): seal EVERY field into a columnar mmap
    /// segment under `dir`, write the collection EID column, attach each reader,
    /// and DROP the now-on-disk forward payload from RAM (the inverted driver
    /// indexes stay). After this the collection is reopenable from `dir` alone
    /// (no CBOR snapshot) and answers every query from the segments + drivers,
    /// reading the forward payload demand-paged off the mmaps. `applied_seq` is
    /// the WAL position the seal is current as of. The per-Vector-field row→eid
    /// mapping is persisted into a small sidecar (`<field>.eids.lseg`) so the
    /// vector index rebuilds on reopen.
    ///
    /// TOMBSTONE GC (Phase 2g-A): each field's per-doc gather is gated on the
    /// AUTHORITATIVE liveness fact `eid_fields[id].contains(field)`. A base doc
    /// deleted post-seal was removed from `eid_fields` (and the inverted driver)
    /// by `delete`/`drop_eid`, but its value still sits on the IMMUTABLE prior
    /// segment; gating the re-seal gather on `eid_fields` writes `None` for that
    /// id so the new segment excludes it and reopen never resurrects it.
    pub fn seal_to_segments(&mut self, dir: &std::path::Path, applied_seq: u64) -> Result<()> {
        self.seal_to_segments_with_layout(dir, applied_seq, CheckpointLayout::Legacy)
    }

    pub(crate) fn seal_to_segments_with_layout(
        &mut self,
        dir: &std::path::Path,
        applied_seq: u64,
        layout: CheckpointLayout,
    ) -> Result<()> {
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow!("create seal dir {}: {e}", dir.display()))?;
        if layout == CheckpointLayout::Encoded {
            std::fs::create_dir_all(dir.join("fields"))?;
        }
        let n_docs = self.interner.to_eid.len() as u32;

        // 1) The collection EID column: external_id of docid i at position i.
        let eids: Vec<&str> = self.interner.to_eid.iter().map(|s| s.as_str()).collect();
        let meta_path = dir.join(EID_META_FILE);
        crate::persistence::infrastructure::segment::eid_writer::write_eid_segment(
            &meta_path,
            applied_seq,
            &eids,
        )?;

        // 2) Seal every field; persist any Vector row→eid sidecar. Borrow
        //    `eid_fields` immutably for the liveness predicate while `fields` is
        //    borrowed mutably — they are disjoint struct fields, so this is sound.
        let eid_fields = &self.eid_fields;
        for (name, fi) in self.fields.iter_mut() {
            let live =
                |id: u32| -> bool { eid_fields.get(&id).is_some_and(|fs| fs.contains(name)) };
            let stem = layout.field_stem(name);
            let row_eids = fi.seal_to_segment(&stem, dir, n_docs, applied_seq, &live)?;
            if let Some(row_eids) = row_eids {
                let sidecar = dir.join(format!("{stem}.eids.lseg"));
                let refs: Vec<&str> = row_eids.iter().map(|s| s.as_str()).collect();
                crate::persistence::infrastructure::segment::eid_writer::write_eid_segment(
                    &sidecar,
                    applied_seq,
                    &refs,
                )?;
            }
        }
        Ok(())
    }

    /// PRODUCTION reopen (Phase 2f-1): reconstruct a collection from the segments
    /// under `dir` with NO CBOR snapshot and NO whole-collection load. Rebuilds
    /// the `Interner` from the EID column, then each field from its `<field>.lseg`
    /// (rebuilding the inverted driver in RAM; the forward payload stays on the
    /// mmap). `schema` is the collection's field specs (carried out-of-band, e.g.
    /// from the catalog), needed to know each field's type before opening its
    /// segment. `eid_fields` (which fields each doc wrote) is reconstructed from
    /// the per-field segment coverage.
    pub fn open_from_segments(
        dir: &std::path::Path,
        schema: BTreeMap<String, FieldSpec>,
        version: u32,
    ) -> Result<Self> {
        Self::open_from_segments_with_vectors(
            dir,
            schema,
            version,
            false,
            CheckpointLayout::Legacy,
            None,
            None,
        )
    }

    pub(crate) fn open_from_segments_with_vectors(
        dir: &std::path::Path,
        schema: BTreeMap<String, FieldSpec>,
        version: u32,
        defer_hnsw: bool,
        layout: CheckpointLayout,
        mapped_rows: Option<&BTreeMap<String, Vec<String>>>,
        recovery_profile: Option<&RecoveryProfile>,
    ) -> Result<Self> {
        #[cfg(test)]
        CHECKPOINT_COLLECTION_OPENS.with(|count| count.set(count.get() + 1));
        // 1) Rebuild the interner from the EID column.
        let meta_path = dir.join(EID_META_FILE);
        let meta = crate::persistence::infrastructure::segment::SegmentReader::open(&meta_path)
            .map_err(|e| anyhow!("open eid meta {}: {e}", meta_path.display()))?;
        let to_eid = meta
            .eids_all()
            .ok_or_else(|| anyhow!("eid meta column torn on reopen"))?;
        let mut interner = Interner::default();
        for eid in &to_eid {
            interner.intern(eid);
        }
        // A compacted field can contain IDs newer than the retained collection
        // metadata file. Intern its validated stable IDs before rebuilding any
        // field's runtime coverage.
        for ids in mapped_rows.into_iter().flat_map(|fields| fields.values()) {
            for eid in ids {
                interner.intern(eid);
            }
        }

        // 2) Rebuild each field from its segment + reconstruct eid_fields from
        //    per-field coverage (a doc "wrote" a field iff that field's segment
        //    has a value for the doc — exactly what the live eid_fields tracked).
        let mut fields: FastHashMap<String, FieldIndex> = FastHashMap::default();
        let mut eid_fields: FastHashMap<u32, FieldCoverage> = FastHashMap::default();
        for (name, spec) in &schema {
            let stem = layout.field_stem(name);
            // Vector fields carry a row→eid sidecar.
            let vec_row_eids = if spec.field_type == FieldType::Vector {
                let sidecar = dir.join(format!("{stem}.eids.lseg"));
                let r = crate::persistence::infrastructure::segment::SegmentReader::open(&sidecar)
                    .map_err(|e| anyhow!("open vector eid sidecar {}: {e}", sidecar.display()))?;
                Some(
                    r.eids_all()
                        .ok_or_else(|| anyhow!("vector eid sidecar `{name}` torn on reopen"))?,
                )
            } else {
                None
            };
            let vector_started = recovery_profile
                .filter(|profile| profile.enabled())
                .and_then(|_| (spec.field_type == FieldType::Vector).then(Instant::now));
            let mut fi = FieldIndex::open_from_segment(spec, dir, &stem, vec_row_eids, defer_hnsw)?;
            if let (Some(profile), Some(started)) = (recovery_profile, vector_started) {
                profile.vector_opened(
                    spec.vector_spec()?
                        .expect("vector field has a spec")
                        .backend,
                    started.elapsed(),
                );
            }
            if let Some(rows) = mapped_rows.and_then(|fields| fields.get(name)) {
                if spec.field_type != FieldType::Vector {
                    let ids = rows
                        .iter()
                        .map(|eid| interner.id(eid).expect("mapped base ID was interned"))
                        .collect();
                    map_loaded_base_rows(&mut fi, ids)?;
                }
            }
            let coverage_started = recovery_profile
                .filter(|profile| profile.enabled())
                .map(|_| Instant::now());
            record_field_coverage(&fi, name, &interner, &mut eid_fields);
            if let (Some(profile), Some(started)) = (recovery_profile, coverage_started) {
                profile.coverage_rebuilt(started.elapsed());
            }
            fields.insert(name.clone(), fi);
        }

        Ok(Self {
            collection_generation: 0,
            data_version: 1,
            checkpoint_origin: None,
            checkpoint_lineage: None,
            checkpoint_lineage_schema: None,
            field_dirty: BTreeMap::new(),
            next_field_dirty_revision: 0,
            change_journal: crate::ingest::domain::change_journal::ChangeJournal::new(),
            requires_full_checkpoint: false,
            journal_complete_since_empty: false,
            version,
            schema,
            fields,
            interner,
            eid_fields,
            seen_requests: VecDeque::new(),
            deleted_at: None,
            last_indexed_at: None,
            search_cache: RwLock::new(FastHashMap::default()),
            cell_versions: FastHashMap::default(),
            doc_versions: FastHashMap::default(),
            field_checksums: FastHashMap::default(),
        })
    }
}

fn map_loaded_base_rows(index: &mut FieldIndex, ids: Vec<u32>) -> Result<()> {
    let segment = match index {
        FieldIndex::Keyword(index) => &mut index.segment,
        FieldIndex::Number(index) => &mut index.segment,
        FieldIndex::Set(index) => &mut index.segment,
        FieldIndex::Hash(index) => &mut index.segment,
        FieldIndex::Text { idx, .. } => &mut idx.segment,
        FieldIndex::Vector { .. } => return Ok(()),
    };
    let reader = segment
        .as_ref()
        .ok_or_else(|| anyhow!("mapped field has no base segment"))?
        .immutable_base_reader();
    *segment = Some(std::sync::Arc::new(
        ComposedSegmentReader::from_mapped_base(reader, ids)?,
    ));
    Ok(())
}

/// Reconstruct, into `eid_fields`, the set of fields each doc-id wrote, from a
/// single reopened field's segment coverage. A doc wrote `name` iff the field's
/// segment holds a value for that doc — the same fact the live `eid_fields`
/// tracked at index time. Phase 2f-1 reopen helper.
#[cfg_attr(not(test), allow(dead_code))]
fn record_field_coverage(
    fi: &FieldIndex,
    name: &str,
    interner: &Interner,
    eid_fields: &mut FastHashMap<u32, FieldCoverage>,
) {
    match fi {
        FieldIndex::Number(n) => {
            if let Some(seg) = &n.segment {
                for id in 0..seg.n_docs() {
                    if n.number_at(id).is_some() {
                        eid_fields.entry(id).or_default().insert(name.to_string());
                    }
                }
            }
        }
        FieldIndex::Hash(h) => {
            if let Some(seg) = &h.segment {
                for id in 0..seg.n_docs() {
                    if h.hash_at(id).is_some() {
                        eid_fields.entry(id).or_default().insert(name.to_string());
                    }
                }
            }
        }
        FieldIndex::Keyword(k) => {
            if let Some(seg) = &k.segment {
                for id in 0..seg.n_docs() {
                    if k.keyword_at(id).is_some() {
                        eid_fields.entry(id).or_default().insert(name.to_string());
                    }
                }
            }
        }
        FieldIndex::Set(s) => {
            if let Some(seg) = &s.segment {
                for id in 0..seg.n_docs() {
                    if seg.set_at(id).is_some() {
                        eid_fields.entry(id).or_default().insert(name.to_string());
                    }
                }
            }
        }
        FieldIndex::Text { idx, .. } => {
            // A doc "wrote" a text field iff its explicit presence bit is set.
            // This includes an explicit empty value. After Phase 2h-4 reopen
            // `distinct` is EMPTY (no RAM rebuild), so drive base coverage from
            // the segment presence column without materializing postings.
            // This is a transient O(n_docs) read of the demand-paged presence column —
            // it does NOT re-materialize `tokens`/`distinct` in RAM. Any live-tail
            // doc indexed after reopen is folded in from `distinct_ids()` (empty
            // until then). Without a segment (live, pre-seal) this falls back to the
            // in-RAM `distinct` ids — byte-identical to the old behavior.
            if let Some(seg) = &idx.segment {
                for id in 0..seg.n_docs() {
                    if seg.text_is_present(id) {
                        eid_fields.entry(id).or_default().insert(name.to_string());
                    }
                }
            }
            for id in idx.distinct_ids().chain(idx.staged_rows.keys().copied()) {
                eid_fields.entry(id).or_default().insert(name.to_string());
            }
        }
        FieldIndex::Vector { idx, .. } => {
            // Every stored vector row's eid wrote this field; resolve each row
            // eid back to its docid through the rebuilt interner.
            for (eid, _) in idx.dump_for_snapshot().into_iter().flat_map(|(v, _)| v) {
                if let Some(id) = interner.id(&eid) {
                    eid_fields.entry(id).or_default().insert(name.to_string());
                }
            }
        }
    }
}
