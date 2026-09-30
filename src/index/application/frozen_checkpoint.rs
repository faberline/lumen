//! A frozen checkpoint: the collections' files cut under the capture barrier
//! and written after it, with no reference to the Engine or its state lock, so
//! a failed write retries from the same immutable payload.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};

use crate::index::application::checkpoint_capture::{
    CheckpointCapture, CheckpointDeltas, PreparedCheckpointField,
};
use crate::index::domain::collection::segments::EID_META_FILE;
use crate::index::domain::collection::Collection;
use crate::index::domain::fast_hash::FastHashMap;
use crate::index::domain::field_coverage::FieldCoverage;
use crate::index::domain::field_index::FieldIndex;
use crate::index::domain::hash_index::HashIndex;
use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::set_index::SetIndex;
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::vector::flat_cpu_index::FlatCpuIndex;
use crate::index::infrastructure::checkpoint_fs::{
    checkpoint_write_boundary, collection_dir_name, hard_link_checkpoint_tree, CheckpointLayout,
    CheckpointSchema, CHECKPOINT_SCHEMA_FILE,
};
use crate::shared_kernel::types::schema::{FieldSpec, VectorSpec};

/// Detached file work. It has no reference to Engine or its state lock.
pub(crate) struct FrozenCheckpoint {
    // File payloads must drop before the capture releases their charge.
    pub(super) files: Vec<(String, FrozenCollectionFiles)>,
    pub(super) capture: CheckpointCapture,
}

pub(super) enum FrozenCollectionFiles {
    Linked(std::path::PathBuf),
    EmptyBase {
        schema: BTreeMap<String, FieldSpec>,
        version: u32,
    },
    Base {
        schema: BTreeMap<String, FieldSpec>,
        version: u32,
        eids: Vec<String>,
        coverage: FastHashMap<u32, FieldCoverage>,
        fields: Vec<(String, FrozenField)>,
    },
}

pub(super) enum FrozenField {
    Column(FieldIndex),
    Vectors {
        spec: VectorSpec,
        rows: Vec<(String, Vec<f32>)>,
    },
}

impl FrozenField {
    pub(super) fn capture(index: &FieldIndex, collection: &Collection, name: &str) -> Result<Self> {
        let column = match index {
            FieldIndex::Keyword(index) => FieldIndex::Keyword(KeywordIndex {
                dense_forward: index.dense_forward.clone(),
                forward: index.forward.clone(),
                segment: index.segment.clone(),
                tombstones: index.tombstones.clone(),
                bytes: index.bytes,
                ..Default::default()
            }),
            FieldIndex::Number(index) => FieldIndex::Number(NumberIndex {
                dense_forward: index.dense_forward.clone(),
                forward: index.forward.clone(),
                segment: index.segment.clone(),
                tombstones: index.tombstones.clone(),
                bytes: index.bytes,
                ..Default::default()
            }),
            FieldIndex::Set(index) => FieldIndex::Set(SetIndex {
                forward: index.forward.clone(),
                segment: index.segment.clone(),
                tombstones: index.tombstones.clone(),
                bytes: index.bytes,
                ..Default::default()
            }),
            FieldIndex::Hash(index) => FieldIndex::Hash(HashIndex {
                forward: index.forward.clone(),
                segment: index.segment.clone(),
                tombstones: index.tombstones.clone(),
                bytes: index.bytes,
                ..Default::default()
            }),
            FieldIndex::Text { analyzer, idx } => FieldIndex::Text {
                analyzer: *analyzer,
                idx: TextIndex {
                    staged_rows: idx.staged_rows.clone(),
                    tokens: idx.tokens.clone(),
                    lens: idx.lens.clone(),
                    distinct: idx.distinct.clone(),
                    delta_docs: idx.delta_docs.clone(),
                    segment: idx.segment.clone(),
                    tombstones: idx.tombstones.clone(),
                    doc_count: idx.doc_count,
                    total_doc_len: idx.total_doc_len,
                    bytes: idx.bytes,
                    ..Default::default()
                },
            },
            FieldIndex::Vector { spec, idx, .. } => {
                // A new base needs its live vectors, never a copy or a traversal
                // of the HNSW graph. Subsequent captures use dirty IDs only.
                let mut rows = Vec::new();
                for (id, coverage) in &collection.eid_fields {
                    if coverage.contains(name) {
                        let eid = collection.interner.resolve(*id);
                        let vector = idx
                            .checkpoint_vector(eid)?
                            .ok_or_else(|| anyhow!("covered vector missing during capture"))?;
                        rows.push((eid.to_owned(), vector));
                    }
                }
                rows.sort_by(|left, right| left.0.cmp(&right.0));
                return Ok(Self::Vectors { spec: *spec, rows });
            }
        };
        Ok(Self::Column(column))
    }
}

impl FrozenCheckpoint {
    pub(crate) fn write(&self, root: &std::path::Path, sequence: u64) -> Result<CheckpointCapture> {
        let mut capture = self.capture.for_frozen_write();
        // Only immutable journal handles crossed the capture barrier. Build
        // encoder row references and revision metadata here, outside it.
        for (name, frozen) in &capture.frozen_changes {
            let dirty = capture.field_dirty.entry(name.clone()).or_default();
            let mut fields: CheckpointDeltas = BTreeMap::new();
            for (field, eid, row) in frozen.rows() {
                dirty
                    .entry(field.to_owned())
                    .or_default()
                    .insert(eid.to_owned(), row.revision());
                if capture.reused.contains(name) || capture.initial_sparse.contains(name) {
                    fields
                        .entry(field.to_owned())
                        .or_default()
                        .push((eid.to_owned(), row.value().cloned()));
                }
            }
            if capture.reused.contains(name) || capture.initial_sparse.contains(name) {
                std::sync::Arc::make_mut(&mut capture.field_deltas).insert(name.clone(), fields);
            }
        }
        for (name, files) in &self.files {
            checkpoint_write_boundary();
            let dir = root.join(collection_dir_name(&name));
            // Build the empty codec inputs here, after the capture barrier.
            // No live index or HNSW graph is inspected or replaced.
            let empty = if let FrozenCollectionFiles::EmptyBase { schema, version } = files {
                let collection = Collection::new(schema.clone())?;
                let fields = collection
                    .fields
                    .iter()
                    .map(|(field, index)| {
                        Ok((
                            field.clone(),
                            FrozenField::capture(index, &collection, field)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                Some(FrozenCollectionFiles::Base {
                    schema: schema.clone(),
                    version: *version,
                    eids: Vec::new(),
                    coverage: FastHashMap::default(),
                    fields,
                })
            } else {
                None
            };
            let files = empty.as_ref().unwrap_or(files);
            match files {
                FrozenCollectionFiles::Linked(origin) => hard_link_checkpoint_tree(origin, &dir)?,
                FrozenCollectionFiles::EmptyBase { .. } => {
                    unreachable!("empty codec inputs were prepared above")
                }
                FrozenCollectionFiles::Base {
                    schema,
                    version,
                    eids,
                    coverage,
                    fields,
                } => {
                    std::fs::create_dir_all(dir.join("fields"))?;
                    let sidecar = CheckpointSchema {
                        version: *version,
                        applied_seq: sequence,
                        fields: schema.clone(),
                        segment_layout: CheckpointLayout::Encoded,
                    };
                    std::fs::write(
                        dir.join(CHECKPOINT_SCHEMA_FILE),
                        serde_json::to_vec_pretty(&sidecar)?,
                    )?;
                    let ids: Vec<&str> = eids.iter().map(String::as_str).collect();
                    crate::persistence::infrastructure::segment::eid_writer::write_eid_segment(
                        &dir.join(EID_META_FILE),
                        sequence,
                        &ids,
                    )?;
                    let count = u32::try_from(eids.len())
                        .map_err(|_| anyhow!("base row count exceeds u32"))?;
                    let collection_name = name.clone();
                    for (name, field) in fields {
                        let stem = CheckpointLayout::Encoded.field_stem(&name);
                        match field {
                            FrozenField::Column(index) => {
                                let live = |id| {
                                    coverage
                                        .get(&id)
                                        .is_some_and(|fields| fields.contains(name.as_str()))
                                };
                                index
                                    .write_segment_borrowed(&stem, &dir, count, sequence, &live)?;
                                let prepared = FieldIndex::open_from_segment(
                                    schema
                                        .get(name)
                                        .ok_or_else(|| anyhow!("frozen field missing schema"))?,
                                    &dir,
                                    &stem,
                                    None,
                                    false,
                                )?;
                                capture
                                    .prepared
                                    .entry(collection_name.clone())
                                    .or_default()
                                    .push(PreparedCheckpointField {
                                        name: name.clone(),
                                        index: prepared,
                                        vector_base: None,
                                    });
                            }
                            FrozenField::Vectors { spec, rows } => {
                                let vectors: Vec<_> = rows
                                    .iter()
                                    .map(|(_, value)| Some(value.as_slice()))
                                    .collect();
                                crate::persistence::infrastructure::segment::vector_writer::write_vector_segment(
                                    &dir.join(format!("{stem}.lseg")),
                                    sequence,
                                    spec.dim as usize,
                                    &vectors,
                                )?;
                                let ids: Vec<_> =
                                    rows.iter().map(|(eid, _)| eid.as_str()).collect();
                                crate::persistence::infrastructure::segment::eid_writer::write_eid_segment(
                                    &dir.join(format!("{stem}.eids.lseg")),
                                    sequence,
                                    &ids,
                                )?;
                                if spec.backend
                                    == crate::shared_kernel::types::schema::VectorBackend::FlatCpu
                                {
                                    let reader =
                                        std::sync::Arc::new(crate::persistence::infrastructure::segment::SegmentReader::open(
                                            &dir.join(format!("{stem}.lseg")),
                                        )?);
                                    let row_eids: Vec<String> =
                                        rows.iter().map(|(eid, _)| eid.clone()).collect();
                                    let idx = FlatCpuIndex::open_from_segment(
                                        *spec,
                                        reader.clone(),
                                        row_eids.clone(),
                                    )?;
                                    capture
                                        .prepared
                                        .entry(collection_name.clone())
                                        .or_default()
                                        .push(PreparedCheckpointField {
                                            name: name.clone(),
                                            vector_base: Some((reader, row_eids)),
                                            index: FieldIndex::Vector {
                                                spec: *spec,
                                                idx: Box::new(idx),
                                                bytes: 0,
                                            },
                                        });
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(capture)
    }
}
