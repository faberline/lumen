//! What a checkpoint captures under the capture barrier: each collection's
//! identity, the journaled values, the prepared field, delta and compaction
//! payloads, the scalar cuts and publications, and the live readers the capture
//! pins.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::index::domain::collection::FieldDirtySnapshot;
use crate::index::domain::field_index::FieldIndex;
use crate::persistence::infrastructure::composed_segment::{
    PreparedScalarPublication, PreparedScalarReplacement, ScalarCheckpointCut,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointCollectionIdentity {
    pub generation: u64,
    pub data_version: u64,
    pub schema_version: u32,
}

#[derive(Debug, Clone)]
pub(crate) enum CheckpointValue {
    StagedText(Arc<crate::storage::staged_text_row::StagedTextRow>),
    StagedVector(Arc<crate::storage::staged_vector_row::StagedVectorRow>),
    /// File-backed value kept by the immutable dirty journal. The reader owns
    /// its private directory until every live reader and checkpoint releases it.
    StagedScalar {
        reader: Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        row: u32,
    },
    Keyword(String),
    Number(f64),
    Set(Vec<String>),
    Hash(u64),
    Vector(Vec<f32>),
    Text {
        doc_len: u32,
        tokens: BTreeMap<String, u32>,
    },
}

pub(crate) struct PreparedCheckpointField {
    pub(super) name: String,
    pub(super) index: FieldIndex,
    pub(super) vector_base: Option<(
        std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        Vec<String>,
    )>,
}

/// A validated sparse field payload opened outside the capture barrier.  It is
/// installed only after the generation is durable and published.
pub(crate) struct PreparedCheckpointDelta {
    pub field: String,
    pub reader: std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
    pub external_ids: Vec<String>,
}

pub(crate) struct PreparedCheckpointCompaction {
    pub field: String,
    pub base: Option<std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>>,
    pub inputs: Vec<std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>>,
    pub reader: std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
    pub external_ids: Vec<String>,
    pub scalar: Option<PreparedScalarReplacement>,
}

pub(crate) type CheckpointDeltas = BTreeMap<
    String,
    Vec<(
        String,
        Option<crate::ingest::domain::change_journal::SharedValue<CheckpointValue>>,
    )>,
>;

pub(crate) struct CheckpointCapture {
    pub prepared: BTreeMap<String, Vec<PreparedCheckpointField>>,
    pub prepared_deltas: BTreeMap<String, Vec<PreparedCheckpointDelta>>,
    pub prepared_compactions: BTreeMap<String, Vec<PreparedCheckpointCompaction>>,
    /// Immutable scalar views pinned at the capture barrier. Preparation turns
    /// these into catalog-only replacements before publication can advance.
    pub scalar_cuts: BTreeMap<String, BTreeMap<String, ScalarCheckpointCut>>,
    pub scalar_publications: BTreeMap<String, BTreeMap<String, PreparedScalarPublication>>,
    /// Stable runtime IDs and dirty-match bits computed outside the apply lease.
    pub scalar_retire: BTreeMap<String, BTreeMap<String, Vec<(u32, String, u64)>>>,
    pub live_delta_inputs: BTreeMap<
        String,
        BTreeMap<
            String,
            Vec<std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>>,
        >,
    >,
    pub live_base_inputs: BTreeMap<
        String,
        BTreeMap<
            String,
            std::sync::Arc<crate::persistence::infrastructure::segment::SegmentReader>,
        >,
    >,
    pub collections: BTreeMap<String, CheckpointCollectionIdentity>,
    pub next_generation: u64,
    pub field_dirty: BTreeMap<String, FieldDirtySnapshot>,
    pub frozen_changes:
        BTreeMap<String, crate::ingest::domain::change_journal::FrozenChanges<CheckpointValue>>,
    pub reused: BTreeSet<String>,
    /// A new empty base plus the complete frozen journal forms this first
    /// generation. These collections do not reuse any predecessor catalog.
    pub initial_sparse: BTreeSet<String>,
    /// Sparse rows captured from a reusable base.  Frozen-write retries share
    /// this immutable payload instead of cloning every row and value.
    pub field_deltas: std::sync::Arc<BTreeMap<String, CheckpointDeltas>>,
    // Last: keep metadata charges until every capture-owned allocation drops.
    pub(crate) record_cut:
        Option<std::sync::Arc<crate::index::application::admission::capacity::RecordCut>>,
}

impl CheckpointCapture {
    /// A frozen write only needs immutable cut metadata plus fresh prepared
    /// readers.  Do not clone a prepared index: that would duplicate the
    /// detached payload that must remain available for a retry.
    pub(super) fn for_frozen_write(&self) -> Self {
        Self {
            prepared: BTreeMap::new(),
            prepared_deltas: BTreeMap::new(),
            prepared_compactions: BTreeMap::new(),
            scalar_cuts: self.scalar_cuts.clone(),
            scalar_publications: BTreeMap::new(),
            scalar_retire: BTreeMap::new(),
            live_delta_inputs: self.live_delta_inputs.clone(),
            live_base_inputs: self.live_base_inputs.clone(),
            collections: self.collections.clone(),
            next_generation: self.next_generation,
            field_dirty: self.field_dirty.clone(),
            frozen_changes: self.frozen_changes.clone(),
            reused: self.reused.clone(),
            initial_sparse: self.initial_sparse.clone(),
            field_deltas: self.field_deltas.clone(),
            record_cut: self.record_cut.clone(),
        }
    }
}
