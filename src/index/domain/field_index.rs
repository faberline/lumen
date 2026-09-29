//! One field's index: the enum over the text, keyword, number, set, vector and
//! hash indexes a field can hold, built from the field's spec, with the
//! per-field reads the collection's stats and reindex audit need.

pub(crate) mod delta;
pub(crate) mod open;
pub(crate) mod seal;
pub(crate) mod snapshot;

use anyhow::{anyhow, Result};

use crate::index::domain::hash_index::HashIndex;
use crate::index::domain::keyword_index::KeywordIndex;
use crate::index::domain::number_index::NumberIndex;
use crate::index::domain::set_index::SetIndex;
use crate::index::domain::text_index::TextIndex;
use crate::index::domain::vector::{open_backend, VectorIndex};
use crate::shared_kernel::types::schema::{Analyzer, FieldSpec, FieldType, VectorSpec};
use crate::storage::{FieldAudit, TEXT_UNAUDITABLE};

/// One field's index. Vector fields hold a heap-allocated trait
/// object pointing at the chosen backend.
pub(crate) enum FieldIndex {
    Text {
        analyzer: Analyzer,
        idx: TextIndex,
    },
    Keyword(KeywordIndex),
    Number(NumberIndex),
    Set(SetIndex),
    Vector {
        spec: VectorSpec,
        idx: Box<dyn VectorIndex>,
        /// Approximate bytes the field is currently holding. Tracked
        /// for `stats` parity with the other variants.
        bytes: u64,
    },
    Hash(HashIndex),
}

impl std::fmt::Debug for FieldIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldIndex::Text { analyzer, idx } => f
                .debug_struct("Text")
                .field("analyzer", analyzer)
                .field("idx", idx)
                .finish(),
            FieldIndex::Keyword(k) => f.debug_tuple("Keyword").field(k).finish(),
            FieldIndex::Number(n) => f.debug_tuple("Number").field(n).finish(),
            FieldIndex::Set(s) => f.debug_tuple("Set").field(s).finish(),
            FieldIndex::Vector { spec, bytes, .. } => f
                .debug_struct("Vector")
                .field("spec", spec)
                .field("bytes", bytes)
                .finish(),
            FieldIndex::Hash(h) => f.debug_tuple("Hash").field(h).finish(),
        }
    }
}

impl FieldIndex {
    pub(crate) fn from_spec(spec: &FieldSpec) -> Result<Self> {
        Ok(match spec.field_type {
            FieldType::Text => FieldIndex::Text {
                analyzer: spec.analyzer.unwrap_or(Analyzer::WhitespaceLower),
                idx: TextIndex::default(),
            },
            FieldType::Keyword => FieldIndex::Keyword(KeywordIndex::default()),
            FieldType::Number => FieldIndex::Number(NumberIndex::default()),
            FieldType::Set => FieldIndex::Set(SetIndex::default()),
            FieldType::Vector => {
                let vs = spec
                    .vector_spec()?
                    .ok_or_else(|| anyhow!("vector field is missing its sub-spec"))?;
                FieldIndex::Vector {
                    spec: vs,
                    idx: open_backend(vs),
                    bytes: 0,
                }
            }
            FieldType::Hash => FieldIndex::Hash(HashIndex::default()),
        })
    }

    pub(crate) fn field_type(&self) -> FieldType {
        match self {
            FieldIndex::Text { .. } => FieldType::Text,
            FieldIndex::Keyword(_) => FieldType::Keyword,
            FieldIndex::Number(_) => FieldType::Number,
            FieldIndex::Set(_) => FieldType::Set,
            FieldIndex::Vector { .. } => FieldType::Vector,
            FieldIndex::Hash(_) => FieldType::Hash,
        }
    }

    pub(crate) fn bytes(&self) -> u64 {
        match self {
            FieldIndex::Text { idx, .. } => idx.bytes,
            FieldIndex::Keyword(k) => k.bytes,
            FieldIndex::Number(n) => n.bytes,
            FieldIndex::Set(s) => s.bytes,
            FieldIndex::Vector { bytes, .. } => *bytes,
            FieldIndex::Hash(h) => h.bytes,
        }
    }

    /// How much of this arm the reindex audit can actually see. See
    /// [`FieldAudit`] — and note that an arm answering
    /// [`FieldAudit::Unauditable`] is reported to the caller as unexamined
    /// rather than silently folded into a clean verdict.
    pub(crate) fn audit_kind(&self) -> FieldAudit {
        match self {
            FieldIndex::Text { .. } => FieldAudit::Unauditable(TEXT_UNAUDITABLE),
            FieldIndex::Keyword(_)
            | FieldIndex::Number(_)
            | FieldIndex::Set(_)
            | FieldIndex::Hash(_) => FieldAudit::PerId,
            FieldIndex::Vector { idx, .. } => FieldAudit::WholeIndex(idx.len() > 0),
        }
    }

    /// Does this field hold a value for `id`, read through the same
    /// segment-aware accessors a query uses?
    ///
    /// Only meaningful for the arms [`FieldIndex::audit_kind`] answers
    /// [`FieldAudit::PerId`] for; the others answer `true` so that a caller
    /// that probes them anyway cannot manufacture a damage report out of an
    /// arm this cannot speak for.
    pub(crate) fn holds(&self, id: u32) -> bool {
        match self {
            FieldIndex::Keyword(k) => k.keyword_at(id).is_some(),
            FieldIndex::Number(n) => n.live_number_at(id).is_some(),
            FieldIndex::Set(s) => s.live_set_members(id).is_some(),
            FieldIndex::Hash(h) => h.hash_at(id).is_some(),
            FieldIndex::Text { .. } | FieldIndex::Vector { .. } => true,
        }
    }

    pub(crate) fn unique_terms(&self) -> u64 {
        match self {
            // Phase 2h-4 FIX: a SEALED Text field has an empty in-RAM `tokens`
            // (dropped at seal), so `tokens.len()` would report 0. Count distinct
            // tokens with >=1 LIVE doc from the segment dict (minus tombstones) +
            // the live tail. Segment OFF: `live_unique_tokens` returns the live
            // `tokens.len()` — byte-for-byte the old value, and identical on the
            // default in-RAM (no-segment) path.
            FieldIndex::Text { idx, .. } => idx.live_unique_tokens(),
            // Phase 2h-1 FIX: a SEALED Keyword field has an empty in-RAM `terms`
            // (dropped at seal), so `terms.len()` would report 0. Count distinct
            // terms with >=1 LIVE doc from the segment dict (minus tombstones) +
            // the live tail. Segment OFF: `live_terms` returns the live `terms`
            // clone, so the count equals `terms.len()` — byte-for-byte the old
            // value, and identical on the default in-RAM (no-segment) path.
            FieldIndex::Keyword(k) => k.live_terms().len() as u64,
            // Phase 2h-3 FIX: a SEALED Number field has an empty in-RAM `values`
            // (dropped at seal), so `values.len()` would report 0. Count distinct
            // values with >=1 LIVE doc from the segment sorted-value column (minus
            // tombstones) + the live tail. Segment OFF: `live_values` returns the
            // live `values` clone, so the count equals `values.len()` —
            // byte-for-byte the old value on the default in-RAM (no-segment) path.
            FieldIndex::Number(n) => n.live_values().len() as u64,
            // Phase 2h-2 FIX: a SEALED Set field has an empty in-RAM `elements`
            // (dropped at seal), so `elements.len()` would report 0. Count
            // distinct elements with >=1 LIVE doc from the segment dict (minus
            // tombstones) + the live tail. Segment OFF: `live_elements` returns
            // the live `elements` clone, so the count equals `elements.len()` —
            // byte-for-byte the old value on the default in-RAM (no-segment) path.
            FieldIndex::Set(s) => s.live_elements().len() as u64,
            // For a vector field "unique terms" doesn't really map —
            // surface the count of distinct vectors held instead.
            FieldIndex::Vector { idx, .. } => idx.len() as u64,
            // Distinct hash values held.
            FieldIndex::Hash(h) => h
                .forward
                .values()
                .collect::<std::collections::HashSet<_>>()
                .len() as u64,
        }
    }

    /// Mean tokens per document on `text` fields; `None` on any other
    /// type. Exposes the BM25 length-normalization denominator so
    /// callers can reason about scoring stability.
    pub(crate) fn avg_doc_len(&self) -> Option<f32> {
        match self {
            FieldIndex::Text { idx, .. } if idx.doc_count > 0 => {
                Some(idx.total_doc_len as f32 / idx.doc_count as f32)
            }
            _ => None,
        }
    }

    pub(crate) fn add_field(&self) {
        // Field indexes are created when a field is added; nothing to do
        // beyond construction. Method exists to mirror future "register
        // analyzer / open SST" hooks on the LSM backend.
    }
}
