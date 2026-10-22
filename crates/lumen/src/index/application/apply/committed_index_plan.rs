//! Plan the supported borrowed fast-Index fields for one retained command.
//!
//! The plan keeps Keyword, Number, Set, Text, Hash, and Vector in one action ledger.
//! Text bytes are filled after its borrowed rows are staged outside state and
//! apply locks. `committed_index_apply.rs` supplies the read-only `PlanView` adapter while
//! holding its state read lock; attachment remains in the committed adapter.
//!
//! A plan owns identifiers and small metadata only.  Every `ordinal` points
//! back into `FastIndexScanner`, so no field value is decoded or copied.

pub(super) mod pass;

use crate::index::application::engine::index::MAX_INDEX_ITEMS;
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::storage_error::StorageError;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::types::schema::FieldType;
use anyhow::{ensure, Result};
use std::collections::BTreeMap;
use std::time::Instant;

/// State read by the planner.  The storage adapter must not allocate or mutate
/// while implementing this trait.  `revision` comes from the capture barrier,
/// not from a new `Collection` field.
pub(super) trait PlanView {
    fn engine_epoch(&self) -> u64;
    fn collection_generation(&self) -> u64;
    fn schema_version(&self) -> u32;
    fn data_version(&self) -> u64;
    fn revision(&self) -> u64;
    fn interner_len(&self) -> usize;
    fn is_live(&self) -> bool;
    fn field_type(&self, field: &str) -> Option<FieldType>;
    /// Vector fields expose their declared dimension without lending values.
    fn vector_dimension(&self, field: &str) -> Option<u32>;
    fn id(&self, external_id: &str) -> Option<u32>;
    fn has_cell(&self, id: u32, field: &str) -> bool;
    fn cell_version(&self, id: u32, field: &str) -> Option<u64>;
    /// `Some(deadline)` means this request is currently deduplicated.  The
    /// adapter scans only; it treats an expired retained entry as absent.  It
    /// must not call the mutating `gc_requests` from planner or `matches`.
    fn request_deadline(&self, request_id: &str) -> Option<Instant>;
}

/// `revision` is a capture-barrier apply-generation counter.  It increments
/// once when an outer apply lease releases, after all its state changes are
/// visible.  The initial snapshot reads it while holding the state read lock;
/// attachment acquires an apply lease, reads it with the state write lock, and
/// calls `matches` before it publishes.  This covers request-id-only applies
/// without adding a counter update at every collection mutation site.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PlanStamp {
    pub(super) engine_epoch: u64,
    pub(super) collection_generation: u64,
    pub(super) schema_version: u32,
    pub(super) data_version: u64,
    pub(super) revision: u64,
    pub(super) interner_watermark: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) struct PlannedCell {
    pub(super) id: u32,
    pub(super) field: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RequestOutcome {
    None,
    /// Attach must insert this key before it applies items, as index_collection
    /// does today.  The actual insertion uses attach-time `Instant::now()`.
    Register(String),
    /// A duplicate result is valid only until this precise expiry deadline.
    Duplicate {
        request_id: String,
        deadline: Instant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum BusinessError {
    UnknownField {
        field: String,
    },
    TypeMismatch {
        field: String,
        expected: FieldType,
        got: &'static str,
    },
    InvalidNumber(String),
    /// Keep the owned apply path's vector dimension wording without decoding
    /// the retained vector into a `Vec<f32>`.
    InvalidVectorDimension {
        field: String,
        expected: u32,
        got: usize,
    },
    /// The scalar parse failed outside state/apply. Defer the existing error
    /// response formatting until the planner has released its state lock.
    InvalidHash {
        ordinal: usize,
    },
}

impl BusinessError {
    pub(super) fn into_error(
        self,
        collection: &str,
        scanner: &FastIndexScanner<'_>,
    ) -> anyhow::Error {
        (match self {
            Self::UnknownField { field } => StorageError::UnknownField {
                collection: collection.to_owned(),
                field,
            },
            Self::TypeMismatch {
                field,
                expected,
                got,
            } => StorageError::TypeMismatch {
                field,
                expected,
                got,
            },
            Self::InvalidNumber(message) => StorageError::InvalidNumber(message),
            Self::InvalidVectorDimension {
                field,
                expected,
                got,
            } => {
                return anyhow::anyhow!(
                    "vector field `{field}` declared dim={expected} but got vector of length {got}"
                );
            }
            Self::InvalidHash { ordinal } => {
                let item = scanner
                    .items()
                    .nth(ordinal)
                    .expect("validated Hash ordinal");
                let FastIndexValue::String(value) = item.value else {
                    unreachable!("invalid Hash parse has a String source")
                };
                return parse_hash(value).expect_err("same immutable Hash source");
            }
        })
        .into()
    }
}

/// `Drop` is retained even when the next action returns an error.  The attach
/// path journals it before reconstructing `business_error`, matching the live
/// `drop_eid`-then-`apply_value` order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ScalarAction {
    Apply {
        ordinal: usize,
        cell: PlannedCell,
        kind: FieldType,
        bytes: u64,
    },
    Drop {
        cell: PlannedCell,
        ordinal: usize,
        kind: FieldType,
    },
}

#[derive(Debug)]
pub(super) struct ScalarPlan {
    pub(super) stamp: PlanStamp,
    pub(super) request: RequestOutcome,
    /// New IDs are allocated at `stamp.interner_watermark + offset`, in this
    /// exact order.  The vector has at most one entry per source item.
    pub(super) new_external_ids: Vec<String>,
    pub(super) actions: Vec<ScalarAction>,
    /// Final supported-field winner per cell. Scalar winners are passed to
    /// `committed_scalar_files::prepare`; Text and Vector winners select
    /// staged rows. Earlier valid repeated writes remain in `actions` so
    /// metrics and indexed counts retain current behavior.
    pub(super) winners: BTreeMap<PlannedCell, usize>,
    pub(super) bytes_by_field: BTreeMap<String, u64>,
    pub(super) applied: u32,
    pub(super) source_items: usize,
    pub(super) business_error: Option<BusinessError>,
    pub(super) reservation: usize,
}

#[derive(Debug)]
pub(super) enum PlanResult {
    Planned(ScalarPlan),
    /// A schema change exposed a Hash field after the caller's parse snapshot.
    /// The caller retries and parses it outside state and apply locks.
    NeedsHash,
    /// Internal routing signal only.  It must fall back to the owned current
    /// path, never become a new HTTP rejection.
    Unsupported {
        ordinal: usize,
        field: String,
        kind: FieldType,
    },
}

impl ScalarPlan {
    pub(super) fn has_text(&self) -> bool {
        self.actions.iter().any(|action| {
            matches!(
                action,
                ScalarAction::Apply {
                    kind: FieldType::Text,
                    ..
                } | ScalarAction::Drop {
                    kind: FieldType::Text,
                    ..
                }
            )
        })
    }

    pub(super) fn text_actions(&self) -> impl Iterator<Item = (&PlannedCell, usize)> {
        self.actions.iter().filter_map(|action| match action {
            ScalarAction::Apply {
                cell,
                ordinal,
                kind: FieldType::Text,
                ..
            } => Some((cell, *ordinal)),
            _ => None,
        })
    }

    /// Text staging happens after planning, so add its exact indexed bytes to
    /// the original action rather than constructing a second response ledger.
    pub(super) fn set_text_bytes(&mut self, ordinal: usize, bytes: u64) -> Result<()> {
        let action = self
            .actions
            .iter_mut()
            .find(|action| matches!(action, ScalarAction::Apply { ordinal: current, kind: FieldType::Text, .. } if *current == ordinal))
            .ok_or_else(|| anyhow::anyhow!("missing planned Text action"))?;
        let ScalarAction::Apply {
            cell,
            bytes: stored,
            ..
        } = action
        else {
            unreachable!("matched Text action")
        };
        *stored = bytes;
        let total = self.bytes_by_field.entry(cell.field.clone()).or_insert(0);
        *total = total
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("Text indexed bytes overflow"))?;
        Ok(())
    }

    /// This is a read-only recheck.  Attachment calls it again after acquiring
    /// `capture_barrier.apply()` and the state write lock.
    pub(super) fn matches<V: PlanView>(&self, view: &V, now: Instant) -> bool {
        let s = self.stamp;
        if !view.is_live()
            || view.engine_epoch() != s.engine_epoch
            || view.collection_generation() != s.collection_generation
            || view.schema_version() != s.schema_version
            || view.data_version() != s.data_version
            || view.revision() != s.revision
            || view.interner_len() != s.interner_watermark
        {
            return false;
        }
        match &self.request {
            RequestOutcome::None => true,
            RequestOutcome::Register(id) => view.request_deadline(id).is_none(),
            RequestOutcome::Duplicate {
                request_id,
                deadline,
            } => now <= *deadline && view.request_deadline(request_id) == Some(*deadline),
        }
    }
}

/// Reserve before the first `String`, `Vec`, or `BTreeMap` allocation in this
/// module.  Values remain borrowed, so their wire spans are deliberately not
/// part of this budget.  This prices every copied identifier, copied field,
/// request key, action, and bounded map node, even if it later becomes stale.
/// Allocation-free upper bound.  Engine admission calls this before it takes a
/// state lock, then lets `plan` use only a no-wait assertion callback.
pub(super) fn metadata_bound(scanner: &FastIndexScanner<'_>) -> Result<usize> {
    let c = scanner.cost();
    ensure!(
        c.item_count <= MAX_INDEX_ITEMS,
        "committed scalar plan exceeds Index item limit"
    );
    // BTreeMap has no reserve API.  Price five worst-case node populations
    // (IDs, cells, versions, winners, field bytes) at a deliberately larger
    // per-node bound, plus the action Vec and copied string capacities.
    const BTREE_NODE_BOUND: usize = 256;
    const MAP_NODE_POPULATIONS: usize = 5;
    let mut copied = scanner.request_id().map_or(0, str::len);
    for item in scanner.items() {
        copied = copied
            .checked_add(
                item.external_id
                    .len()
                    .checked_mul(2)
                    .ok_or_else(|| anyhow::anyhow!("external id reservation overflow"))?,
            )
            .and_then(|n| n.checked_add(item.field.len().checked_mul(4)?))
            .ok_or_else(|| anyhow::anyhow!("identifier metadata reservation overflow"))?;
    }
    let per_item = std::mem::size_of::<ScalarAction>()
        .checked_add(
            std::mem::size_of::<PlannedCell>()
                .checked_mul(3)
                .ok_or_else(|| anyhow::anyhow!("cell metadata overflow"))?,
        )
        .and_then(|n| n.checked_add(BTREE_NODE_BOUND.checked_mul(MAP_NODE_POPULATIONS)?))
        .and_then(|n| n.checked_add(128))
        .ok_or_else(|| anyhow::anyhow!("planner metadata reservation overflow"))?;
    copied
        .checked_add(
            c.item_count
                .checked_mul(per_item)
                .ok_or_else(|| anyhow::anyhow!("planner item reservation overflow"))?,
        )
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| anyhow::anyhow!("planner reservation overflow"))
}

/// `None` is an invalid numeric value. Missing keys have not been parsed yet.
pub(super) type ParsedHashes = BTreeMap<usize, Option<u64>>;

#[cfg(test)]
mod tests;
