//! Plan one committed `docs:replace` command without retaining its values.
//!
//! The caller writes a private fast-Index wire image whose flattened fields
//! match `ReplaceDocDescriptor` ranges.  This module only owns bounded
//! identifiers, action metadata, and the final replacement metadata ledger.
//! It never decodes a `String`, vector, or set into an owned request value.

pub(super) mod pass;
mod source_value;

use super::committed_index_plan::{PlanView, PlannedCell, ScalarAction, ScalarPlan};
use crate::index::domain::hash_index::parse_hash;
use crate::index::domain::storage_error::StorageError;
use crate::ingest::infrastructure::wal::fast_index_scanner::{FastIndexScanner, FastIndexValue};
use crate::shared_kernel::types::{document::MAX_BATCH_REPLACE_SIZE, schema::FieldType};
use anyhow::{bail, Result};
use std::collections::BTreeMap;

/// One document in request order. `fields_start..fields_end` names a sorted,
/// unique range in the flattened source scanner. Empty documents use an empty
/// range. The adapter must prove the range fields belong to `external_id`.
pub(super) use crate::ingest::infrastructure::wal::borrowed_replace_spool::BorrowedReplaceSpoolDoc as ReplaceDocDescriptor;

/// The read-only replacement facts. All methods must remain allocation-free.
/// `existing_unchanged` compares a source ordinal with an existing live cell.
/// `old_fields` is sorted by the adapter before it reaches this planner.
pub(super) trait ReplacePlanView: PlanView {
    fn doc_version(&self, id: u32) -> Option<u64>;
    fn old_fields(&self, id: u32) -> Option<&[String]>;
    /// Exact allocation-free price for cloning this coverage snapshot. The
    /// planner asks before it allocates any ID, action, or descriptor map.
    fn old_fields_bound(&self, id: u32) -> OldFieldsBound;
    fn existing_unchanged(
        &self,
        id: u32,
        field: &str,
        kind: FieldType,
        ordinal: usize,
        checksum: Option<u64>,
    ) -> bool;
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct OldFieldsBound {
    pub(super) count: usize,
    pub(super) copied_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReplaceItemOutcome {
    Ok {
        fields_written: u32,
        fields_skipped: u32,
    },
    Dropped {
        current_version: u64,
    },
    Error {
        error: ReplaceItemError,
    },
}

/// Retain only bounded error metadata. Hash parsing is rendered from its
/// source ordinal after the state read lock has been released.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ReplaceItemError {
    UnknownField {
        field: String,
    },
    TypeMismatch {
        field: String,
        expected: FieldType,
        got: &'static str,
    },
    InvalidNumber(String),
    InvalidVectorDimension {
        field: String,
        expected: u32,
        got: usize,
    },
    InvalidHash {
        ordinal: usize,
    },
}

impl ReplaceItemError {
    pub(super) fn render(
        self,
        collection: &str,
        scanner: &FastIndexScanner<'_>,
    ) -> RenderedReplaceItemOutcome {
        let error: anyhow::Error = match self {
            Self::UnknownField { field } => StorageError::UnknownField {
                collection: collection.to_owned(),
                field,
            }
            .into(),
            Self::TypeMismatch {
                field,
                expected,
                got,
            } => StorageError::TypeMismatch {
                field,
                expected,
                got,
            }
            .into(),
            Self::InvalidNumber(message) => StorageError::InvalidNumber(message).into(),
            Self::InvalidVectorDimension {
                field,
                expected,
                got,
            } => anyhow::anyhow!(
                "vector field `{field}` declared dim={expected} but got vector of length {got}"
            ),
            Self::InvalidHash { ordinal } => {
                let item = scanner
                    .items()
                    .nth(ordinal)
                    .expect("validated replacement hash ordinal");
                let FastIndexValue::String(value) = item.value else {
                    unreachable!("validated replacement Hash source")
                };
                parse_hash(value).expect_err("same immutable replacement Hash source")
            }
        };
        let code = if matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::UnknownField { .. })
        ) {
            "unknown_field"
        } else {
            "type_mismatch"
        };
        RenderedReplaceItemOutcome::Error {
            code,
            message: error.to_string(),
        }
    }
}

/// Convert `ReplaceItemOutcome::Error` through `ReplaceItemError::render`
/// only after planning releases its state-read adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum RenderedReplaceItemOutcome {
    Ok {
        fields_written: u32,
        fields_skipped: u32,
    },
    Dropped {
        current_version: u64,
    },
    Error {
        code: &'static str,
        message: String,
    },
}

/// Final document metadata. The common adapter applies actions in source
/// order, then installs this ledger while it holds the same apply lease.
#[derive(Clone, Debug)]
pub(super) struct ReplaceDocFinal {
    pub(super) id: u32,
    pub(super) fields: Vec<String>,
    pub(super) version: Option<u64>,
}

/// Text/Vector checksums are replacement-path state. `None` removes a stale
/// checksum after an omitted field or an empty replacement.
#[derive(Clone, Debug)]
pub(super) struct ReplaceChecksumFinal {
    pub(super) cell: PlannedCell,
    pub(super) checksum: Option<u64>,
}

#[derive(Debug)]
pub(super) struct ReplacePlan {
    pub(super) scalar: ScalarPlan,
    pub(super) outcomes: Vec<ReplaceItemOutcome>,
    pub(super) final_docs: Vec<ReplaceDocFinal>,
    pub(super) final_checksums: Vec<ReplaceChecksumFinal>,
    pub(super) fields_written: u64,
    pub(super) fields_skipped: u64,
    pub(super) reservation: usize,
}

/// Replacement has a document limit, not the generic Index field-item limit.
/// This bound covers all copied names and every metadata map/vector before
/// this module allocates. Source value bytes remain in the scanner wire image.
pub(super) fn metadata_bound(
    scanner: &FastIndexScanner<'_>,
    docs: &[ReplaceDocDescriptor<'_>],
    view: &impl ReplacePlanView,
) -> Result<usize> {
    if docs.len() > MAX_BATCH_REPLACE_SIZE {
        return Err(StorageError::BulkLimit {
            got: docs.len(),
            max: MAX_BATCH_REPLACE_SIZE,
        }
        .into());
    }
    let fields = scanner.cost().item_count;
    validate_descriptors(scanner, docs)?;
    let mut copied = 0usize;
    let mut old = OldFieldsBound::default();
    for doc in docs {
        copied = copied
            .checked_add(
                doc.external_id
                    .len()
                    .checked_mul(2)
                    .ok_or_else(|| anyhow::anyhow!("replace id bound overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("replace metadata overflow"))?;
        let id = view.id(doc.external_id);
        if let Some(id) = id {
            let bound = view.old_fields_bound(id);
            old.count = old
                .count
                .checked_add(bound.count)
                .ok_or_else(|| anyhow::anyhow!("replace old coverage count overflow"))?;
            old.copied_bytes = old
                .copied_bytes
                .checked_add(bound.copied_bytes)
                .ok_or_else(|| anyhow::anyhow!("replace old coverage bytes overflow"))?;
        }
    }
    for item in scanner.items() {
        copied = copied
            .checked_add(
                item.field
                    .len()
                    .checked_mul(6)
                    .ok_or_else(|| anyhow::anyhow!("replace field bound overflow"))?,
            )
            .ok_or_else(|| anyhow::anyhow!("replace metadata overflow"))?;
    }
    // Source fields can coexist in item/validated/supplied/action/winner/
    // checksum/final-doc ledgers. Old coverage can coexist with its omitted
    // action/checksum rows and an adapter snapshot, so price it separately.
    const NODE: usize = 256;
    let per_field = std::mem::size_of::<ScalarAction>()
        .checked_add(std::mem::size_of::<PlannedCell>() * 5)
        .and_then(|n| n.checked_add(NODE * 16))
        .and_then(|n| n.checked_add(128))
        .ok_or_else(|| anyhow::anyhow!("replace field metadata overflow"))?;
    copied
        .checked_add(
            old.copied_bytes
                .checked_mul(12)
                .ok_or_else(|| anyhow::anyhow!("replace old copy overflow"))?,
        )
        .and_then(|n| n.checked_add(fields.checked_mul(per_field)?))
        .and_then(|n| {
            n.checked_add(old.count.checked_mul(
                std::mem::size_of::<ScalarAction>()
                    + std::mem::size_of::<PlannedCell>() * 5
                    + NODE * 14,
            )?)
        })
        .and_then(|n| n.checked_add(docs.len().checked_mul(NODE * 6 + 256)?))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| anyhow::anyhow!("replace metadata overflow"))
}

/// The private spool may be malformed even though its original CBOR scan was
/// valid. Refuse overlapping, gapped, reordered, or non-canonical descriptor
/// rows before admission and before stale items can skip this proof.
fn validate_descriptors(
    scanner: &FastIndexScanner<'_>,
    docs: &[ReplaceDocDescriptor<'_>],
) -> Result<()> {
    let fields = scanner.cost().item_count;
    let mut expected = 0usize;
    for doc in docs {
        if doc.fields_start != expected
            || doc.fields_start > doc.fields_end
            || doc.fields_end > fields
        {
            bail!("invalid non-canonical replacement descriptor range");
        }
        let mut prior = None;
        for (_, item) in scanner
            .items()
            .enumerate()
            .skip(doc.fields_start)
            .take(doc.fields_end - doc.fields_start)
        {
            if item.external_id != doc.external_id {
                bail!("replacement descriptor source id mismatch");
            }
            if prior.is_some_and(|old: &str| old >= item.field) {
                bail!("replacement descriptor fields are not sorted and unique");
            }
            prior = Some(item.field);
        }
        expected = doc.fields_end;
    }
    if expected != fields {
        bail!("replacement descriptor does not cover source fields");
    }
    Ok(())
}

/// Precomputed Hash parses and Text/Vector checksums by flattened ordinal.
/// Missing Hash parse means an item error; missing Text/Vector checksum is a
/// programmer error in the root prepass because equality must not hash under
/// a state/apply lease.
pub(super) type ParsedValues = BTreeMap<usize, ParsedValue>;

#[derive(Clone, Copy, Debug)]
pub(super) struct ParsedValue {
    pub(super) hash: Option<u64>,
    pub(super) checksum: Option<u64>,
}

#[cfg(test)]
mod tests;
