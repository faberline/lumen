//! Scratch proposal for Lumen #4246: admission-owned change-memory cost.
//!
//! This file is deliberately self-contained.  The controller can move this
//! into `src/` after choosing the admission owner.

pub(crate) mod field_cost;

use crate::ingest::domain::change_memory_cost::field_cost::field_cost;

/// `FastHashMap` / `HashMap` bucket, key/value slots, and control-byte slack.
/// The stored Rust values are smaller, but 64 B stays above the 64-bit current
/// layouts after a table rounds its capacity.
pub const MAP_ENTRY_BYTES: usize = 64;

/// One `BTreeMap<String, u64>` dirty row: node, `String`, revision, and links.
pub const DIRTY_ID_ENTRY_BYTES: usize = 96;

/// `Postings` keeps two `Vec<u32>` streams.  Capacity rounding is charged by
/// `allocation`, so 16 B covers their two values and vector bookkeeping.
pub const POSTING_ROW_BYTES: usize = 16;

const ALLOC_HEADER_BYTES: usize = 16;

const ROARING_MEMBER_BYTES: usize = 32;

const BTREE_MEMBER_BYTES: usize = 64;

const DENSE_DOC_SLOT_BYTES: usize = 8;

/// `BTreeMap<String, Postings>` stores up to eleven `(String, Postings)` keys
/// per node. On 64-bit Rust the payload is `11 * (24 + 48) = 792` bytes, its
/// twelve child pointers add 96 bytes, and headers/alignment leave roughly
/// 920 bytes. A non-root node may hold only five keys, so 192 bytes per term
/// stays above the amortized node cost without assuming a 64-byte map entry.
const TEXT_BTREE_TERM_BYTES: usize = 192;

/// The first ordered dictionary node is allocated before its per-key capacity
/// amortizes. This also leaves explicit slack for root-node layout changes.
const TEXT_BTREE_FIRST_NODE_SLACK_BYTES: usize = 1024;

/// A changed Text document lives in sparse `delta_docs: HashMap<u32,
/// (u32, TokenSet)>`. This covers the map bucket, SmallVec's eight inline
/// Strings, and the first rounded hash allocation after the row grows past it.
/// It replaces a dense slot and never scales with the stable document ID.
const SPARSE_TEXT_DOCUMENT_BUCKET_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorBackendCost {
    Flat,
    Hnsw,
}

/// Input is normalized after schema lookup and text analysis.  It deliberately
/// carries term counts and bytes, not the encoded WAL size and not a cloned
/// postings expansion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldCost {
    Keyword {
        value_bytes: usize,
    },
    Number,
    Set {
        members: usize,
        /// Total UTF-8 bytes over all members, not bytes per member.
        member_bytes: usize,
    },
    Hash,
    Text {
        distinct_terms: usize,
        total_term_bytes: usize,
    },
    Vector {
        dim: usize,
        backend: VectorBackendCost,
        quantized_sq: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Index {
        external_id_bytes: usize,
        new_document: bool,
        field: FieldCost,
        /// `cell_versions` entries for explicit item versions.
        volatile_metadata_bytes: usize,
    },
    Replace {
        external_id_bytes: usize,
        new_document: bool,
        fields: Vec<FieldCost>,
        /// `doc_versions` plus Text/Vector `field_checksums`. These maps are
        /// intentionally in-memory only, so they have no frozen clone charge.
        volatile_metadata_bytes: usize,
    },
    /// A complete unindex of one document. `field_count` is the current
    /// coverage count, obtained from `eid_fields`; it is not caller supplied.
    Unindex {
        external_id_bytes: usize,
        field_count: usize,
    },
    /// `docs:truncate` retains the old collection in the reclaimer.  The owner
    /// must pass its measured live owned bytes, including its interner.
    Truncate { retired_live_bytes: usize },
    /// Create/drop-field and create/drop-collection metadata after validation.
    Schema { metadata_bytes: usize },
    /// Direct engine and raft apply metadata, e.g. request-id or log routing.
    Direct { metadata_bytes: usize },
    /// Reshard accumulator key/value metadata. Payload buffering is charged by
    /// its owning reshard code separately.
    Reshard { metadata_bytes: usize },
}

impl Change {
    pub fn index_new_document(external_id_bytes: usize, field: FieldCost) -> Self {
        Self::Index {
            external_id_bytes,
            new_document: true,
            field,
            volatile_metadata_bytes: 0,
        }
    }
    pub fn index_existing_document(external_id_bytes: usize, field: FieldCost) -> Self {
        Self::Index {
            external_id_bytes,
            new_document: false,
            field,
            volatile_metadata_bytes: 0,
        }
    }
    pub fn unindex(external_id_bytes: usize, field_count: usize) -> Self {
        Self::Unindex {
            external_id_bytes,
            field_count,
        }
    }
    pub fn truncate() -> Self {
        Self::Truncate {
            retired_live_bytes: 0,
        }
    }
    pub fn schema(metadata_bytes: usize) -> Self {
        Self::Schema { metadata_bytes }
    }
    pub fn reshard(metadata_bytes: usize) -> Self {
        Self::Reshard { metadata_bytes }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cost {
    /// New mutable index, maps, and dirty bookkeeping owned after apply.
    pub active: usize,
    /// One detached checkpoint representation.  A first base captures every
    /// current row; an incremental checkpoint captures each dirty row.
    pub frozen: usize,
    /// Capture-time references and dirty-ID table space that coexist until
    /// durable publication; it is separate so admission can expose it.
    pub prepublish: usize,
}

impl Cost {
    pub fn total(self) -> usize {
        self.active
            .saturating_add(self.frozen)
            .saturating_add(self.prepublish)
    }
    fn checked_total(self) -> Result<usize, CostError> {
        add(add(self.active, self.frozen)?, self.prepublish)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostError {
    Overflow,
}

/// Estimate the *additional owned resident memory* caused by one accepted
/// change.  Admission must add this to an owner-maintained reservation for
/// already accepted uncheckpointed rows.  It must seed that reservation from
/// every mutable row that existed before #4246, because first checkpoint base
/// capture clones those rows too.
pub fn estimate_change(change: &Change) -> Result<Cost, CostError> {
    let cost = match change {
        Change::Index {
            external_id_bytes,
            new_document,
            field,
            volatile_metadata_bytes,
        } => add_volatile(
            index_cost(
                *external_id_bytes,
                *new_document,
                std::slice::from_ref(field),
            )?,
            *volatile_metadata_bytes,
        )?,
        Change::Replace {
            external_id_bytes,
            new_document,
            fields,
            volatile_metadata_bytes,
        } => add_volatile(
            index_cost(*external_id_bytes, *new_document, fields)?,
            *volatile_metadata_bytes,
        )?,
        Change::Unindex {
            external_id_bytes,
            field_count,
        } => {
            // Removing a sealed value creates a field dirty record and may make
            // a tombstone bitmap.  It never credits old capacity before the
            // owning collection proves that Rust released it.
            let dirty = mul(*field_count, dirty_id(*external_id_bytes)?)?;
            Cost {
                active: dirty,
                frozen: dirty,
                prepublish: dirty,
            }
        }
        Change::Truncate { retired_live_bytes } => Cost {
            // The active collection is replaced, but `RetiredGeneration` keeps
            // the old collection until the asynchronous reclaimer drains it.
            active: *retired_live_bytes,
            frozen: 0,
            prepublish: MAP_ENTRY_BYTES,
        },
        Change::Schema { metadata_bytes }
        | Change::Direct { metadata_bytes }
        | Change::Reshard { metadata_bytes } => {
            let metadata = allocation(*metadata_bytes)?;
            Cost {
                active: metadata,
                frozen: metadata,
                prepublish: metadata,
            }
        }
    };
    // Reject before a wrapped counter can turn an impossible record into a
    // small reservation. `checked_total` also validates truncate inputs.
    cost.checked_total()?;
    Ok(cost)
}

fn index_cost(
    external_id_bytes: usize,
    new_document: bool,
    fields: &[FieldCost],
) -> Result<Cost, CostError> {
    let mut active = 0;
    let mut frozen = 0;
    let mut prepublish = 0;
    if new_document {
        // Interner `to_eid`, hash lookup, and `eid_fields` coverage entry.
        let id = add(
            add(string_bytes(external_id_bytes)?, MAP_ENTRY_BYTES)?,
            MAP_ENTRY_BYTES,
        )?;
        active = add(active, id)?;
        frozen = add(frozen, id)?; // base capture clones eids + coverage
    }
    for field in fields {
        let (field_active, field_frozen, field_prepublish) = field_cost(external_id_bytes, field)?;
        active = add(active, field_active)?;
        frozen = add(frozen, field_frozen)?;
        prepublish = add(prepublish, field_prepublish)?;
        // `field_dirty` is live state, cloned into `CheckpointCapture`, and
        // remains present until durable publication acknowledges it.
        let dirty = dirty_id(external_id_bytes)?;
        active = add(active, dirty)?;
        frozen = add(frozen, dirty)?;
        prepublish = add(prepublish, dirty)?;
    }
    Ok(Cost {
        active,
        frozen,
        prepublish,
    })
}

fn add_volatile(mut cost: Cost, bytes: usize) -> Result<Cost, CostError> {
    // `cell_versions`, `doc_versions`, and `field_checksums` do not appear in
    // FrozenField::capture. They still coexist with every live change.
    cost.active = add(cost.active, allocation(bytes)?)?;
    Ok(cost)
}

fn dirty_id(external_id_bytes: usize) -> Result<usize, CostError> {
    add(DIRTY_ID_ENTRY_BYTES, string_bytes(external_id_bytes)?)
}

fn string_bytes(bytes: usize) -> Result<usize, CostError> {
    allocation(bytes)
}

/// Upper bound for `count` independently allocated strings with combined UTF-8
/// bytes. Every allocation is below twice `(length + header)`, so this stays
/// linear in bytes and count without needing every term/member value.
fn string_collection_bytes(bytes: usize, count: usize) -> Result<usize, CostError> {
    add(mul(2, bytes)?, mul(2 * ALLOC_HEADER_BYTES, count)?)
}

/// A practical capacity upper bound for allocations made from one change.
/// Rust permits an allocator to return at least the request; rounding a
/// request plus header to the next power of two also covers ordinary Vec/String
/// growth without selecting an arbitrary multi-megabyte constant.
fn allocation(bytes: usize) -> Result<usize, CostError> {
    if bytes == 0 {
        return Ok(0);
    }
    add(bytes, ALLOC_HEADER_BYTES)?
        .checked_next_power_of_two()
        .ok_or(CostError::Overflow)
}

fn add(a: usize, b: usize) -> Result<usize, CostError> {
    a.checked_add(b).ok_or(CostError::Overflow)
}

fn mul(a: usize, b: usize) -> Result<usize, CostError> {
    a.checked_mul(b).ok_or(CostError::Overflow)
}

#[cfg(test)]
mod tests;
