//! `crate::storage` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.
//!
//! In-memory storage and query execution.
//!
//! The engine is `BTreeMap`-backed inverted indexes per field,
//! constructed by [`Engine::new`]. Single-pod, single-shard; durability
//! comes from the CBOR RDB snapshot path and (when segment persistence is
//! selected) the columnar mmap segment tier — not from this module.
//!
//! The shape below maps 1:1 to the field-type table in the README:
//!
//! | FieldType | Index                                  |
//! |-----------|----------------------------------------|
//! | `text`    | `BTreeMap<token, BTreeSet<eid>>`       |
//! | `keyword` | `BTreeMap<value, BTreeSet<eid>>`       |
//! | `number`  | `BTreeMap<SortableF64, BTreeSet<eid>>` |
//! | `set`     | `BTreeMap<element, BTreeSet<eid>>`     |
//!
//! Every field also carries a per-`external_id` "forward" map so
//! re-indexing the same `(eid, field)` cleanly evicts the old postings
//! before appending the new ones.

pub use crate::index::application::engine::collections::DropOutcome;
pub use crate::index::application::engine::index::MAX_INDEX_ITEMS;
pub use crate::index::application::engine::raft_dispatch::ApplyOutcome;
pub use crate::index::application::engine::reshard_apply::{
    ReshardApplyOutcome, ReshardEvictOutcome,
};
pub use crate::index::application::engine::reshard_prune::ReshardPruneOutcome;
pub use crate::index::application::engine::Engine;
pub use crate::index::domain::collection::coverage::{FieldNotAudited, ReindexNeeded};
pub use crate::index::domain::query::sort::MAX_SORT_KEYS;
pub use crate::index::domain::query::validate_query;
pub use crate::index::domain::sortable_f64::SortableF64;
pub use crate::index::domain::storage_error::StorageError;
pub use crate::index::infrastructure::collection_retirement::{
    collection_reclaimer_snapshot, CollectionReclaimerSnapshot,
};
pub use crate::index::infrastructure::snapshot_v1::{
    CollectionSnapshot, FieldIndexSnapshot, LegacyInvertedIndex, SnapshotV1,
};
