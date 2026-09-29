//! The typed errors the storage engine returns for a missing collection, a
//! schema mismatch or an exhausted limit, which the API maps to its status
//! codes.

use thiserror::Error;

use crate::shared_kernel::types::schema::FieldType;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("collection not found: {0}")]
    CollectionNotFound(String),
    #[error(
        "invalid collection id `{0}`: `:` is reserved for custom-method routes (e.g. `POST /collections:search`) and cannot appear in a collection id"
    )]
    InvalidCollectionName(String),
    #[error("unknown field `{field}` in collection `{collection}`")]
    UnknownField { collection: String, field: String },
    #[error("type mismatch on field `{field}`: expected {expected:?}, got {got}")]
    TypeMismatch {
        field: String,
        expected: FieldType,
        got: &'static str,
    },
    #[error("duplicates not supported on text field `{0}`")]
    DuplicatesOnText(String),
    #[error("invalid number value: {0}")]
    InvalidNumber(String),
    #[error("bulk index limit exceeded: got {got} items (max {max})")]
    BulkLimit { got: usize, max: usize },
    #[error("query too complex: {0}")]
    QueryTooComplex(String),
    #[error("invalid pagination: {0}")]
    InvalidPagination(String),
    #[error("unsupported sort: {0}")]
    UnsupportedSort(String),
    #[error("collection `{0}` was deleted and is pending physical removal")]
    Gone(String),
    /// #1467 R4: `ReshardPruneChunk::total_chunks` sanity cap — a spoofed
    /// or buggy sender declaring an enormous `total_chunks` would otherwise
    /// let a single chunk hold the receiver's prune accumulator open
    /// indefinitely (it can never reach "ready" without that many chunks
    /// actually arriving).
    #[error("prune chunk total_chunks {total_chunks} invalid (must be 1..={max})")]
    InvalidPruneChunk { total_chunks: u32, max: u32 },
    /// #1467 R4: hard cap on distinct in-flight prune accumulator groups —
    /// bounds memory against an abandoned migration (driver crash mid-pass)
    /// or a flood of distinct bogus keys, on top of the age-based GC that
    /// runs on every call.
    #[error("prune accumulator full: {count} in-flight groups (max {max})")]
    PruneAccumulatorFull { count: usize, max: usize },
}
