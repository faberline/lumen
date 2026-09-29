//! The index model: the analyzers that turn a text field's value into terms,
//! the HNSW and flat backends that answer a vector field's kNN searches, and
//! the building blocks the field indexes share: the external-id interner,
//! posting lists and the token probe over them, the order-preserving number
//! key, the fast hash maps, and the storage error type.

pub(crate) mod analysis;
pub(crate) mod collection;
pub(crate) mod fast_hash;
pub(crate) mod field_coverage;
pub(crate) mod field_index;
pub(crate) mod hash_index;
pub(crate) mod interner;
pub(crate) mod keyword_index;
pub(crate) mod number_index;
pub(crate) mod postings;
pub(crate) mod record_ram;
pub(crate) mod schema_validation;
pub(crate) mod set_index;
pub(crate) mod sortable_f64;
pub(crate) mod storage_error;
pub(crate) mod text_index;
pub(crate) mod tok_probe;
pub(crate) mod token_set;
pub(crate) mod vector;
