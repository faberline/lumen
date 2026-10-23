//! The index model: the analyzers that turn a text field's value into terms,
//! the HNSW and flat backends that answer a vector field's kNN searches, and
//! the building blocks the field indexes share: the external-id interner,
//! posting lists and the token probe over them, the order-preserving number
//! key, the fast hash maps, and the storage error type.

pub mod analysis;
pub mod collection;
pub mod engine_state;
pub mod fast_hash;
pub mod field_coverage;
pub mod field_index;
pub mod hash_index;
mod interner;
pub mod keyword_index;
pub mod number_index;
pub mod postings;
pub mod query;
pub mod record_ram;
pub mod schema_validation;
pub mod set_index;
pub mod sortable_f64;
pub mod storage_error;
pub mod text_index;
mod tok_probe;
pub mod token_set;
pub mod vector;
