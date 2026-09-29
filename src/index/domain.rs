//! The index model: the analyzers that turn a text field's value into terms,
//! and the HNSW and flat backends that answer a vector field's kNN searches.

pub(crate) mod analysis;
pub(crate) mod vector;
