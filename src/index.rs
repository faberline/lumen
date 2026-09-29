//! Index: the searchable forms of a collection. Its text, keyword, number, set
//! and vector field indexes, the analyzers that turn a text value into terms,
//! query evaluation, and the Engine that applies records to them.

pub(crate) mod domain;
pub(crate) mod infrastructure;
