//! Index: the searchable forms of a collection. Its text, keyword, number, set
//! and vector field indexes, the analyzers that turn a text value into terms,
//! query evaluation, and the Engine that applies records to them.

pub mod application;
pub mod domain;
pub mod infrastructure;
pub mod interfaces;
