//! The HTTP handlers for a collection's documents: indexing and the streaming
//! bulk reindex, the deletes, truncate and unindex, and the full-replacement
//! upserts.

pub(crate) mod delete;
pub(crate) mod index;
pub(crate) mod replace;
