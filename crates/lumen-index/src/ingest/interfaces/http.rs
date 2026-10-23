//! The HTTP handlers for a collection's documents: indexing and the streaming
//! bulk reindex, the deletes, truncate and unindex, and the full-replacement
//! upserts.

pub mod delete;
pub mod index;
pub mod replace;
