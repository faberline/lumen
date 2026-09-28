//! Shard fan-out over in-process engines: scattered search with a global merge,
//! routed writes, and the client-side shard URL helper.

pub mod consumer;
pub(crate) mod engine_shard_search;
pub(crate) mod engine_shard_write;
pub(crate) mod search_fanout;
