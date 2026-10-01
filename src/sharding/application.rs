//! Shard fan-out over in-process engines: scattered search with a global merge,
//! routed writes, and the client-side shard URL helper; and the routed-backend
//! port cross-pod shard routing serves the HTTP handlers through.

pub mod consumer;
pub(crate) mod engine_shard_search;
pub(crate) mod engine_shard_write;
pub(crate) mod ports;
pub(crate) mod search_fanout;
