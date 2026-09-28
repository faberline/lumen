//! Sharding: which physical shard owns a document, how requests reach it, and
//! the primitives that move buckets between shards during a split.
//!
//! Ownership is a versioned virtual-bucket map: a document hashes to one of a
//! fixed number of buckets, and the map assigns each bucket to a physical
//! shard. A split reassigns buckets without changing the hash, so `shardCount`
//! never becomes a permanent `hash % shardCount` data contract.

pub(crate) mod application;
pub(crate) mod domain;
pub(crate) mod infrastructure;
