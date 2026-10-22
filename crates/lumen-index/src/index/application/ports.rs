//! The ports the index context's callers read through: the search backend the
//! HTTP handlers call, which the local Engine implements here and sharding
//! implements over in-process shards.

pub mod search_backend;
