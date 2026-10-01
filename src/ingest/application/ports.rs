//! The ports the write path's callers submit through: the write backend the
//! HTTP handlers call, which the local write coordinator implements here and
//! sharding implements over in-process shards.

pub(crate) mod write_backend;
