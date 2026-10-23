//! The ports the admin verbs persist through: the checkpoint sink behind
//! /admin/checkpoint and the planned-restart HNSW cache seal, and the restore
//! sink behind /admin/restore, each with the default a process without segment
//! persistence uses.

pub mod checkpoint_sink;
pub mod restore_sink;
