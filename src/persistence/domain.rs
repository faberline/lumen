//! The checkpoint catalog model: the generation manifest a checkpoint
//! publishes, rebasing a manifest onto a background merge's verified output,
//! and the repository seam RDB snapshots are saved to and loaded from.

pub(crate) mod generation_manifest;
pub(crate) mod merge_rebase;
pub(crate) mod rdb_store;
