//! The checkpoint catalog model: the generation manifest a checkpoint
//! publishes, rebasing a manifest onto a background merge's verified output,
//! the repository seam RDB snapshots are saved to and loaded from, and the
//! schedule that decides when the periodic driver starts a checkpoint.

pub(crate) mod checkpoint_schedule;
pub(crate) mod generation_manifest;
pub(crate) mod merge_rebase;
pub(crate) mod rdb_store;
