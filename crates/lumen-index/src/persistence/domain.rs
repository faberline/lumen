//! The checkpoint catalog model: the generation manifest a checkpoint
//! publishes, rebasing a manifest onto a background merge's verified output,
//! the repository seam RDB snapshots are saved to and loaded from, and the
//! schedule that decides when the periodic driver starts a checkpoint.

pub mod checkpoint_schedule;
pub mod generation_manifest;
pub mod merge_rebase;
pub mod rdb_store;
