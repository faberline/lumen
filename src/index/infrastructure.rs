//! What the index keeps in private files: the analysis workspaces that spill to
//! disk, the optional HNSW graph cache, the Text and Vector rows staged before
//! apply, the scalar files prepared from committed records, the checkpoint
//! projections that write staged rows as segments, and the checkpoint directory
//! they are written under. Beside them, the snapshot document's wire types and
//! the background reclaimer for collections a truncate detached.

pub(crate) mod analysis;
pub(crate) mod checkpoint_fs;
pub(crate) mod checkpoint_projection;
pub(crate) mod collection_retirement;
pub(super) mod committed_scalar_files;
pub(crate) mod snapshot_v1;
pub(super) mod staging;
pub(crate) mod vector;
