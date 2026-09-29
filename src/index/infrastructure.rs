//! What the index keeps in private files: the analysis workspaces that spill to
//! disk, the optional HNSW graph cache, the Text and Vector rows staged before
//! apply, the scalar files prepared from committed records, and the checkpoint
//! projections that write staged rows as segments.

pub(crate) mod analysis;
pub(crate) mod checkpoint_projection;
pub(super) mod committed_scalar_files;
pub(crate) mod staging;
pub(crate) mod vector;
