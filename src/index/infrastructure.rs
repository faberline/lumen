//! What the index keeps in private files: the analysis workspaces that spill to
//! disk, and the optional HNSW graph cache.

pub(crate) mod analysis;
pub(crate) mod vector;
