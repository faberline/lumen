//! What the index keeps in private files: the analysis workspaces that spill to
//! disk, the optional HNSW graph cache, the Text and Vector rows staged before
//! apply, the scalar files prepared from committed records, the checkpoint
//! projections that write staged rows as segments, and the checkpoint directory
//! they are written under. Beside them, the snapshot document's wire types, the
//! background reclaimer for collections a truncate detached, and the bounded
//! blocking executor every synchronous search runs on.

pub mod analysis;
pub mod checkpoint_fs;
pub mod checkpoint_projection;
pub mod collection_retirement;
pub mod committed_scalar_files;
pub mod search_executor;
pub mod snapshot_v1;
pub mod staging;
pub mod vector;
