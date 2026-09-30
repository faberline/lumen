//! Vector index files: the optional HNSW graph cache a planned restart saves
//! beside the checkpoint and the next open may load.

pub(in crate::index) mod graph_cache;
