//! What persistence writes and reads on disk: the columnar segment file format,
//! its writers and its zero-copy reader, the composed view that reads one base
//! segment through its ordered sparse deltas, the checkpoint generation store
//! that publishes them, and the process-wide background merge worker.

pub(crate) mod composed_segment;
pub(crate) mod merge_worker;
pub(crate) mod segment;
pub(crate) mod segment_rdb_store;
