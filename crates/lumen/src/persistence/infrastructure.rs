//! What persistence writes and reads on disk: the columnar segment file format,
//! its writers and its zero-copy reader, the composed view that reads one base
//! segment through its ordered sparse deltas, the checkpoint generation store
//! that publishes them, the process-wide background merge worker, the
//! append-only file that carries the applied writes a checkpoint has not yet
//! covered, the RDB snapshot store, the shared backup sink contract, the
//! private directories pending-change spills write to, and the process memory
//! the checkpoint sink keeps beside them.

pub(crate) mod aof;
pub(crate) mod backup_sink;
pub(crate) mod checkpoint_process_state;
pub(crate) mod composed_segment;
pub(crate) mod merge_worker;
pub(crate) mod rdb;
pub(crate) mod segment;
pub(crate) mod segment_rdb_store;
pub(crate) mod spill_directory;
