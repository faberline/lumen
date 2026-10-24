//! What persistence writes and reads on disk beside the segment files that
//! `lumen-storage` owns: the checkpoint generation store that publishes them, the process-wide background merge worker, the
//! append-only file that carries the applied writes a checkpoint has not yet
//! covered, the RDB snapshot store, the shared backup sink contract, the
//! private directories pending-change spills write to, and the process memory
//! the checkpoint sink keeps beside them.

pub mod aof;
pub mod backup_sink;
pub mod checkpoint_process_state;
pub mod merge_worker;
pub mod rdb;
pub mod rebuild_operation_store;
pub mod segment_rdb_store;
pub mod spill_directory;
