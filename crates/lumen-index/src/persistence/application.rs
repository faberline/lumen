//! Persistence's use cases: the segment checkpoint sink, which manual, periodic
//! and capacity checkpoints save through, with its periodic driver and the
//! bootstrap pending-change spill; the background merge, which compacts a
//! field's delta segments outside the checkpoint lock and publishes the result
//! onto the newest complete generation; the capacity worker, which runs the
//! checkpoints and merges that committed apply waits on for change-budget
//! capacity; the durable single-process segment restore; and, behind the
//! `backup` feature, fetching a serving fleet's snapshot for a backup sink and
//! posting one back to restore. Beside them, the checkpoint and restore ports
//! the admin verbs call, with the defaults a process without segment
//! persistence uses.

pub mod background_merge;
#[cfg(feature = "backup")]
pub mod backup;
pub mod capacity;
pub mod ports;
pub mod restore;
pub mod segment_checkpoint_sink;
