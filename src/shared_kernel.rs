//! The vocabulary every context shares: the wire types of the public API, the
//! committed-mutation log entry, and the apply/capture barrier the index,
//! ingest and persistence contexts coordinate checkpoints through.

pub(crate) mod capture_barrier;
pub mod log_entry;
pub(crate) mod types;
