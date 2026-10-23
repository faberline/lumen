//! `crate::coordinator` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::ingest::application::write_coordinator::errors::{
    is_storage_full, RestartRequired, StorageFullError, SubmitStalled,
};
pub use crate::ingest::application::write_coordinator::mutation_gate::MutationGate;
pub use crate::ingest::application::write_coordinator::{SharedAof, WriteCoordinator, WriteSink};
