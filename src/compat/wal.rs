//! `crate::wal` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::ingest::domain::wal_log::{SharedWal, WalAdmissionStream, WalLog, WalStream};
pub use crate::ingest::domain::wal_record::{
    WalRecord, WAL_CONTROL_FORMAT_VERSION, WAL_FORMAT_VERSION,
};
pub use crate::ingest::infrastructure::wal::delivery::{
    WalDelivery, WalSourceRecord, WalSourceRelease,
};
pub use crate::ingest::infrastructure::wal::mem_wal::MemWal;
