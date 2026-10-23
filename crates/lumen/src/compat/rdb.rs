//! `crate::rdb` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::persistence::domain::rdb_store::RdbStore;
pub use crate::persistence::infrastructure::rdb::{LocalFsRdbStore, RdbSnapshot};
