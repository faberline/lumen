//! Persistence's use cases: the background merge, which compacts a field's
//! delta segments outside the checkpoint lock and publishes the result onto the
//! newest complete generation; the capacity worker, which runs the checkpoints
//! and merges that committed apply waits on for change-budget capacity; the
//! durable single-process segment restore; and, behind the `backup` feature,
//! fetching a serving fleet's snapshot for a backup sink and posting one back
//! to restore.

pub(crate) mod background_merge;
#[cfg(feature = "backup")]
pub(crate) mod backup;
pub(crate) mod capacity;
pub(crate) mod restore;
