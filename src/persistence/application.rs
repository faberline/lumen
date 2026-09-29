//! Persistence's use cases: the background merge, which compacts a field's
//! delta segments outside the checkpoint lock and publishes the result onto the
//! newest complete generation, and, behind the `backup` feature, fetching a
//! serving fleet's snapshot for a backup sink and posting one back to restore.

pub(crate) mod background_merge;
#[cfg(feature = "backup")]
pub(crate) mod backup;
