//! The RDB repository seam: [`RdbStore`], where point-in-time engine snapshots
//! are saved, loaded and pruned.

use anyhow::Result;
use async_trait::async_trait;

use crate::persistence::infrastructure::rdb::RdbSnapshot;

/// Where RDB snapshots are persisted. Object-store adapters (S3/GCS)
/// implement this with the same byte layout as [`LocalFsRdbStore`].
///
/// [`LocalFsRdbStore`]: crate::persistence::infrastructure::rdb::LocalFsRdbStore
#[async_trait]
pub trait RdbStore: Send + Sync {
    /// Persist `rdb` and make it the new latest.
    async fn save(&self, rdb: &RdbSnapshot) -> Result<()>;

    /// Load the most recent snapshot, or `None` if the store is empty.
    async fn load_latest(&self) -> Result<Option<RdbSnapshot>>;

    /// Drop snapshots older than the newest `keep` (retention).
    async fn prune(&self, keep: usize) -> Result<usize>;
}
