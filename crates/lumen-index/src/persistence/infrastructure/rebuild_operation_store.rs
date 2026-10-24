//! Crash-safe file store for durable rebuild operations.
//!
//! Arbitrates single-writer rebuild ownership per collection and persists
//! state updates atomically through [`storage_durable::atomic_write_strict`].

use std::path::{Path, PathBuf};

use crate::index::domain::rebuild_operation::{RebuildError, RebuildOperation};

#[cfg(test)]
mod tests;

/// Errors returned by the rebuild operation store.
#[derive(Debug, thiserror::Error)]
pub enum RebuildStoreError {
    #[error("competing rebuild operation for collection '{collection}' is active: {active_operation_id}")]
    Conflict {
        collection: String,
        active_operation_id: String,
    },
    #[error("corrupt or invalid rebuild record for collection '{collection}': {error}")]
    CorruptRecord { collection: String, error: String },
    #[error("atomic write error: {0}")]
    AtomicWrite(#[from] storage_durable::StrictAtomicWriteError),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("rebuild operation transition error: {0}")]
    Operation(#[from] RebuildError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Durable file-backed store for collection rebuild operations.
#[derive(Debug, Clone)]
pub struct RebuildOperationStore {
    base_dir: PathBuf,
}

impl RebuildOperationStore {
    /// Create a new store rooted at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Path to the base directory holding rebuild records.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Path to the persisted record for `collection`.
    pub fn record_path(&self, collection: &str) -> PathBuf {
        self.base_dir.join(format!("{collection}.rebuild.json"))
    }

    /// Load the current rebuild operation for `collection`, if one exists.
    /// Fails closed if the file is corrupted or truncated.
    pub fn load(&self, collection: &str) -> Result<Option<RebuildOperation>, RebuildStoreError> {
        let path = self.record_path(collection);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        if bytes.is_empty() {
            return Err(RebuildStoreError::CorruptRecord {
                collection: collection.to_string(),
                error: "empty record file".to_string(),
            });
        }
        let op: RebuildOperation =
            serde_json::from_slice(&bytes).map_err(|e| RebuildStoreError::CorruptRecord {
                collection: collection.to_string(),
                error: e.to_string(),
            })?;
        Ok(Some(op))
    }

    /// Persist the rebuild operation state atomically to disk.
    pub fn save(&self, op: &RebuildOperation) -> Result<(), RebuildStoreError> {
        if !self.base_dir.exists() {
            std::fs::create_dir_all(&self.base_dir)?;
        }
        let bytes = serde_json::to_vec_pretty(op)?;
        let path = self.record_path(&op.collection);
        storage_durable::atomic_write_strict(&path, &bytes)?;
        Ok(())
    }

    /// Single-writer arbitration: begin a new rebuild operation for `collection`.
    /// Returns `Conflict` while a non-terminal operation exists for that collection.
    /// Succeeds again once the previous operation is `Done`, `Cancelled`, or `Failed`.
    pub fn begin(
        &self,
        collection: &str,
        operation_id: &str,
    ) -> Result<RebuildOperation, RebuildStoreError> {
        if let Some(existing) = self.load(collection)? {
            if !existing.phase.is_terminal() {
                return Err(RebuildStoreError::Conflict {
                    collection: collection.to_string(),
                    active_operation_id: existing.operation_id,
                });
            }
        }
        let op = RebuildOperation::new(collection, operation_id);
        self.save(&op)?;
        Ok(op)
    }

    /// Transition to `Streaming` and persist atomically.
    pub fn start_streaming(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        op.start_streaming()?;
        self.save(op)?;
        Ok(())
    }

    /// Accept a chunk in order and persist atomically.
    pub fn accept_chunk(
        &self,
        op: &mut RebuildOperation,
        seq: u64,
        docs: u64,
    ) -> Result<(), RebuildStoreError> {
        op.accept_chunk(seq, docs)?;
        self.save(op)?;
        Ok(())
    }

    /// Transition to `Sealing` and persist atomically.
    pub fn seal(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        op.seal()?;
        self.save(op)?;
        Ok(())
    }

    /// Transition to `Done` and persist atomically.
    pub fn complete(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        op.complete()?;
        self.save(op)?;
        Ok(())
    }

    /// Cancel the operation and persist atomically.
    pub fn cancel(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        op.cancel()?;
        self.save(op)?;
        Ok(())
    }

    /// Fail the operation and persist atomically.
    pub fn fail(
        &self,
        op: &mut RebuildOperation,
        reason: impl Into<String>,
    ) -> Result<(), RebuildStoreError> {
        op.fail(reason)?;
        self.save(op)?;
        Ok(())
    }
}
