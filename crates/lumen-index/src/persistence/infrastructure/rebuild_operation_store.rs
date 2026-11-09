//! Crash-safe file store for durable rebuild operations.
//!
//! Arbitrates single-writer rebuild ownership per collection and persists
//! state updates atomically through [`storage_durable::atomic_write_strict`].
//! Every write checks that the record on disk still belongs to the operation
//! being written, under a per-record lock, so a stale handle cannot overwrite
//! a newer operation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

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
    #[error("rebuild operation '{operation_id}' for collection '{collection}' has no record and is no longer active")]
    Stale {
        collection: String,
        operation_id: String,
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

    /// Path to the persisted record for `collection`. The name is hex-encoded
    /// so that no separator or `..` in a collection name can leave `base_dir`.
    pub fn record_path(&self, collection: &str) -> PathBuf {
        let encoded: String = collection
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        self.base_dir.join(format!("{encoded}.rebuild.json"))
    }

    /// Load the current rebuild operation for `collection`, if one exists.
    /// Fails closed if the file is corrupted or truncated. The error names
    /// only the decode category and position, never the record bytes.
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
                error: describe_decode_error(&e),
            })?;
        Ok(Some(op))
    }

    /// Single-writer arbitration: begin a new rebuild operation for `collection`.
    /// Returns `Conflict` while a non-terminal operation exists for that collection.
    /// Succeeds again once the previous operation is `Done`, `Cancelled`, or `Failed`.
    pub fn begin(
        &self,
        collection: &str,
        operation_id: &str,
    ) -> Result<RebuildOperation, RebuildStoreError> {
        let lock = record_lock(&self.record_path(collection));
        let _held = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = self.load(collection)? {
            if !existing.phase.is_terminal() {
                return Err(RebuildStoreError::Conflict {
                    collection: collection.to_string(),
                    active_operation_id: existing.operation_id,
                });
            }
        }
        let op = RebuildOperation::new(collection, operation_id);
        self.write_record(&op)?;
        Ok(op)
    }

    /// Transition to `Streaming` and persist atomically.
    pub fn start_streaming(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        self.transition(op, RebuildOperation::start_streaming)
    }

    /// Accept a chunk in order and persist atomically.
    pub fn accept_chunk(
        &self,
        op: &mut RebuildOperation,
        seq: u64,
        docs: u64,
    ) -> Result<(), RebuildStoreError> {
        self.transition(op, |next| next.accept_chunk(seq, docs))
    }

    /// Transition to `Sealing` and persist atomically.
    pub fn seal(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        self.transition(op, RebuildOperation::seal)
    }

    /// Transition to `Done` and persist atomically.
    pub fn complete(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        self.transition(op, RebuildOperation::complete)
    }

    /// Cancel the operation and persist atomically.
    pub fn cancel(&self, op: &mut RebuildOperation) -> Result<(), RebuildStoreError> {
        self.transition(op, RebuildOperation::cancel)
    }

    /// Fail the operation and persist atomically.
    pub fn fail(
        &self,
        op: &mut RebuildOperation,
        reason: impl Into<String>,
    ) -> Result<(), RebuildStoreError> {
        let reason = reason.into();
        self.transition(op, move |next| next.fail(reason))
    }

    /// Apply one transition to a copy, save the copy, and assign it to `op`
    /// only after the save succeeds. A failed save leaves `op` unchanged, so
    /// the caller can retry the same step.
    fn transition(
        &self,
        op: &mut RebuildOperation,
        step: impl FnOnce(&mut RebuildOperation) -> Result<(), RebuildError>,
    ) -> Result<(), RebuildStoreError> {
        let expected = op.clone();
        let mut next = op.clone();
        step(&mut next)?;
        self.save_if_current(&expected, &next)?;
        *op = next;
        Ok(())
    }

    /// Compare-and-swap: writes `next` only when the record on disk equals
    /// `expected`, the copy the caller held before the step. A stale copy of
    /// the same operation, including one taken before a cancel, is refused
    /// with `Conflict` and cannot overwrite the newer record. Callers hold
    /// the record lock.
    fn save_if_current(
        &self,
        expected: &RebuildOperation,
        next: &RebuildOperation,
    ) -> Result<(), RebuildStoreError> {
        let lock = record_lock(&self.record_path(&expected.collection));
        let _held = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.load(&expected.collection)? {
            None => Err(RebuildStoreError::Stale {
                collection: expected.collection.clone(),
                operation_id: expected.operation_id.clone(),
            }),
            Some(current) if current != *expected => Err(RebuildStoreError::Conflict {
                collection: expected.collection.clone(),
                active_operation_id: current.operation_id,
            }),
            Some(_) => self.write_record(next),
        }
    }

    fn write_record(&self, op: &RebuildOperation) -> Result<(), RebuildStoreError> {
        if !self.base_dir.exists() {
            std::fs::create_dir_all(&self.base_dir)?;
        }
        let bytes = serde_json::to_vec_pretty(op)?;
        let path = self.record_path(&op.collection);
        storage_durable::atomic_write_strict(&path, &bytes)?;
        Ok(())
    }
}

/// One lock per record path within this process. Competing `begin` calls and
/// stale handles check and write the same record under it.
fn record_lock(path: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(Mutex::default);
    let mut map = locks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.entry(path.to_path_buf()).or_default().clone()
}

/// Keeps the decode category and position. The message text can quote record
/// bytes, so it is not kept.
fn describe_decode_error(error: &serde_json::Error) -> String {
    format!(
        "{:?} at line {} column {}",
        error.classify(),
        error.line(),
        error.column()
    )
}
