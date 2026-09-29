//! The errors a write is refused with that the API maps to their own status: a
//! stalled submit, a full disk, and a process that must restart first.

/// A `submit()` waiter was released without a genuine
/// [`ApplyOutcome`](crate::index::application::engine::raft_dispatch::ApplyOutcome) (#1486 R2): either the apply
/// loop's redelivery-dedup guard skipped the waiter's sequence (already
/// at/below `applied`), or the wait exceeded
/// [`SUBMIT_TIMEOUT`](super::SUBMIT_TIMEOUT). Both are transient/retryable,
/// never a client input error — `src/api.rs`'s `From<anyhow::Error> for ApiErr` downcasts
/// this to a `503` instead of falling through to the generic `400`
/// default, so a stranded write is loud (a 5xx) rather than silent (an
/// infinite hang, the original defect) or misleading (a 4xx).
#[derive(Debug, Clone)]
pub struct SubmitStalled(pub String);

impl std::fmt::Display for SubmitStalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SubmitStalled {}

/// A durable write path (local AOF append/flush/sync, a segment/RDB
/// checkpoint save, or — under the `raft-wal` feature — a raft log append)
/// hit `io::ErrorKind::StorageFull` (ENOSPC) or a wrapped equivalent (#2516).
/// Reported as a distinct, stable error so `src/api.rs`'s
/// `From<anyhow::Error> for ApiErr` maps it to `507 Insufficient Storage`
/// with the machine-readable `storage_full` code instead of falling through
/// to the generic `400` default. Every origin that produces one MUST first
/// call `Metrics::mark_storage_degraded` — this type only carries the
/// message, it does not itself flip the sticky degraded flag.
#[derive(Debug, Clone)]
pub struct StorageFullError(pub String);

impl std::fmt::Display for StorageFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for StorageFullError {}

/// This process observed a durability boundary that it cannot safely resolve
/// while it keeps serving mutations. Only a restart can rebuild one exact state
/// from `CURRENT` plus the AOF and clear this latch.
#[derive(Debug, Clone)]
pub struct RestartRequired(pub String);

impl std::fmt::Display for RestartRequired {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RestartRequired {}

/// #2516: true when `e`'s error chain contains an `io::Error` whose kind is
/// `StorageFull` (ENOSPC) — the seam every durable-write call site (AOF
/// persist, segment/RDB checkpoint save, raft log append) probes to decide
/// whether to flip the node into degraded read-only mode. Walks the full
/// `anyhow` context chain (not just the outer error) because every durable
/// write path wraps the root `std::io::Error` with `.context(...)`.
pub fn is_storage_full(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_e| io_e.kind() == std::io::ErrorKind::StorageFull)
    })
}
