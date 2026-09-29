//! Write coordinator — the seam between the HTTP write handlers and the
//! log-driven apply loop.
//!
//! Design (see `wal` for the why): a write handler does **not** touch
//! the index. It calls [`WriteCoordinator::submit`], which:
//!
//! 1. publishes the mutation to the [`WalLog`] (the log assigns a global
//!    sequence — the total order),
//! 2. waits until **this node's** apply loop has folded the stream up to
//!    that sequence (read-your-write), and
//! 3. returns the [`ApplyOutcome`] the apply loop computed for it.
//!
//! Apply happens in exactly one place — the background loop subscribed to
//! the log — so every node converges by applying the same ordered
//! stream, and the node that received the write holds no special state.
//! For [`MemWal`](crate::ingest::infrastructure::wal::mem_wal::MemWal) the
//! loop runs in-process and the round-trip is sub-millisecond, so
//! single-node writes feel synchronous and existing tests see their writes
//! immediately.
//!
//! Apply errors (e.g. a type mismatch caught at apply time) are routed
//! back as the original `anyhow::Error` — carrying the `StorageError` —
//! so the handler still maps them to the right HTTP status.

pub(crate) mod aof_sync;
pub(crate) mod apply_loop;
pub(crate) mod capacity;
mod committed_scalar;
pub(crate) mod errors;
pub(crate) mod local_record;
pub(crate) mod mutation_gate;
pub(crate) mod submit;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use anyhow::Result;
use raft_runtime::OutcomeWindow;
use rustc_hash::FxHashMap;
use tokio::sync::{oneshot, Mutex as AsyncMutex, OwnedRwLockReadGuard};

use crate::ingest::application::write_coordinator::mutation_gate::MutationGate;
use crate::ingest::domain::wal_log::SharedWal;
use crate::ingest::infrastructure::wal::delivery::WalDelivery;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::storage::{ApplyOutcome, Engine, RecordReservation, RecordTransientReservation};

/// How many recent outcomes to retain, via [`OutcomeWindow`]. A publisher
/// reads its outcome within microseconds of the apply loop reaching its
/// sequence, far inside this window; outcomes for sequences no local
/// handler is waiting on (writes that originated on other nodes) age out.
const OUTCOME_WINDOW: u64 = 8192;
/// Bound on `submit()`'s wait for local apply (#1486 R2). Comfortably above
/// realistic single-record apply latency, so this only fires on a genuine
/// stall — turning what would otherwise be an infinite hang into a
/// retryable 5xx.
const SUBMIT_TIMEOUT_SECS: u64 = 30;
const SUBMIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(SUBMIT_TIMEOUT_SECS);
// Admission must leave time for an already admitted record to publish and
// apply inside the same 30-second submit deadline.
const LOCAL_CAPACITY_APPLY_RESERVE: std::time::Duration = std::time::Duration::from_secs(1);

struct PendingApply {
    seq: u64,
    delivery: WalDelivery,
    admitted_bytes: usize,
    reservation: Option<RecordReservation>,
    transient: Option<RecordTransientReservation>,
    enqueued_at: std::time::Instant,
}

/// A local submit makes one atomic pre-publication reservation. Its apply
/// portion remains eligible for repricing and source retention; the transient
/// portion covers transport, encode, and local AOF copies until completion.
/// Keeping both values in one map entry makes lookup and capacity relief
/// atomic with respect to the sequence.
struct LocalRecordReservation {
    apply: RecordReservation,
    transient: Option<RecordTransientReservation>,
}

impl LocalRecordReservation {
    fn bytes(&self) -> usize {
        self.apply
            .bytes()
            .checked_add(
                self.transient
                    .as_ref()
                    .map(RecordTransientReservation::bytes)
                    .unwrap_or_default(),
            )
            .expect("local reservation total overflow")
    }
}

struct CompletionState {
    outcomes: OutcomeWindow<Result<ApplyOutcome>>,
    /// An unresolved committed head can fail before its publisher installs a
    /// waiter. Keep its typed failure separate from applied outcomes.
    unresolved: FxHashMap<u64, Result<ApplyOutcome>>,
    waiters: FxHashMap<u64, oneshot::Sender<Result<ApplyOutcome>>>,
    /// A permit moves here immediately after publish and stays bound to the
    /// sequence until the apply loop completes it. Caller cancellation or a
    /// submit timeout only drops the waiter; it must never open the restore
    /// fence while the published record can still apply later.
    mutation_permits: FxHashMap<u64, OwnedRwLockReadGuard<()>>,
}

/// The optional local AOF the apply loop appends every applied record to (Stage
/// 2 Phase 2f-3). Wrapped in a `Mutex` because the apply loop appends from the
/// async task while the periodic checkpoint snapshotter calls `truncate_through`
/// from another task. The default writer fsyncs each successfully applied
/// record before the apply loop acknowledges its request, so an immediate
/// replacement process can recover every acknowledged write.
/// `None` on the default / non-AOF path, so `start_from` is byte-identical to
/// today.
pub type SharedAof = Arc<Mutex<crate::persistence::infrastructure::aof::aof_writer::AofWriter>>;

pub struct WriteCoordinator {
    wal: SharedWal,
    engine: Arc<Engine>,
    /// A local submit holds this lock from WAL publication through installing
    /// its sequence-keyed reservation. The apply loop takes and removes the
    /// reservation before it enters `spawn_blocking`; therefore a subscriber
    /// that observes a record while `publish` is still returning cannot apply
    /// it through the unadmitted path.
    local_reservations: AsyncMutex<FxHashMap<u64, LocalRecordReservation>>,
    has_local_aof: bool,
    /// A refused local request can need source relief without an apply waiter.
    capacity_relief_requested: AtomicBool,
    /// The last capacity-request revision reported by optional diagnostics.
    /// This is telemetry-only and does not take part in admission or retry.
    diagnostic_refusal_revision: AtomicU64,
    #[cfg(test)]
    diagnostic_capture: Mutex<Option<crate::persistence::infrastructure::segment_rdb_store::diagnostic::DiagnosticCaptureToken>>,
    layer_capacity_owner: Mutex<Option<crate::segment_capacity::Fallback>>,
    applied: AtomicU64,
    completions: Mutex<CompletionState>,
    /// A failed committed head can still retain its WAL source. Keep any
    /// reservation that the engine did not adopt until process exit.
    failed_head_reservations: Mutex<Vec<RecordReservation>>,
    /// A failed local head must keep its transport/AOF reservation too. This
    /// stays separate from the apply reservation because it never owns source
    /// retention or Engine state.
    failed_head_transient_reservations: Mutex<Vec<RecordTransientReservation>>,
    failed_head_retentions:
        Mutex<FxHashMap<u64, crate::ingest::domain::change_budget::SourceRetention>>,
    /// A non-owning self reference lets a cancelled request leave a publisher
    /// task alive without retaining the coordinator forever.
    self_weak: Weak<WriteCoordinator>,
    /// Serializes one Standalone-wide replacement against ordinary writes.
    ///
    /// `submit` holds a shared permit from publish through local apply. A
    /// durable restore takes the exclusive permit, which therefore starts only
    /// after every earlier submit has completed and prevents every later submit
    /// from publishing until activation finishes. This fence is sufficient for
    /// the embedded single-process Standalone path. External producers that can
    /// publish directly to a shared WAL remain outside its contract.
    mutation_gate: MutationGate,
}

impl WriteCoordinator {
    /// Spawn the apply loop and return the coordinator. The loop tails
    /// the log from the beginning and folds it into `engine`.
    pub fn start(wal: SharedWal, engine: Arc<Engine>) -> Arc<Self> {
        Self::start_from(wal, engine, 0)
    }

    /// Like [`start`](Self::start) but begins applying after `from_seq`
    /// — used when a snapshot (RDB) already seeded the engine up to that
    /// sequence.
    pub fn start_from(wal: SharedWal, engine: Arc<Engine>, from_seq: u64) -> Arc<Self> {
        // The default / non-AOF path. Delegates with no AOF, so the apply loop is
        // byte-identical to today.
        Self::start_from_inner(wal, engine, from_seq, None)
    }

    /// Like [`start_from`](Self::start_from) but also appends every APPLIED
    /// `(seq, record)` to a local AOF (Stage 2 Phase 2f-3), AFTER the apply
    /// succeeds and `applied` advances. The default / non-AOF path is unchanged —
    /// this is the only entry point that wires an AOF in.
    pub fn start_from_with_aof(
        wal: SharedWal,
        engine: Arc<Engine>,
        from_seq: u64,
        aof: SharedAof,
    ) -> Arc<Self> {
        Self::start_from_inner(wal, engine, from_seq, Some(aof))
    }

    /// Highest sequence this node has applied.
    pub fn applied_seq(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }
}

/// The write seam the API binds to: submit a log entry, get its applied outcome,
/// and report the applied head. Implemented by [`WriteCoordinator`] (the WAL-seam
/// path for embedded WAL) and by `RaftWriteSink` (the raft-runtime path).
#[async_trait::async_trait]
pub trait WriteSink: Send + Sync {
    async fn submit(&self, entry: RaftLogEntry) -> Result<ApplyOutcome>;
    fn applied_seq(&self) -> u64;
    fn restart_required(&self) -> bool {
        false
    }
    fn mutation_gate(&self) -> Option<MutationGate> {
        None
    }
}

#[async_trait::async_trait]
impl WriteSink for WriteCoordinator {
    async fn submit(&self, entry: RaftLogEntry) -> Result<ApplyOutcome> {
        WriteCoordinator::submit(self, entry).await
    }
    fn applied_seq(&self) -> u64 {
        WriteCoordinator::applied_seq(self)
    }
    fn restart_required(&self) -> bool {
        WriteCoordinator::is_restart_required(self)
    }
    fn mutation_gate(&self) -> Option<MutationGate> {
        Some(WriteCoordinator::mutation_gate(self))
    }
}

#[cfg(test)]
mod tests;
