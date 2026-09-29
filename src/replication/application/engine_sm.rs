//! `EngineSm` — lumen's [`Engine`] as a [`raft_runtime::RaftStateMachine`].
//!
//! This is lumen's convergence onto the shared raft host (epic #524): the host
//! is the sole applier, so the `WriteCoordinator`/`WalLog` seam is no longer
//! needed for the raft path. `apply` folds a committed
//! command into the engine and records the rich [`ApplyOutcome`] in a small
//! window so the write handler can return it (read-your-write); `snapshot`/
//! `restore` bridge to the engine's RDB checkpoint (the "backup layer").
//!
//! The raft log index **is** the WAL seq (both 1-based), so `apply_raft_entry`,
//! the RDB `up_to_seq` tag, and the outcome key all share the same `Index`.

pub(crate) mod state_machine;
pub(crate) mod write_sink;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use raft_runtime::{Index, OutcomeWindow};

use crate::ingest::domain::change_budget::AdmissionError;
use crate::ingest::domain::wal_record::WalRecord;
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::storage::{
    ApplyOutcome, Engine, RecordAdmissionError, RecordReservation, RepriceRecord,
};

/// How many recent apply outcomes to retain for the write handler to claim,
/// via [`OutcomeWindow`].
const OUTCOME_WINDOW: u64 = 8192;

/// lumen's engine driven as a raft state machine.
pub struct EngineSm {
    engine: Arc<Engine>,
    applied: AtomicU64,
    outcomes: Mutex<OutcomeWindow<Result<ApplyOutcome>>>,
    failed: AtomicBool,
    segment_store: Option<Arc<SegmentRdbStore>>,
    layer_capacity_owner: Mutex<Option<crate::segment_capacity::Fallback>>,
}

impl EngineSm {
    /// Wrap `engine`, seeded at `from_seq` (the seq the engine was cold-started
    /// to, e.g. from an RDB checkpoint — `0` for a fresh engine).
    pub fn new(engine: Arc<Engine>, from_seq: u64) -> Arc<Self> {
        Self::with_snapshot_store(engine, from_seq, None)
    }

    fn with_snapshot_store(
        engine: Arc<Engine>,
        from_seq: u64,
        segment_store: Option<Arc<SegmentRdbStore>>,
    ) -> Arc<Self> {
        engine.capture_barrier.apply().initialize_sequence(from_seq);
        Arc::new(EngineSm {
            engine,
            applied: AtomicU64::new(from_seq),
            outcomes: Mutex::new(OutcomeWindow::new(OUTCOME_WINDOW)),
            failed: AtomicBool::new(false),
            segment_store,
            layer_capacity_owner: Mutex::new(None),
        })
    }

    /// Select the segment snapshot backend for this state machine.
    pub fn new_with_segment_store(
        engine: Arc<Engine>,
        from_seq: u64,
        store: Arc<SegmentRdbStore>,
    ) -> Arc<Self> {
        Self::with_snapshot_store(engine, from_seq, Some(store))
    }

    /// #2516: the wrapped engine, so [`RaftWriteSink::submit`] can flip the
    /// sticky degraded-storage gauge when the raft log append itself hits
    /// ENOSPC (before there is any committed entry to apply).
    ///
    /// [`RaftWriteSink::submit`]: crate::replication::application::engine_sm::write_sink::RaftWriteSink#method.submit
    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Claim the outcome for `index` (the host's `propose` returns the index;
    /// the write handler then takes the rich outcome the local apply produced).
    pub fn take_outcome(&self, index: u64) -> Result<ApplyOutcome> {
        self.outcomes
            .lock()
            .expect("outcomes poisoned")
            .claim(index)
            .unwrap_or_else(|| Err(anyhow::anyhow!("outcome for seq {index} unavailable")))
    }
}

/// One host-owned proposal carrier. It moves by the Raft (index, term), never
/// by request identity or equal command bytes. The host retains it if a caller
/// cancels or a post-index durable append fails.
struct AdmittedRaftRecord {
    entry: RaftLogEntry,
    reservation: RecordReservation,
}

impl EngineSm {
    /// Start or reuse this state machine's independent capacity maintainer.
    /// Call only before a committed-record capacity wait; local proposal
    /// admission must still return Full without creating background work.
    fn ensure_capacity_owner(&self) -> Result<()> {
        let mut owner = self
            .layer_capacity_owner
            .lock()
            .map_err(|_| anyhow::anyhow!("capacity owner poisoned"))?;
        crate::segment_capacity::Fallback::ensure(
            &mut owner,
            &self.engine,
            self.segment_store.clone(),
        )
    }

    fn decode_admitted(
        &self,
        command: &[u8],
        before_publication: bool,
    ) -> Result<AdmittedRaftRecord> {
        anyhow::ensure!(
            !self.failed.load(Ordering::Acquire),
            "Raft apply requires restart after an unresolved record"
        );
        let workspace =
            crate::ingest::infrastructure::wire_cost::scan_workspace_bytes(command.len())?;
        // The node owns its original log command and the current apply delivery.
        // Admission also precedes the decoder's token and untagged-value buffers.
        let transport = command
            .len()
            .checked_mul(2)
            .ok_or(RecordAdmissionError::Overflow)?;
        let request = self
            .engine
            .record_ram_request_from_bound(workspace, transport);
        let mut reservation = match self.engine.try_reserve_record_ram(&request) {
            Ok(reservation) => reservation,
            Err(RecordAdmissionError::Capacity(AdmissionError::Full { .. }))
                if !before_publication =>
            {
                self.ensure_capacity_owner()?;
                self.engine.wait_reserve_record_ram(&request)?
            }
            Err(error) => return Err(error.into()),
        };
        let decoded_peak = crate::ingest::infrastructure::wire_cost::decoded_peak_bound(command)?;
        let required = decoded_peak
            .checked_add(transport)
            .ok_or(RecordAdmissionError::Overflow)?;
        match reservation.try_grow_to(required) {
            Ok(()) => (),
            Err(AdmissionError::Full { .. }) if !before_publication => {
                self.ensure_capacity_owner()?;
                self.engine.request_pending_checkpoint();
                reservation
                    .wait_grow_to(required)
                    .map_err(RecordAdmissionError::Capacity)?;
            }
            Err(error) => return Err(RecordAdmissionError::Capacity(error).into()),
        }
        let record = WalRecord::decode(command)?;
        self.engine.price_decoded_record(
            &record.entry,
            &mut reservation,
            decoded_peak,
            transport,
            before_publication,
        )?;
        Ok(AdmittedRaftRecord {
            entry: record.entry,
            reservation,
        })
    }

    fn apply_record(&self, index: Index, mut record: AdmittedRaftRecord) -> Result<()> {
        loop {
            match self
                .engine
                .begin_admitted_record(record.entry, record.reservation)
            {
                Ok(mut guard) => {
                    let outcome = self.engine.apply_prepared_raft_entry(&mut guard);
                    let mut outcomes = self.outcomes.lock().expect("outcomes poisoned");
                    outcomes.insert(index, outcome);
                    outcomes.advance(index);
                    // A normal field validation error may follow a valid prefix.
                    // Its complete mutation and both watermarks share this lease.
                    guard.apply_lease().advance_sequence(index);
                    self.applied.store(index, Ordering::Release);
                    return Ok(());
                }
                Err(RepriceRecord {
                    entry,
                    mut reservation,
                    required: Some(required),
                    ..
                }) => {
                    match reservation.try_grow_to(required) {
                        Ok(()) => (),
                        Err(AdmissionError::Full { .. }) => {
                            self.ensure_capacity_owner()?;
                            self.engine.request_pending_checkpoint();
                            reservation
                                .wait_grow_to(required)
                                .map_err(RecordAdmissionError::Capacity)?;
                        }
                        Err(error) => return Err(RecordAdmissionError::Capacity(error).into()),
                    }
                    record = AdmittedRaftRecord { entry, reservation };
                }
                Err(reprice) => return Err(reprice.error.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests;
