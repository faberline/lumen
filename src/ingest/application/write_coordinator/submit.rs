//! `submit` and its completion: publishing a record, registering the waiter the
//! apply loop completes, and releasing waiters stranded on a skipped or failed
//! sequence.

use std::sync::atomic::Ordering;

use anyhow::{bail, Result};
use tokio::sync::{oneshot, OwnedRwLockReadGuard};

use crate::index::application::admission::record_reservation::{
    RecordReservation, RecordTransientReservation,
};
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::ingest::application::write_coordinator::errors::SubmitStalled;
use crate::ingest::application::write_coordinator::{
    WriteCoordinator, LOCAL_CAPACITY_APPLY_RESERVE, SUBMIT_TIMEOUT, SUBMIT_TIMEOUT_SECS,
};
use crate::ingest::domain::change_admission::PendingChangeCapacity;
use crate::ingest::domain::wal_record::WalRecord;
use crate::shared_kernel::log_entry::RaftLogEntry;

impl WriteCoordinator {
    /// Defense-in-depth (#1486): release any waiter stranded on a sequence
    /// the apply loop's redelivery-dedup guard is about to skip (already at
    /// or below `applied`). Unlike [`complete`](Self::complete), this is
    /// NOT reporting a real apply outcome — the dedup guard means this
    /// sequence's record was never folded into the engine on this pass, so
    /// there is no genuine `ApplyOutcome` to hand back. A distinct,
    /// explicitly-retryable error lets the caller's error message (and any
    /// HTTP status mapping) tell this apart from a real apply failure, and
    /// — critically — completes the waiter at all, instead of leaving
    /// `submit()`'s `rx.await` hanging forever. Deliberately does not touch
    /// `applied` or the outcomes window: `seq` is already accounted for by
    /// the watermark, so there is nothing further to record.
    pub(super) fn complete_stale(&self, seq: u64) {
        let (waiter, mutation_permit) = {
            let mut m = self.completions.lock().expect("completions poisoned");
            (m.waiters.remove(&seq), m.mutation_permits.remove(&seq))
        };
        if let Some(tx) = waiter {
            let _ = tx.send(Err(anyhow::Error::new(SubmitStalled(format!(
                "sequence {seq} arrived at or below the applied watermark (stale redelivery \
                or a sequence-domain mismatch); the write was not applied on this pass — retry"
            )))));
        }
        drop(mutation_permit);
    }

    pub(super) fn finalize_prepublication_error(&self, error: anyhow::Error) -> anyhow::Error {
        if error.downcast_ref::<PendingChangeCapacity>().is_some() {
            self.engine.metrics().incr_segment_backpressure();
        }
        error
    }

    pub(super) fn complete(&self, seq: u64, outcome: Result<ApplyOutcome>) {
        let mut direct = None;
        let mutation_permit;
        {
            let mut m = self.completions.lock().expect("completions poisoned");
            mutation_permit = m.mutation_permits.remove(&seq);
            if let Some(tx) = m.waiters.remove(&seq) {
                direct = Some((tx, outcome));
            } else {
                m.outcomes.insert(seq, outcome);
            }
            // Prune everything older than the retention window.
            m.outcomes.advance(seq);
            // Publish the new applied head while the completion lock still
            // hides the outcome from register_waiter. Otherwise an apply that
            // wins the publish/register race can expose its outcome, let the
            // caller-owned permit drop, and open the restore fence before this
            // watermark advances.
            self.applied.store(seq, Ordering::Release);
        }
        // The restore fence may open only after the applied watermark above
        // describes this completed record. The caller future may already have
        // been cancelled or timed out; the permit is sequence-owned here.
        drop(mutation_permit);
        if let Some((tx, outcome)) = direct {
            let _ = tx.send(outcome);
        }
    }

    /// Notify the caller that a committed head is unresolved without claiming
    /// it was applied. The sequence permit stays closed and the watermark
    /// stays at the last known-good prefix.
    pub(super) fn fail_unresolved(
        &self,
        seq: u64,
        outcome: Result<ApplyOutcome>,
        reservation: Option<RecordReservation>,
    ) {
        if let Some(reservation) = reservation {
            self.failed_head_reservations
                .lock()
                .expect("failed-head reservations poisoned")
                .push(reservation);
        }
        let mut completions = self.completions.lock().expect("completions poisoned");
        if let Some(waiter) = completions.waiters.remove(&seq) {
            let _ = waiter.send(outcome);
        } else {
            completions.unresolved.insert(seq, outcome);
        }
    }

    /// Keep the local transient half until the uncertain process exits. Its
    /// source is not retained here; the matching apply half owns that bridge.
    pub(super) fn retain_failed_transient(&self, transient: Option<RecordTransientReservation>) {
        if let Some(transient) = transient {
            self.failed_head_transient_reservations
                .lock()
                .expect("failed-head transient reservations poisoned")
                .push(transient);
        }
    }

    pub(super) fn register_waiter(
        &self,
        seq: u64,
        mutation_permit: OwnedRwLockReadGuard<()>,
    ) -> Result<oneshot::Receiver<Result<ApplyOutcome>>> {
        let mut m = self.completions.lock().expect("completions poisoned");
        if let Some(result) = m.outcomes.claim(seq) {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(result);
            return Ok(rx);
        }
        if let Some(result) = m.unresolved.remove(&seq) {
            if m.mutation_permits.contains_key(&seq) {
                bail!("duplicate mutation permit for sequence {seq}");
            }
            let (tx, rx) = oneshot::channel();
            // The failed head still owns the shared permit. Install it before
            // exposing the typed error to a caller that may immediately start
            // a restore attempt.
            m.mutation_permits.insert(seq, mutation_permit);
            let _ = tx.send(result);
            return Ok(rx);
        }
        if seq <= self.applied.load(Ordering::Acquire) {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Err(anyhow::Error::new(SubmitStalled(format!(
                "sequence {seq} completed as a stale redelivery before its waiter registered; \
                 the write was not applied on this pass — retry"
            )))));
            return Ok(rx);
        }
        let (tx, rx) = oneshot::channel();
        if m.waiters.contains_key(&seq) {
            bail!("duplicate waiter for sequence {seq}");
        }
        if m.mutation_permits.contains_key(&seq) {
            bail!("duplicate mutation permit for sequence {seq}");
        }
        m.waiters.insert(seq, tx);
        m.mutation_permits.insert(seq, mutation_permit);
        Ok(rx)
    }

    /// Publish `entry`, wait for local apply, and return its outcome.
    ///
    /// Admission and local apply share [`SUBMIT_TIMEOUT`]. A stray sequence
    /// mismatch or apply-loop stall surfaces as a retryable 5xx instead of
    /// retaining a server task without a deadline.
    pub async fn submit(&self, entry: RaftLogEntry) -> Result<ApplyOutcome> {
        // Full local admission is a retryable refusal before this record can
        // consume a WAL sequence. Oversized and context-dependent records
        // preserve the old path until root wires durable preparation.
        let kind = crate::metrics::ApplyKind::from_entry(&entry);
        let admission_started_at = std::time::Instant::now();
        let submit_deadline = tokio::time::Instant::now() + SUBMIT_TIMEOUT;
        let admission_deadline = submit_deadline - LOCAL_CAPACITY_APPLY_RESERVE;
        let reservation = self
            .admit_local_record_with_relief(&entry, admission_deadline)
            .await?;
        if tokio::time::Instant::now() >= admission_deadline {
            return Err(anyhow::Error::new(SubmitStalled(
                "local admission used the submit deadline before WAL publication".into(),
            )));
        }
        // Keep the shared permit through publish AND local apply. An exclusive
        // restore fence can therefore observe one exact applied/WAL boundary:
        // no earlier submit remains in flight and no later submit has obtained
        // a sequence yet.
        let mutation_permit =
            tokio::time::timeout_at(admission_deadline, self.mutation_gate.shared())
                .await
                .map_err(|_| {
                    anyhow::Error::new(SubmitStalled(
                        "mutation permit was unavailable before the submit deadline".into(),
                    ))
                })??;
        self.engine.metrics().observe_coordinator_stage(
            kind,
            crate::metrics::CoordinatorStage::AdmissionToMutationGate,
            admission_started_at.elapsed(),
        );
        let (published_tx, published_rx) = oneshot::channel();
        let publisher = self
            .self_weak
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("write coordinator stopped before publish"))?;
        let wal = self.wal.clone();
        tokio::spawn(async move {
            // This task owns the shared permit from before WAL publication. A
            // caller may cancel after the WAL accepts its record, but it cannot
            // release the restore fence before sequence ownership is installed.
            let result = if tokio::time::Instant::now() >= admission_deadline {
                Err(anyhow::Error::new(SubmitStalled(
                    "submit deadline expired before WAL publication".into(),
                )))
            } else if let Some(reservation) = reservation {
                // The subscriber cannot take this ledger while publication is
                // still returning, so it sees the reservation for this exact
                // sequence before it can start local apply.
                let mut ledger = publisher.local_reservations.lock().await;
                match wal.publish(WalRecord::new(entry)).await {
                    Ok(seq) => {
                        if ledger.insert(seq, reservation).is_some() {
                            Err(anyhow::anyhow!(
                                "duplicate local reservation for sequence {seq}"
                            ))
                        } else {
                            drop(ledger);
                            publisher
                                .register_waiter(seq, mutation_permit)
                                .map(|receiver| (seq, receiver))
                        }
                    }
                    Err(error) => Err(error),
                }
            } else {
                match wal.publish(WalRecord::new(entry)).await {
                    Ok(seq) => publisher
                        .register_waiter(seq, mutation_permit)
                        .map(|receiver| (seq, receiver)),
                    Err(error) => Err(error),
                }
            };
            let _ = published_tx.send(result);
        });
        let (seq, rx) = match published_rx.await {
            Ok(Ok(pair)) => pair,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(anyhow::anyhow!(
                    "publish task stopped before registering a waiter"
                ));
            }
        };
        match tokio::time::timeout_at(submit_deadline, rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(anyhow::anyhow!(
                "apply loop stopped before sequence {seq} was applied"
            )),
            Err(_) => {
                // The waiter entry may still be sitting in `completions.waiters`
                // (a very-late `complete`/`complete_stale` will just find no live
                // receiver and drop the result) — nothing to clean up here beyond
                // returning the bounded error.
                Err(anyhow::Error::new(SubmitStalled(format!(
                    "timed out after {SUBMIT_TIMEOUT_SECS}s total waiting for sequence {seq} to apply"
                ))))
            }
        }
    }
}
