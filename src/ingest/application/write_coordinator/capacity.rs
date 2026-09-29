//! Capacity relief: the checkpoint owner a committed record waits on for
//! capacity, the background task that stages applied sources to free it, and
//! the numeric admission-refusal diagnostics.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;

use crate::ingest::application::write_coordinator::WriteCoordinator;

pub(super) fn claim_diagnostic_refusal_revision(last_seen: &AtomicU64, revision: u64) -> bool {
    // Store revision + 1, leaving zero as the unclaimed sentinel. This also
    // lets the bounded unlinked event use its required numeric revision 0.
    let claimed = revision.saturating_add(1);
    last_seen.fetch_max(claimed, Ordering::AcqRel) < claimed
}

impl WriteCoordinator {
    /// Start or reuse the native checkpoint owner before this coordinator waits
    /// on capacity for a committed record. This holds no apply lease.
    pub(super) fn ensure_capacity_owner(&self) -> Result<()> {
        let mut owner = self
            .layer_capacity_owner
            .lock()
            .map_err(|_| anyhow::anyhow!("capacity owner poisoned"))?;
        crate::segment_capacity::Fallback::ensure(&mut owner, &self.engine, None)
    }

    pub(super) fn trace_admission_refusal(
        &self,
        revision: Option<u64>,
        requested_bytes: usize,
        used_bytes: usize,
        hard_limit_bytes: usize,
    ) {
        let capacity_request_present = revision.is_some();
        let revision = revision.unwrap_or_default();
        #[cfg(test)]
        let capture = self.diagnostic_capture.lock().ok().and_then(|token| *token);
        if !(crate::persistence::infrastructure::segment_rdb_store::diagnostic::checkpoint_diagnostic_enabled()
            || cfg!(test) && {
                #[cfg(test)]
                {
                    capture.is_some()
                }
                #[cfg(not(test))]
                {
                    false
                }
            })
            || !claim_diagnostic_refusal_revision(&self.diagnostic_refusal_revision, revision)
        {
            return;
        }
        #[cfg(test)]
        if let Some(token) = capture {
            crate::persistence::infrastructure::segment_rdb_store::diagnostic::send_numeric_event(
                token,
                crate::persistence::infrastructure::segment_rdb_store::diagnostic::NumericCanonicalEvent::Refusal {
                    revision,
                    present: capacity_request_present,
                    requested: requested_bytes,
                    used: used_bytes,
                    hard_limit: hard_limit_bytes,
                },
            );
        }
        tracing::info!(
            event = "segment_capacity_admission_refusal",
            capacity_request_revision = revision,
            capacity_request_present,
            requested_bytes,
            used_bytes,
            hard_limit_bytes,
            "segment capacity admission refusal"
        );
    }

    #[cfg(test)]
    pub(crate) fn set_diagnostic_capture(
        &self,
        token: crate::persistence::infrastructure::segment_rdb_store::diagnostic::DiagnosticCaptureToken,
    ) {
        *self.diagnostic_capture.lock().unwrap() = Some(token);
    }

    /// Capacity can be held by later publications or by already applied
    /// sources pinned by a slow reader. This task needs neither an apply lease
    /// nor MutationGate. Source charges remain until native replacement proves
    /// that the original payload has left RAM.
    ///
    /// `capacity_relief_requested` (a pre-publication door refusal, e.g. an
    /// HTTP 429) always drives the applied-source staging scan below — it
    /// releases sources of records already folded into the engine and needs
    /// no waiter. It alone must never steal a *pending* local reservation
    /// from `local_reservations`: that ledger holds already-admitted records
    /// simply waiting their turn to apply, not spare capacity. Stealing one
    /// forces its apply to re-reserve from scratch and can park it in
    /// `wait_reserve_record_ram` until checkpoint frees bytes. The ledger
    /// steal below runs only when `has_capacity_waiters()` reports a real
    /// committed apply or replay blocked on capacity.
    pub(super) fn start_capacity_relief(coord: &Arc<Self>) {
        let weak = Arc::downgrade(coord);
        let mut staged_through = coord.applied_seq();
        tokio::spawn(async move {
            let mut requested_through = staged_through;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                let Some(coord) = weak.upgrade() else {
                    return;
                };
                let requested = coord
                    .capacity_relief_requested
                    .swap(false, Ordering::AcqRel);
                let waiting = coord.engine.has_capacity_waiters();
                if requested || waiting {
                    requested_through = requested_through.max(coord.applied_seq());
                }
                // Scan each applied sequence once, in bounded groups. Even a
                // refused request with no waiting apply must finish its group.
                // Checkpoint may have retired the journal while native readers
                // still retain these sources and their independent charges.
                for _ in 0..64 {
                    if staged_through >= requested_through {
                        break;
                    }
                    let seq = staged_through + 1;
                    match coord.wal.stage_source(seq).await {
                        Ok(None) => staged_through = seq,
                        Ok(Some(proof)) if proof.sequence() == seq => staged_through = seq,
                        Ok(Some(_)) => {
                            tracing::error!(
                                seq,
                                "WAL source returned an offload proof for another sequence"
                            );
                            break;
                        }
                        Err(error) => {
                            tracing::warn!(seq, %error, "applied WAL source staging failed; retaining source charge");
                            break;
                        }
                    }
                }
                if !waiting {
                    // A door refusal (`requested`) with no committed apply
                    // blocked on capacity has already been served above: the
                    // applied-source staging scan advanced `staged_through`.
                    // Stealing a pending ledger reservation here would only
                    // rob an already-admitted, not-yet-applied local record
                    // that is simply waiting its turn — forcing its apply to
                    // re-reserve from scratch and park in
                    // `wait_reserve_record_ram` for no genuine waiter.
                    continue;
                }
                let mut ledger = coord.local_reservations.lock().await;
                // Select without copying the pending ledger into a second buffer.
                // One attempt per tick lets the waiting head take released capacity.
                if let Some(seq) = ledger
                    .iter()
                    .max_by_key(|(&seq, reservation)| (reservation.bytes(), std::cmp::Reverse(seq)))
                    .map(|(&seq, _)| seq)
                {
                    match coord.wal.stage_source(seq).await {
                        Ok(Some(proof)) if proof.sequence() == seq => {
                            // No apply worker can take this reservation while the ledger
                            // is locked. Its decoded delivery and normalized data do not
                            // exist yet. The source payload is now gone as well.
                            drop(ledger.remove(&seq));
                        }
                        Ok(None) => {}
                        Ok(Some(_)) => {
                            tracing::error!(
                                seq,
                                "WAL source returned an offload proof for another sequence"
                            );
                        }
                        Err(error) => {
                            tracing::warn!(seq, %error, "WAL source staging failed; retaining pending reservation");
                        }
                    }
                }
            }
        });
    }
}
