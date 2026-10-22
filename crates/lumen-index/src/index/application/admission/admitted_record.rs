//! An admitted record at the apply boundary: checked against the Engine that
//! admitted it and revalidated against the state it will change, handed back
//! for growth outside the lease when a stale schema or larger coverage needs
//! more, and dispatched once under its guard, its charge retained first.

use std::cell::Cell;

use crate::index::application::admission::record_reservation::RecordReservation;
use crate::index::application::admission::{RecordAdmissionError, RecordApplyGuard, RepriceRecord};
use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::AdmissionError;
use crate::shared_kernel::log_entry::RaftLogEntry;

impl Engine {
    /// Revalidate against the state that will be changed. A stale schema or
    /// larger deletion coverage returns ownership for growth outside this lease.
    pub(crate) fn begin_admitted_record(
        &self,
        entry: RaftLogEntry,
        mut reserved: RecordReservation,
    ) -> Result<RecordApplyGuard<'_>, RepriceRecord> {
        if reserved.runtime != self.changes.id {
            return Err(RepriceRecord {
                entry,
                reservation: reserved,
                required: None,
                error: RecordAdmissionError::WrongEngine,
            });
        }
        loop {
            if let Err((required, error)) = self.ensure_ngram_cost_workspace(&entry, &mut reserved)
            {
                return Err(RepriceRecord {
                    entry,
                    reservation: reserved,
                    required,
                    error,
                });
            }
            // Decide representation before taking the apply lease. External WAL
            // delivery starts with only its raw reservation and reaches this path too.
            if !reserved.staged_text
                && self
                    .record_memory_bound_with_workspace(&entry, &reserved, false)
                    .is_ok_and(|bytes| {
                        bytes.saturating_add(reserved.ngram_cost_workspace_bytes)
                            > crate::ingest::domain::change_budget::HARD_LIMIT
                    })
            {
                reserved.staged_text = true;
            }
            if reserved.staged_text {
                let minimum = match self.record_memory_bound(&entry, reserved.extra_owned, true) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        return Err(RepriceRecord {
                            entry,
                            reservation: reserved,
                            required: None,
                            error,
                        })
                    }
                };
                if reserved.bytes() < minimum {
                    return Err(RepriceRecord {
                        entry,
                        reservation: reserved,
                        required: Some(minimum),
                        error: RecordAdmissionError::Capacity(AdmissionError::Full {
                            requested: minimum,
                            used: self.changes.budget.snapshot().total,
                            hard_limit: crate::ingest::domain::change_budget::HARD_LIMIT,
                        }),
                    });
                }
                if reserved.prepared_text.is_none() {
                    match self.prepare_text_rows(&entry, &mut reserved) {
                        Ok(rows) => reserved.prepared_text = Some(rows),
                        Err(error) => {
                            let error = error
                                .downcast_ref::<RecordAdmissionError>()
                                .cloned()
                                .unwrap_or_else(|| {
                                    RecordAdmissionError::Preparation(error.to_string())
                                });
                            let error = reserved
                                .discard_preparation()
                                .err()
                                .map(RecordAdmissionError::Capacity)
                                .unwrap_or(error);
                            return Err(RepriceRecord {
                                entry,
                                reservation: reserved,
                                required: None,
                                error,
                            });
                        }
                    }
                }
            }
            let apply = self.capture_barrier.apply();
            if reserved
                .prepared_text
                .as_ref()
                .is_some_and(|rows| !rows.matches(self))
            {
                drop(apply);
                if let Err(error) = reserved.discard_preparation() {
                    return Err(RepriceRecord {
                        entry,
                        reservation: reserved,
                        required: None,
                        error: RecordAdmissionError::Capacity(error),
                    });
                }
                continue;
            }
            let required = match self
                .record_memory_bound_with_workspace(&entry, &reserved, reserved.staged_text)
                .and_then(|bytes| {
                    bytes
                        .checked_add(reserved.preparation_bytes)
                        .ok_or(RecordAdmissionError::Overflow)
                }) {
                Ok(required) => required,
                Err(error) => {
                    drop(apply);
                    return Err(RepriceRecord {
                        entry,
                        reservation: reserved,
                        required: None,
                        error,
                    });
                }
            };
            // Keep room for another exact state recheck when ownership must
            // return to the apply caller for growth outside this lease.
            let retry_required = match required.checked_add(reserved.ngram_cost_workspace_bytes) {
                Some(bytes) => bytes,
                None => {
                    drop(apply);
                    return Err(RepriceRecord {
                        entry,
                        reservation: reserved,
                        required: None,
                        error: RecordAdmissionError::Overflow,
                    });
                }
            };
            if required > reserved.bytes() {
                let error = RecordAdmissionError::Capacity(AdmissionError::Full {
                    requested: retry_required,
                    used: self.changes.budget.snapshot().total,
                    hard_limit: crate::ingest::domain::change_budget::HARD_LIMIT,
                });
                drop(apply);
                return Err(RepriceRecord {
                    entry,
                    reservation: reserved,
                    required: Some(retry_required),
                    error,
                });
            }
            // Exact pricing's fixed table is no longer alive. Remove its
            // workspace and any old normalized overestimate before retention.
            if reserved.ngram_cost_workspace_bytes != 0 {
                reserved
                    .reservation
                    .shrink_to(required)
                    .expect("exact final bound was checked against reserved ownership");
                reserved.ngram_cost_workspace_bytes = 0;
            }
            // EngineChanges never retires an owner while it still has reservations.
            // Thus this conversion cannot lose ownership to a concurrent restore.
            let charge = reserved
                .reservation
                .commit_retained()
                .expect("live Engine reservation owner cannot retire during apply preparation");
            return Ok(RecordApplyGuard {
                entry: Some(entry),
                prepared_text: reserved.prepared_text,
                charge,
                runtime: self.changes.id,
                used: Cell::new(false),
                apply,
            });
        }
    }

    pub(crate) fn apply_prepared_raft_entry(
        &self,
        prepared: &mut RecordApplyGuard<'_>,
    ) -> anyhow::Result<crate::index::application::engine::raft_dispatch::ApplyOutcome> {
        anyhow::ensure!(
            prepared.runtime == self.changes.id,
            "prepared record belongs to another Engine"
        );
        anyhow::ensure!(
            !prepared.used.replace(true),
            "prepared record was already applied"
        );
        // Retain before dispatch: a normal validation error can follow earlier
        // mutations or metadata changes, and must not release their charge.
        self.changes.records.retain(prepared.charge.clone());
        self.dispatch_raft_entry(
            prepared
                .entry
                .take()
                .expect("prepared entry already applied"),
            Some(&prepared.charge),
            prepared.prepared_text.as_ref(),
        )
    }
}
