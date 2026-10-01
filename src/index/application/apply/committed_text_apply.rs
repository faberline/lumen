//! Apply borrowed Text input through immutable prepared rows.
//!
//! The small Index entry carries identity, type and version metadata only.
//! Its String placeholders are never persistence input. The caller retains
//! the original scanner bytes through the same-lease completion callback.

use anyhow::{anyhow, bail, Result};

use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::index::application::engine::Engine;
use crate::index::application::text_preparation;
use crate::index::domain::field_index::FieldIndex;
use crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner;
use crate::shared_kernel::capture_barrier::ApplyLease;

impl Engine {
    pub(crate) fn try_apply_committed_index(
        &self,
        scanner: &FastIndexScanner<'_>,
        sequence: u64,
        complete: impl FnOnce(&ApplyLease<'_>, Result<ApplyOutcome>),
    ) -> Result<bool> {
        self.try_apply_committed_index_with_capacity_owner(
            scanner,
            sequence,
            || bail!("committed layer capacity needs a caller-owned maintainer"),
            complete,
        )
    }

    pub(crate) fn try_apply_committed_index_with_capacity_owner(
        &self,
        scanner: &FastIndexScanner<'_>,
        sequence: u64,
        mut ensure_owner: impl FnMut() -> Result<()>,
        complete: impl FnOnce(&ApplyLease<'_>, Result<ApplyOutcome>),
    ) -> Result<bool> {
        let mut complete = Some(complete);
        if self.try_apply_committed_scalar_with_capacity_owner(
            scanner,
            sequence,
            &mut ensure_owner,
            |apply, outcome| {
                complete.take().expect("one completion")(apply, outcome);
            },
        )? {
            return Ok(true);
        }
        self.try_apply_committed_text(scanner, |apply, outcome| {
            complete.take().expect("one completion")(apply, outcome);
        })
    }

    fn try_apply_committed_text(
        &self,
        scanner: &FastIndexScanner<'_>,
        complete: impl FnOnce(&ApplyLease<'_>, Result<ApplyOutcome>),
    ) -> Result<bool> {
        let metadata = text_preparation::borrowed_text_metadata_bound(scanner)?
            .checked_mul(2)
            .ok_or(RecordAdmissionError::Overflow)?;
        let initial = metadata
            .checked_add(text_preparation::TEXT_SCRATCH_BYTES)
            .ok_or(RecordAdmissionError::Overflow)?;
        let request = self.record_ram_request_from_bound(initial, 0);
        let mut reservation = self.wait_reserve_record_ram(&request)?;
        'prepare: loop {
            if reservation.bytes() < initial {
                reservation
                    .wait_grow_to(initial)
                    .map_err(RecordAdmissionError::Capacity)?;
            }
            let (entry, rows) = match self.prepare_borrowed_text_rows(scanner, &mut reservation) {
                Ok(Some(prepared)) => prepared,
                Ok(None) => return Ok(false),
                Err(error) => {
                    let Some(required) = error
                        .downcast_ref::<text_preparation::RequiredBorrowedTextWorkspace>()
                        .map(|workspace| workspace.required_bytes)
                    else {
                        return Err(error);
                    };
                    reservation
                        .wait_grow_to(required)
                        .map_err(RecordAdmissionError::Capacity)?;
                    continue 'prepare;
                }
            };
            loop {
                // Pin the old file owners that dispatch can replace. Their
                // last drop must happen after apply, not during a state write.
                let (revision, old_rows) = {
                    let state = self.state.read().map_err(|_| anyhow!("state poisoned"))?;
                    let mut old_rows = Vec::with_capacity(scanner.cost().item_count);
                    if let Some(coll) = state.collections.get(scanner.collection_id()) {
                        for item in scanner.items() {
                            let Some(id) = coll.interner.id(item.external_id) else {
                                continue;
                            };
                            if let Some(FieldIndex::Text { idx, .. }) = coll.fields.get(item.field)
                            {
                                if let Some(row) = idx.staged_rows.get(&id) {
                                    old_rows.push(row.clone());
                                }
                            }
                        }
                    }
                    (self.capture_barrier.apply_revision(), old_rows)
                };
                // Placeholder strings allocate no token map. Normal live
                // metadata and error-prefix costs still come from the same
                // estimator used by ordinary Index admission.
                let cost = match self.estimate_record_cost(&entry) {
                    crate::ingest::domain::change_record_cost::RecordEstimate::Ready(cost) => cost,
                    crate::ingest::domain::change_record_cost::RecordEstimate::Retain { cause } => {
                        return Err(RecordAdmissionError::NeedsPreparation(cause).into());
                    }
                };
                let retained = Self::record_owned_bytes(&entry)?
                    .checked_add(cost.active)
                    .and_then(|n| n.checked_add(cost.frozen))
                    .and_then(|n| n.checked_add(cost.prepublish))
                    .and_then(|n| n.checked_add(metadata))
                    .and_then(|n| n.checked_add(rows.retained_reader_bytes()))
                    .ok_or(RecordAdmissionError::Overflow)?;
                if reservation.bytes() < retained {
                    drop(old_rows);
                    reservation
                        .wait_grow_to(retained)
                        .map_err(RecordAdmissionError::Capacity)?;
                    continue;
                }
                // All IO and token workspaces have gone. Only the private
                // entry, row owners, reader metadata and future changes remain.
                reservation
                    .finish_borrowed_preparation(retained)
                    .map_err(RecordAdmissionError::Capacity)?;
                let apply = self.capture_barrier.apply();
                if !rows.matches(self) {
                    drop(apply);
                    drop(old_rows);
                    drop(rows);
                    drop(entry);
                    reservation
                        .finish_borrowed_preparation(reservation.bytes().min(initial))
                        .map_err(RecordAdmissionError::Capacity)?;
                    continue 'prepare;
                }
                if self.capture_barrier.apply_revision() != revision {
                    drop(apply);
                    drop(old_rows);
                    continue;
                }
                let charge = self.retain_borrowed_reservation(reservation)?;
                let outcome = self.dispatch_raft_entry(entry, Some(&charge), Some(&rows));
                complete(&apply, outcome);
                drop(apply);
                drop(old_rows);
                drop(rows);
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests;
