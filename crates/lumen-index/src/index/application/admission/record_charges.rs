//! Private ownership for a whole admitted record.
//!
//! This module deliberately separates an admission reservation from the
//! thread-affine apply interval. Callers must reserve before they acquire an
//! `ApplyLease`; a record that reprices returns its reservation untouched.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use crate::ingest::domain::change_budget::RetainedCharge;

#[derive(Default)]
struct State {
    next_batch: u64,
    active: Vec<RetainedCharge>,
    /// Metadata detached from a replacement Engine. This holds only Arcs, so
    /// adoption never copies the candidate's record payload or charges.
    adopted: Vec<Arc<DetachedRecordCharges>>,
    frozen: BTreeMap<u64, Arc<FrozenChargeGroup>>,
}

/// All metadata charge owners covered by one live checkpoint cut.
struct FrozenChargeGroup {
    direct: Vec<RetainedCharge>,
    adopted: Vec<Arc<DetachedRecordCharges>>,
}

/// Engine-wide metadata ownership for records whose effects are not represented
/// by a collection row (schema, request-id, truncate, and force-drop work).
/// Its vector moves at a capture cut; it never visits individual rows.
pub(in crate::index::application) struct RecordChargeJournal {
    inner: Arc<Mutex<State>>,
}

impl Default for RecordChargeJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordChargeJournal {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(State::default())),
        }
    }

    /// One clone per accepted record, regardless of how many collection rows
    /// carry clones of the same charge.
    pub(super) fn retain(&self, charge: RetainedCharge) {
        self.inner
            .lock()
            .expect("record charge journal poisoned")
            .active
            .push(charge);
    }

    pub(super) fn freeze(&self) -> FrozenRecordCharges {
        let mut state = self.inner.lock().expect("record charge journal poisoned");
        state.next_batch = state
            .next_batch
            .checked_add(1)
            .expect("record charge batch exhausted");
        let id = state.next_batch;
        let charges = Arc::new(FrozenChargeGroup {
            direct: std::mem::take(&mut state.active),
            adopted: std::mem::take(&mut state.adopted),
        });
        state.frozen.insert(id, charges.clone());
        FrozenRecordCharges {
            journal: Arc::downgrade(&self.inner),
            id,
            charges,
        }
    }

    pub(super) fn acknowledge_through(&self, frozen: &FrozenRecordCharges) -> bool {
        let Some(inner) = frozen.journal.upgrade() else {
            return false;
        };
        if !Arc::ptr_eq(&self.inner, &inner) {
            return false;
        }
        let mut state = self.inner.lock().expect("record charge journal poisoned");
        let found = state.frozen.contains_key(&frozen.id);
        // A restore can detach this cut while a newer record freezes into the
        // same journal.  A stale handle must not use its older id to remove
        // that newer frozen work.
        if found {
            state.frozen.retain(|id, _| *id > frozen.id);
        }
        found
    }

    /// Detach all charge owners held by the pre-restore journal in O(1).
    ///
    /// The live journal keeps its batch counter so a stale frozen handle can
    /// never alias a later capture. The returned guard owns the old active and
    /// frozen containers until the restore path has dropped the replaced live
    /// payload and every retained capture clone.
    #[must_use = "the detached guard owns record charges until it is dropped"]
    pub(in crate::index::application) fn discard_for_restore(&self) -> DetachedRecordCharges {
        let mut state = self.inner.lock().expect("record charge journal poisoned");
        DetachedRecordCharges {
            active: std::mem::take(&mut state.active),
            adopted: std::mem::take(&mut state.adopted),
            frozen: std::mem::take(&mut state.frozen),
        }
    }

    /// Move a replacement Engine's metadata ownership into this journal in
    /// O(1). The source owner's retained charges keep their original budget
    /// identity; this journal adds only the Arc that keeps them alive until a
    /// live checkpoint acknowledges the adopted cut.
    pub(in crate::index::application) fn adopt_for_restore(&self, detached: DetachedRecordCharges) {
        self.inner
            .lock()
            .expect("record charge journal poisoned")
            .adopted
            .push(Arc::new(detached));
    }
}

/// Detached metadata ownership from the state replaced by restore.
///
/// This has no acknowledgement method. Dropping it releases only the journal's
/// own handles; retryable frozen capture clones remain their real owners.
pub(in crate::index::application) struct DetachedRecordCharges {
    active: Vec<RetainedCharge>,
    adopted: Vec<Arc<DetachedRecordCharges>>,
    frozen: BTreeMap<u64, Arc<FrozenChargeGroup>>,
}

/// Retryable frozen ownership. Cloning is O(1), and dropping a publication
/// handle never frees the contained charges while another handle survives.
pub(super) struct FrozenRecordCharges {
    journal: Weak<Mutex<State>>,
    id: u64,
    #[allow(dead_code)]
    charges: Arc<FrozenChargeGroup>,
}

impl Clone for FrozenRecordCharges {
    fn clone(&self) -> Self {
        Self {
            journal: self.journal.clone(),
            id: self.id,
            charges: self.charges.clone(),
        }
    }
}

#[cfg(test)]
mod tests;
