//! The admission door: record RAM reserved for a delivery, or a whole record
//! reserved before publication, falling back to the raw and transport floor
//! when its exact price needs context, so no record is ever published with
//! nothing reserved.

use crate::index::application::admission::record_reservation::{
    RecordRamRequest, RecordReservation,
};
use crate::index::application::admission::RecordAdmissionError;
use crate::index::application::engine::Engine;
use crate::shared_kernel::log_entry::RaftLogEntry;

impl Engine {
    /// Price only the one owned, decoded delivery. Normalized changes and
    /// transport clones do not exist yet. `begin_admitted_record` must grow this
    /// reservation before creating either kind of payload.
    pub(crate) fn record_ram_request(
        &self,
        entry: &RaftLogEntry,
        extra_owned: usize,
    ) -> Result<RecordRamRequest, RecordAdmissionError> {
        Ok(self.record_ram_request_from_bound(Self::record_owned_bytes(entry)?, extra_owned))
    }

    /// The native WAL computes this bound while it owns the source record.
    /// Using it does not clone or decode the pending delivery.
    pub(crate) fn record_ram_request_from_bound(
        &self,
        bytes: usize,
        extra_owned: usize,
    ) -> RecordRamRequest {
        RecordRamRequest {
            runtime: self.changes.id,
            bytes,
            extra_owned,
        }
    }

    pub(crate) fn try_reserve_record_ram(
        &self,
        request: &RecordRamRequest,
    ) -> Result<RecordReservation, RecordAdmissionError> {
        self.reserve_record_ram(request, false)
    }

    /// The caller has released the delivered working copy and pinned its WAL
    /// source before this wait. No Engine apply lease may be held here.
    pub(crate) fn wait_reserve_record_ram(
        &self,
        request: &RecordRamRequest,
    ) -> Result<RecordReservation, RecordAdmissionError> {
        self.request_pending_checkpoint();
        self.reserve_record_ram(request, true)
    }

    fn reserve_record_ram(
        &self,
        request: &RecordRamRequest,
        wait: bool,
    ) -> Result<RecordReservation, RecordAdmissionError> {
        if request.runtime != self.changes.id {
            return Err(RecordAdmissionError::WrongEngine);
        }
        let bytes = request
            .bytes
            .checked_add(request.extra_owned)
            .ok_or(RecordAdmissionError::Overflow)?;
        let reservation = if wait {
            self.changes.owner.wait_reserve(bytes)
        } else {
            self.changes.owner.try_reserve(bytes)
        }
        .map_err(RecordAdmissionError::Capacity)?;
        Ok(RecordReservation {
            runtime: request.runtime,
            reservation,
            extra_owned: request.extra_owned,
            ngram_cost_workspace_bytes: 0,
            staged_text: false,
            preparation_bytes: 0,
            prepared_text: None,
            wait_for_preparation: true,
        })
    }

    /// No apply lease is taken here. Full local admission maps to HTTP429;
    /// oversized input is handed to durable preparation.
    ///
    /// Exact domain pricing can need context this door does not have yet
    /// (`RecordAdmissionError::NeedsPreparation`, e.g. an unresolved schema
    /// or live-coverage read) — that is never a reason to discard a valid
    /// record, but it also must never become a **publish with zero
    /// reservation**: the WAL delivery loop's post-publication path
    /// (`try_reserve_record_ram` / `wait_reserve_record_ram`) has no bound
    /// on how long it waits for capacity, and a record with no reservation
    /// at all reaches exactly that unbounded wait. So on `NeedsPreparation`
    /// this reserves the same raw+transport floor apply already knows how
    /// to charge before it can price exactly — the same bound
    /// `raft_sm::decode_admitted` reserves before it decodes — via
    /// [`Self::minimal_context_pending_reservation`]. Any growth beyond that
    /// floor is priced later, with real context, by `begin_admitted_record`;
    /// a Full observed even at this minimal floor is refused here, before
    /// publication, like any other capacity failure.
    pub(crate) fn try_reserve_record(
        &self,
        entry: &RaftLogEntry,
        extra_owned: usize,
    ) -> Result<RecordReservation, RecordAdmissionError> {
        if self.may_need_default_ngram_workspace(entry) {
            // Raw input is retained while exact pricing runs. Returning this
            // reservation is permission to publish, so finish normalized pricing
            // before returning it, not only later in begin_admitted_record.
            let request = self.record_ram_request(entry, extra_owned)?;
            let mut reserved = self.try_reserve_record_ram(&request)?;
            return match self.price_before_publication(entry, &mut reserved) {
                Ok(()) => Ok(reserved),
                Err(RecordAdmissionError::NeedsPreparation(_)) => {
                    // `reserved` already holds exactly the raw+transport
                    // floor; keep that ownership rather than dropping and
                    // re-acquiring it, which would open a capacity race.
                    Ok(reserved)
                }
                Err(error) => Err(error),
            };
        }
        let ordinary = match self.record_memory_bound(entry, extra_owned, false) {
            Ok(bound) => bound,
            Err(RecordAdmissionError::NeedsPreparation(_)) => {
                return self.minimal_context_pending_reservation(entry, extra_owned);
            }
            Err(error) => return Err(error),
        };
        let staged_text = ordinary > crate::ingest::domain::change_budget::HARD_LIMIT;
        let bytes = if staged_text {
            self.record_memory_bound(entry, extra_owned, true)?
        } else {
            ordinary
        };
        let reservation = self
            .changes
            .owner
            .try_reserve(bytes)
            .map_err(RecordAdmissionError::Capacity)?;
        Ok(RecordReservation {
            runtime: self.changes.id,
            reservation,
            extra_owned,
            ngram_cost_workspace_bytes: 0,
            staged_text,
            preparation_bytes: 0,
            prepared_text: None,
            wait_for_preparation: true,
        })
    }

    /// The raw+transport floor for a record whose exact price needs
    /// context this door cannot resolve. See [`Self::try_reserve_record`].
    /// A Full here is a genuine capacity refusal — classified through the
    /// same [`RecordAdmissionError::Capacity`] path as every other
    /// prepublication admission failure — never a silent zero-reservation
    /// publish.
    fn minimal_context_pending_reservation(
        &self,
        entry: &RaftLogEntry,
        extra_owned: usize,
    ) -> Result<RecordReservation, RecordAdmissionError> {
        let request = self.record_ram_request(entry, extra_owned)?;
        self.try_reserve_record_ram(&request)
    }
}
