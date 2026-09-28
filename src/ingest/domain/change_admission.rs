//! Ownership bridge for a committed in-memory record and its durable stage.
//!
//! A committed budget charge stays attached to its payload while staging reads
//! it. A failed stage returns that same carrier. A successful stage drops the
//! payload before it returns the durable receipt, then consumes the narrowly
//! typed pre-apply RAM charge. The durable stage keeps only disk metadata.

use std::fmt;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::committed_stage::{DurableStage, SourceIdentity, StageStore};
use crate::ingest::domain::change_budget::{AdmissionError, RamCharge, Reservation};

/// A local write could not reserve the pending-change budget before it was
/// published to the WAL. Full, oversized, and unrepresentable local requests
/// are refused here. Committed records never use this classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PendingChangeCapacity {
    Local {
        requested: usize,
        used: usize,
        hard_limit: usize,
    },
    Oversized {
        requested: usize,
        hard_limit: usize,
    },
    Overflow,
    #[cfg(feature = "raft-wal")]
    Raft {
        reason: String,
    },
}

impl PendingChangeCapacity {
    /// Classify every local admission failure before publication.
    pub(crate) fn from_prepublication(error: AdmissionError) -> Result<Self, AdmissionError> {
        match error {
            AdmissionError::Full {
                requested,
                used,
                hard_limit,
            } => Ok(Self::Local {
                requested,
                used,
                hard_limit,
            }),
            AdmissionError::Oversized {
                requested,
                hard_limit,
            } => Ok(Self::Oversized {
                requested,
                hard_limit,
            }),
            other => Err(other),
        }
    }

    pub(crate) fn from_record_prepublication(
        error: &crate::storage::RecordAdmissionError,
    ) -> Option<Self> {
        match error {
            crate::storage::RecordAdmissionError::Capacity(error) => {
                Self::from_prepublication(*error).ok()
            }
            crate::storage::RecordAdmissionError::Overflow => Some(Self::Overflow),
            _ => None,
        }
    }
}

impl fmt::Display for PendingChangeCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local {
                requested,
                used,
                hard_limit,
            } => write!(
                formatter,
                "pending change capacity is full: requested {} bytes with {} of {} bytes in use",
                requested, used, hard_limit
            ),
            Self::Oversized {
                requested,
                hard_limit,
            } => write!(
                formatter,
                "pending change is too large: requested {} bytes exceeds {} bytes",
                requested, hard_limit
            ),
            Self::Overflow => formatter.write_str("pending change size overflow"),
            #[cfg(feature = "raft-wal")]
            Self::Raft { reason } => formatter.write_str(reason),
        }
    }
}

impl std::error::Error for PendingChangeCapacity {}

#[cfg(test)]
mod prepublication_tests {
    use super::*;

    #[test]
    fn prepublication_capacity_classifies_full_and_oversized_without_used_bytes() {
        assert!(matches!(
            PendingChangeCapacity::from_prepublication(AdmissionError::Full {
                requested: 8,
                used: 7,
                hard_limit: 10,
            }),
            Ok(PendingChangeCapacity::Local { .. })
        ));
        assert!(matches!(
            PendingChangeCapacity::from_prepublication(AdmissionError::Oversized {
                requested: 11,
                hard_limit: 10,
            }),
            Ok(PendingChangeCapacity::Oversized { .. })
        ));
        assert!(matches!(
            PendingChangeCapacity::from_record_prepublication(
                &crate::storage::RecordAdmissionError::Overflow
            ),
            Some(PendingChangeCapacity::Overflow)
        ));
        assert!(PendingChangeCapacity::from_record_prepublication(
            &crate::storage::RecordAdmissionError::Capacity(AdmissionError::Retired)
        )
        .is_none());
        assert!(PendingChangeCapacity::from_record_prepublication(
            &crate::storage::RecordAdmissionError::WrongEngine
        )
        .is_none());
    }
}

pub(crate) struct CommittedRecord<P> {
    charge: RamCharge,
    payload: P,
    sequence: u64,
    engine_epoch: u64,
    source: SourceIdentity,
}

pub(crate) struct StagedCommitted {
    stage: DurableStage,
}

pub(crate) struct StageFailure<P> {
    error: io::Error,
    record: CommittedRecord<P>,
}

/// Admission can race a restore retiring the old budget owner. Return the
/// committed bytes and their identity so the apply worker can route or stage
/// them under the replacement owner. An error must not destroy this payload.
pub(crate) struct AdmissionFailure<P> {
    error: AdmissionError,
    payload: P,
    sequence: u64,
    engine_epoch: u64,
    source: SourceIdentity,
}

impl<P> fmt::Debug for AdmissionFailure<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionFailure")
            .field("error", &self.error)
            .field("sequence", &self.sequence)
            .field("engine_epoch", &self.engine_epoch)
            .finish_non_exhaustive()
    }
}

impl<P> AdmissionFailure<P> {
    pub(crate) fn into_parts(self) -> (AdmissionError, P, u64, u64, SourceIdentity) {
        (
            self.error,
            self.payload,
            self.sequence,
            self.engine_epoch,
            self.source,
        )
    }
}

/// A retryable owned payload that writes its exact wire representation into a
/// bounded staging sink. `Record` can implement this with `serde_json::to_writer`
/// and therefore never needs a full encoded `Vec`.
pub(crate) trait StagePayload {
    fn write_stage(&mut self, output: &mut dyn Write) -> io::Result<()>;
}

/// Adapter for existing rewindable readers. New production record payloads can
/// implement [`StagePayload`] directly with their serializer.
pub(crate) struct RewindablePayload<P>(pub(crate) P);

impl<P: Read + Seek> StagePayload for RewindablePayload<P> {
    fn write_stage(&mut self, output: &mut dyn Write) -> io::Result<()> {
        self.0.seek(SeekFrom::Start(0))?;
        io::copy(&mut self.0, output).map(|_| ())
    }
}

/// Private proof emitted only after `CommittedRecord::stage` has obtained a
/// durable receipt and destroyed the RAM payload. Its fields cannot be forged
/// outside this module.
pub(crate) struct RamReleased {
    sequence: u64,
    engine_epoch: u64,
    source: SourceIdentity,
}

impl RamReleased {
    fn after_payload_drop(stage: &DurableStage) -> Self {
        Self {
            sequence: stage.sequence(),
            engine_epoch: stage.engine_epoch(),
            source: stage.source().clone(),
        }
    }
    pub(crate) fn matches(
        &self,
        sequence: u64,
        engine_epoch: u64,
        source: &SourceIdentity,
    ) -> bool {
        self.sequence == sequence && self.engine_epoch == engine_epoch && &self.source == source
    }
}

impl<P> fmt::Debug for StageFailure<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StageFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<P> CommittedRecord<P> {
    /// Commit an already admitted reservation and attach it to the payload.
    pub(crate) fn new(
        reservation: Reservation,
        payload: P,
        sequence: u64,
        engine_epoch: u64,
        source: SourceIdentity,
    ) -> Result<Self, AdmissionFailure<P>> {
        let charge = match reservation.commit_ram(sequence, engine_epoch, source.clone()) {
            Ok(charge) => charge,
            Err(error) => {
                return Err(AdmissionFailure {
                    error,
                    payload,
                    sequence,
                    engine_epoch,
                    source,
                })
            }
        };
        Ok(Self {
            charge,
            payload,
            sequence,
            engine_epoch,
            source,
        })
    }

    pub(crate) fn charge_bytes(&self) -> usize {
        self.charge.bytes()
    }
}

impl<P: StagePayload> CommittedRecord<P> {
    /// Stream the owned payload. Failure returns the exact carrier for retry.
    pub(crate) fn stage(self, store: &StageStore) -> Result<StagedCommitted, StageFailure<P>> {
        let Self {
            charge,
            mut payload,
            sequence,
            engine_epoch,
            source,
        } = self;
        match store.stage_with_writer(sequence, engine_epoch, source.clone(), |output| {
            payload.write_stage(output)
        }) {
            Ok(stage) => {
                drop(payload);
                let proof = RamReleased::after_payload_drop(&stage);
                if charge.release_after_stage(proof).is_err() {
                    panic!(
                        "the carrier's own durable proof must release its matching live RAM charge"
                    );
                }
                Ok(StagedCommitted { stage })
            }
            Err(error) => Err(StageFailure {
                error,
                record: Self {
                    charge,
                    payload,
                    sequence,
                    engine_epoch,
                    source,
                },
            }),
        }
    }
}

impl<P> StageFailure<P> {
    pub(crate) fn error(&self) -> &io::Error {
        &self.error
    }
    pub(crate) fn into_record(self) -> CommittedRecord<P> {
        self.record
    }
}

impl StagedCommitted {
    pub(crate) fn stage(&self) -> &DurableStage {
        &self.stage
    }
    pub(crate) fn into_stage(self) -> DurableStage {
        self.stage
    }
}

#[cfg(test)]
mod tests;
