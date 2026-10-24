//! Pure domain representation of a durable rebuild operation.
//!
//! Tracks progress and state transitions across phases:
//! `Pending -> Streaming -> Sealing -> Done`, plus `Cancelled` and `Failed`.

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests;

/// Rebuild operation phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebuildPhase {
    Pending,
    Streaming,
    Sealing,
    Done,
    Cancelled,
    Failed,
}

impl RebuildPhase {
    /// Returns true if this phase is a terminal state (Done, Cancelled, Failed).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Failed)
    }
}

/// Errors returned by rebuild operation transitions and chunk acceptance.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RebuildError {
    #[error("illegal rebuild transition from {from:?} to {to:?}")]
    IllegalTransition {
        from: RebuildPhase,
        to: RebuildPhase,
    },
    #[error("out-of-order chunk: expected sequence {expected}, got {actual}")]
    OutOfOrderChunk { expected: u64, actual: u64 },
}

/// A durable rebuild operation state record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebuildOperation {
    pub collection: String,
    pub operation_id: String,
    pub phase: RebuildPhase,
    pub chunks_applied: u64,
    pub docs_applied: u64,
    pub writes_to_active_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

impl RebuildOperation {
    /// Create a new rebuild operation in the `Pending` phase.
    pub fn new(collection: impl Into<String>, operation_id: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            operation_id: operation_id.into(),
            phase: RebuildPhase::Pending,
            chunks_applied: 0,
            docs_applied: 0,
            writes_to_active_only: false,
            failure_reason: None,
        }
    }

    /// Transition from `Pending` to `Streaming`.
    pub fn start_streaming(&mut self) -> Result<(), RebuildError> {
        if self.phase != RebuildPhase::Pending {
            return Err(RebuildError::IllegalTransition {
                from: self.phase,
                to: RebuildPhase::Streaming,
            });
        }
        self.phase = RebuildPhase::Streaming;
        Ok(())
    }

    /// Accept the next chunk in order. Requires `Streaming` phase.
    /// Fails if `seq != chunks_applied` and leaves progress unchanged.
    pub fn accept_chunk(&mut self, seq: u64, docs: u64) -> Result<(), RebuildError> {
        if self.phase != RebuildPhase::Streaming {
            return Err(RebuildError::IllegalTransition {
                from: self.phase,
                to: RebuildPhase::Streaming,
            });
        }
        if seq != self.chunks_applied {
            return Err(RebuildError::OutOfOrderChunk {
                expected: self.chunks_applied,
                actual: seq,
            });
        }
        self.chunks_applied += 1;
        self.docs_applied += docs;
        Ok(())
    }

    /// Transition from `Streaming` to `Sealing`.
    pub fn seal(&mut self) -> Result<(), RebuildError> {
        if self.phase != RebuildPhase::Streaming {
            return Err(RebuildError::IllegalTransition {
                from: self.phase,
                to: RebuildPhase::Sealing,
            });
        }
        self.phase = RebuildPhase::Sealing;
        Ok(())
    }

    /// Transition from `Sealing` to `Done`.
    pub fn complete(&mut self) -> Result<(), RebuildError> {
        if self.phase != RebuildPhase::Sealing {
            return Err(RebuildError::IllegalTransition {
                from: self.phase,
                to: RebuildPhase::Done,
            });
        }
        self.phase = RebuildPhase::Done;
        Ok(())
    }

    /// Cancel the operation.
    /// Valid in every non-terminal phase (Pending, Streaming, Sealing).
    /// Idempotent if already Cancelled.
    /// Records that writes go to the active generation only.
    pub fn cancel(&mut self) -> Result<(), RebuildError> {
        if self.phase == RebuildPhase::Cancelled {
            return Ok(());
        }
        if self.phase.is_terminal() {
            return Err(RebuildError::IllegalTransition {
                from: self.phase,
                to: RebuildPhase::Cancelled,
            });
        }
        self.phase = RebuildPhase::Cancelled;
        self.writes_to_active_only = true;
        Ok(())
    }

    /// Fail the operation. Valid from any non-terminal phase.
    pub fn fail(&mut self, reason: impl Into<String>) -> Result<(), RebuildError> {
        if self.phase.is_terminal() {
            return Err(RebuildError::IllegalTransition {
                from: self.phase,
                to: RebuildPhase::Failed,
            });
        }
        self.phase = RebuildPhase::Failed;
        self.failure_reason = Some(reason.into());
        Ok(())
    }
}
