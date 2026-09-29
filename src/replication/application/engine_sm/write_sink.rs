//! `RaftWriteSink`: the `--wal raft` write path. A write proposes through the
//! shared `RaftHost` and claims its outcome from the local `EngineSm` apply.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::Result;
use raft_runtime::{ProposalBackpressure, RaftHost, RaftStateMachine};

use crate::ingest::application::write_coordinator::WriteSink;
use crate::ingest::domain::wal_record::WalRecord;
use crate::replication::application::engine_sm::EngineSm;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::{
    index::application::engine::{raft_dispatch::ApplyOutcome, Engine},
    storage::RecordAdmissionError,
};

/// The [`WriteSink`] for `--wal raft`: a write proposes through the shared
/// [`RaftHost`] (which handles leader-redirect + read-your-write), and the rich
/// [`ApplyOutcome`] is claimed from the local [`EngineSm`] apply (the host
/// applies on every node, so a follower has its own outcome).
pub struct RaftWriteSink {
    host: Arc<RaftHost>,
    sm: Arc<EngineSm>,
}

impl RaftWriteSink {
    pub fn new(host: Arc<RaftHost>, sm: Arc<EngineSm>) -> Self {
        Self { host, sm }
    }

    /// Count exactly the terminal local pre-publication capacity refusal. The
    /// host has not appended a command when this returns an error.
    fn prepublication_backpressure(
        &self,
        pending: crate::ingest::domain::change_admission::PendingChangeCapacity,
    ) -> anyhow::Error {
        self.sm.engine().metrics().incr_segment_backpressure();
        anyhow::Error::new(pending)
    }
}

#[async_trait::async_trait]
impl WriteSink for RaftWriteSink {
    async fn submit(&self, entry: RaftLogEntry) -> Result<ApplyOutcome> {
        let record = WalRecord::new(entry);
        let raw = Engine::record_owned_bytes(&record.entry).map_err(|error| match error {
            RecordAdmissionError::Overflow => self.prepublication_backpressure(
                crate::ingest::domain::change_admission::PendingChangeCapacity::Overflow,
            ),
            other => anyhow::Error::new(other),
        })?;
        let extra = raw.checked_mul(2).ok_or_else(|| {
            self.prepublication_backpressure(
                crate::ingest::domain::change_admission::PendingChangeCapacity::Overflow,
            )
        })?;
        let request = self.sm.engine.record_ram_request_from_bound(raw, extra);
        // This origin reservation covers the decoded request and its encoder.
        // Leader admission is separate and follows the command into the host.
        let _origin = self
            .sm
            .engine
            .try_reserve_record_ram(&request)
            .map_err(|error| {
                match crate::ingest::domain::change_admission::PendingChangeCapacity::from_record_prepublication(
                    &error,
                ) {
                    Some(pending) => self.prepublication_backpressure(pending),
                    None => anyhow::Error::new(error),
                }
            })?;
        let command = record.encode()?;
        drop(record);
        let index = match self.host.propose(command).await {
            Ok(index) => index,
            Err(e) => {
                if let Some(backpressure) = e.downcast_ref::<ProposalBackpressure>() {
                    return Err(self.prepublication_backpressure(
                        crate::ingest::domain::change_admission::PendingChangeCapacity::Raft {
                            reason: backpressure.reason.clone(),
                        },
                    ));
                }
                // #2516: the raft log append is itself a durable write path
                // (named explicitly alongside AOF/segment/snapshot writes) —
                // an ENOSPC here (surfaced through `propose`'s error chain the
                // same way an AOF ENOSPC is) must enter the same sticky
                // degraded read-only mode, not just propagate a generic error.
                if crate::ingest::application::write_coordinator::errors::is_storage_full(&e) {
                    tracing::error!(
                        error = %e,
                        "raft log append hit ENOSPC — entering degraded read-only mode"
                    );
                    self.sm.engine().metrics().mark_storage_degraded();
                    return Err(anyhow::Error::new(
                        crate::ingest::application::write_coordinator::errors::StorageFullError(
                            "local storage is full (ENOSPC) appending to the raft log; node \
                         entered degraded read-only mode"
                                .to_string(),
                        ),
                    ));
                }
                return Err(e);
            }
        };
        self.sm.take_outcome(index)
    }
    fn applied_seq(&self) -> u64 {
        self.sm.applied_index()
    }
    fn restart_required(&self) -> bool {
        self.sm.failed.load(Ordering::Acquire)
    }
}
