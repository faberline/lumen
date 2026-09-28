//! `EngineSm` as `raft_runtime`'s `RaftStateMachine`: apply a committed
//! command, and snapshot and restore through the segment RDB archive.

use std::io::Read;
use std::sync::atomic::Ordering;

use anyhow::Result;
use raft_runtime::{
    AdmissionPermit, Index, PreparedSnapshot, ProposalBackpressure, RaftStateMachine,
    SnapshotPreparation,
};

use crate::rdb::RdbSnapshot;
use crate::replication::application::engine_sm::{AdmittedRaftRecord, EngineSm};
use crate::storage::{Engine, RecordAdmissionError};

impl RaftStateMachine for EngineSm {
    fn admit_proposal(&self, command: &[u8]) -> Result<Option<AdmissionPermit>> {
        match self.decode_admitted(command, true) {
            Ok(record) => Ok(Some(Box::new(record))),
            Err(error) => {
                if let Some(pending) = error.downcast_ref::<RecordAdmissionError>().and_then(
                    crate::ingest::domain::change_admission::PendingChangeCapacity::from_record_prepublication,
                ) {
                    return Err(ProposalBackpressure {
                        reason: pending.to_string(),
                        retry_after_seconds: 1,
                    }
                    .into());
                }
                Err(error)
            }
        }
    }

    fn apply(&self, index: Index, command: &[u8]) -> Result<()> {
        self.apply_admitted(index, command, None)
    }

    fn apply_admitted(
        &self,
        index: Index,
        command: &[u8],
        permit: Option<AdmissionPermit>,
    ) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            anyhow::bail!("Raft apply requires restart after an unresolved record");
        }
        let result = (|| {
            // Retained oversized fast-Index input must not enter the owned
            // decoder. Its scalar values can be projected from the pinned
            // command into private immutable files before the apply interval.
            // Local proposals still go through pre-publication admission.
            if permit.is_none()
                && command.len() > crate::ingest::domain::change_budget::HARD_LIMIT / 8
            {
                if let Ok(scanner) =
                    crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner::parse(
                        command,
                    )
                {
                    if self.engine.try_apply_committed_index_with_capacity_owner(
                        &scanner,
                        index,
                        || self.ensure_capacity_owner(),
                        |apply, outcome| {
                            let mut outcomes = self.outcomes.lock().expect("outcomes poisoned");
                            outcomes.insert(index, outcome);
                            outcomes.advance(index);
                            apply.advance_sequence(index);
                            self.applied.store(index, Ordering::Release);
                        },
                    )? {
                        return Ok(());
                    }
                }
                if self
                    .engine
                    .try_apply_committed_replace_with_capacity_owner(
                        command,
                        index,
                        &mut || self.ensure_capacity_owner(),
                        |apply, outcome| {
                            let mut outcomes = self.outcomes.lock().expect("outcomes poisoned");
                            outcomes.insert(index, outcome);
                            outcomes.advance(index);
                            apply.advance_sequence(index);
                            self.applied.store(index, Ordering::Release);
                        },
                    )?
                {
                    return Ok(());
                }
            }
            let record = match permit {
                Some(permit) => *permit.downcast::<AdmittedRaftRecord>().map_err(|_| {
                    anyhow::anyhow!("Raft admission belongs to another state machine")
                })?,
                None => self.decode_admitted(command, false)?,
            };
            self.apply_record(index, record)
        })();
        if result.is_err() {
            // Preparation/IO failures are not business no-ops. Preserve the
            // source log and the old watermark, and refuse later publication.
            self.failed.store(true, Ordering::Release);
            self.engine.capture_barrier.apply().mark_uncertain();
        }
        result
    }

    fn preflight_snapshot(&self) -> Result<Option<Box<dyn SnapshotPreparation>>> {
        self.segment_store
            .as_ref()
            .map(|store| {
                store
                    .raft_snapshot_preflight(self.engine.clone())
                    .map(|preparation| Box::new(preparation) as Box<dyn SnapshotPreparation>)
            })
            .transpose()
    }

    fn snapshot(&self, writer: &mut dyn std::io::Write) -> Result<()> {
        if let Some(preparation) = self.preflight_snapshot()? {
            return preparation
                .capture_at(self.applied_index())?
                .write_to(writer);
        }
        let capture = self
            .engine
            .capture_barrier
            .capture(self.applied_index())
            .map_err(|error| anyhow::anyhow!(error))?;
        let snapshot = self.engine.snapshot()?;
        let up_to_seq = capture.stamp().sequence;
        drop(capture);
        let bytes = RdbSnapshot {
            up_to_seq,
            snapshot,
        }
        .encode()?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    fn validate_snapshot(&self, reader: &mut dyn Read) -> Result<()> {
        let mut prefix = Vec::with_capacity(8);
        reader.take(8).read_to_end(&mut prefix)?;
        let mut input = prefix.as_slice().chain(reader);
        if prefix == crate::segment_rdb::raft_archive::MAGIC {
            let store = self
                .segment_store
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("segment archive needs a segment snapshot store"))?;
            store.validate_raft_archive(&mut input)
        } else {
            let rdb = read_legacy_snapshot(&mut input)?;
            Engine::new().restore(rdb.snapshot)
        }
    }

    fn restore(&self, reader: &mut dyn Read) -> Result<()> {
        let mut prefix = Vec::with_capacity(8);
        reader.take(8).read_to_end(&mut prefix)?;
        let mut input = prefix.as_slice().chain(reader);
        let sequence = if prefix == crate::segment_rdb::raft_archive::MAGIC {
            let store = self
                .segment_store
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("segment archive needs a segment snapshot store"))?;
            store.restore_raft_archive(&self.engine, &mut input, |sequence| {
                self.applied.store(sequence, Ordering::Release)
            })?
        } else {
            let rdb = read_legacy_snapshot(&mut input)?;
            if let Some(store) = &self.segment_store {
                store.restore_legacy_raft_snapshot(&self.engine, rdb, |sequence| {
                    self.applied.store(sequence, Ordering::Release)
                })?
            } else {
                let apply = self.engine.capture_barrier.apply();
                self.engine.restore(rdb.snapshot)?;
                apply.initialize_sequence(rdb.up_to_seq);
                self.applied.store(rdb.up_to_seq, Ordering::Release);
                rdb.up_to_seq
            }
        };
        self.applied.store(sequence, Ordering::Release);
        Ok(())
    }

    fn applied_index(&self) -> Index {
        self.applied.load(Ordering::Acquire)
    }
}

impl SnapshotPreparation for crate::segment_rdb::raft_capture::SegmentRaftPreparation {
    fn capture_at(self: Box<Self>, index: Index) -> Result<Box<dyn PreparedSnapshot>> {
        Ok(Box::new((*self).capture_at(index)?))
    }
}

impl PreparedSnapshot for crate::segment_rdb::raft_capture::SegmentRaftCapture {
    fn write_to(self: Box<Self>, writer: &mut dyn std::io::Write) -> Result<()> {
        let generation = (*self).publish()?;
        crate::segment_rdb::raft_archive::write_archive(
            generation.path(),
            generation.sequence(),
            writer,
        )
    }
}

fn read_legacy_snapshot(reader: &mut dyn Read) -> Result<RdbSnapshot> {
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    let header: [u8; 4] = bytes
        .get(..4)
        .ok_or_else(|| anyhow::anyhow!("truncated legacy RDB header"))?
        .try_into()?;
    let expanded = u32::from_le_bytes(header) as usize;
    // Every LZ4 length extension consumes a byte for at most 255 output bytes.
    // Reject an impossible claim before the legacy decoder allocates its output.
    anyhow::ensure!(
        expanded <= bytes.len().saturating_sub(4).saturating_mul(255),
        "legacy RDB expanded length exceeds its compressed input bound"
    );
    RdbSnapshot::decode(&bytes)
}
