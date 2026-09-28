//! What a subscription delivers: a resident record, or a deferred one whose
//! source may already be staged to disk, and the release a staged source hands
//! back.

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};

use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::wal::mem_wal::MemWalSlot;
use crate::ingest::infrastructure::wal_source_stage::{
    MappedFastIndexPayload, MappedGenericCborPayload,
};
use crate::storage::Engine;

#[doc(hidden)]
pub enum WalDelivery {
    Resident(WalRecord),
    Deferred(WalSourceRecord),
}

#[doc(hidden)]
pub struct WalSourceRecord {
    pub(super) sequence: u64,
    pub(super) slot: Arc<Mutex<MemWalSlot>>,
}

#[doc(hidden)]
pub struct WalSourceRelease {
    pub(super) sequence: u64,
}

impl WalSourceRelease {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}

fn check_resident_admission(record: &WalRecord, admitted_bytes: usize) -> Result<()> {
    let raw = Engine::record_owned_bytes(&record.entry)?;
    anyhow::ensure!(
        admitted_bytes >= raw,
        "resident WAL record was read without decoded-record admission"
    );
    Ok(())
}

impl WalDelivery {
    /// Return a pinned mapped fast Index payload after dropping the source-slot
    /// mutex. The owner keeps the staged directory and inode alive for callers
    /// that wait for capacity or move work to a blocking task.
    pub(crate) fn mapped_fast_index(&self) -> Result<Option<MappedFastIndexPayload>> {
        let Self::Deferred(source) = self else {
            return Ok(None);
        };
        source.mapped_fast_index()
    }
    /// Return a pinned mapped generic-CBOR body after releasing the source
    /// slot mutex. The returned bytes exclude the private stage header.
    pub(crate) fn mapped_generic_cbor(&self) -> Result<Option<MappedGenericCborPayload>> {
        let Self::Deferred(source) = self else {
            return Ok(None);
        };
        source.mapped_generic_cbor()
    }
    /// Associate source-backed reservation ownership with a native deferred
    /// source until its resident slot is staged or truncated.
    pub(crate) fn retain_source(
        &self,
        retention: crate::ingest::domain::change_budget::SourceRetention,
    ) -> Result<()> {
        let Self::Deferred(source) = self else {
            return Ok(());
        };
        let mut slot = source
            .slot
            .lock()
            .map_err(|_| anyhow!("MemWal source slot poisoned"))?;
        if let MemWalSlot::Resident(_, retentions) = &mut *slot {
            retentions.push(retention);
        }
        Ok(())
    }

    pub(crate) fn decoded_owned_bytes(&self) -> Result<usize> {
        match self {
            Self::Resident(record) => Engine::record_owned_bytes(&record.entry).map_err(Into::into),
            Self::Deferred(record) => record.decoded_owned_bytes(),
        }
    }
    pub(crate) fn read_scratch_bytes(&self) -> usize {
        match self {
            Self::Resident(_) => 0,
            Self::Deferred(record) => record.read_scratch_bytes(),
        }
    }
    /// A staged source has no retained raw transport record. Reserve its
    /// decoder workspace and, when used, the AOF clone/encoder separately.
    pub(crate) fn extra_delivery_bytes(&self, has_aof: bool) -> Result<usize> {
        let raw = self.decoded_owned_bytes()?;
        let staged = match self {
            Self::Resident(_) => false,
            Self::Deferred(source) => matches!(
                &*source
                    .slot
                    .lock()
                    .map_err(|_| anyhow!("MemWal source slot poisoned"))?,
                MemWalSlot::Staged(_)
            ),
        };
        let copies = match (staged, has_aof) {
            (true, false) => 0,
            (true, true) => 3,
            (false, false) => 2,
            (false, true) => 4,
        };
        raw.checked_mul(copies)
            .and_then(|extra| extra.checked_add(self.read_scratch_bytes()))
            .ok_or_else(|| anyhow!("WAL delivery memory bound overflow"))
    }

    pub(crate) fn read(self, admitted_bytes: usize) -> Result<WalRecord> {
        match self {
            Self::Resident(record) => {
                check_resident_admission(&record, admitted_bytes)?;
                Ok(record)
            }
            Self::Deferred(record) => record.read(admitted_bytes),
        }
    }
}

impl WalSourceRecord {
    fn mapped_fast_index(&self) -> Result<Option<MappedFastIndexPayload>> {
        let stage = {
            let slot = self
                .slot
                .lock()
                .map_err(|_| anyhow!("MemWal source slot poisoned"))?;
            match &*slot {
                MemWalSlot::Resident(_, _) => return Ok(None),
                MemWalSlot::Staged(stage) => {
                    anyhow::ensure!(
                        stage.sequence() == self.sequence,
                        "staged MemWal descriptor sequence mismatch"
                    );
                    stage.clone()
                }
            }
        };
        stage.mapped_fast_index().map_err(Into::into)
    }
    fn mapped_generic_cbor(&self) -> Result<Option<MappedGenericCborPayload>> {
        let stage = {
            let slot = self
                .slot
                .lock()
                .map_err(|_| anyhow!("MemWal source slot poisoned"))?;
            match &*slot {
                MemWalSlot::Resident(_, _) => return Ok(None),
                MemWalSlot::Staged(stage) => {
                    anyhow::ensure!(
                        stage.sequence() == self.sequence,
                        "staged MemWal descriptor sequence mismatch"
                    );
                    stage.clone()
                }
            }
        };
        stage.mapped_generic_cbor().map_err(Into::into)
    }
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
    pub(crate) fn decoded_owned_bytes(&self) -> Result<usize> {
        let slot = self
            .slot
            .lock()
            .map_err(|_| anyhow!("MemWal source slot poisoned"))?;
        match &*slot {
            MemWalSlot::Resident(record, _) => {
                Engine::record_owned_bytes(&record.entry).map_err(Into::into)
            }
            MemWalSlot::Staged(stage) => Ok(stage.decoded_owned_bytes()),
        }
    }
    pub(crate) fn read_scratch_bytes(&self) -> usize {
        self.slot
            .lock()
            .ok()
            .map(|slot| match &*slot {
                MemWalSlot::Resident(_, _) => 0,
                MemWalSlot::Staged(stage) => stage.read_scratch_bytes(),
            })
            .unwrap_or(0)
    }
    pub(crate) fn read(self, admitted_bytes: usize) -> Result<WalRecord> {
        let slot = self
            .slot
            .lock()
            .map_err(|_| anyhow!("MemWal source slot poisoned"))?;
        match &*slot {
            MemWalSlot::Resident(record, _) => {
                check_resident_admission(record, admitted_bytes)?;
                Ok(record.clone())
            }
            MemWalSlot::Staged(stage) => {
                anyhow::ensure!(
                    stage.sequence() == self.sequence,
                    "staged MemWal descriptor sequence mismatch"
                );
                stage.read(admitted_bytes).map_err(Into::into)
            }
        }
    }
}
