//! Private durable staging for one in-memory WAL source.
//!
//! The source keeps its payload while this module streams it to a `StageStore`.
//! Only native `MemWal` may replace its source slot and issue the later RAM
//! release proof.  A staged handle pins this source's private directory until
//! the last handle is dropped.

use memmap2::Mmap;
use std::fs;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::ingest::domain::change_admission::StagePayload;
use crate::ingest::domain::wal_record::WalRecord;
use crate::ingest::infrastructure::committed_record_codec::{
    read_staged_wal_record, staged_generic_cbor_payload,
};
#[cfg(test)]
use crate::ingest::infrastructure::committed_stage::StageFailureInjector;
use crate::ingest::infrastructure::committed_stage::{
    DurableStage, SourceIdentity, SourceKind, StageStore,
};
use crate::{index::application::engine::Engine, storage::RecordAdmissionError};

/// The bounded read buffer is charged by the caller together with the decoded
/// record before it asks this bridge to deserialize the record.
const READ_TRANSPORT_SCRATCH_BYTES: usize = 64 * 1024;
const CBOR_DECODE_SCRATCH_BYTES: usize = 4096;
const MEM_WAL_STAGE_EPOCH: u64 = 0;
static NEXT_MEM_WAL_SOURCE: AtomicU64 = AtomicU64::new(1);
static NEXT_STAGE_DIRECTORY: AtomicU64 = AtomicU64::new(0);

/// One process-private directory.  It is deliberately ephemeral: durable
/// receipt is a handoff within this process, never crash recovery policy.
struct StageDirectory {
    path: PathBuf,
}

impl StageDirectory {
    fn create() -> io::Result<Self> {
        Self::create_in(&std::env::temp_dir(), "lumen-mem-wal-stage")
    }

    fn create_in(parent: &Path, prefix: &str) -> io::Result<Self> {
        let process = std::process::id();
        for _ in 0..128 {
            let nonce = NEXT_STAGE_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!("{prefix}-{process}-{nonce}"));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate a private MemWal stage directory",
        ))
    }
}

impl Drop for StageDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct SourceStageRoot {
    directory: StageDirectory,
    source_id: u64,
    source: SourceIdentity,
    #[cfg(test)]
    injector: Option<Arc<dyn StageFailureInjector>>,
}

/// Each receipt owns one child directory and removes only that directory on
/// its final drop. The parent source root remains available to other slots.
struct RecordStageRoot {
    directory: StageDirectory,
    store: StageStore,
}

/// A per-`MemWal` stager.  Clones share source identity and one private root.
#[derive(Clone)]
pub(crate) struct WalSourceStager {
    root: Arc<SourceStageRoot>,
}

/// Opaque durable receipt for a source WAL entry.  It deliberately carries no
/// RAM-release proof: successfully copying a record leaves the source slot
/// intact until `MemWal` performs its atomic replacement.
#[doc(hidden)]
pub(crate) struct StagedWalRecord {
    stage: DurableStage,
    source: Arc<SourceStageRoot>,
    record_root: Arc<RecordStageRoot>,
    sequence: u64,
    decoded_owned_bytes: usize,
    codec: StageCodec,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum StageCodec {
    Cbor,
    FastIndex,
}

/// Pins one validated native fast-Index source. The stage Arc retains the
/// private directory and the mmap retains the exact inode for a coordinator
/// capacity wait and later scalar apply.
pub(crate) struct MappedFastIndexPayload {
    _stage: Arc<StagedWalRecord>,
    mmap: Mmap,
}

/// Pins one validated generic-CBOR source. The mmap borrows only the private
/// `LWCS` body, and the stage Arc retains the private directory and inode.
pub(crate) struct MappedGenericCborPayload {
    _stage: Arc<StagedWalRecord>,
    mmap: Mmap,
    payload_start: usize,
}

impl MappedGenericCborPayload {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.mmap[self.payload_start..]
    }
}

impl MappedFastIndexPayload {
    pub(crate) fn payload(&self) -> &[u8] {
        &self.mmap
    }
}

impl WalSourceStager {
    /// Create a distinct source identity and a private root for one `MemWal`.
    pub(crate) fn for_mem_wal() -> io::Result<Self> {
        let directory = StageDirectory::create()?;
        Self::with_directory(directory)
    }

    fn with_directory(directory: StageDirectory) -> io::Result<Self> {
        let source_id = NEXT_MEM_WAL_SOURCE
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| io::Error::other("MemWal stage source identity exhausted"))?;
        let source = SourceIdentity::new(SourceKind::Local, format!("mem-wal:{source_id}"))?;
        Ok(Self {
            root: Arc::new(SourceStageRoot {
                directory,
                source_id,
                source,
                #[cfg(test)]
                injector: None,
            }),
        })
    }

    /// Stable for the process lifetime.  It has no relationship to a path or
    /// to a caller-supplied identifier.
    pub(crate) fn source_id(&self) -> u64 {
        self.root.source_id
    }

    /// Stream directly from the caller's record.  `StagePayload` serializes
    /// through the existing private CBOR codec and never builds a whole-record
    /// encoded buffer.  Errors return before this method changes `record`.
    pub(crate) fn stage(
        &self,
        sequence: u64,
        record: &mut WalRecord,
    ) -> io::Result<StagedWalRecord> {
        // Ciborium text growth, serde's temporary Content arrays and final
        // decoded values can overlap. Price those owners before writing the
        // source receipt; original String/Vec capacities alone are not enough.
        let decoded_owned_bytes =
            Engine::record_decode_peak(&record.entry).map_err(record_admission_io_error)?;
        let directory = StageDirectory::create_in(&self.root.directory.path, "record")?;
        #[cfg(test)]
        let store = match &self.root.injector {
            Some(injector) => StageStore::with_injector(&directory.path, injector.clone())?,
            None => StageStore::new(&directory.path)?,
        };
        #[cfg(not(test))]
        let store = StageStore::new(&directory.path)?;
        let record_root = Arc::new(RecordStageRoot { directory, store });
        let stage = record_root.store.stage_with_writer(
            sequence,
            MEM_WAL_STAGE_EPOCH,
            self.root.source.clone(),
            |output| record.write_stage(output),
        )?;
        Ok(StagedWalRecord {
            stage,
            source: self.root.clone(),
            record_root,
            sequence,
            decoded_owned_bytes,
            codec: StageCodec::Cbor,
        })
    }

    /// Stage only native v1 fast Index bytes. The caller retains its owned
    /// record until MemWal atomically swaps the slot after this returns.
    pub(crate) fn stage_fast_index(
        &self,
        sequence: u64,
        record: &WalRecord,
    ) -> io::Result<Option<StagedWalRecord>> {
        if !record.is_fast_index_wire() {
            return Ok(None);
        }
        let directory = StageDirectory::create_in(&self.root.directory.path, "record")?;
        #[cfg(test)]
        let store = match &self.root.injector {
            Some(injector) => StageStore::with_injector(&directory.path, injector.clone())?,
            None => StageStore::new(&directory.path)?,
        };
        #[cfg(not(test))]
        let store = StageStore::new(&directory.path)?;
        let record_root = Arc::new(RecordStageRoot { directory, store });
        let stage = record_root.store.stage_with_writer(
            sequence,
            MEM_WAL_STAGE_EPOCH,
            self.root.source.clone(),
            |output| record.write_fast_index_wire(output),
        )?;
        let decoded_owned_bytes =
            Engine::record_decode_peak(&record.entry).map_err(record_admission_io_error)?;
        Ok(Some(StagedWalRecord {
            stage,
            source: self.root.clone(),
            record_root,
            sequence,
            decoded_owned_bytes,
            codec: StageCodec::FastIndex,
        }))
    }

    #[cfg(test)]
    pub(crate) fn for_mem_wal_with_injector(
        injector: Arc<dyn StageFailureInjector>,
    ) -> io::Result<Self> {
        let mut stager = Self::for_mem_wal()?;
        Arc::get_mut(&mut stager.root)
            .expect("fresh stager root")
            .injector = Some(injector);
        Ok(stager)
    }
}

impl StagedWalRecord {
    pub(crate) fn source_id(&self) -> u64 {
        self.source.source_id
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Conservative owned bytes for the decoded `WalRecord`, calculated from
    /// the original source record before staging.
    pub(crate) fn decoded_owned_bytes(&self) -> usize {
        self.decoded_owned_bytes
    }

    /// Decode only after the caller has admitted the original decoded bound
    /// plus this fixed transport scratch.  This does not grant capacity or
    /// release the native WAL payload.
    pub(crate) fn read(&self, admitted_bytes: usize) -> io::Result<WalRecord> {
        let scratch = self.read_scratch_bytes();
        let required = self
            .decoded_owned_bytes
            .checked_add(scratch)
            .ok_or_else(|| io::Error::other("staged WAL read admission overflow"))?;
        if admitted_bytes < required {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "staged WAL record was read without decoded-record and transport admission",
            ));
        }
        if self.stage.sequence() != self.sequence
            || self.stage.engine_epoch() != MEM_WAL_STAGE_EPOCH
            || self.stage.source() != &self.source.source
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged WAL receipt does not belong to its MemWal source",
            ));
        }
        let file = self.stage.open()?;
        match self.codec {
            StageCodec::Cbor => {
                let mut reader =
                    BufReader::with_capacity(scratch - CBOR_DECODE_SCRATCH_BYTES, file);
                read_staged_wal_record(&mut reader)
            }
            StageCodec::FastIndex => {
                let mmap = unsafe { Mmap::map(&file)? };
                WalRecord::decode(&mmap)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
            }
        }
    }

    pub(crate) fn read_scratch_bytes(&self) -> usize {
        self.decoded_owned_bytes.min(READ_TRANSPORT_SCRATCH_BYTES) + CBOR_DECODE_SCRATCH_BYTES
    }

    pub(crate) fn mapped_fast_index(
        self: &Arc<Self>,
    ) -> io::Result<Option<MappedFastIndexPayload>> {
        if self.codec != StageCodec::FastIndex {
            return Ok(None);
        }
        if self.stage.sequence() != self.sequence
            || self.stage.engine_epoch() != MEM_WAL_STAGE_EPOCH
            || self.stage.source() != &self.source.source
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged fast Index receipt does not belong to its MemWal source",
            ));
        }
        let file = self.stage.open()?;
        let mmap = unsafe { Mmap::map(&file)? };
        crate::ingest::infrastructure::wal::fast_index_scanner::FastIndexScanner::parse(&mmap)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        Ok(Some(MappedFastIndexPayload {
            _stage: self.clone(),
            mmap,
        }))
    }

    /// Map a private generic-CBOR stage without decoding its `WalRecord`.
    /// The stage identity is checked before opening it; `DurableStage::open`
    /// verifies the committed receipt before the mmap can lend its bytes.
    pub(crate) fn mapped_generic_cbor(
        self: &Arc<Self>,
    ) -> io::Result<Option<MappedGenericCborPayload>> {
        if self.codec != StageCodec::Cbor {
            return Ok(None);
        }
        if self.stage.sequence() != self.sequence
            || self.stage.engine_epoch() != MEM_WAL_STAGE_EPOCH
            || self.stage.source() != &self.source.source
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "staged generic CBOR receipt does not belong to its MemWal source",
            ));
        }
        let file = self.stage.open()?;
        let mmap = unsafe { Mmap::map(&file)? };
        let bytes = staged_generic_cbor_payload(&mmap)?;
        let payload_start = bytes.as_ptr() as usize - mmap.as_ptr() as usize;
        Ok(Some(MappedGenericCborPayload {
            _stage: self.clone(),
            mmap,
            payload_start,
        }))
    }

    #[cfg(test)]
    fn root_path(&self) -> &Path {
        &self.record_root.directory.path
    }
}

fn record_admission_io_error(error: RecordAdmissionError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

#[cfg(test)]
mod tests;
