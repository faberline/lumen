//! Durable, sequence-ordered staging for committed records.
//!
//! A [`DurableStage`] is proof that the exact record bytes were copied out of
//! RAM, synced, named, and described by a separately synced marker.  The
//! handle has private fields so a caller cannot forge that proof or release a
//! change-budget reservation before the payload transition completed.
//!
//! This module deliberately does not decode records.  Its caller streams an
//! already durable wire representation through [`StageStore::stage`], keeping
//! the staging copy bounded by [`COPY_BUFFER_BYTES`].

pub(crate) mod files;
pub(crate) mod marker;
pub(crate) mod store;

use std::fs::File;
use std::io::{self, ErrorKind, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::ingest::infrastructure::committed_stage::files::{digest_reader, open_regular_readonly};

const FORMAT_MAGIC: &[u8; 8] = b"LCSTAGE\0";
const FORMAT_VERSION: u8 = 1;
const MARKER_SUFFIX: &str = ".commit";
const RECORD_SUFFIX: &str = ".record";
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_SOURCE_ID_BYTES: usize = 4096;

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

/// Identity of the source that committed a record.  This is stored in the
/// marker and checked on recovery; it is never used as a filesystem path.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct SourceIdentity {
    kind: SourceKind,
    origin: String,
}

impl SourceIdentity {
    pub(crate) fn new(kind: SourceKind, origin: impl Into<String>) -> io::Result<Self> {
        let origin = origin.into();
        if origin.is_empty() || origin.len() > MAX_SOURCE_ID_BYTES {
            return Err(invalid("source identity has invalid byte length"));
        }
        Ok(Self { kind, origin })
    }
}

/// Kept deliberately small and versioned.  An unrecognized tag is a recovery
/// error, never a silently reclassified external record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(u8)]
pub(crate) enum SourceKind {
    Local = 1,
    External = 2,
    Replay = 3,
    Raft = 4,
}

impl SourceKind {
    fn decode(tag: u8) -> io::Result<Self> {
        match tag {
            1 => Ok(Self::Local),
            2 => Ok(Self::External),
            3 => Ok(Self::Replay),
            4 => Ok(Self::Raft),
            _ => Err(invalid("committed-stage marker has unknown source kind")),
        }
    }
}

/// Failure points are only for tests and callers that deliberately inject
/// local durable-storage faults.  They run before the corresponding syscall,
/// so an error never yields a durable receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageFailurePoint {
    SyncPayload,
    RenamePayload,
    SyncPayloadDirectory,
    SyncMarker,
    PublishMarker,
    SyncMarkerDirectory,
}

pub(crate) trait StageFailureInjector: Send + Sync {
    fn check(&self, point: StageFailurePoint) -> io::Result<()>;
}

#[derive(Default)]
struct NoFailures;

impl StageFailureInjector for NoFailures {
    fn check(&self, _: StageFailurePoint) -> io::Result<()> {
        Ok(())
    }
}

/// A private durable staging root.  It has separate `records` and `markers`
/// directories so a marker is the sole recovery-visible commit point.
pub(crate) struct StageStore {
    root: PathBuf,
    injector: Arc<dyn StageFailureInjector>,
}

/// A durable, validated stage receipt.  Construction and fields are private to
/// this module.  Its removal requires a caller-provided verified watermark.
#[derive(Debug)]
pub(crate) struct DurableStage {
    store_root: PathBuf,
    sequence: u64,
    engine_epoch: u64,
    source: SourceIdentity,
    byte_len: u64,
    digest: [u8; 32],
    record_path: PathBuf,
    marker_path: PathBuf,
}

/// Caller-owned proof that a particular Engine epoch and source identity are
/// covered by a durable watermark.  It cannot be constructed outside this
/// module or reused for a different staging root.
pub(crate) struct CleanupProof {
    store_root: PathBuf,
    source: SourceIdentity,
    engine_epoch: u64,
    verified_watermark: u64,
}

impl DurableStage {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn engine_epoch(&self) -> u64 {
        self.engine_epoch
    }

    pub(crate) fn source(&self) -> &SourceIdentity {
        &self.source
    }

    pub(crate) fn byte_len(&self) -> u64 {
        self.byte_len
    }

    /// Reopen through the validated payload path.  This does not deserialize
    /// the record and does not allocate a full-record `Vec`.
    pub(crate) fn open(&self) -> io::Result<File> {
        let mut file = open_regular_readonly(&self.record_path)?;
        let (len, digest) = digest_reader(&mut file)?;
        if len != self.byte_len || digest != self.digest {
            return Err(invalid("committed-stage payload differs from marker"));
        }
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Marker {
    sequence: u64,
    engine_epoch: u64,
    source: SourceIdentity,
    byte_len: u64,
    digest: [u8; 32],
}

fn stage_name(sequence: u64, engine_epoch: u64) -> String {
    format!("{sequence:020}-{engine_epoch:016x}")
}

fn temp_name(prefix: &str) -> String {
    let id = NEXT_TEMP.fetch_add(1, AtomicOrdering::Relaxed);
    format!(".{prefix}-{}-{id:016x}.tmp", std::process::id())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
