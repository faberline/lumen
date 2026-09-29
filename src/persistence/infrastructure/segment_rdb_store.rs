//! Durable segment-checkpoint generations.
//!
//! A checkpoint stores one directory per collection. Each collection contains
//! mmap segments and `_schema.json`. New checkpoints also contain one
//! top-level `_generation.json` manifest. The shared `storage-durable`
//! generation store fsyncs the complete tree and changes `CURRENT` atomically.
//! `CURRENT` is the only source of truth. A complete but unpointed directory is
//! never selected during restart.
//!
//! New directory names are `gen-<seq>-rev-<revision>`. A same-sequence save
//! creates a new immutable revision. A background save below the active
//! sequence is a no-op. Exact 0.4.28 `gen-<seq>` directories remain readable:
//! on the first 0.4.29 restart, Lumen validates the exact highest legacy
//! directory and then writes `CURRENT` once. It never falls back from a corrupt
//! highest legacy directory.

pub(crate) mod catalog;
pub(crate) mod compacted_fields;
pub(crate) mod compaction;
pub(crate) mod delta_integrity;
pub(crate) mod diagnostic;
pub(crate) mod field_deltas;
pub(crate) mod flat_layout;
pub(crate) mod generation_validation;
pub(crate) mod generations;
pub(crate) mod load;
pub(crate) mod manifest_io;
pub(crate) mod merge_selection;
pub(crate) mod open;
#[cfg(feature = "raft-wal")]
pub(crate) mod raft_archive;
#[cfg(feature = "raft-wal")]
pub(crate) mod raft_capture;
pub(crate) mod records;
pub(crate) mod reopen;
pub(crate) mod save;
pub(crate) mod save_attempt;
#[path = "segment_save_gate.rs"]
mod save_gate;
pub(crate) mod startup;
pub(crate) mod telemetry;

use crate::persistence::application::background_merge::RootWork;
use crate::persistence::infrastructure::segment_rdb_store::startup::StartupBootstrap;
use crate::shared_kernel::capture_barrier::CaptureStamp;
use crate::storage::{Engine, FrozenCheckpoint, RecoveryProfile};
use anyhow::Result;
use save_gate::SaveGate;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use storage_durable::{
    CurrentGenerationStaging, GenerationName, GenerationStore, StagedGeneration,
};

const GENERATION_MANIFEST_FILE: &str = "_generation.json";
const GENERATION_MANIFEST_SCHEMA_VERSION: u32 = 2;

pub(in crate::persistence) const GENERATION_MANIFEST_V2: u32 = 2;

pub(in crate::persistence) const GENERATION_MANIFEST_V3: u32 = 3;

pub(in crate::persistence) const FLAT_PAYLOAD_DIR: &str = "payload";

const CHECKPOINT_SCHEMA_FILE: &str = "_schema.json";
const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TEMP_FILE: &str = "CURRENT.tmp";
const AOF_FILE: &str = "aof.log";
const AOF_COMPACT_TEMP_FILE: &str = "aof.log.compact.tmp";
const HNSW_GRAPH_CACHE_DIR: &str = "hnsw-graph-cache";
// The shipped image pre-populates its declared volume with this inert regular
// file so Docker recognizes a non-empty data directory. It carries no Lumen
// state and is part of a semantically new root.
const CONTAINER_VOLUME_SEED_FILE: &str = ".lumen-volume-seed";
// ext-family filesystems create this direct child at the root of a fresh
// volume. It is safe only when it remains an empty real directory.
const EXT_FILESYSTEM_METADATA_DIR: &str = "lost+found";

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergePhase {
    BeforeEncode,
    BeforePublish,
    AfterPublish,
}
#[doc(hidden)]
pub trait MergeObserver: Send + Sync {
    fn observe(&self, phase: MergePhase) -> std::io::Result<()>;
}
struct NoMergeObserver;
impl MergeObserver for NoMergeObserver {
    fn observe(&self, _: MergePhase) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(in crate::persistence) struct GenerationRecord {
    pub(in crate::persistence) name: GenerationName,
    pub(in crate::persistence) path: PathBuf,
    pub(in crate::persistence) sequence: u64,
    pub(in crate::persistence) revision: u64,
    pub(in crate::persistence) legacy: bool,
    pub(in crate::persistence) previous: Option<GenerationName>,
}

/// The exact generation selected by `CURRENT`, reopened into a fresh engine.
#[derive(Clone)]
pub struct LoadedSegmentGeneration {
    pub name: GenerationName,
    pub sequence: u64,
    pub engine: Arc<Engine>,
}

impl GenerationRecord {
    fn order_key(&self) -> (u8, u64) {
        if self.legacy {
            (0, self.sequence)
        } else {
            (1, self.revision)
        }
    }
}

pub(crate) type CheckpointRootGuard = Arc<dyn Send + Sync>;

/// Filesystem-backed segment checkpoints selected through one durable
/// `CURRENT` pointer.
///
/// Clones and independently opened handles for the same canonical root share
/// `save_gate` inside one process. The gate covers preparation, abandoned-stage
/// cleanup, activation, reopen, and prune. As required by `GenerationStore`, one
/// process must own all mutations for a root; cross-process writers are not
/// supported.
#[derive(Clone)]
pub struct SegmentRdbStore {
    pub(in crate::persistence) root: PathBuf,
    pub(in crate::persistence) save_gate: Arc<SaveGate>,
    pub(in crate::persistence) generations: GenerationStore,
    bootstrap: StartupBootstrap,
    // Clones share proof of the last verified immutable manifest. Unknown
    // generations require payload validation before the hard-link fast path.
    pub(in crate::persistence) verified_catalog: Arc<Mutex<Option<(GenerationName, Vec<u8>)>>>,
    pub(in crate::persistence) pending_frozen: Arc<Mutex<Option<PendingFrozenCheckpoint>>>,
    pub(in crate::persistence) merge_observer: Arc<dyn MergeObserver>,
    pub(in crate::persistence) background: Arc<RootWork>,
    root_guard: Option<CheckpointRootGuard>,
    pub(in crate::persistence) publication_fence: Option<crate::segment_capacity::PublicationFence>,
    recovery_profile: RecoveryProfile,
    recovery_timings: Arc<Mutex<RecoveryTimings>>,
}

/// Aggregate timings for the pre-bind portion of durable recovery.
///
/// Keep this intentionally scalar-only. The profile must never retain or emit
/// paths, collection IDs, field names, document IDs, or document values.
#[derive(Default)]
struct RecoveryTimings {
    layout_validation_ms: u64,
    manifest_decode_ms: u64,
    base_decode_ms: u64,
    delta_decode_ms: u64,
    reopen_ms: u64,
    identity_hydration_ms: u64,
    vector_finish_ms: u64,
}

/// Keeps one immutable generation present while an external snapshot writer
/// reads it.  The generation may stop being `CURRENT`; pruning still observes
/// this root-local reference until the pin drops.
pub(crate) struct SegmentArchivePin {
    background: Arc<RootWork>,
    _root_guard: Option<CheckpointRootGuard>,
    name: String,
    path: PathBuf,
    sequence: u64,
}

impl SegmentArchivePin {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Drop for SegmentArchivePin {
    fn drop(&mut self) {
        self.background.unpin(&self.name);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingPredecessor {
    name: Option<String>,
    sequence: Option<u64>,
    revision: Option<u64>,
    legacy: Option<bool>,
}

impl PendingPredecessor {
    fn from_current(current: Option<&GenerationRecord>) -> Self {
        Self {
            name: current.map(|record| record.name.as_str().to_owned()),
            sequence: current.map(|record| record.sequence),
            revision: current.map(|record| record.revision),
            legacy: current.map(|record| record.legacy),
        }
    }
}

/// Ordinary saves never move a cut backward. A Raft restore replaces product
/// state from the authoritative durable Raft snapshot before replaying its log.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SaveIntent {
    Ordinary,
    ExactRaft,
    RaftRestore,
}

enum SaveAttempt {
    Complete(GenerationName),
    CapacityWait(u64),
    FreshCapture,
}

#[derive(Clone, Copy)]
pub(in crate::persistence) enum StagingSelection {
    Generic,
    CurrentIfDurable,
    BackgroundScratch,
}

pub(in crate::persistence) enum GenerationStaging {
    Generic(StagedGeneration),
    Current(CurrentGenerationStaging),
}

impl GenerationStaging {
    pub(in crate::persistence) fn generation(&self) -> &GenerationName {
        match self {
            Self::Generic(staged) => staged.generation(),
            Self::Current(staged) => staged.generation(),
        }
    }

    pub(in crate::persistence) fn path(&self) -> &Path {
        match self {
            Self::Generic(staged) => staged.path(),
            Self::Current(staged) => staged.path(),
        }
    }

    pub(in crate::persistence) fn commit_with_publication_guard<G>(
        self,
        store: &GenerationStore,
        acquire: impl FnOnce() -> std::io::Result<G>,
    ) -> Result<GenerationName, storage_durable::CommitError> {
        match self {
            Self::Generic(staged) => store.commit_with_publication_guard(staged, acquire),
            Self::Current(staged) => {
                store.commit_from_current_with_publication_guard(staged, acquire)
            }
        }
    }
}

pub(in crate::persistence) struct PendingFrozenCheckpoint {
    engine: Weak<Engine>,
    stamp: CaptureStamp,
    sequence: u64,
    predecessor: PendingPredecessor,
    frozen: FrozenCheckpoint,
    detached_capture_ns: u64,
    _layer_window: crate::segment_capacity::FrozenWindow,
}

/// Restores detached payload ownership to the shared slot if any fallible
/// pre-publication step returns. `disarm` is valid only after durable publish
/// plus live Engine binding have both succeeded.
struct PendingFrozenLease {
    slot: Arc<Mutex<Option<PendingFrozenCheckpoint>>>,
    pending: Option<PendingFrozenCheckpoint>,
}

impl PendingFrozenLease {
    fn pending(&self) -> &PendingFrozenCheckpoint {
        self.pending
            .as_ref()
            .expect("pending frozen lease is armed")
    }

    fn disarm(&mut self) {
        self.pending.take();
    }
}

impl Drop for PendingFrozenLease {
    fn drop(&mut self) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let mut slot = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            slot.is_none(),
            "save lock must serialize pending frozen ownership"
        );
        *slot = Some(pending);
    }
}

impl fmt::Debug for SegmentRdbStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SegmentRdbStore")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
