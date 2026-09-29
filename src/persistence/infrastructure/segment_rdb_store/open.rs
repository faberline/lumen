//! Opening a store: the process-wide save gate and pending checkpoint shared by
//! every handle on one root, the constructors, and the handle accessors the
//! engine and the background merge use.

use crate::index::application::{engine::Engine, recovery_profile::RecoveryProfile};
use crate::persistence::infrastructure::merge_worker::shared;
use crate::persistence::infrastructure::segment_rdb_store::save_gate::SaveGate;
use crate::persistence::infrastructure::segment_rdb_store::startup::StartupBootstrap;
use crate::persistence::infrastructure::segment_rdb_store::{
    CheckpointRootGuard, MergeObserver, NoMergeObserver, PendingFrozenCheckpoint, RecoveryTimings,
    SegmentArchivePin, SegmentRdbStore,
};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use storage_durable::{FailureInjector, GenerationName, GenerationStore, NoFailures};

impl SegmentRdbStore {
    /// Pin an already published immutable generation for a detached archive
    /// reader.  Callers hold the save permit while creating this pin, so prune
    /// cannot observe the generation between publication and protection.
    pub(crate) fn pin_published_generation(
        &self,
        name: &GenerationName,
    ) -> Result<SegmentArchivePin> {
        let record = self.record_for_name(name.clone())?;
        self.background.pin(name.as_str().to_owned());
        Ok(SegmentArchivePin {
            background: self.background.clone(),
            _root_guard: self.root_guard.clone(),
            name: name.as_str().to_owned(),
            path: record.path,
            sequence: record.sequence,
        })
    }

    pub(crate) fn with_root_guard(mut self, guard: CheckpointRootGuard) -> Self {
        self.root_guard = Some(guard);
        self
    }

    pub(crate) fn with_publication_fence(
        mut self,
        fence: crate::persistence::application::capacity::PublicationFence,
    ) -> Self {
        self.publication_fence = Some(fence);
        self
    }

    pub(crate) fn has_publication_fence(&self) -> bool {
        self.publication_fence.is_some()
    }

    pub(crate) fn request_capacity_merge(&self, engine: &Arc<Engine>) -> Result<u64> {
        self.request_merge_for_capacity_retry(engine, None, None)
    }

    /// Wait only for the capacity request's next root publication or an idle
    /// queue. A later unrelated root job does not delay a new checkpoint.
    pub(crate) fn wait_for_capacity_merge_progress(
        &self,
        revision: u64,
        timeout: Duration,
    ) -> Result<()> {
        self.background
            .wait_for_capacity_progress_after(revision, Instant::now() + timeout)
            .map(|_| ())
    }

    pub(crate) fn has_current_generation(&self) -> Result<bool> {
        Ok(self.current_record()?.is_some())
    }

    pub(super) fn retain_root_for(&self, engine: &Engine) {
        if let Some(guard) = &self.root_guard {
            engine.retain_checkpoint_root(guard.clone());
        }
    }

    pub(super) fn reset_recovery_profile(&self) {
        self.recovery_profile.reset();
        if self.recovery_profile.enabled() {
            *self
                .recovery_timings
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = RecoveryTimings::default();
        }
    }

    pub(super) fn add_recovery_timing(&self, update: impl FnOnce(&mut RecoveryTimings)) {
        if self.recovery_profile.enabled() {
            update(
                &mut self
                    .recovery_timings
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            );
        }
    }

    pub(super) fn emit_recovery_profile(&self) {
        let Some(profile) = self.recovery_profile.snapshot() else {
            return;
        };
        let timings = self
            .recovery_timings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tracing::info!(
            layout_validation_ms = timings.layout_validation_ms,
            manifest_decode_ms = timings.manifest_decode_ms,
            base_decode_ms = timings.base_decode_ms,
            delta_decode_ms = timings.delta_decode_ms,
            reopen_ms = timings.reopen_ms,
            collection_open_count = profile.collection_open_count,
            collection_open_total_ms = profile.collection_open_total_ms,
            collection_open_max_ms = profile.collection_open_max_ms,
            vector_flat_open_count = profile.vector_flat_open_count,
            vector_flat_open_ms = profile.vector_flat_open_ms,
            vector_hnsw_open_count = profile.vector_hnsw_open_count,
            vector_hnsw_open_ms = profile.vector_hnsw_open_ms,
            coverage_rebuild_ms = profile.coverage_rebuild_ms,
            identity_hydration_ms = timings.identity_hydration_ms,
            vector_finish_ms = timings.vector_finish_ms,
            "segment durable recovery profile"
        );
    }

    /// Open or create the checkpoint root.
    ///
    /// A genuinely empty root receives the explicit empty `CURRENT` sentinel.
    /// A root with exact 0.4.28 legacy generations remains uninitialized until
    /// [`Self::reopen_into_with_outcome`] validates and adopts the highest one.
    /// A non-empty root with an unknown layout fails before this method writes
    /// `CURRENT` or removes any entry.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_injector_and_observer(root, Arc::new(NoFailures), Arc::new(NoMergeObserver))
    }

    fn open_with_injector(
        root: impl Into<PathBuf>,
        injector: Arc<dyn FailureInjector>,
    ) -> Result<Self> {
        Self::open_with_injector_and_observer(root, injector, Arc::new(NoMergeObserver))
    }

    fn open_with_injector_and_observer(
        root: impl Into<PathBuf>,
        injector: Arc<dyn FailureInjector>,
        observer: Arc<dyn MergeObserver>,
    ) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .with_context(|| format!("create segment-checkpoint dir {}", root.display()))?;
        let generations = GenerationStore::open_with_injector(&root, injector)
            .with_context(|| format!("open generation store {}", root.display()))?;
        let root = std::fs::canonicalize(&root)
            .with_context(|| format!("canonicalize checkpoint root {}", root.display()))?;
        let store = Self {
            save_gate: shared_save_gate(&root)?,
            pending_frozen: shared_pending_frozen(&root)?,
            merge_observer: observer,
            background: shared(&root)?,
            root_guard: None,
            publication_fence: None,
            root,
            generations,
            bootstrap: StartupBootstrap::ExistingCurrent { staging_cleaned: 0 },
            verified_catalog: Arc::new(Mutex::new(None)),
            recovery_profile: RecoveryProfile::from_env(),
            recovery_timings: Arc::new(Mutex::new(RecoveryTimings::default())),
        };
        let _guard = store.save_gate.lock_owned();
        let bootstrap = store.prepare_startup_root()?;
        drop(_guard);
        Ok(Self { bootstrap, ..store })
    }

    #[doc(hidden)]
    pub fn with_merge_observer(
        root: impl Into<PathBuf>,
        observer: Arc<dyn MergeObserver>,
    ) -> Result<Self> {
        Self::open_with_injector_and_observer(root, Arc::new(NoFailures), observer)
    }

    /// Drive deterministic durability and merge interleavings in integration tests.
    #[doc(hidden)]
    pub fn with_failure_injector_and_merge_observer(
        root: impl Into<PathBuf>,
        injector: Arc<dyn FailureInjector>,
        observer: Arc<dyn MergeObserver>,
    ) -> Result<Self> {
        Self::open_with_injector_and_observer(root, injector, observer)
    }

    /// Open a store with deterministic filesystem failures for restore tests.
    #[cfg(test)]
    pub(crate) fn new_with_failure_injector(
        root: impl Into<PathBuf>,
        injector: Arc<dyn FailureInjector>,
    ) -> Result<Self> {
        Self::open_with_injector(root, injector)
    }
}

fn shared_save_gate(root: &Path) -> Result<Arc<SaveGate>> {
    static ROOT_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<SaveGate>>>> = OnceLock::new();

    let registry = ROOT_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .map_err(|_| anyhow!("segment root-lock registry poisoned"))?;
    registry.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = registry.get(root).and_then(Weak::upgrade) {
        return Ok(lock);
    }

    let gate = Arc::new(SaveGate::default());
    registry.insert(root.to_path_buf(), Arc::downgrade(&gate));
    Ok(gate)
}

fn shared_pending_frozen(root: &Path) -> Result<Arc<Mutex<Option<PendingFrozenCheckpoint>>>> {
    static ROOT_PENDING: OnceLock<
        Mutex<HashMap<PathBuf, Weak<Mutex<Option<PendingFrozenCheckpoint>>>>>,
    > = OnceLock::new();
    let registry = ROOT_PENDING.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .map_err(|_| anyhow!("segment pending-frozen registry poisoned"))?;
    registry.retain(|_, pending| pending.strong_count() > 0);
    if let Some(pending) = registry.get(root).and_then(Weak::upgrade) {
        return Ok(pending);
    }
    let pending = Arc::new(Mutex::new(None));
    registry.insert(root.to_path_buf(), Arc::downgrade(&pending));
    Ok(pending)
}
