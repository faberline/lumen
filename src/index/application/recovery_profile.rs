//! The opt-in recovery profile: how long each phase of a checkpoint recovery
//! took and how many collections and vector fields it opened, kept as
//! aggregates only and logged once when recovery completes.

use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Opt-in, aggregate-only measurements for a durable checkpoint recovery.
///
/// This deliberately keeps no checkpoint paths, collection names, field names,
/// external IDs, or document values.  The caller owns the one structured log
/// line emitted after recovery has completed.
#[derive(Clone)]
pub(crate) struct RecoveryProfile {
    enabled: bool,
    inner: Arc<Mutex<RecoveryProfileData>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RecoveryPhase {
    LayoutValidation,
    ManifestDecode,
    BaseRowsDecode,
    FlatCompatibilityTree,
    EngineReopen,
    DeltaDecodeApply,
    CheckpointHnswGraph,
    IdentityHydration,
}

impl RecoveryPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::LayoutValidation => "layout_validation",
            Self::ManifestDecode => "manifest_decode",
            Self::BaseRowsDecode => "base_rows_decode",
            Self::FlatCompatibilityTree => "flat_compatibility_tree",
            Self::EngineReopen => "engine_reopen",
            Self::DeltaDecodeApply => "delta_decode_apply",
            Self::CheckpointHnswGraph => "checkpoint_hnsw_graph",
            Self::IdentityHydration => "identity_hydration",
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct RecoveryProfileData {
    pub collection_open_count: u64,
    pub collection_open_total_ms: u64,
    pub collection_open_max_ms: u64,
    pub vector_flat_open_count: u64,
    pub vector_flat_open_ms: u64,
    pub vector_hnsw_open_count: u64,
    pub vector_hnsw_open_ms: u64,
    pub coverage_rebuild_ms: u64,
}

impl RecoveryProfile {
    pub(crate) fn from_env() -> Self {
        Self::new(std::env::var_os("LUMEN_RECOVERY_PROFILE").is_some_and(|value| value == "1"))
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(true)
    }

    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            inner: Arc::new(Mutex::new(RecoveryProfileData::default())),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn phase_start<T>(&self, phase: RecoveryPhase, work: impl FnOnce() -> T) -> T {
        if self.enabled {
            tracing::info!(phase = phase.label(), state = "start", "recovery phase");
        }
        work()
    }

    pub(crate) fn reset(&self) {
        if self.enabled {
            *self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = RecoveryProfileData::default();
        }
    }

    pub(crate) fn collection_opened(&self, elapsed: Duration) {
        if self.enabled {
            let elapsed_ms = elapsed.as_millis() as u64;
            let mut data = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            data.collection_open_count += 1;
            data.collection_open_total_ms += elapsed_ms;
            data.collection_open_max_ms = data.collection_open_max_ms.max(elapsed_ms);
        }
    }

    pub(crate) fn vector_opened(
        &self,
        backend: crate::shared_kernel::types::schema::VectorBackend,
        elapsed: Duration,
    ) {
        if self.enabled {
            let elapsed_ms = elapsed.as_millis() as u64;
            let mut data = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match backend {
                crate::shared_kernel::types::schema::VectorBackend::FlatCpu => {
                    data.vector_flat_open_count += 1;
                    data.vector_flat_open_ms += elapsed_ms;
                }
                crate::shared_kernel::types::schema::VectorBackend::HnswCpu => {
                    data.vector_hnsw_open_count += 1;
                    data.vector_hnsw_open_ms += elapsed_ms;
                }
            }
        }
    }

    pub(crate) fn coverage_rebuilt(&self, elapsed: Duration) {
        if self.enabled {
            self.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .coverage_rebuild_ms += elapsed.as_millis() as u64;
        }
    }

    pub(crate) fn snapshot(&self) -> Option<RecoveryProfileData> {
        self.enabled.then(|| {
            self.inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        })
    }
}

#[cfg(test)]
mod tests;
