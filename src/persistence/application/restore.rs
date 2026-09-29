//! Durable, single-process segment restore.

pub(crate) mod durable;

use std::io;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::FutureExt;

use crate::api::RestoreSink;
use crate::ingest::application::write_coordinator::{
    mutation_gate::MutationGate, SharedAof, WriteSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::shared_kernel::capture_barrier::RestoreInhibition;
use crate::storage::{Engine, SnapshotV1};

#[derive(Debug)]
/// A durable restore failed before it could move `CURRENT` or live state.
pub(crate) struct RestoreNotCommitted(pub(crate) String);

impl std::fmt::Display for RestoreNotCommitted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RestoreNotCommitted {}

/// Durable restore is unavailable because this deployment cannot provide the
/// embedded single-process mutation contract.
#[derive(Debug)]
pub(crate) struct RestoreUnavailable(pub(crate) String);

impl std::fmt::Display for RestoreUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RestoreUnavailable {}

/// Test-only ordering hook for the durable restore publication boundary.
///
/// Production callers leave this unset. It is public only so integration
/// tests can pause after candidate CURRENT publication.
#[doc(hidden)]
#[async_trait]
pub trait RestorePublicationObserver: Send + Sync {
    async fn after_candidate_current_published(&self, generation: String, sequence: u64);
}

/// Fail-closed restore sink for deployments that cannot perform embedded
/// single-process segment restore.
pub struct UnavailableRestoreSink {
    reason: String,
}

impl UnavailableRestoreSink {
    /// Create a sink that rejects every restore with the supplied reason.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

#[async_trait]
impl RestoreSink for UnavailableRestoreSink {
    async fn restore(&self, _snapshot: SnapshotV1) -> Result<()> {
        Err(anyhow::Error::new(RestoreUnavailable(self.reason.clone())))
    }
}

/// Restore sink for embedded segment persistence.
#[derive(Clone)]
pub struct SegmentRestoreSink {
    live_engine: Arc<Engine>,
    store: Arc<SegmentRdbStore>,
    writer: Arc<dyn WriteSink>,
    aof: SharedAof,
    gate: MutationGate,
    publication_observer: Option<Arc<dyn RestorePublicationObserver>>,
    #[cfg(test)]
    reload_integrity_mismatch: Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    activation_failure: Arc<std::sync::atomic::AtomicBool>,
}

impl SegmentRestoreSink {
    /// Construct a sink only for a writer that shares this process's mutation
    /// gate. Cluster writers cannot provide the atomic replacement contract.
    pub fn new(
        live_engine: Arc<Engine>,
        store: Arc<SegmentRdbStore>,
        writer: Arc<dyn WriteSink>,
        aof: SharedAof,
    ) -> Result<Self> {
        let gate = writer.mutation_gate().ok_or_else(|| {
            anyhow!("durable segment restore requires a process-local mutation gate")
        })?;
        Ok(Self {
            live_engine,
            store,
            writer,
            aof,
            gate,
            publication_observer: None,
            #[cfg(test)]
            reload_integrity_mismatch: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            activation_failure: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Attach the deterministic publication observer used by integration tests.
    #[doc(hidden)]
    pub fn with_publication_observer(
        mut self,
        observer: Arc<dyn RestorePublicationObserver>,
    ) -> Self {
        self.publication_observer = Some(observer);
        self
    }

    #[cfg(test)]
    pub(crate) fn force_reload_integrity_mismatch(&self) {
        self.reload_integrity_mismatch
            .store(true, std::sync::atomic::Ordering::Release);
    }
    #[cfg(test)]
    pub(crate) fn force_activation_failure(&self) {
        self.activation_failure
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn storage_full(error: &anyhow::Error) -> bool {
        error.chain().any(|cause| {
            cause
                .downcast_ref::<io::Error>()
                .is_some_and(|e| e.kind() == io::ErrorKind::StorageFull)
        })
    }

    fn not_committed(error: anyhow::Error) -> anyhow::Error {
        anyhow::Error::new(RestoreNotCommitted(error.to_string())).context(error)
    }

    fn mark_full(&self) {
        self.live_engine.metrics().mark_storage_degraded();
    }

    fn restart(&self) {
        self.gate.require_restart();
        self.live_engine.capture_barrier.apply().mark_uncertain();
    }

    /// The restore inhibition intentionally blocks ordinary apply. Latch a
    /// restart through its capability instead of trying to acquire ordinary
    /// apply while the inhibition is still responsible for the transition.
    fn restart_while_inhibited(&self, inhibition: &RestoreInhibition<'_>) {
        self.gate.require_restart();
        inhibition.mark_uncertain();
    }
}

#[async_trait]
impl RestoreSink for SegmentRestoreSink {
    async fn restore(&self, snapshot: SnapshotV1) -> Result<()> {
        // Decode malformed input before creating durable work. Cancellation here
        // has no publication side effect.
        let candidate = tokio::task::spawn_blocking(move || {
            let candidate = Arc::new(Engine::new());
            candidate.restore(snapshot).map(|()| candidate)
        })
        .await
        .map_err(|join| Self::not_committed(anyhow!("candidate restore task failed: {join}")))??;

        // From this point the owned task owns the exclusive fence, publication
        // inhibition, and all blocking work through activation. Dropping the
        // request future only drops its JoinHandle; the durable transition
        // continues to a terminal state.
        let owned = self.clone();
        let task = tokio::spawn(async move {
            match AssertUnwindSafe(owned.restore_durable(candidate))
                .catch_unwind()
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    owned.restart();
                    Err(anyhow::Error::new(
                        crate::ingest::application::write_coordinator::errors::RestartRequired(
                            "segment restore task panicked; restart required".into(),
                        ),
                    ))
                }
            }
        });
        task.await.map_err(|join| {
            self.restart();
            anyhow::Error::new(
                crate::ingest::application::write_coordinator::errors::RestartRequired(
                    "segment restore task stopped; restart required".into(),
                ),
            )
            .context(anyhow!("restore task failed: {join}"))
        })?
    }
}

#[cfg(test)]
mod tests;
