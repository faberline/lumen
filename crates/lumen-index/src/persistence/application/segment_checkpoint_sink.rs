//! Shared durable segment checkpoint path for manual requests and background work.
//! File encoding and AOF trimming happen outside the apply barrier.

pub mod driver;
pub mod hnsw_cache;
pub mod pending_spill;

use crate::index::application::engine::Engine;
use anyhow::{Context, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::persistence::application::ports::checkpoint_sink::{
    CheckpointSink, HnswCacheSealReceipt,
};
use crate::persistence::infrastructure::checkpoint_process_state::NEXT_CHECKPOINT_ATTEMPT_ID;
use crate::persistence::infrastructure::segment_rdb_store::diagnostic::{
    checkpoint_diagnostic_enabled, CheckpointDiagnosticContext,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;

/// Real [`CheckpointSink`] wiring for segment-persistence mode
/// (#1389): forces the same synchronous stage-then-rename checkpoint the
/// periodic snapshotter performs (`SegmentRdbStore::save`), but synchronously
/// on demand — this is what `POST /admin/checkpoint` answers, and what the
/// reshard driver's cutover gate (`service_k8s::reshard_driver::
/// checkpoint_touched_shards`) awaits per touched shard before triggering the
/// cutover rolling restart. Also prunes + trims the AOF through the
/// checkpointed sequence, mirroring the periodic path exactly, so an
/// on-demand checkpoint leaves the AOF in the same state a periodic one
/// would (and a reshard cutover right after one doesn't leave a redundant,
/// ever-growing AOF tail).
pub struct SegmentCheckpointSink {
    pub engine: Arc<Engine>,
    pub store: Arc<SegmentRdbStore>,
    pub writer: Arc<dyn crate::ingest::application::write_coordinator::WriteSink>,
    pub aof: Option<crate::ingest::application::write_coordinator::SharedAof>,
}

/// Identifies the scheduler that started a durable checkpoint. This is trace
/// data only; all variants use the same checkpoint implementation.
#[derive(Clone, Copy)]
pub(crate) enum CheckpointTraceOrigin {
    Manual,
    Periodic,
    CapacityOwner,
}

#[derive(Clone, Copy)]
struct CheckpointSelection {
    reason: &'static str,
    source: &'static str,
    pending: crate::ingest::domain::change_budget::Snapshot,
}

impl CheckpointTraceOrigin {
    const fn label(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Periodic => "periodic",
            Self::CapacityOwner => "capacity_owner",
        }
    }
}

/// Keeps checkpoint-attempt telemetry balanced across every synchronous return
/// path. It deliberately owns no checkpoint state, so it cannot change save,
/// trim, or publication ordering.
struct CheckpointAttempt<'a> {
    metrics: &'a crate::app::observability::metrics::Metrics,
    failed: bool,
}

impl<'a> CheckpointAttempt<'a> {
    fn start(metrics: &'a crate::app::observability::metrics::Metrics) -> Self {
        metrics.start_segment_checkpoint_attempt();
        Self {
            metrics,
            failed: false,
        }
    }

    fn mark_failed(&mut self) {
        self.failed = true;
    }
}

impl Drop for CheckpointAttempt<'_> {
    fn drop(&mut self) {
        self.metrics.finish_segment_checkpoint_attempt(self.failed);
    }
}

impl SegmentCheckpointSink {
    async fn manual_boundary_is_durable(&self, target: u64) -> Result<bool> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            Ok(store
                .current_record()?
                .is_some_and(|record| record.sequence >= target))
        })
        .await
        .context("checkpoint boundary lookup task panicked")?
    }

    async fn checkpoint_with_fence(
        &self,
        fence: Option<crate::persistence::application::capacity::PublicationFence>,
        origin: CheckpointTraceOrigin,
        diagnostic_context: Option<CheckpointDiagnosticContext>,
    ) -> Result<bool> {
        self.checkpoint_requested_with_fence(fence, origin, diagnostic_context, None, None)
            .await
    }

    async fn checkpoint_requested_with_fence(
        &self,
        fence: Option<crate::persistence::application::capacity::PublicationFence>,
        origin: CheckpointTraceOrigin,
        diagnostic_context: Option<CheckpointDiagnosticContext>,
        request_revision: Option<u64>,
        selection: Option<CheckpointSelection>,
    ) -> Result<bool> {
        let diagnostic_context = diagnostic_context.or_else(|| {
            (matches!(origin, CheckpointTraceOrigin::Manual) && checkpoint_diagnostic_enabled())
                .then(|| {
                    CheckpointDiagnosticContext::new(
                        origin.label(),
                        Some(NEXT_CHECKPOINT_ATTEMPT_ID.fetch_add(1, Ordering::Relaxed)),
                    )
                })
        });
        let sink_engine = self.engine.clone();
        let sink_store = self.store.clone();
        let sink_writer = self.writer.clone();
        let sink_aof = self.aof.clone();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let _span = span.enter();
            (|| {
                let sink = Arc::new(SegmentCheckpointSink {
                    engine: sink_engine,
                    store: sink_store,
                    writer: sink_writer,
                    aof: sink_aof,
                });
                // Capacity refusal can happen while a manual checkpoint is frozen.
                // Register this sink first so relief uses the same root, rather than
                // retaining another full capture in a temporary spill store. Keep
                // the owner in the blocking task even if the async caller cancels.
                let _manual_owner = if fence.is_none() {
                    match crate::persistence::application::capacity::worker::Owner::start(
                        sink.clone(),
                        false,
                    ) {
                        Ok(owner) => owner,
                        Err(error) => {
                            Self::trace_checkpoint_started(diagnostic_context, selection);
                            let result = Err(error);
                            if let Some(context) = diagnostic_context {
                                context.trace_terminal(&result);
                            }
                            return result.map(|()| false);
                        }
                    }
                } else {
                    None
                };
                // Manual publication and its background merge may outlive this
                // scoped owner. Only a persistent driver's supplied fence applies
                // to them; a later manual request must not invalidate that merge.
                let store = match fence {
                    Some(fence) => sink.store.as_ref().clone().with_publication_fence(fence),
                    None => sink.store.as_ref().clone(),
                };
                sink.checkpoint_sync_selected(
                    &store,
                    origin,
                    diagnostic_context,
                    request_revision,
                    selection,
                )
            })()
        })
        .await
        .context("checkpoint task panicked")?
    }

    pub(crate) fn checkpoint_sync(&self, store: &SegmentRdbStore) -> Result<()> {
        let origin = if store.has_publication_fence() {
            CheckpointTraceOrigin::CapacityOwner
        } else {
            CheckpointTraceOrigin::Manual
        };
        self.checkpoint_sync_selected(store, origin, None, None, None)
            .map(|_| ())
    }

    pub(crate) fn checkpoint_sync_requested(
        &self,
        store: &SegmentRdbStore,
        request_revision: Option<u64>,
    ) -> Result<()> {
        self.checkpoint_sync_selected(
            store,
            CheckpointTraceOrigin::CapacityOwner,
            None,
            request_revision,
            None,
        )
        .map(|_| ())
    }

    fn checkpoint_sync_selected(
        &self,
        store: &SegmentRdbStore,
        origin: CheckpointTraceOrigin,
        diagnostic_context: Option<CheckpointDiagnosticContext>,
        request_revision: Option<u64>,
        selection: Option<CheckpointSelection>,
    ) -> Result<bool> {
        let progress = store.checkpoint_progress();
        let _execution = progress.as_ref().map(|state| state.lock_execution());
        if progress
            .as_ref()
            .zip(request_revision)
            .is_some_and(|(state, revision)| state.has_relieved(revision))
        {
            // This owner already completed the requested publication while
            // this caller waited. Merge/consumption still belongs to its relay.
            return Ok(false);
        }
        Self::trace_checkpoint_started(diagnostic_context, selection);
        let result = self.checkpoint_sync_with_origin(store, origin, diagnostic_context);
        if result.is_ok() && progress.as_ref().is_some_and(|state| state.completed()) {
            self.engine.notify_checkpoint_progress();
        }
        if let Some(context) = diagnostic_context {
            context.trace_terminal(&result);
        }
        result.map(|_| true)
    }

    fn trace_checkpoint_started(
        diagnostic_context: Option<CheckpointDiagnosticContext>,
        selection: Option<CheckpointSelection>,
    ) {
        if let Some(context) = diagnostic_context {
            if let Some(selection) = selection {
                context.trace_scheduler_selected(
                    selection.reason,
                    selection.source,
                    selection.pending,
                );
            }
            context.trace_phase("checkpoint_started");
        }
    }

    fn checkpoint_sync_with_origin(
        &self,
        store: &SegmentRdbStore,
        _origin: CheckpointTraceOrigin,
        diagnostic_context: Option<CheckpointDiagnosticContext>,
    ) -> Result<()> {
        let mut attempt = CheckpointAttempt::start(self.engine.metrics());
        let result = (|| {
            if self
                .writer
                .mutation_gate()
                .is_some_and(|gate| gate.is_restart_required())
            {
                return Err(anyhow::Error::new(
                    crate::ingest::application::write_coordinator::errors::RestartRequired(
                        "checkpoint refused: restart required".into(),
                    ),
                ));
            }
            let sequence = store.save_with_sequence_diagnostic_context(
                &self.engine,
                self.writer.applied_seq(),
                diagnostic_context,
            )?;
            store.prune(3)?;
            match store.disk_bytes() {
                Ok(bytes) => self.engine.metrics().set_segment_disk_bytes(bytes),
                Err(error) => tracing::warn!(%error, "segment disk metric unavailable after prune"),
            }
            if let Some(aof) = &self.aof {
                #[cfg(unix)]
                let trim = (|| {
                    let mut plan = {
                        let mut writer = aof
                            .lock()
                            .map_err(|_| anyhow::anyhow!("aof writer poisoned"))?;
                        writer.begin_trim(sequence)?
                    };
                    plan.copy_stable_prefix()?;
                    let mut writer = aof
                        .lock()
                        .map_err(|_| anyhow::anyhow!("aof writer poisoned"))?;
                    writer.finish_trim(plan)
                })();
                #[cfg(not(unix))]
                let trim = aof
                    .lock()
                    .map_err(|_| anyhow::anyhow!("aof writer poisoned"))
                    .and_then(|mut writer| writer.truncate_through(sequence));
                if let Err(error) = trim {
                    tracing::warn!(%error, "AOF trim after checkpoint failed");
                }
            }
            Ok(())
        })();
        if result.is_err() {
            attempt.mark_failed();
        }
        result
    }
}

#[async_trait::async_trait]
impl CheckpointSink for SegmentCheckpointSink {
    async fn checkpoint_now(&self) -> Result<bool> {
        let target = self.writer.checkpoint_target_seq().await?;
        let persisted = self
            .checkpoint_with_fence(None, CheckpointTraceOrigin::Manual, None)
            .await?;
        if self.manual_boundary_is_durable(target).await? {
            return Ok(persisted);
        }
        // The first publication must run before this wait: blocked committed
        // apply may need its capacity. No save/apply/restore permit crosses an
        // await here. Only the boundary captured at entry is required, so later
        // traffic cannot keep moving the goal or starve this request.
        tokio::time::timeout(
            crate::ingest::application::write_coordinator::SUBMIT_TIMEOUT,
            async {
                while self.writer.applied_seq() < target {
                    if self.writer.restart_required() {
                        return Err(anyhow::Error::new(
                            crate::ingest::application::write_coordinator::errors::RestartRequired(
                                "checkpoint boundary refused: restart required".into(),
                            ),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Ok::<_, anyhow::Error>(())
            },
        )
        .await
        .context("checkpoint committed boundary did not apply within the submit deadline")??;
        let published = self
            .checkpoint_with_fence(None, CheckpointTraceOrigin::Manual, None)
            .await?;
        anyhow::ensure!(
            self.manual_boundary_is_durable(target).await?,
            "checkpoint publication did not cover its committed boundary {target}"
        );
        Ok(persisted || published)
    }

    async fn seal_hnsw_graph_cache(&self) -> Result<HnswCacheSealReceipt> {
        Arc::new(SegmentCheckpointSink {
            engine: self.engine.clone(),
            store: self.store.clone(),
            writer: self.writer.clone(),
            aof: self.aof.clone(),
        })
        .seal_hnsw_graph_cache()
        .await
    }
}

/// A read-only `WriteSink` for bootstrap pending-change spill checkpoints.
///
/// It samples the Engine capture barrier because the coordinator may not exist
/// before AOF or Raft replay. `SegmentRdbStore` captures again during save, so
/// this fallback can never fabricate a later sequence.
#[doc(hidden)]
pub struct EngineWatermarkSink {
    engine: Arc<Engine>,
}

impl EngineWatermarkSink {
    #[doc(hidden)]
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

#[async_trait::async_trait]
impl crate::ingest::application::write_coordinator::WriteSink for EngineWatermarkSink {
    async fn submit(
        &self,
        _: crate::shared_kernel::log_entry::RaftLogEntry,
    ) -> Result<crate::index::application::engine::raft_dispatch::ApplyOutcome> {
        anyhow::bail!("bootstrap checkpoint watermark is not a write sink")
    }

    fn applied_seq(&self) -> u64 {
        self.engine
            .capture_barrier
            .capture(0)
            .map(|lease| lease.stamp().sequence)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests;
