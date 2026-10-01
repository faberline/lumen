//! The background task that syncs the local AOF every 100 ms without holding
//! the writer's lock through the sync, and requires a restart when a sync
//! fails.

use std::sync::Arc;

use crate::ingest::application::write_coordinator::{SharedAof, WriteCoordinator};

impl WriteCoordinator {
    pub(super) fn start_aof_sync(coord: &Arc<Self>, aof: SharedAof) {
        let weak = Arc::downgrade(coord);
        let engine = coord.engine.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if weak.upgrade().is_none() {
                    return;
                }
                // Never wait for the synchronous AOF mutex on a Tokio worker.
                // A test or a foreground append may intentionally hold it while
                // unrelated collections must continue serving queries.
                let plan = match tokio::task::spawn_blocking({
                    let aof = aof.clone();
                    move || {
                        let mut writer = match aof.try_lock() {
                            Ok(writer) => writer,
                            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                            Err(std::sync::TryLockError::Poisoned(_)) => {
                                return Err(anyhow::anyhow!("AOF writer poisoned"));
                            }
                        };
                        writer.begin_sync()
                    }
                })
                .await
                {
                    Ok(Ok(plan)) => plan,
                    Ok(Err(error)) => {
                        tracing::error!(%error, "AOF sync preparation failed");
                        engine.capture_barrier.apply().mark_uncertain();
                        if let Some(coord) = weak.upgrade() {
                            coord.mutation_gate.require_restart();
                        }
                        return;
                    }
                    Err(error) => {
                        tracing::error!(%error, "AOF sync preparation task failed");
                        engine.capture_barrier.apply().mark_uncertain();
                        if let Some(coord) = weak.upgrade() {
                            coord.mutation_gate.require_restart();
                        }
                        return;
                    }
                };
                let Some(plan) = plan else { continue };
                let sync_result = tokio::task::spawn_blocking(move || {
                    let mut plan = plan;
                    let result = plan.sync_off_lock();
                    (plan, result)
                })
                .await;
                let result = match sync_result {
                    Ok((plan, result)) => match tokio::task::spawn_blocking({
                        let aof = aof.clone();
                        move || {
                            let mut writer = aof
                                .lock()
                                .map_err(|_| anyhow::anyhow!("AOF writer poisoned"))?;
                            result.and_then(|()| writer.complete_sync(plan))
                        }
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(error) => {
                            Err(anyhow::anyhow!("AOF sync completion task failed: {error}"))
                        }
                    },
                    Err(error) => Err(anyhow::anyhow!("AOF sync task failed: {error}")),
                };
                if let Err(error) = result {
                    tracing::error!(%error, "AOF sync failed; restart required");
                    engine.capture_barrier.apply().mark_uncertain();
                    if let Some(coord) = weak.upgrade() {
                        coord.mutation_gate.require_restart();
                    }
                    return;
                }
            }
        });
    }
}
