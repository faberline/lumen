//! The HNSW graph cache around a planned restart: sealing the cache behind the
//! exclusive writer gate, with a marker for the exact engine, store and
//! mutation stamp it matches, and the shutdown-only cache save that a current
//! marker lets skip.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::sync::oneshot;

use crate::persistence::application::segment_checkpoint_sink::SegmentCheckpointSink;
use crate::persistence::infrastructure::checkpoint_process_state::{
    HnswCacheSealKey, HnswCacheSealMarker, HNSW_CACHE_SEALS,
};

impl SegmentCheckpointSink {
    fn hnsw_cache_seal_key(&self) -> HnswCacheSealKey {
        (
            Arc::as_ptr(&self.engine) as usize,
            Arc::as_ptr(&self.store) as usize,
        )
    }

    fn current_hnsw_cache_seal(&self) -> bool {
        let stamp = self.engine.capture_barrier.mutation_stamp();
        let Ok(mut markers) = HNSW_CACHE_SEALS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
        else {
            return false;
        };
        markers.retain(|_, marker| {
            marker.engine.upgrade().is_some() && marker.store.upgrade().is_some()
        });
        markers
            .get(&self.hnsw_cache_seal_key())
            .is_some_and(|marker| {
                marker.stamp == stamp
                    && marker
                        .engine
                        .upgrade()
                        .is_some_and(|engine| Arc::ptr_eq(&engine, &self.engine))
                    && marker
                        .store
                        .upgrade()
                        .is_some_and(|store| Arc::ptr_eq(&store, &self.store))
            })
    }

    pub(super) fn record_hnsw_cache_seal(
        &self,
        stamp: crate::shared_kernel::capture_barrier::MutationStamp,
    ) -> Result<()> {
        let mut markers = HNSW_CACHE_SEALS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| anyhow::anyhow!("HNSW cache seal marker poisoned"))?;
        markers.insert(
            self.hnsw_cache_seal_key(),
            HnswCacheSealMarker {
                engine: Arc::downgrade(&self.engine),
                store: Arc::downgrade(&self.store),
                stamp,
            },
        );
        Ok(())
    }

    /// An unsuccessful seal must not let a prior receipt suppress shutdown's
    /// normal graph-cache write. A poisoned marker lock is also fail-closed:
    /// `current_hnsw_cache_seal` treats it as no marker.
    fn clear_hnsw_cache_seal(&self) {
        if let Ok(mut markers) = HNSW_CACHE_SEALS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
        {
            markers.remove(&self.hnsw_cache_seal_key());
        }
    }

    #[cfg(test)]
    pub(super) fn has_current_hnsw_cache_seal(&self) -> bool {
        self.current_hnsw_cache_seal()
    }

    /// Publish a planned-restart cache while ordinary writers and direct
    /// reshard mutations are held behind the existing exclusive writer gate.
    /// A background checkpoint can still advance the capture stamp, so the
    /// marker is recorded only when the one-lock stamps before and after cache
    /// publication match exactly.
    pub async fn seal_hnsw_graph_cache(
        self: Arc<Self>,
    ) -> Result<crate::api::HnswCacheSealReceipt> {
        let gate = self.writer.mutation_gate().ok_or_else(|| {
            anyhow::Error::new(crate::api::HnswCacheSealUnavailable(
                "HNSW restart cache sealing requires the standalone writer fence".to_string(),
            ))
        })?;
        let permit = gate.exclusive().await?;
        let (send, receive) = oneshot::channel();
        std::thread::Builder::new()
            .name("lumen-hnsw-restart-cache-seal".into())
            .spawn(move || {
                let _permit = permit;
                self.clear_hnsw_cache_seal();
                let result = (|| {
                    if !self.engine.has_hnsw_graphs()? {
                        return Err(anyhow::Error::new(crate::api::HnswCacheSealUnavailable(
                            "no live HNSW graph is available for the planned restart".to_string(),
                        )));
                    }
                    let mut before = self.engine.capture_barrier.mutation_stamp();
                    let durability = if let Some(aof) = &self.aof {
                        aof.lock()
                            .map_err(|_| anyhow::anyhow!("aof writer poisoned"))?
                            .sync()?;
                        crate::api::HnswCacheDurability::AofSynced
                    } else {
                        // The fallback must commit a complete authoritative
                        // checkpoint before exposing an optional graph cache.
                        self.checkpoint_sync(&self.store)?;
                        // This checkpoint deliberately advances the barrier.
                        // Begin the graph-publication comparison after its own
                        // durable mutation has completed.
                        before = self.engine.capture_barrier.mutation_stamp();
                        crate::api::HnswCacheDurability::CheckpointCommitted
                    };
                    let cache_fields = self.store.save_hnsw_graph_caches(&self.engine)?;
                    if cache_fields == 0 {
                        return Err(anyhow::Error::new(crate::api::HnswCacheSealUnavailable(
                            "no HNSW graph cache fields were published".to_string(),
                        )));
                    }
                    let after = self.engine.capture_barrier.mutation_stamp();
                    if after != before {
                        return Err(anyhow::Error::new(crate::api::HnswCacheSealInvalidated(
                            "live mutation stamp changed while HNSW cache sealing ran".to_string(),
                        )));
                    }
                    self.record_hnsw_cache_seal(after)?;
                    Ok(crate::api::HnswCacheSealReceipt {
                        cache_fields,
                        durability,
                        mutation_epoch: after.epoch,
                        mutation_apply_revision: after.apply_revision,
                    })
                })();
                let _ = send.send(result);
            })
            .context("start HNSW restart cache seal worker")?;
        receive
            .await
            .context("HNSW restart cache seal worker stopped")?
    }

    /// Standalone shutdown-only preparation. The caller owns its absolute
    /// timeout. A detached native thread cannot make Tokio runtime shutdown
    /// wait forever for optional graph IO; incomplete files remain cache misses.
    #[doc(hidden)]
    pub async fn save_shutdown_graph_cache(self: Arc<Self>) -> Result<usize> {
        let gate = self
            .writer
            .mutation_gate()
            .context("shutdown cache requires a writer fence")?;
        let permit = gate.exclusive().await?;
        let (send, receive) = oneshot::channel();
        std::thread::Builder::new()
            .name("lumen-hnsw-shutdown-cache".into())
            .spawn(move || {
                let _permit = permit;
                let result = (|| {
                    if self.current_hnsw_cache_seal() {
                        return Ok(0);
                    }
                    if !self.engine.has_hnsw_graphs()? && !self.store.has_hnsw_graph_cache() {
                        return Ok(0);
                    }
                    if let Some(aof) = &self.aof {
                        // The exclusive writer fence includes every completed
                        // append. Preserve that durable tail instead of spending
                        // Docker's stop window rewriting the full checkpoint.
                        aof.lock()
                            .map_err(|_| anyhow::anyhow!("aof writer poisoned"))?
                            .sync()?;
                    } else {
                        self.checkpoint_sync(&self.store)?;
                    }
                    self.store.save_hnsw_graph_caches(&self.engine)
                })();
                let _ = send.send(result);
            })
            .context("start optional shutdown cache worker")?;
        receive.await.context("shutdown cache worker stopped")?
    }
}
