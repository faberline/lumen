//! The bootstrap pending-change spill: a checkpoint sink and budget driver over
//! a private temporary root, or over the configured root during startup replay
//! with the AOF left out, so changes can be checkpointed before the serving
//! sink exists.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use crate::index::application::engine::Engine;
use crate::ingest::domain::change_budget::ChangeBudget;
use crate::persistence::application::segment_checkpoint_sink::driver::SegmentCheckpointDriver;
use crate::persistence::application::segment_checkpoint_sink::{
    EngineWatermarkSink, SegmentCheckpointSink,
};
use crate::persistence::infrastructure::segment_rdb_store::SegmentRdbStore;
use crate::persistence::infrastructure::spill_directory::SpillDirectory;

/// Owns a private spill root plus an early budget driver. It never becomes the
/// public checkpoint sink and is never read on a later process start.
#[doc(hidden)]
pub struct PendingChangeSpill {
    pub(super) sink: Arc<SegmentCheckpointSink>,
    pub(super) driver: Option<SegmentCheckpointDriver>,
    pub(super) temporary_root: Option<Arc<SpillDirectory>>,
}

impl PendingChangeSpill {
    #[doc(hidden)]
    pub fn temporary(engine: Arc<Engine>, period: Duration) -> Result<Self> {
        let root = Arc::new(SpillDirectory::create()?);
        let store = Arc::new(SegmentRdbStore::new(&root.path)?.with_root_guard(root.clone()));
        Ok(Self::start(engine, store, period, Some(root)))
    }

    /// Uses the configured segment root during startup replay. AOF is omitted,
    /// so this bootstrap path cannot trim the replay source.
    #[doc(hidden)]
    pub fn configured_replay(
        engine: Arc<Engine>,
        store: Arc<SegmentRdbStore>,
        period: Duration,
    ) -> Self {
        Self::start(engine, store, period, None)
    }

    fn start(
        engine: Arc<Engine>,
        store: Arc<SegmentRdbStore>,
        period: Duration,
        temporary_root: Option<Arc<SpillDirectory>>,
    ) -> Self {
        let sink = Arc::new(SegmentCheckpointSink {
            writer: Arc::new(EngineWatermarkSink::new(engine.clone())),
            engine,
            store,
            aof: None,
        });
        let driver = sink.clone().spawn_driver_with_owner_kind(
            period,
            ChangeBudget::process_shared(),
            temporary_root.is_none(),
        );
        Self {
            sink,
            driver: Some(driver),
            temporary_root,
        }
    }

    /// The caller must await this before starting another driver for the same
    /// configured root. It keeps the private root alive.
    #[doc(hidden)]
    pub async fn stop_bootstrap(&mut self) -> Result<()> {
        if let Some(driver) = &mut self.driver {
            driver.shutdown().await?;
        }
        self.driver.take();
        Ok(())
    }

    #[doc(hidden)]
    pub fn store(&self) -> &Arc<SegmentRdbStore> {
        &self.sink.store
    }
}
