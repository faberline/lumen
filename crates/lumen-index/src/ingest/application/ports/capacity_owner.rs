//! The capacity-owner port: the checkpoint owner an apply coordinator or
//! committed record path ensures before waiting on layer capacity.

use std::sync::Arc;

use anyhow::Result;

use crate::index::application::engine::Engine;

pub trait CapacityOwner: Send + Sync {
    /// Start or attach the capacity checkpoint owner for this engine before
    /// waiting on layer capacity.
    fn ensure(&mut self, engine: &Arc<Engine>) -> Result<()>;

    /// Whether this owner has started an active worker or attached a relay.
    fn is_active(&self) -> bool {
        false
    }

    /// Stop any background worker or relay held by this capacity owner.
    fn stop(&mut self) {}
}

/// A no-op capacity owner used when no fallback capacity maintenance is required.
#[derive(Default)]
pub struct NoopCapacityOwner;

impl CapacityOwner for NoopCapacityOwner {
    fn ensure(&mut self, _engine: &Arc<Engine>) -> Result<()> {
        Ok(())
    }
}
