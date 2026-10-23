//! RestoreSink, the administrative restore's backend, and the in-memory default
//! that restores into a disposable candidate before it takes the exclusive
//! mutation gate and swaps the live state.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::index::application::engine::Engine;
use crate::index::infrastructure::snapshot_v1::SnapshotV1;
use crate::ingest::application::write_coordinator::mutation_gate::MutationGate;

/// Restore backend for the administrative restore operation.
#[async_trait]
pub trait RestoreSink: Send + Sync {
    async fn restore(&self, snapshot: SnapshotV1) -> Result<()>;
}

/// Default restore implementation for the in-memory engine.
///
/// The snapshot is restored into a disposable candidate before the exclusive
/// mutation gate is acquired. This keeps malformed snapshots from waiting
/// behind in-flight writes and makes the live replacement a single swap.
pub(crate) struct InMemoryRestoreSink {
    engine: Arc<Engine>,
    mutation_gate: Option<MutationGate>,
}

impl InMemoryRestoreSink {
    pub(crate) fn new(engine: Arc<Engine>, mutation_gate: Option<MutationGate>) -> Self {
        Self {
            engine,
            mutation_gate,
        }
    }
}

#[async_trait]
impl RestoreSink for InMemoryRestoreSink {
    async fn restore(&self, snapshot: SnapshotV1) -> Result<()> {
        let candidate = Engine::new();
        candidate.restore(snapshot)?;
        let _permit = match &self.mutation_gate {
            Some(gate) => Some(gate.exclusive().await?),
            None => None,
        };
        self.engine.activate_replacement(candidate)
    }
}

#[cfg(test)]
mod tests;
