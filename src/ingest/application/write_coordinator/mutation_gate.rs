//! `MutationGate`, the process-wide fence between ordinary writes and
//! checkpoints on one side and a durable restore on the other, and its one-way
//! restart latch.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::ingest::application::write_coordinator::errors::RestartRequired;
use crate::ingest::application::write_coordinator::WriteCoordinator;

/// One process-local mutation boundary for embedded Standalone.
///
/// Shared permits cover ordinary writes and checkpoints. Durable replacement
/// takes the exclusive permit. `restart_required` is a one-way latch for this
/// process: a successful disk-space probe must never clear a commit-uncertain
/// or AOF-gap decision.
#[derive(Clone)]
pub struct MutationGate {
    lock: Arc<RwLock<()>>,
    restart_required: Arc<AtomicBool>,
}

impl Default for MutationGate {
    fn default() -> Self {
        Self {
            lock: Arc::new(RwLock::new(())),
            restart_required: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl MutationGate {
    /// Acquire a shared mutation/checkpoint permit unless the process already
    /// requires restart. The second check closes the race with a latch set while
    /// this caller waited behind an exclusive replacement.
    pub async fn shared(&self) -> Result<OwnedRwLockReadGuard<()>> {
        self.ensure_serving()?;
        let permit = self.lock.clone().read_owned().await;
        self.ensure_serving()?;
        Ok(permit)
    }

    /// Acquire the exclusive replacement permit unless a prior durability
    /// failure already requires restart.
    pub async fn exclusive(&self) -> Result<OwnedRwLockWriteGuard<()>> {
        self.ensure_serving()?;
        let permit = self.lock.clone().write_owned().await;
        self.ensure_serving()?;
        Ok(permit)
    }

    /// Permanently reject new mutations for this process.
    pub fn require_restart(&self) {
        self.restart_required.store(true, Ordering::Release);
    }

    pub fn is_restart_required(&self) -> bool {
        self.restart_required.load(Ordering::Acquire)
    }

    fn ensure_serving(&self) -> Result<()> {
        if self.is_restart_required() {
            return Err(anyhow::Error::new(RestartRequired(
                "durability state is uncertain; restart this Lumen process before retrying any mutation"
                    .to_string(),
            )));
        }
        Ok(())
    }
}

impl WriteCoordinator {
    /// Fence every [`Self::submit`] call in this process.
    ///
    /// The returned owned guard keeps the fence closed until it is dropped.
    /// Tokio's fair write-preferring queue also prevents a stream of new
    /// submits from starving a waiting restore.
    pub async fn fence_mutations(&self) -> Result<OwnedRwLockWriteGuard<()>> {
        self.mutation_gate.exclusive().await
    }

    /// Keep an ordinary checkpoint from crossing an exclusive restore.
    pub async fn checkpoint_permit(&self) -> Result<OwnedRwLockReadGuard<()>> {
        self.mutation_gate.shared().await
    }

    /// Clone the process-local gate for components that must join the same
    /// checkpoint/restore boundary.
    pub fn mutation_gate(&self) -> MutationGate {
        self.mutation_gate.clone()
    }

    pub fn require_restart(&self) {
        self.mutation_gate.require_restart();
    }

    pub fn is_restart_required(&self) -> bool {
        self.mutation_gate.is_restart_required()
    }
}
