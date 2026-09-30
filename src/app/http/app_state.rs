//! AppState, the state every handler shares: the engine and auth, the read and
//! write backends, the checkpoint and restore sinks, the write fence and the
//! routed deployment's cross-pod router, with the builders that wire them.

use std::sync::Arc;

use crate::access::application::auth_config::AuthConfig;
use crate::access::infrastructure::lumen_verifier::LumenVerifier;
use crate::app::http::write_fence::WriteFence;
use crate::index::application::engine::Engine;
use crate::index::application::ports::search_backend::{LocalEngineSearch, SearchBackend};
use crate::index::infrastructure::search_executor::BlockingSearchExecutor;
use crate::ingest::application::ports::write_backend::{LocalWriteBackend, WriteBackend};
use crate::ingest::application::write_coordinator::{WriteCoordinator, WriteSink};
use crate::ingest::domain::wal_log::SharedWal;
use crate::ingest::infrastructure::wal::mem_wal::MemWal;
use crate::persistence::application::ports::checkpoint_sink::{CheckpointSink, NoopCheckpoint};
use crate::persistence::application::ports::restore_sink::{InMemoryRestoreSink, RestoreSink};
use crate::sharding::application::ports::routed_backend::RoutedBackend;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub auth: Arc<AuthConfig>,
    pub(super) verifier: Arc<LumenVerifier>,
    pub cluster: Option<Arc<crate::replication::domain::cluster_state::ClusterState>>,
    /// Read/search backend. Defaults to the local engine; sharded serving can
    /// replace it with a fan-in router while keeping writes/stats local.
    pub search_backend: Arc<dyn SearchBackend>,
    /// Shared bounded bridge for every local synchronous HTTP search path.
    pub(crate) search_executor: BlockingSearchExecutor,
    /// Writes go through a [`WriteSink`]: the WAL-seam coordinator for
    /// embedded, or the raft host for `--wal raft`. Reads use
    /// `engine` directly. See `coordinator` / `wal` / `raft_sm`.
    pub writer: Arc<dyn WriteSink>,
    /// Write/mutation backend. Defaults to the local coordinator; sharded
    /// serving can replace it with a document-router that fans out writes
    /// across independent shard coordinators.
    pub write_backend: Arc<dyn WriteBackend>,
    /// Durability-on-demand seam for `POST /admin/checkpoint` (#1389).
    /// Defaults to [`NoopCheckpoint`]; the server binary wires a real
    /// segment-checkpoint implementation when segment persistence is
    /// configured. See [`CheckpointSink`].
    pub checkpoint: Arc<dyn CheckpointSink>,
    /// Restore backend for `POST /admin/restore`. Defaults to an in-memory
    /// candidate-and-swap sink; the server binary may wire a durable variant.
    pub(crate) restore_sink: Arc<dyn RestoreSink>,
    /// Bounded write pause on still-moving virtual buckets during a
    /// reshard's final `CatchingUp` pass (#1396 R2). Defaults to unarmed
    /// (every write passes through unchanged); the reshard driver arms it
    /// via `POST /admin/reshard:fence`. See [`WriteFence`].
    pub write_fence: WriteFence,
    /// Cross-pod shard router for operator/k8s serving (#1398 R1-R3).
    /// `None` for every deployment shape except the routed one (`SHARD_COUNT`
    /// > 1, `replicasPerShard <= 1`, no `--search-shard-segment-dirs`) — see
    /// [`RoutedBackend`]. The server binary wires a real
    /// `routing_remote::RoutedRouter` via [`Self::with_routed`]; tests and
    /// every other deployment shape leave this `None`.
    pub routed: Option<Arc<dyn RoutedBackend>>,
}

impl AppState {
    /// Build state with an explicit write log. Spawns the apply loop.
    pub fn with_wal(engine: Arc<Engine>, auth: Arc<AuthConfig>, wal: SharedWal) -> Self {
        let writer = WriteCoordinator::start(wal, engine.clone());
        Self::with_components(engine, auth, writer)
    }

    /// Build state from an already-constructed coordinator — used by the
    /// server binary, which wires the WAL + RDB bootstrap itself and
    /// hands in the resulting coordinator.
    pub fn with_components(
        engine: Arc<Engine>,
        auth: Arc<AuthConfig>,
        writer: Arc<dyn WriteSink>,
    ) -> Self {
        Self {
            search_backend: Arc::new(LocalEngineSearch {
                engine: engine.clone(),
            }),
            search_executor: BlockingSearchExecutor::new(),
            write_backend: Arc::new(LocalWriteBackend {
                writer: writer.clone(),
            }),
            engine: engine.clone(),
            verifier: Arc::new(LumenVerifier::new(auth.clone())),
            auth,
            cluster: None,
            writer: writer.clone(),
            checkpoint: Arc::new(NoopCheckpoint),
            restore_sink: Arc::new(InMemoryRestoreSink::new(
                engine.clone(),
                writer.mutation_gate(),
            )),
            write_fence: WriteFence::default(),
            routed: None,
        }
    }

    /// Build state with an in-process [`MemWal`] — single-node /
    /// dev / tests. Writes feel synchronous.
    pub fn new(engine: Arc<Engine>, auth: Arc<AuthConfig>) -> Self {
        Self::with_wal(engine, auth, Arc::new(MemWal::new()))
    }

    pub fn with_cluster(
        mut self,
        cluster: Arc<crate::replication::domain::cluster_state::ClusterState>,
    ) -> Self {
        self.cluster = Some(cluster);
        self
    }

    pub fn with_search_backend(mut self, search_backend: Arc<dyn SearchBackend>) -> Self {
        self.search_backend = search_backend;
        self
    }

    pub fn with_write_backend(mut self, write_backend: Arc<dyn WriteBackend>) -> Self {
        self.write_backend = write_backend;
        self
    }

    /// Wire a real [`CheckpointSink`] (#1389) — used by the server binary
    /// when segment persistence is configured, and by tests that need to
    /// control/observe `POST /admin/checkpoint` behavior.
    pub fn with_checkpoint(mut self, checkpoint: Arc<dyn CheckpointSink>) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    pub fn with_restore_sink(mut self, restore_sink: Arc<dyn RestoreSink>) -> Self {
        self.restore_sink = restore_sink;
        self
    }

    /// Wire a [`RoutedBackend`] (#1398) — the server binary calls this only
    /// in the routed serving topology (`SHARD_COUNT` env > 1 at
    /// `replicasPerShard <= 1`, no `--search-shard-segment-dirs`); every
    /// other deployment shape leaves `routed` at its `None` default.
    pub fn with_routed(mut self, routed: Arc<dyn RoutedBackend>) -> Self {
        self.routed = Some(routed);
        self
    }

    /// The exact auth verifier used by every router built from this state.
    pub fn verifier(&self) -> Arc<LumenVerifier> {
        Arc::clone(&self.verifier)
    }

    /// Install a verifier the caller built itself.
    ///
    /// [`with_components`](Self::with_components) can only build the two
    /// verifiers that need nothing: open, and required-but-unwired. A delegated
    /// verifier has to reach kube-apiserver and prove its delegation grant
    /// before it exists, which is async and can fail — so the serving binary
    /// builds it and hands it in here (#2869).
    pub fn with_verifier(mut self, verifier: Arc<LumenVerifier>) -> Self {
        self.verifier = verifier;
        self
    }

    /// No-auth state over an in-process log. Used by tests and the
    /// simplest single-node runs.
    pub fn open(engine: Arc<Engine>) -> Self {
        Self::with_wal(
            engine,
            Arc::new(AuthConfig::open()),
            Arc::new(MemWal::new()),
        )
    }
}

#[cfg(test)]
mod tests;
