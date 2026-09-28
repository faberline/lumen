//! Per-shard replication surface.
//!
//! This module carries the public cluster-state DTOs — readiness, peer DNS
//! map, role inspection, read-consistency parsing, and the wire shape of
//! `/debug/cluster` — plus [`ClusterState`]'s live-mutable role/leader/lag
//! fields. Write ordering + replication is `libs/raft-core` via
//! `libs/raft-runtime` (#515/#524); `lumen serve --wal raft` keeps
//! `ClusterState` current for the process lifetime by polling the same
//! `RaftHost` the write path already drives (`spawn_cluster_state_poller` in
//! `src/bin/lumen.rs`, #1349) — this module owns the DTOs and the
//! interior-mutable update surface (`role`/`set_role`,
//! `leader_index`/`set_leader_index`, `snapshot`), not the polling itself.
//!
//! Lumen's multi-pod auto path uses Lumen-owned primary/replica replication.
//!
//! `RaftGroup::from_config`'s peer enumeration (pod-ordinal math + the
//! `LUMEN_PEERS` override parsing) delegates to `libs/raft-runtime::cluster`
//! (#1002) so it can't drift from `raft_runtime::cluster::ClusterTopology::
//! from_env`, the implementation the actual raft-wal peer wiring uses.
//!
//! The read-consistency header contract and the role/cluster-view model are
//! the same shape every raft_core service exposes, so their canonical
//! definitions now live in `libs/raft-runtime` (#1003). `ReadConsistency` is
//! re-exported directly (it carries no OpenAPI surface); `RaftRole`,
//! `PeerAddr`, and `ClusterStateView` keep lumen-side `utoipa::ToSchema`
//! wrappers here — with `From<>` conversions to the `raft_runtime` shapes —
//! since deriving `ToSchema` on the shared types would force every adopter
//! (keep/relay/loom) to pull in utoipa whether or not it exposes an OpenAPI
//! doc.

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};

use crate::app::config::ClusterConfig;
use crate::replication::domain::cluster_state_view::ClusterStateView;
use crate::replication::domain::peer_addr::{PeerAddr, RaftGroup};
use crate::replication::domain::raft_role::RaftRole;

/// Sentinel for [`ClusterState::leader_index`]: no leader currently known
/// (mid-election, or the group hasn't elected one yet).
const NO_LEADER: u32 = u32::MAX;

pub use raft_runtime::ReadConsistency;

/// Live cluster snapshot for `/debug/cluster`. Cheap to clone (held behind
/// `Arc`); `role`/`leader_index`/`applied_index`/`leader_term`/
/// `replication_lag_ms` are updated in place from a background task that
/// polls the raft engine's own election state (`RaftHost::is_leader`/
/// `leader`) for the process lifetime — see `spawn_cluster_state_poller` in
/// `src/bin/lumen.rs` (#1349). `group`'s peer addresses are immutable
/// (derived once from static topology config); only role membership is
/// live, computed in [`ClusterState::snapshot`] from `leader_index`.
#[derive(Debug)]
pub struct ClusterState {
    pub pod_name: String,
    pub shard_index: u32,
    pub replica_index: u32,
    /// This pod's own role, as an `AtomicU8` encoding of [`RaftRole`] (see
    /// [`RaftRole::from_u8`]/[`ClusterState::set_role`]) so a background
    /// task can update it without replacing the whole `Arc<ClusterState>`.
    /// Read via [`ClusterState::role`], not this field directly.
    role: AtomicU8,
    pub group: RaftGroup,
    pub applied_index: AtomicU64,
    pub leader_term: AtomicU64,
    pub replication_lag_ms: AtomicU64,
    /// The replica index (== raft `NodeId`, see `RaftGroup::from_config`)
    /// this pod currently believes holds shard leadership, or [`NO_LEADER`]
    /// when unknown. Drives both this pod's own `Leader`/`Follower` split
    /// and the live `peers[].role` view in [`ClusterState::snapshot`].
    leader_index: AtomicU32,
}

impl ClusterState {
    pub fn new(cfg: &ClusterConfig, group: RaftGroup) -> anyhow::Result<Self> {
        let is_voter = cfg.is_voter()?;
        let replica_index = cfg.replica_index()?;
        // Bootstrap default before the live poller's first tick: matches the
        // pre-#1349 static "replica 0 is leader" stub so standalone/not-yet-
        // polled construction (e.g. tests) stays deterministic. The poller
        // overwrites this with the raft engine's real election result once
        // `--wal raft` starts serving.
        let role = if is_voter {
            if replica_index == 0 {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            }
        } else {
            RaftRole::Learner
        };
        let leader_index = if is_voter { 0 } else { NO_LEADER };
        Ok(Self {
            pod_name: cfg.pod_name.clone(),
            shard_index: cfg.shard_index()?,
            replica_index,
            role: AtomicU8::new(role as u8),
            group,
            applied_index: AtomicU64::new(0),
            leader_term: AtomicU64::new(1),
            replication_lag_ms: AtomicU64::new(0),
            leader_index: AtomicU32::new(leader_index),
        })
    }

    /// Build a `ClusterState` with an explicit initial role/leader/lag,
    /// bypassing config derivation (`new`'s bootstrap-default path is still
    /// the one `lumen serve` uses before its live poller's first tick).
    /// This constructor is for callers that already know the exact snapshot
    /// they want to start from — e.g. tests exercising
    /// `enforce_read_consistency` against a deterministic primary-replica
    /// state without a running raft cluster.
    #[allow(clippy::too_many_arguments)]
    pub fn from_snapshot(
        pod_name: String,
        shard_index: u32,
        replica_index: u32,
        role: RaftRole,
        group: RaftGroup,
        applied_index: u64,
        leader_term: u64,
        replication_lag_ms: u64,
    ) -> Self {
        let leader_index = match role {
            RaftRole::Leader => replica_index,
            _ => group
                .peers
                .iter()
                .position(|p| p.role == RaftRole::Leader)
                .map(|i| i as u32)
                .unwrap_or(NO_LEADER),
        };
        Self {
            pod_name,
            shard_index,
            replica_index,
            role: AtomicU8::new(role as u8),
            group,
            applied_index: AtomicU64::new(applied_index),
            leader_term: AtomicU64::new(leader_term),
            replication_lag_ms: AtomicU64::new(replication_lag_ms),
            leader_index: AtomicU32::new(leader_index),
        }
    }

    /// This pod's current role. Live once a background poller is running
    /// (`--wal raft`); a fixed bootstrap value otherwise (see `new`/
    /// `from_snapshot`).
    pub fn role(&self) -> RaftRole {
        RaftRole::from_u8(self.role.load(Ordering::Relaxed))
    }

    /// Set this pod's live role (background poller only).
    pub fn set_role(&self, role: RaftRole) {
        self.role.store(role as u8, Ordering::Relaxed);
    }

    /// The replica index this pod currently believes is shard leader, or
    /// `None` when unknown (mid-election).
    pub fn leader_index(&self) -> Option<u32> {
        match self.leader_index.load(Ordering::Relaxed) {
            NO_LEADER => None,
            idx => Some(idx),
        }
    }

    /// Set the currently known leader's replica index (background poller
    /// only); `None` clears it back to "unknown".
    pub fn set_leader_index(&self, leader: Option<u32>) {
        self.leader_index
            .store(leader.unwrap_or(NO_LEADER), Ordering::Relaxed);
    }

    /// The peer entry for the currently known leader, if any — replaces
    /// `RaftGroup::leader()`'s static "replica 0" stub for live lookups
    /// (e.g. the `read_consistency_not_leader` error message).
    pub fn leader_peer(&self) -> Option<&PeerAddr> {
        self.leader_index()
            .and_then(|idx| self.group.peers.get(idx as usize))
    }

    pub fn snapshot(&self) -> ClusterStateView {
        let leader_index = self.leader_index();
        // Learner membership is static config (learners never contest
        // leadership); voter role is live, derived from `leader_index` —
        // this is what turns `group`'s static peer roles into the actual
        // live view for `/debug/cluster` (#1349 AC1).
        let peers = self
            .group
            .peers
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let role = if p.role == RaftRole::Learner {
                    RaftRole::Learner
                } else if leader_index == Some(i as u32) {
                    RaftRole::Leader
                } else {
                    RaftRole::Follower
                };
                PeerAddr { role, ..p.clone() }
            })
            .collect();
        ClusterStateView {
            pod_name: self.pod_name.clone(),
            shard_index: self.shard_index,
            replica_index: self.replica_index,
            role: self.role(),
            peers,
            applied_index: self.applied_index.load(Ordering::Relaxed),
            leader_term: self.leader_term.load(Ordering::Relaxed),
            replication_lag_ms: self.replication_lag_ms.load(Ordering::Relaxed),
        }
    }
}
