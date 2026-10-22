//! The raft side of a serving node: the peer listener's bounded shutdown and
//! the poller that keeps the cluster state current.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use lumen::storage::Engine;

#[cfg(feature = "raft-wal")]
pub(super) struct RaftPeerServer {
    pub(super) shutdown_tx: tokio::sync::oneshot::Sender<()>,
    pub(super) task: tokio::task::JoinHandle<anyhow::Result<()>>,
}

#[cfg(feature = "raft-wal")]
async fn close_raft_peer_listener_within(
    mut peer_server: RaftPeerServer,
    deadline: server_lifecycle::ShutdownDeadline,
) {
    let _ = peer_server.shutdown_tx.send(());
    match tokio::time::timeout_at(deadline.expires_at, &mut peer_server.task).await {
        Ok(Ok(Ok(()))) => tracing::info!(
            event = "raft_peer_listener_closed",
            message = "raft_peer_listener_closed",
        ),
        Ok(Ok(Err(error))) => tracing::warn!(%error, "raft peer listener failed during shutdown"),
        Ok(Err(error)) => {
            tracing::warn!(%error, "raft peer listener task panicked during shutdown")
        }
        Err(_) => {
            peer_server.task.abort();
            let _ = peer_server.task.await;
            tracing::info!(
                event = "raft_peer_listener_aborted",
                message = "raft_peer_listener_aborted"
            );
        }
    }
}

#[cfg(feature = "raft-wal")]
async fn finish_raft_listener_shutdown(
    action: crate::shutdown::PeerListenerAction,
    peer_server: RaftPeerServer,
    deadline: server_lifecycle::ShutdownDeadline,
    shutdown_result: Result<()>,
) -> Result<()> {
    match action {
        crate::shutdown::PeerListenerAction::CloseAfterReport => {
            close_raft_peer_listener_within(peer_server, deadline).await;
            shutdown_result
        }
        crate::shutdown::PeerListenerAction::AbortAfterReport => {
            // Request cancellation without extending the shared deadline.
            peer_server.task.abort();
            tracing::info!(
                event = "raft_peer_listener_aborted",
                message = "raft_peer_listener_aborted"
            );
            shutdown_result.context("raft shutdown is incomplete")
        }
    }
}

/// Complete the Raft part of shutdown before changing the peer listener.
///
/// The report is logged before either listener action. The caller continues
/// public HTTP drain until the same absolute deadline expires.
#[cfg(feature = "raft-wal")]
pub(super) async fn shutdown_raft_within(
    host: Arc<raft_runtime::RaftHost>,
    peer_server: Option<RaftPeerServer>,
    deadline: server_lifecycle::ShutdownDeadline,
) -> Result<()> {
    use crate::shutdown::RaftShutdownCoordinator;

    let report = host.shutdown_within(deadline).await;
    let event = RaftShutdownCoordinator::event(&report, deadline.total.as_millis() as u64);
    tracing::info!(
        event = "raft_shutdown",
        message = "raft_shutdown",
        shutdown_budget_ms = event.shutdown_budget_ms,
        proposal_admission = event.proposal_admission,
        handoff = event.handoff,
        incomplete_phase = event.incomplete_phase,
        peer_listener_close_safe = event.peer_listener_close_safe,
    );

    let shutdown_result = report.clone().into_result();
    let Some(peer_server) = peer_server else {
        return shutdown_result;
    };
    finish_raft_listener_shutdown(
        event.listener_action,
        peer_server,
        deadline,
        shutdown_result,
    )
    .await
}

/// Keeps `AppState.cluster` (#1310's read-consistency enforcement seam)
/// current for the process lifetime (#1349): polls the already-running
/// `RaftHost` for its live role/leader view (`is_leader`/`leader`, both
/// pre-existing — no new raft-runtime surface added) and republishes it onto
/// the shared `ClusterState` via its atomic setters, so every concurrently
/// running request handler observes the latest election result without a
/// restart. Runs for the life of the `serve` process; errors from the raft
/// host (e.g. transient watch-channel lag) are not fatal to serving and are
/// simply retried on the next tick.
///
/// Replication lag is reported as `0` on the leader and `u64::MAX`
/// ("unknown") on every follower/learner: deriving a true milliseconds-lag
/// figure would need new peer RPC surface this WI intentionally does not
/// add (see #1349's scope guardrail), so `ReadConsistency::Bounded` is kept
/// conservative — an unknown lag always fails the bound rather than
/// silently serving a stale follower.
#[cfg(feature = "raft-wal")]
pub(super) fn spawn_cluster_state_poller(
    host: Arc<raft_runtime::RaftHost>,
    cluster: Arc<lumen::raft::ClusterState>,
    is_voter: bool,
    engine: Arc<Engine>,
) {
    use lumen::raft::RaftRole;
    let applied_rx = host.applied_watch();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        loop {
            ticker.tick().await;
            let is_leader = host.is_leader().await;
            let leader = host.leader().await;
            let role = if !is_voter {
                RaftRole::Learner
            } else if is_leader {
                RaftRole::Leader
            } else if leader.is_some() {
                RaftRole::Follower
            } else {
                RaftRole::Candidate
            };
            let prev = cluster.role();
            cluster.set_role(role);
            cluster.set_leader_index(leader.map(|n| n as u32));
            // #2475: publish `lumen_raft_leader_known{shard}` off the same
            // live election read `enforce_read_consistency` trusts, so
            // `render::prometheus_rule`'s `LumenRaftLeaderAbsent` alerts on
            // a real signal rather than a synthesized one.
            engine
                .metrics()
                .set_raft_leader_known(cluster.shard_index, leader.is_some());
            cluster.replication_lag_ms.store(
                if role == RaftRole::Leader {
                    0
                } else {
                    u64::MAX
                },
                std::sync::atomic::Ordering::Relaxed,
            );
            cluster
                .applied_index
                .store(*applied_rx.borrow(), std::sync::atomic::Ordering::Relaxed);
            if role != prev {
                tracing::info!(pod = %cluster.pod_name, from = ?prev, to = ?role, "raft role changed");
            }
        }
    });
}

#[cfg(test)]
mod tests;
