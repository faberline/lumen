use std::sync::Arc;
use std::time::Duration;

use lumen::storage::Engine;

use crate::serve::raft::{
    finish_raft_listener_shutdown, spawn_cluster_state_poller, RaftPeerServer,
};
use crate::shutdown;

#[cfg(feature = "raft-wal")]
#[tokio::test]
async fn abort_after_report_aborts_peer_without_graceful_close_before_deadline() {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        ready_tx.send(()).expect("listener readiness handshake");
        // The real graceful listener also exits if this sender is dropped.
        // Either completion must leave a marker; cancellation must not.
        let _ = shutdown_rx.await;
        closed_tx.send(()).expect("record normal listener exit");
        Ok(())
    });
    ready_rx.await.expect("listener is ready for shutdown");
    let abort_handle = task.abort_handle();
    let peer_server = RaftPeerServer { shutdown_tx, task };
    let deadline =
        server_lifecycle::ShutdownDeadline::from_now(Duration::from_secs(1), Duration::ZERO)
            .unwrap();

    let result = finish_raft_listener_shutdown(
        shutdown::PeerListenerAction::AbortAfterReport,
        peer_server,
        deadline,
        Err(anyhow::anyhow!("incomplete report")),
    )
    .await;

    let graceful_exit = tokio::time::timeout_at(deadline.expires_at, closed_rx)
        .await
        .expect("aborted listener cleanup must finish within the positive deadline");
    assert!(
        tokio::time::Instant::now() < deadline.expires_at,
        "the abort action must finish before timer expiry"
    );
    assert!(
        graceful_exit.is_err(),
        "abort action must cancel the listener without reaching its graceful-exit marker"
    );
    assert!(
        abort_handle.is_finished(),
        "aborted listener must be cleaned up"
    );
    let error = result.expect_err("incomplete report remains an error");
    assert!(error.to_string().contains("raft shutdown is incomplete"));
    assert_eq!(error.root_cause().to_string(), "incomplete report");
}

#[cfg(feature = "raft-wal")]
#[tokio::test]
async fn close_after_report_closes_peer_normally_before_deadline() {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        ready_tx.send(()).expect("listener readiness handshake");
        shutdown_rx.await.expect("listener receives shutdown");
        closed_tx.send(()).expect("record normal listener exit");
        Ok(())
    });
    ready_rx.await.expect("listener is ready for shutdown");
    let peer_server = RaftPeerServer { shutdown_tx, task };
    let deadline =
        server_lifecycle::ShutdownDeadline::from_now(Duration::from_secs(1), Duration::ZERO)
            .unwrap();

    finish_raft_listener_shutdown(
        shutdown::PeerListenerAction::CloseAfterReport,
        peer_server,
        deadline,
        Ok(()),
    )
    .await
    .expect("a complete report preserves normal-close success");

    closed_rx.await.expect("listener must finish normally");
    assert!(
        tokio::time::Instant::now() < deadline.expires_at,
        "normal close must finish before the positive deadline"
    );
}

// -----------------------------------------------------------------
// `spawn_cluster_state_poller` (#1349)
// -----------------------------------------------------------------

/// #1349 AC1/AC2 (unit-level): a single-voter `RaftHost` always wins its
/// own election, so `spawn_cluster_state_poller` must converge the
/// shared `ClusterState` from its pre-poller bootstrap value to
/// `RaftRole::Leader` — driven by the real raft engine's own
/// `is_leader`/`leader` results, not a manually-set role. This is the
/// same seam `enforce_read_consistency` (#1310, `src/app/http/guards.rs`) reads via
/// `AppState.cluster`; the live 3-node localhost cluster in this WI's
/// report additionally proves the HTTP-facing accept/reject behavior
/// end-to-end.
#[cfg(feature = "raft-wal")]
#[tokio::test]
async fn cluster_state_poller_converges_role_to_live_election_result() {
    use lumen::raft::{ClusterState, PeerAddr, RaftGroup, RaftRole};

    let tmp = std::env::temp_dir().join(format!(
        "lumen-cluster-poller-test-{}-{}",
        std::process::id(),
        std::thread::current().name().unwrap_or("t")
    ));
    let _ = std::fs::create_dir_all(&tmp);
    let sm = lumen::raft_sm::EngineSm::new(Arc::new(Engine::new()), 0);
    let host = Arc::new(raft_runtime::RaftHost::spawn(
        0,
        raft_runtime::Membership {
            voters: vec![0],
            learners: vec![],
        },
        std::collections::HashMap::new(),
        raft_runtime::RaftStore::open(tmp.to_str().unwrap(), 0, raft_runtime::FsyncPolicy::Os)
            .unwrap(),
        sm.clone() as Arc<dyn raft_runtime::RaftStateMachine>,
        raft_runtime::HostConfig::default(),
    ));

    // Bootstrap value deliberately wrong (Follower/no-leader), matching
    // how a real pod starts before its first poller tick — proves the
    // assertion below observes the poller's live update, not the
    // constructor's static default.
    let cluster = Arc::new(ClusterState::from_snapshot(
        "lumen-0".to_string(),
        0,
        0,
        RaftRole::Follower,
        RaftGroup {
            shard_index: 0,
            peers: vec![PeerAddr {
                pod_name: "lumen-0".to_string(),
                host: "127.0.0.1".to_string(),
                raft_port: 0,
                client_port: 0,
                role: RaftRole::Follower,
            }],
        },
        0,
        1,
        u64::MAX,
    ));
    assert_eq!(cluster.role(), RaftRole::Follower, "bootstrap sanity check");

    // #2475: a fresh `Engine`'s `lumen_raft_leader_known` starts
    // unpublished (sentinel `raft_shard`, see `src/app/observability/metrics.rs`) until this
    // poller ticks; asserted below alongside role convergence.
    let poller_engine = Arc::new(Engine::new());
    assert!(
        !poller_engine
            .metrics()
            .render()
            .contains("lumen_raft_leader_known"),
        "raft leader metric must be absent before the poller's first tick"
    );

    spawn_cluster_state_poller(host, cluster.clone(), true, poller_engine.clone());

    let converged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cluster.role() == RaftRole::Leader {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        converged.is_ok(),
        "poller did not converge role to Leader within bound"
    );
    assert_eq!(cluster.leader_index(), Some(0));
    assert_eq!(
        cluster
            .replication_lag_ms
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "leader reports zero lag, not the unknown sentinel"
    );

    // #2475: the poller's real election read also publishes
    // `lumen_raft_leader_known{shard="0"} 1` on `/metrics` (a
    // single-voter host always wins its own election) — the metric
    // `render::prometheus_rule`'s `LumenRaftLeaderAbsent` alert reads.
    let metrics_out = poller_engine.metrics().render();
    assert!(
        metrics_out.contains("lumen_raft_leader_known{shard=\"0\"} 1"),
        "expected lumen_raft_leader_known in:\n{metrics_out}"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
