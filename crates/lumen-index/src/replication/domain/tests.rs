use crate::replication::domain::cluster_state::ClusterState;
use crate::replication::domain::cluster_state_view::ClusterStateView;
use crate::replication::domain::peer_addr::{PeerAddr, RaftGroup};
use crate::replication::domain::raft_role::RaftRole;

#[test]
fn wrapper_types_convert_from_raft_runtime_canonical_shape() {
    // The header contract itself is a pure re-export — no local wrapper —
    // so its parsing tests moved down to raft-runtime (#1003).
    for (host_role, local_role) in [
        (raft_runtime::RaftRole::Leader, RaftRole::Leader),
        (raft_runtime::RaftRole::Follower, RaftRole::Follower),
        (raft_runtime::RaftRole::Learner, RaftRole::Learner),
        (raft_runtime::RaftRole::Candidate, RaftRole::Candidate),
    ] {
        assert_eq!(RaftRole::from(host_role), local_role);
    }

    let host_peer = raft_runtime::PeerAddr {
        pod_name: "lumen-1".into(),
        host: "lumen-1.lumen-peer".into(),
        raft_port: 8082,
        client_port: 8080,
        role: raft_runtime::RaftRole::Follower,
    };
    let peer: PeerAddr = host_peer.clone().into();
    assert_eq!(peer.pod_name, host_peer.pod_name);
    assert_eq!(peer.role, RaftRole::Follower);

    let host_view = raft_runtime::ClusterStateView {
        pod_name: "lumen-1".into(),
        shard_index: 0,
        replica_index: 1,
        role: raft_runtime::RaftRole::Follower,
        peers: vec![host_peer],
        applied_index: 5,
        leader_term: 2,
        replication_lag_ms: 10,
    };
    let view: ClusterStateView = host_view.into();
    assert_eq!(view.pod_name, "lumen-1");
    assert_eq!(view.role, RaftRole::Follower);
    assert_eq!(view.peers.len(), 1);
    assert_eq!(view.applied_index, 5);
}

use crate::app::config::ClusterConfig;
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn cfg(shards: u32, replicas: u32, voters: u32, pod: &str) -> ClusterConfig {
    ClusterConfig {
        shard_count: shards,
        replicas_per_shard: replicas,
        voter_count: voters,
        pod_name: pod.into(),
    }
}

fn clear_lumen_peers() {
    unsafe {
        std::env::remove_var("LUMEN_PEERS");
    }
}

#[test]
fn raft_group_from_config_enumerates_peers_in_shard() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_lumen_peers();
    // 3 shards × 3 replicas, this pod is lumen-4 → shard 1, replica 1.
    // The group's peers are the 3 replicas of shard 1: ordinals 1, 4, 7.
    let g = RaftGroup::from_config(&cfg(3, 3, 3, "lumen-4"), "lumen", "lumen-peer", 8082, 8080)
        .unwrap();
    assert_eq!(g.shard_index, 1);
    assert_eq!(g.peers.len(), 3);
    assert_eq!(g.peers[0].pod_name, "lumen-1");
    assert_eq!(g.peers[1].pod_name, "lumen-4");
    assert_eq!(g.peers[2].pod_name, "lumen-7");
    // Hostnames go through the headless service suffix.
    for p in &g.peers {
        assert!(p.host.ends_with(".lumen-peer"), "host={}", p.host);
        assert_eq!(p.raft_port, 8082);
        assert_eq!(p.client_port, 8080);
    }
}

#[test]
fn raft_group_marks_first_voter_as_leader_and_rest_as_followers() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_lumen_peers();
    let g = RaftGroup::from_config(&cfg(1, 5, 3, "lumen-0"), "lumen", "lumen-peer", 8082, 8080)
        .unwrap();
    assert_eq!(g.peers[0].role, RaftRole::Leader);
    assert_eq!(g.peers[1].role, RaftRole::Follower);
    assert_eq!(g.peers[2].role, RaftRole::Follower);
    // 4th and 5th replicas exceed voter_count=3 → learners.
    assert_eq!(g.peers[3].role, RaftRole::Learner);
    assert_eq!(g.peers[4].role, RaftRole::Learner);
    assert_eq!(g.leader().unwrap().pod_name, "lumen-0");
}

#[test]
fn raft_group_peer_dns_follows_the_pod_not_the_binary_name() {
    // Companion to raft-runtime's
    // `peer_dns_prefix_follows_the_pod_not_the_callers_binary_name`. Both
    // derivations must agree, because this one names the peers that
    // `/debug/cluster` reports and that read-consistency routes to, while
    // that one names the peers raft actually votes with. A CR named
    // `quorum` produces pods `quorum-N`; addressing them as `lumen-N` is a
    // well-formed URL for a host that does not exist.
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_lumen_peers();
    let g = RaftGroup::from_config(
        &cfg(1, 2, 2, "quorum-0"),
        "lumen",
        "quorum-headless",
        7374,
        7373,
    )
    .unwrap();
    assert_eq!(g.peers[0].pod_name, "quorum-0");
    assert_eq!(g.peers[1].pod_name, "quorum-1");
    assert_eq!(g.peers[1].host, "quorum-1.quorum-headless");
}

#[test]
fn raft_group_lumen_peers_override_replaces_dns() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var(
            "LUMEN_PEERS",
            "127.0.0.1:9080,127.0.0.1:9081,127.0.0.1:9082",
        );
    }
    let g = RaftGroup::from_config(&cfg(1, 3, 3, "lumen-0"), "lumen", "lumen-peer", 8082, 8080)
        .unwrap();
    assert_eq!(g.peers[0].host, "127.0.0.1");
    assert_eq!(g.peers[0].raft_port, 9080);
    assert_eq!(g.peers[1].raft_port, 9081);
    assert_eq!(g.peers[2].raft_port, 9082);
    clear_lumen_peers();
}

#[test]
fn raft_group_lumen_peers_partial_override_keeps_remaining_dns() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    unsafe {
        std::env::set_var("LUMEN_PEERS", "127.0.0.1:9080");
    }
    let g = RaftGroup::from_config(&cfg(1, 3, 3, "lumen-0"), "lumen", "lumen-peer", 8082, 8080)
        .unwrap();
    assert_eq!(g.peers[0].host, "127.0.0.1");
    // Peer 1 and 2 keep the headless DNS form.
    assert!(g.peers[1].host.ends_with(".lumen-peer"));
    assert!(g.peers[2].host.ends_with(".lumen-peer"));
    clear_lumen_peers();
}

#[test]
fn leader_returns_none_when_no_voter_in_group() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_lumen_peers();
    // 0 voters → every replica is a learner; leader() returns None.
    let g = RaftGroup::from_config(&cfg(1, 3, 0, "lumen-0"), "lumen", "lumen-peer", 8082, 8080)
        .unwrap();
    assert!(g.leader().is_none());
}

#[test]
fn cluster_state_view_round_trips() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    clear_lumen_peers();
    let c = cfg(1, 3, 3, "lumen-1");
    let group = RaftGroup::from_config(&c, "lumen", "lumen-peer", 8082, 8080).unwrap();
    let st = ClusterState::new(&c, group).unwrap();
    let v = st.snapshot();
    assert_eq!(v.pod_name, "lumen-1");
    assert_eq!(v.replica_index, 1);
    assert_eq!(v.role, RaftRole::Follower);
    // applied_index / term default to 0 / 1.
    assert_eq!(v.applied_index, 0);
    assert_eq!(v.leader_term, 1);
}
