//! One shard's peers: pod-ordinal DNS names from the cluster shape, with the
//! `LUMEN_PEERS` local-dev override.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::app::config::ClusterConfig;
use crate::replication::domain::raft_role::RaftRole;

/// Peer list for one shard. The address scheme is the same for every
/// deployment: `lumen-{ordinal}.{headless_service}:{port}` where the
/// pod ordinal is `replica * shard_count + shard`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroup {
    pub shard_index: u32,
    pub peers: Vec<PeerAddr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerAddr {
    pub pod_name: String,
    pub host: String,
    pub raft_port: u16,
    pub client_port: u16,
    pub role: RaftRole,
}

impl From<raft_runtime::PeerAddr> for PeerAddr {
    fn from(p: raft_runtime::PeerAddr) -> Self {
        Self {
            pod_name: p.pod_name,
            host: p.host,
            raft_port: p.raft_port,
            client_port: p.client_port,
            role: p.role.into(),
        }
    }
}

impl RaftGroup {
    /// `fallback_prefix` is used only when `POD_NAME` carries no StatefulSet
    /// name of its own. The pod's name wins for the same reason it does in
    /// `ClusterTopology` — the operator names the StatefulSet after the custom
    /// resource, so a caller's binary name is only ever a guess, and a wrong
    /// guess here means `/debug/cluster` and read-consistency routing both
    /// describe peers that do not exist.
    pub fn from_config(
        cfg: &ClusterConfig,
        fallback_prefix: &str,
        headless_service: &str,
        raft_port: u16,
        client_port: u16,
    ) -> anyhow::Result<Self> {
        let shard = cfg.shard_index()?;
        let prefix = cfg
            .pod_prefix()
            .unwrap_or_else(|_| fallback_prefix.to_string());
        let mut peers = Vec::with_capacity(cfg.replicas_per_shard as usize);
        for replica in 0..cfg.replicas_per_shard {
            // Pod-ordinal math shared with `raft_runtime::cluster::ClusterTopology`
            // (#1002) — no local `%`/`/` peer-DNS arithmetic.
            let ordinal = raft_runtime::cluster::peer_ordinal(cfg.shard_count, shard, replica);
            let pod_name = format!("{prefix}-{ordinal}");
            let host = format!("{pod_name}.{headless_service}");
            let role = if replica < cfg.voter_count {
                if replica == 0 {
                    // Stub: pod 0 always claims leader. Real Raft
                    // will own this assignment.
                    RaftRole::Leader
                } else {
                    RaftRole::Follower
                }
            } else {
                RaftRole::Learner
            };
            peers.push(PeerAddr {
                pod_name,
                host,
                raft_port,
                client_port,
                role,
            });
        }

        // Local-dev override: `LUMEN_PEERS=host:peer-port,host:peer-port,...`
        // replaces the K8s headless-DNS addresses with explicit
        // host:port pairs. Useful for running a 3-pod cluster on a
        // single machine; index N maps to replica N in this shard. Parsing
        // is shared with `ClusterTopology::from_env`'s peer override (#1002).
        let overrides = raft_runtime::cluster::parse_peer_overrides("LUMEN_PEERS");
        for (i, peer) in peers.iter_mut().enumerate() {
            if let Some(addr) = overrides.get(i) {
                if let Some((host, port)) = addr.rsplit_once(':') {
                    peer.host = host.to_string();
                    peer.raft_port = port.parse().unwrap_or(peer.raft_port);
                } else {
                    peer.host = addr.clone();
                }
            }
        }

        Ok(Self {
            shard_index: shard,
            peers,
        })
    }

    pub fn leader(&self) -> Option<&PeerAddr> {
        self.peers.iter().find(|p| p.role == RaftRole::Leader)
    }
}
