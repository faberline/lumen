//! One raft peer as `/debug/cluster` reports it.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::raft_role::RaftRole;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PeerAddr {
    pub pod_name: String,
    pub host: String,
    pub raft_port: u16,
    pub client_port: u16,
    pub role: RaftRole,
}
