//! A pod's role in its shard's raft group, as the OpenAPI-visible mirror of
//! `raft_runtime`'s role. `lumen-replication` converts between the two.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum RaftRole {
    Leader,
    Follower,
    Learner,
    Candidate,
}
