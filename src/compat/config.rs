//! `crate::config` before the DDD split. Re-exports its public items from their
//! new homes so callers outside the crate keep compiling.

pub use crate::app::config::ClusterConfig;
pub use crate::sharding::infrastructure::shard_map_env::{
    check_fan_in_shard_count, fan_in_shard_count, routed_activation_shard_count,
    routed_pod_topology, routed_shard_count_from_env, shard_map_from_env,
};
