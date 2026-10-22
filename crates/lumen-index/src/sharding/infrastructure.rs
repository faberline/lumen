//! Cross-pod forwarding over h2c, and the shard map and body limit read from
//! the environment.

pub mod body_limit;
#[cfg(feature = "operator")]
pub mod routed_router;
pub mod shard_map_env;
