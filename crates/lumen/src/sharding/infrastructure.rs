//! Cross-pod forwarding over h2c, and the shard map and body limit read from
//! the environment.

pub(crate) mod body_limit;
#[cfg(feature = "operator")]
pub(crate) mod routed_router;
pub(crate) mod shard_map_env;
