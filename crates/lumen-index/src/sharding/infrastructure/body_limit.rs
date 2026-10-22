//! The data-plane request body limit, read from `LUMEN_BODY_LIMIT_BYTES`.

use crate::sharding::domain::reshard_batch::ADMIN_ROUTE_BODY_LIMIT_BYTES;

/// Resolve the data-plane body limit from `LUMEN_BODY_LIMIT_BYTES`, falling
/// back to [`ADMIN_ROUTE_BODY_LIMIT_BYTES`] (8 MiB) when unset or invalid.
pub fn body_limit_bytes_from_env() -> usize {
    std::env::var("LUMEN_BODY_LIMIT_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(ADMIN_ROUTE_BODY_LIMIT_BYTES)
}
