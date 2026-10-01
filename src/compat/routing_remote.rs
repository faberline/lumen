//! `crate::routing_remote` before the DDD split. Re-exports its public items
//! from their new homes so callers outside the crate keep compiling.

pub use crate::sharding::infrastructure::routed_router::RoutedRouter;
