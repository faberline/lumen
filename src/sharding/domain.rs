//! The virtual-bucket shard map and routes, and the reshard primitives: moved
//! buckets, bounded snapshot batches, prune chunks, snapshot subsets and delta
//! merges; and the errors a one-hop shard forward fails with.

pub(crate) mod bucket_move;
pub(crate) mod forward_error;
pub(crate) mod merge_delta;
pub(crate) mod prune_chunk;
pub(crate) mod reshard_batch;
pub(crate) mod shard_index;
pub(crate) mod shard_route;
pub(crate) mod snapshot_subset;
pub(crate) mod virtual_bucket_shard_map;

#[cfg(test)]
mod tests;
