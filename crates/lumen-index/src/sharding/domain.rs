//! The virtual-bucket shard map and routes, and the reshard primitives: moved
//! buckets, bounded snapshot batches, prune chunks, snapshot subsets and delta
//! merges; and the errors a one-hop shard forward fails with.

pub mod bucket_move;
pub mod forward_error;
pub mod merge_delta;
pub mod prune_chunk;
pub mod reshard_batch;
pub mod shard_index;
pub mod shard_route;
pub mod snapshot_subset;
pub mod virtual_bucket_shard_map;

#[cfg(test)]
mod tests;
