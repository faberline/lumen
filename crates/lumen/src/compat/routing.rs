//! `crate::routing` before the DDD split. Re-exports its public items from
//! their new homes so callers outside the crate keep compiling.

pub use crate::sharding::application::engine_shard_search::EngineShardSearch;
pub use crate::sharding::application::engine_shard_write::EngineShardWrite;
pub use crate::sharding::application::search_fanout::{
    merge_shard_search_responses, search_shards_parallel,
};
pub use crate::sharding::domain::shard_index::{document_shard_index, shard_host, shard_index};
pub use crate::sharding::domain::shard_route::{SearchShardTarget, ShardRoute};
pub use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};
