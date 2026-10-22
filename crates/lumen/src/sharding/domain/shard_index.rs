//! A collection's or document's shard under the balanced default map, and a
//! shard's stable DNS name.

use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};

pub fn shard_index(collection_id: &str, shard_count: u32) -> u32 {
    let map = VirtualBucketShardMap::balanced(0, DEFAULT_VIRTUAL_BUCKET_COUNT, shard_count)
        .expect("shard_count must be > 0");
    map.route_key(collection_id, collection_id).shard
}

/// Row/document routing for a sharded local serving node. Collection-level
/// routing now uses the same virtual-bucket map as cluster routing, with
/// `external_id` as the default routing key. That lets one large collection
/// spread across shards while each document remains owned by exactly one shard.
pub fn document_shard_index(collection_id: &str, external_id: &str, shard_count: usize) -> usize {
    let physical = u32::try_from(shard_count).expect("shard_count must fit in u32");
    let map = VirtualBucketShardMap::balanced(0, DEFAULT_VIRTUAL_BUCKET_COUNT, physical)
        .expect("shard_count must be > 0");
    map.route_document(collection_id, None, external_id).shard as usize
}

/// DNS for a given shard's stable client entry (any replica will do —
/// the server forwards writes internally).
pub fn shard_host(prefix: &str, shard: u32, headless_service: &str) -> String {
    format!("{prefix}-{shard}.{headless_service}")
}
