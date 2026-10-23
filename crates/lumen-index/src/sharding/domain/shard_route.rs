//! Where a document or a search lands under one map version.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardRoute {
    pub map_version: u64,
    pub virtual_bucket_count: u32,
    pub physical_shard_count: u32,
    pub bucket: u32,
    pub shard: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchShardTarget {
    All,
    One(ShardRoute),
}
