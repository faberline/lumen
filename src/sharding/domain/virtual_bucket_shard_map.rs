//! Shard routing.
//!
//! The stable routing contract is virtual-bucket based:
//! `bucket = hash(collection_id, routing_key || external_id) % N`, then a
//! versioned bucket-to-physical-shard map decides ownership. That keeps
//! `shardCount` from becoming a permanent `hash % shardCount` data contract,
//! which is required for operator-managed shard splits.

use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::sharding::domain::shard_route::{SearchShardTarget, ShardRoute};

pub const DEFAULT_VIRTUAL_BUCKET_COUNT: u32 = 4096;

fn route_hash(collection_id: &str, key: &str) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(collection_id.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.as_bytes());
    hasher.finalize()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualBucketShardMap {
    version: u64,
    assignments: Arc<Vec<u32>>,
    physical_shard_count: u32,
}

impl VirtualBucketShardMap {
    pub fn balanced(
        version: u64,
        virtual_bucket_count: u32,
        physical_shard_count: u32,
    ) -> Result<Self> {
        if virtual_bucket_count == 0 {
            bail!("virtual_bucket_count must be > 0");
        }
        if physical_shard_count == 0 {
            bail!("physical_shard_count must be > 0");
        }
        let assignments = (0..virtual_bucket_count)
            .map(|bucket| bucket % physical_shard_count)
            .collect();
        Self::new(version, assignments, physical_shard_count)
    }

    pub fn new(version: u64, assignments: Vec<u32>, physical_shard_count: u32) -> Result<Self> {
        if assignments.is_empty() {
            bail!("shard map must contain at least one virtual bucket");
        }
        if physical_shard_count == 0 {
            bail!("physical_shard_count must be > 0");
        }
        for &shard in &assignments {
            if shard >= physical_shard_count {
                bail!(
                    "bucket assignment points to shard {shard}, but physical_shard_count is {physical_shard_count}"
                );
            }
        }
        Ok(Self {
            version,
            assignments: Arc::new(assignments),
            physical_shard_count,
        })
    }

    pub fn single() -> Self {
        Self::new(0, vec![0], 1).expect("single shard map is valid")
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn virtual_bucket_count(&self) -> u32 {
        self.assignments.len() as u32
    }

    pub fn physical_shard_count(&self) -> u32 {
        self.physical_shard_count
    }

    pub fn assignment_for_bucket(&self, bucket: u32) -> Option<u32> {
        self.assignments.get(bucket as usize).copied()
    }

    pub fn route_document(
        &self,
        collection_id: &str,
        routing_key: Option<&str>,
        external_id: &str,
    ) -> ShardRoute {
        self.route_key(collection_id, routing_key.unwrap_or(external_id))
    }

    pub fn route_key(&self, collection_id: &str, routing_key: &str) -> ShardRoute {
        let bucket = route_hash(collection_id, routing_key) % self.virtual_bucket_count();
        let shard = self.assignments[bucket as usize];
        ShardRoute {
            map_version: self.version,
            virtual_bucket_count: self.virtual_bucket_count(),
            physical_shard_count: self.physical_shard_count,
            bucket,
            shard,
        }
    }

    pub fn search_target(
        &self,
        collection_id: &str,
        routing_key: Option<&str>,
    ) -> SearchShardTarget {
        match routing_key {
            Some(key) => SearchShardTarget::One(self.route_key(collection_id, key)),
            None => SearchShardTarget::All,
        }
    }

    /// Target map for growing this map by exactly one physical shard, moving
    /// the minimum number of virtual buckets. From each existing shard, the
    /// lowest-numbered `buckets_on_that_shard / new_physical_shard_count`
    /// buckets move to the new shard (appended at index
    /// `physical_shard_count`); no bucket ever moves directly between two
    /// existing shards. That keeps a single split a bounded, per-source-shard
    /// migration (one batch stream per old shard into the new shard) rather
    /// than a full cluster-wide rebalance.
    pub fn split_one_shard(&self, new_version: u64) -> Result<Self> {
        let new_physical_shard_count = self
            .physical_shard_count
            .checked_add(1)
            .context("physical_shard_count overflow computing shard split")?;
        let new_shard = self.physical_shard_count;

        let mut buckets_by_shard: Vec<Vec<u32>> =
            vec![Vec::new(); self.physical_shard_count as usize];
        for (bucket, &shard) in self.assignments.iter().enumerate() {
            buckets_by_shard[shard as usize].push(bucket as u32);
        }

        let mut assignments = (*self.assignments).clone();
        for buckets in &buckets_by_shard {
            let move_count = buckets.len() / new_physical_shard_count as usize;
            for &bucket in buckets.iter().take(move_count) {
                assignments[bucket as usize] = new_shard;
            }
        }

        Self::new(new_version, assignments, new_physical_shard_count)
    }
}

#[cfg(test)]
mod tests;
