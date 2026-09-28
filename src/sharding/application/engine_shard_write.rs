//! `EngineShardWrite`: a write backend over in-process shard coordinators that
//! sends each document to the shard that owns it.

pub(crate) mod backend;

use std::sync::Arc;

use anyhow::{bail, Result};

use crate::ingest::application::write_coordinator::WriteCoordinator;
use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};

#[derive(Clone)]
pub struct EngineShardWrite {
    writers: Arc<Vec<Arc<WriteCoordinator>>>,
    shard_map: VirtualBucketShardMap,
}

impl EngineShardWrite {
    pub fn new(writers: Vec<Arc<WriteCoordinator>>) -> Self {
        let shard_count = u32::try_from(writers.len()).expect("shard count must fit in u32");
        let shard_map =
            VirtualBucketShardMap::balanced(0, DEFAULT_VIRTUAL_BUCKET_COUNT, shard_count.max(1))
                .expect("balanced shard map");
        Self::new_with_shard_map(writers, shard_map)
    }

    pub fn new_with_shard_map(
        writers: Vec<Arc<WriteCoordinator>>,
        shard_map: VirtualBucketShardMap,
    ) -> Self {
        Self {
            writers: Arc::new(writers),
            shard_map,
        }
    }

    pub fn len(&self) -> usize {
        self.writers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.writers.is_empty()
    }

    fn require_shards(&self) -> Result<()> {
        if self.writers.is_empty() {
            bail!("sharded write backend has no shards");
        }
        Ok(())
    }
}
