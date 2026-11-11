//! `EngineShardSearch`: a search backend over in-process shard engines that
//! always scatters to every shard; a `routing_key` never narrows the fan-out.

use std::sync::Arc;

use anyhow::Result;

use crate::index::application::engine::Engine;
use crate::index::application::ports::search_backend::SearchBackend;
use crate::sharding::application::search_fanout::search_shards_parallel;
use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

#[derive(Clone)]
pub struct EngineShardSearch {
    shards: Arc<Vec<Arc<Engine>>>,
    // Kept for the public `new_with_shard_map` contract. Searches no longer
    // consult it: every search scatters to all shards.
    _shard_map: VirtualBucketShardMap,
}

impl EngineShardSearch {
    pub fn new(shards: Vec<Arc<Engine>>) -> Self {
        let shard_count = u32::try_from(shards.len()).expect("shard count must fit in u32");
        let shard_map =
            VirtualBucketShardMap::balanced(0, DEFAULT_VIRTUAL_BUCKET_COUNT, shard_count.max(1))
                .expect("balanced shard map");
        Self::new_with_shard_map(shards, shard_map)
    }

    pub fn new_with_shard_map(shards: Vec<Arc<Engine>>, shard_map: VirtualBucketShardMap) -> Self {
        Self {
            shards: Arc::new(shards),
            _shard_map: shard_map,
        }
    }

    pub fn len(&self) -> usize {
        self.shards.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }
}

impl SearchBackend for EngineShardSearch {
    fn search(&self, collection_id: &str, mut req: SearchRequest) -> Result<SearchResponse> {
        // Writes place every document by `external_id` and no write carries
        // `routing_key`, so narrowing to the key's shard would omit documents
        // (ADR 0002 D8). Always fan out; core's shard routing takes over
        // placement in Phase 4 (D15).
        req.routing_key = None;
        let selected_shards: Vec<Arc<Engine>> = self.shards.iter().cloned().collect();
        search_shards_parallel(
            collection_id,
            req,
            selected_shards.as_slice(),
            |engine, collection_id, req| engine.search(collection_id, req),
            |hit, field| {
                self.shards.iter().find_map(|engine| {
                    engine
                        .number_value_for_external_id(collection_id, &hit.external_id, field)
                        .ok()
                        .flatten()
                })
            },
        )
    }
}
