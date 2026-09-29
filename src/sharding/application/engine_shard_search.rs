//! `EngineShardSearch`: a search backend over in-process shard engines that
//! targets the owning shard when a routing key is given and scatters otherwise.

use std::sync::Arc;

use anyhow::{bail, Result};

use crate::api::SearchBackend;
use crate::index::application::engine::Engine;
use crate::sharding::application::search_fanout::search_shards_parallel;
use crate::sharding::domain::shard_route::SearchShardTarget;
use crate::sharding::domain::virtual_bucket_shard_map::{
    VirtualBucketShardMap, DEFAULT_VIRTUAL_BUCKET_COUNT,
};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

#[derive(Clone)]
pub struct EngineShardSearch {
    shards: Arc<Vec<Arc<Engine>>>,
    shard_map: VirtualBucketShardMap,
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
            shard_map,
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
    fn search(&self, collection_id: &str, req: SearchRequest) -> Result<SearchResponse> {
        let selected_shards: Vec<Arc<Engine>> = match self
            .shard_map
            .search_target(collection_id, req.routing_key.as_deref())
        {
            SearchShardTarget::All => self.shards.iter().cloned().collect(),
            SearchShardTarget::One(route) => {
                let Some(engine) = self.shards.get(route.shard as usize) else {
                    bail!("shard map routed to missing shard {}", route.shard);
                };
                vec![engine.clone()]
            }
        };
        search_shards_parallel(
            collection_id,
            req,
            selected_shards.as_slice(),
            |engine, collection_id, req| Ok(engine.search(collection_id, req)?),
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
