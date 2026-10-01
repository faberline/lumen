//! SearchBackend, the search port the HTTP handlers call, and its local
//! implementation that searches this process's Engine.

use std::sync::Arc;

use anyhow::Result;

use crate::index::application::engine::Engine;
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

pub trait SearchBackend: Send + Sync {
    fn search(&self, collection_id: &str, req: SearchRequest) -> Result<SearchResponse>;
}

#[derive(Clone)]
pub(crate) struct LocalEngineSearch {
    pub(crate) engine: Arc<Engine>,
}

impl SearchBackend for LocalEngineSearch {
    fn search(&self, collection_id: &str, req: SearchRequest) -> Result<SearchResponse> {
        self.engine.search(collection_id, req)
    }
}
