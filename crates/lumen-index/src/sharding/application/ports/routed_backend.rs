//! RoutedBackend, the cross-pod shard routing a serving pod's handlers consult
//! before the local backends: every method takes the inbound headers, so the
//! router can check the one-hop guard and forward the caller's credentials.

use anyhow::Result;
use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::index::application::engine::collections::DropOutcome;
use crate::shared_kernel::types::document::{
    BatchUnindexDocsRequest, IndexRequest, IndexResponse, ReplaceDocsRequest, ReplaceDocsResponse,
};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, CreateCollectionResponse};
use crate::shared_kernel::types::search::{SearchRequest, SearchResponse};

#[cfg(doc)]
use crate::ingest::application::ports::write_backend::WriteBackend;

/// Cross-pod shard routing for operator/k8s serving pods (#1398 R1-R3).
/// `AppState::routed` is `None` for every non-routed deployment (standalone,
/// primary/replica, and the `--search-shard-segment-dirs` fan-in path) —
/// handlers consult it first and fall back to `search_backend`/
/// `write_backend` unchanged when it is absent, so `shardCount:1` serving
/// never even constructs an implementation (AC5: no forwarding overhead).
///
/// Every method takes the inbound request's `headers` verbatim: the sole
/// concrete implementation ([`crate::sharding::infrastructure::routed_router::RoutedRouter`], behind
/// the `operator` feature) checks the `x-lumen-forwarded` one-hop guard
/// first and, when forwarding, carries the caller's `Authorization` bearer
/// and `x-read-consistency` through unchanged (R3).
#[async_trait]
pub trait RoutedBackend: Send + Sync {
    /// #2496: collection lifecycle has no single owning shard — every
    /// physical shard must register the schema, or a write that later
    /// routes to a shard that never heard `create_collection` 404s with
    /// `CollectionNotFound` even though the collection genuinely exists.
    /// The sole implementation ([`crate::sharding::infrastructure::routed_router::RoutedRouter`])
    /// fans this out to every physical shard (local direct call plus one
    /// forward per remote shard), mirroring
    /// [`crate::sharding::application::engine_shard_write::EngineShardWrite::create_collection`]'s in-process
    /// fan-out/merge semantics over cross-pod HTTP instead of an in-process
    /// writer submit.
    async fn create_collection(
        &self,
        collection_id: String,
        req: CreateCollectionRequest,
        headers: &HeaderMap,
    ) -> Result<CreateCollectionResponse>;

    /// #2496: same fan-out-to-every-shard requirement as
    /// [`Self::create_collection`], merged with
    /// [`crate::sharding::application::engine_shard_write::EngineShardWrite::drop_collection`]'s
    /// `Physical > Marked > AlreadyMarked > NotFound` precedence.
    async fn drop_collection(
        &self,
        collection_id: String,
        force: bool,
        headers: &HeaderMap,
    ) -> Result<DropOutcome>;

    async fn search(
        &self,
        collection_id: &str,
        req: SearchRequest,
        headers: &HeaderMap,
    ) -> Result<SearchResponse>;

    async fn index(
        &self,
        collection_id: String,
        req: IndexRequest,
        headers: &HeaderMap,
    ) -> Result<IndexResponse>;

    async fn replace_docs(
        &self,
        collection_id: String,
        req: ReplaceDocsRequest,
        headers: &HeaderMap,
    ) -> Result<ReplaceDocsResponse>;

    async fn truncate_docs(&self, collection_id: String, headers: &HeaderMap) -> Result<()>;

    async fn unindex_docs(
        &self,
        collection_id: String,
        req: BatchUnindexDocsRequest,
        headers: &HeaderMap,
    ) -> Result<()>;

    async fn delete(
        &self,
        collection_id: String,
        external_id: String,
        field: Option<String>,
        headers: &HeaderMap,
    ) -> Result<()>;
}
