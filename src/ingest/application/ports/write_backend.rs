//! WriteBackend, the write port the HTTP handlers call, and its local
//! implementation that submits each mutation as a log entry to the write sink
//! and unwraps the apply outcome it expects.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::index::application::engine::collections::DropOutcome;
use crate::index::application::engine::raft_dispatch::ApplyOutcome;
use crate::ingest::application::write_coordinator::WriteSink;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::document::{
    validate_batch_unindex_docs_request, BatchUnindexDocsRequest, IndexRequest, IndexResponse,
    ReplaceDocsRequest, ReplaceDocsResponse,
};
use crate::shared_kernel::types::schema::{CreateCollectionRequest, CreateCollectionResponse};

#[async_trait]
pub trait WriteBackend: Send + Sync {
    async fn create_collection(
        &self,
        collection_id: String,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse>;

    async fn drop_collection(&self, collection_id: String, force: bool) -> Result<DropOutcome>;

    async fn index(&self, collection_id: String, req: IndexRequest) -> Result<IndexResponse>;

    async fn replace_docs(
        &self,
        collection_id: String,
        req: ReplaceDocsRequest,
    ) -> Result<ReplaceDocsResponse>;

    async fn truncate_docs(&self, collection_id: String) -> Result<()>;

    async fn unindex_docs(&self, collection_id: String, req: BatchUnindexDocsRequest)
        -> Result<()>;

    async fn delete(
        &self,
        collection_id: String,
        external_id: String,
        field: Option<String>,
    ) -> Result<()>;

    async fn drop_field(&self, collection_id: String, field_name: String) -> Result<u32>;
}

#[derive(Clone)]
pub(crate) struct LocalWriteBackend {
    pub(crate) writer: Arc<dyn WriteSink>,
}

impl LocalWriteBackend {
    fn unexpected(outcome: ApplyOutcome) -> anyhow::Error {
        anyhow::anyhow!("unexpected apply outcome: {outcome:?}")
    }
}

#[async_trait]
impl WriteBackend for LocalWriteBackend {
    async fn create_collection(
        &self,
        collection_id: String,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        match self
            .writer
            .submit(RaftLogEntry::CreateCollection { collection_id, req })
            .await?
        {
            ApplyOutcome::Created(r) => Ok(r),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn drop_collection(&self, collection_id: String, force: bool) -> Result<DropOutcome> {
        match self
            .writer
            .submit(RaftLogEntry::DropCollection {
                collection_id,
                force,
            })
            .await?
        {
            ApplyOutcome::Dropped(o) => Ok(o),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn index(&self, collection_id: String, req: IndexRequest) -> Result<IndexResponse> {
        match self
            .writer
            .submit(RaftLogEntry::Index { collection_id, req })
            .await?
        {
            ApplyOutcome::Indexed(r) => Ok(r),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn replace_docs(
        &self,
        collection_id: String,
        req: ReplaceDocsRequest,
    ) -> Result<ReplaceDocsResponse> {
        match self
            .writer
            .submit(RaftLogEntry::ReplaceDocs { collection_id, req })
            .await?
        {
            ApplyOutcome::Replaced(r) => Ok(r),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn truncate_docs(&self, collection_id: String) -> Result<()> {
        match self
            .writer
            .submit(RaftLogEntry::TruncateDocs { collection_id })
            .await?
        {
            ApplyOutcome::DocsTruncated => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn unindex_docs(
        &self,
        collection_id: String,
        req: BatchUnindexDocsRequest,
    ) -> Result<()> {
        validate_batch_unindex_docs_request(&req)?;
        match self
            .writer
            .submit(RaftLogEntry::UnindexDocs { collection_id, req })
            .await?
        {
            ApplyOutcome::DocsUnindexed => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn delete(
        &self,
        collection_id: String,
        external_id: String,
        field: Option<String>,
    ) -> Result<()> {
        match self
            .writer
            .submit(RaftLogEntry::Delete {
                collection_id,
                external_id,
                field,
            })
            .await?
        {
            ApplyOutcome::Deleted => Ok(()),
            other => Err(Self::unexpected(other)),
        }
    }

    async fn drop_field(&self, collection_id: String, field_name: String) -> Result<u32> {
        match self
            .writer
            .submit(RaftLogEntry::DropField {
                collection_id,
                field_name,
            })
            .await?
        {
            ApplyOutcome::FieldChanged(v) => Ok(v),
            other => Err(Self::unexpected(other)),
        }
    }
}

#[cfg(test)]
mod tests;
