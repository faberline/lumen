//! `EngineShardWrite` as the API's `WriteBackend`.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::future::{join_all, try_join_all};

use crate::index::application::engine::{collections::DropOutcome, raft_dispatch::ApplyOutcome};
use crate::ingest::application::ports::write_backend::WriteBackend;
use crate::sharding::application::engine_shard_write::EngineShardWrite;
use crate::shared_kernel::log_entry::RaftLogEntry;
use crate::shared_kernel::types::{
    document::{
        validate_batch_unindex_docs_request, BatchUnindexDocsRequest, IndexRequest, IndexResponse,
        ReplaceDocItem, ReplaceDocResult, ReplaceDocsRequest, ReplaceDocsResponse,
    },
    schema::{CreateCollectionRequest, CreateCollectionResponse},
};

#[async_trait]
impl WriteBackend for EngineShardWrite {
    async fn create_collection(
        &self,
        collection_id: String,
        req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        self.require_shards()?;
        let outcomes = try_join_all(self.writers.iter().map(|writer| {
            let writer = writer.clone();
            let collection_id = collection_id.clone();
            let req = req.clone();
            async move {
                writer
                    .submit(RaftLogEntry::CreateCollection { collection_id, req })
                    .await
            }
        }))
        .await?;

        let mut first: Option<CreateCollectionResponse> = None;
        for outcome in outcomes {
            match outcome {
                ApplyOutcome::Created(resp) => {
                    if let Some(existing) = &first {
                        if existing.version != resp.version
                            || existing.fields_count != resp.fields_count
                        {
                            bail!("shard collection-create responses diverged");
                        }
                    } else {
                        first = Some(resp);
                    }
                }
                other => bail!("unexpected apply outcome: {other:?}"),
            }
        }
        first.ok_or_else(|| anyhow::anyhow!("sharded create produced no responses"))
    }

    async fn drop_collection(&self, collection_id: String, force: bool) -> Result<DropOutcome> {
        self.require_shards()?;
        let outcomes = try_join_all(self.writers.iter().map(|writer| {
            let writer = writer.clone();
            let collection_id = collection_id.clone();
            async move {
                writer
                    .submit(RaftLogEntry::DropCollection {
                        collection_id,
                        force,
                    })
                    .await
            }
        }))
        .await?;

        let mut merged = DropOutcome::NotFound;
        for outcome in outcomes {
            let ApplyOutcome::Dropped(outcome) = outcome else {
                bail!("unexpected apply outcome: {outcome:?}");
            };
            merged = match (merged, outcome) {
                (DropOutcome::Physical, _) | (_, DropOutcome::Physical) => DropOutcome::Physical,
                (DropOutcome::Marked, _) | (_, DropOutcome::Marked) => DropOutcome::Marked,
                (DropOutcome::AlreadyMarked, _) | (_, DropOutcome::AlreadyMarked) => {
                    DropOutcome::AlreadyMarked
                }
                (DropOutcome::NotFound, DropOutcome::NotFound) => DropOutcome::NotFound,
            };
        }
        Ok(merged)
    }

    async fn index(&self, collection_id: String, req: IndexRequest) -> Result<IndexResponse> {
        self.require_shards()?;
        let mut shard_reqs: Vec<IndexRequest> = (0..self.writers.len())
            .map(|_| IndexRequest {
                items: Vec::new(),
                request_id: req.request_id.clone(),
            })
            .collect();

        for item in req.items {
            let shard = self
                .shard_map
                .route_document(&collection_id, None, &item.external_id)
                .shard as usize;
            shard_reqs[shard].items.push(item);
        }

        let has_items = shard_reqs.iter().any(|req| !req.items.is_empty());
        let mut futures = Vec::new();
        for (shard, req) in shard_reqs.into_iter().enumerate() {
            if has_items && req.items.is_empty() {
                continue;
            }
            let writer = self.writers[shard].clone();
            let collection_id = collection_id.clone();
            futures.push(async move {
                writer
                    .submit(RaftLogEntry::Index { collection_id, req })
                    .await
            });
            if !has_items {
                break;
            }
        }

        let outcomes = try_join_all(futures).await?;
        let mut indexed = 0u32;
        let mut bytes_written = BTreeMap::new();
        let mut shard_lag_ms = 0u64;
        for outcome in outcomes {
            let ApplyOutcome::Indexed(resp) = outcome else {
                bail!("unexpected apply outcome: {outcome:?}");
            };
            indexed = indexed.saturating_add(resp.indexed);
            shard_lag_ms = shard_lag_ms.max(resp.shard_lag_ms);
            for (field, bytes) in resp.bytes_written {
                *bytes_written.entry(field).or_insert(0) += bytes;
            }
        }
        Ok(IndexResponse {
            indexed,
            bytes_written,
            shard_lag_ms,
        })
    }

    async fn replace_docs(
        &self,
        collection_id: String,
        req: ReplaceDocsRequest,
    ) -> Result<ReplaceDocsResponse> {
        self.require_shards()?;
        let total = req.docs.len();
        let mut shard_reqs: Vec<Vec<ReplaceDocItem>> =
            (0..self.writers.len()).map(|_| Vec::new()).collect();
        let mut shard_positions: Vec<Vec<usize>> =
            (0..self.writers.len()).map(|_| Vec::new()).collect();

        for (idx, item) in req.docs.into_iter().enumerate() {
            let shard = self
                .shard_map
                .route_document(&collection_id, None, &item.external_id)
                .shard as usize;
            shard_positions[shard].push(idx);
            shard_reqs[shard].push(item);
        }

        let mut futures = Vec::new();
        let mut shard_order = Vec::new();
        for (shard, docs) in shard_reqs.into_iter().enumerate() {
            if docs.is_empty() {
                continue;
            }
            let writer = self.writers[shard].clone();
            let collection_id = collection_id.clone();
            shard_order.push(shard);
            futures.push(async move {
                writer
                    .submit(RaftLogEntry::ReplaceDocs {
                        collection_id,
                        req: ReplaceDocsRequest { docs },
                    })
                    .await
            });
        }

        let outcomes = try_join_all(futures).await?;
        // Reassemble in the caller's original `req.docs` order — shard
        // fan-out only preserves order *within* a shard, so each result
        // is placed back at its origin index rather than appended in
        // shard-completion order.
        let mut results: Vec<Option<ReplaceDocResult>> = (0..total).map(|_| None).collect();
        for (shard, outcome) in shard_order.into_iter().zip(outcomes) {
            let ApplyOutcome::Replaced(resp) = outcome else {
                bail!("unexpected apply outcome: {outcome:?}");
            };
            for (pos, result) in shard_positions[shard].iter().zip(resp.results) {
                results[*pos] = Some(result);
            }
        }
        let results: Vec<ReplaceDocResult> = results
            .into_iter()
            .map(|r| r.expect("every original index assigned exactly one shard result"))
            .collect();
        Ok(ReplaceDocsResponse { results })
    }

    async fn truncate_docs(&self, collection_id: String) -> Result<()> {
        self.require_shards()?;
        // Do not use `try_join_all`: a collection-wide command may already
        // have committed on another physical shard when one reply fails.  Poll
        // every proposal to completion, then surface a routed error with no
        // rollback, which is the public per-shard atomicity contract.
        let outcomes = join_all(self.writers.iter().map(|writer| {
            let writer = writer.clone();
            let collection_id = collection_id.clone();
            async move {
                writer
                    .submit(RaftLogEntry::TruncateDocs { collection_id })
                    .await
            }
        }))
        .await;

        let mut first_error = None;
        for outcome in outcomes {
            match outcome {
                Ok(ApplyOutcome::DocsTruncated) => {}
                Ok(other) => {
                    if first_error.is_none() {
                        first_error = Some(anyhow::anyhow!("unexpected apply outcome: {other:?}"));
                    }
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn unindex_docs(
        &self,
        collection_id: String,
        req: BatchUnindexDocsRequest,
    ) -> Result<()> {
        validate_batch_unindex_docs_request(&req)?;
        self.require_shards()?;

        let mut shard_ids: Vec<Vec<String>> = (0..self.writers.len()).map(|_| Vec::new()).collect();
        for external_id in req.external_ids {
            let shard = self
                .shard_map
                .route_document(&collection_id, None, &external_id)
                .shard as usize;
            shard_ids[shard].push(external_id);
        }

        // The public bound gives every active physical shard at most one
        // command.  Poll every nonempty shard to completion: a sibling may
        // have committed before another replies with an error, and there is
        // intentionally no unsafe cross-shard rollback.
        let outcomes = join_all(
            shard_ids
                .into_iter()
                .enumerate()
                .filter_map(|(shard, ids)| {
                    (!ids.is_empty()).then(|| {
                        let writer = self.writers[shard].clone();
                        let collection_id = collection_id.clone();
                        async move {
                            writer
                                .submit(RaftLogEntry::UnindexDocs {
                                    collection_id,
                                    req: BatchUnindexDocsRequest { external_ids: ids },
                                })
                                .await
                        }
                    })
                }),
        )
        .await;

        let mut first_error = None;
        for outcome in outcomes {
            match outcome {
                Ok(ApplyOutcome::DocsUnindexed) => {}
                Ok(other) if first_error.is_none() => {
                    first_error = Some(anyhow::anyhow!("unexpected apply outcome: {other:?}"));
                }
                Err(error) if first_error.is_none() => first_error = Some(error),
                _ => {}
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn delete(
        &self,
        collection_id: String,
        external_id: String,
        field: Option<String>,
    ) -> Result<()> {
        self.require_shards()?;
        let shard = self
            .shard_map
            .route_document(&collection_id, None, &external_id)
            .shard as usize;
        match self.writers[shard]
            .submit(RaftLogEntry::Delete {
                collection_id,
                external_id,
                field,
            })
            .await?
        {
            ApplyOutcome::Deleted => Ok(()),
            other => bail!("unexpected apply outcome: {other:?}"),
        }
    }

    async fn drop_field(&self, collection_id: String, field_name: String) -> Result<u32> {
        self.require_shards()?;
        let outcomes = try_join_all(self.writers.iter().map(|writer| {
            let writer = writer.clone();
            let collection_id = collection_id.clone();
            let field_name = field_name.clone();
            async move {
                writer
                    .submit(RaftLogEntry::DropField {
                        collection_id,
                        field_name,
                    })
                    .await
            }
        }))
        .await?;

        let mut version = None;
        for outcome in outcomes {
            let ApplyOutcome::FieldChanged(v) = outcome else {
                bail!("unexpected apply outcome: {outcome:?}");
            };
            if let Some(existing) = version {
                if existing != v {
                    bail!("shard drop-field versions diverged");
                }
            } else {
                version = Some(v);
            }
        }
        version.ok_or_else(|| anyhow::anyhow!("sharded drop-field produced no responses"))
    }
}
