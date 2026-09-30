//! `RoutedRouter` as the API's `RoutedBackend`: each call answers locally when
//! this pod owns the bucket, and forwards one hop otherwise.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::http::HeaderMap;
use futures::future::{join_all, try_join_all};

use crate::index::application::engine::collections::DropOutcome;
use crate::sharding::application::ports::routed_backend::RoutedBackend;
use crate::sharding::domain::forward_error::{ShardForwardMisrouted, ShardForwardRemoteError};
use crate::sharding::domain::shard_route::SearchShardTarget;
use crate::sharding::infrastructure::routed_router::{
    drop_outcome_from_status, merge_drop_outcomes, percent_encode_component, RoutedRouter,
};
use crate::shared_kernel::types::{
    document::{
        validate_batch_unindex_docs_request, BatchUnindexDocsRequest, IndexItem, IndexRequest,
        IndexResponse, ReplaceDocItem, ReplaceDocsRequest, ReplaceDocsResponse,
    },
    schema::{CreateCollectionRequest, CreateCollectionResponse},
    search::{SearchRequest, SearchResponse},
};

#[async_trait]
impl RoutedBackend for RoutedRouter {
    async fn create_collection(
        &self,
        collection_id: String,
        req: CreateCollectionRequest,
        headers: &HeaderMap,
    ) -> Result<CreateCollectionResponse> {
        if Self::already_forwarded(headers) {
            // #2496: no single bucket owns a collection, so — like
            // `scatter_search`'s keyless arm — a forwarded sub-request just
            // applies locally and never re-fans-out, keeping forwarding one
            // hop deep.
            return self.local_write.create_collection(collection_id, req).await;
        }
        let path = format!("/collections/{}", percent_encode_component(&collection_id));
        let mut remote_futures = Vec::new();
        for shard in 0..self.shard_map.physical_shard_count() {
            if shard == self.local_shard {
                continue;
            }
            remote_futures.push(
                self.forward_json::<CreateCollectionRequest, CreateCollectionResponse>(
                    shard,
                    reqwest::Method::PUT,
                    &path,
                    Some(req.clone()),
                    headers,
                ),
            );
        }
        let local_resp = self
            .local_write
            .create_collection(collection_id, req)
            .await?;
        let remote_resps = try_join_all(remote_futures).await?;
        for resp in &remote_resps {
            if resp.version != local_resp.version || resp.fields_count != local_resp.fields_count {
                anyhow::bail!("shard collection-create responses diverged");
            }
        }
        Ok(local_resp)
    }

    async fn drop_collection(
        &self,
        collection_id: String,
        force: bool,
        headers: &HeaderMap,
    ) -> Result<DropOutcome> {
        if Self::already_forwarded(headers) {
            return self.local_write.drop_collection(collection_id, force).await;
        }
        let mut path = format!("/collections/{}", percent_encode_component(&collection_id));
        if force {
            path.push_str("?force=true");
        }
        let mut remote_futures = Vec::new();
        for shard in 0..self.shard_map.physical_shard_count() {
            if shard == self.local_shard {
                continue;
            }
            remote_futures.push(self.forward_drop_status(shard, &path, headers));
        }
        let local_outcome = self
            .local_write
            .drop_collection(collection_id, force)
            .await?;
        let remote_statuses = try_join_all(remote_futures).await?;
        let mut merged = local_outcome;
        for status in remote_statuses {
            merged = merge_drop_outcomes(merged, drop_outcome_from_status(status, force)?);
        }
        Ok(merged)
    }

    async fn search(
        &self,
        collection_id: &str,
        req: SearchRequest,
        headers: &HeaderMap,
    ) -> Result<SearchResponse> {
        if Self::already_forwarded(headers) {
            // #1457 R4: only a keyed forward has a single owner-of-record to
            // validate, so only that arm re-checks the sender's map version
            // (#1442 R2) and recomputes ownership (#1442 R1). A keyless
            // (scatter) sub-request has no single owner and this pod always
            // answers truthfully for its own local data regardless of which
            // map version the scattering pod ran — checking the map version
            // there only starved scatter search during every rolling
            // restart after a completed split (see `assert_owns`'s doc
            // comment and this module's header for the full rationale).
            if let Some(key) = req.routing_key.as_deref() {
                self.check_forwarded_map_version(headers)?;
                let route = self.shard_map.route_key(collection_id, key);
                if route.shard != self.local_shard {
                    return Err(anyhow::Error::new(ShardForwardMisrouted {
                        bucket: route.bucket,
                        owner_shard: route.shard,
                        local_shard: self.local_shard,
                    }));
                }
            } else if let Some(sender_version) = Self::forwarded_map_version(headers) {
                // #1467 R6: a keyless (scatter) sub-request is still exempt
                // from the hard `check_forwarded_map_version` rejection above
                // — availability over completeness, see this module's header
                // doc — but a disagreement here means the scattering pod's
                // view of the topology and this pod's are momentarily out of
                // sync (a mixed-map rolling-restart window), which can make a
                // scatter search's result set silently incomplete or
                // overlapping rather than merely stale. Surface that
                // non-fatally instead of staying invisible.
                let local_version = self.shard_map.version();
                if sender_version != local_version {
                    self.engine.metrics().incr_scatter_map_version_mismatch();
                    tracing::warn!(
                        collection_id,
                        sender_version,
                        local_version,
                        "routed scatter search: responding pod's shard-map version differs \
                         from the scattering pod's declared version; result set may be \
                         momentarily incomplete during a rolling restart"
                    );
                }
            }
            let engine = Arc::clone(&self.engine);
            let collection_id = collection_id.to_string();
            return self
                .search_executor
                .run(move || engine.search(&collection_id, req))
                .await;
        }
        match self
            .shard_map
            .search_target(collection_id, req.routing_key.as_deref())
        {
            SearchShardTarget::One(route) if route.shard == self.local_shard => {
                let engine = Arc::clone(&self.engine);
                let collection_id = collection_id.to_string();
                self.search_executor
                    .run(move || engine.search(&collection_id, req))
                    .await
            }
            SearchShardTarget::One(route) => {
                // #1457 R3: percent-encode the caller-controlled collection
                // id (see `scatter_search`'s comment for the rationale).
                let path = format!(
                    "/collections/{}/search",
                    percent_encode_component(collection_id)
                );
                self.forward_json(
                    route.shard,
                    reqwest::Method::POST,
                    &path,
                    Some(req),
                    headers,
                )
                .await
            }
            SearchShardTarget::All => self.scatter_search(collection_id, req, headers).await,
        }
    }

    async fn index(
        &self,
        collection_id: String,
        req: IndexRequest,
        headers: &HeaderMap,
    ) -> Result<IndexResponse> {
        if Self::already_forwarded(headers) {
            self.check_forwarded_map_version(headers)?;
            for item in &req.items {
                self.assert_owns(&collection_id, &item.external_id)?;
            }
            return self.local_write.index(collection_id, req).await;
        }
        let shard_count = self.shard_map.physical_shard_count() as usize;
        let mut shard_items: Vec<Vec<IndexItem>> = (0..shard_count).map(|_| Vec::new()).collect();
        for item in req.items {
            let shard = self
                .shard_map
                .route_document(&collection_id, None, &item.external_id)
                .shard as usize;
            shard_items[shard].push(item);
        }

        // #1457 R3: percent-encode the caller-controlled collection id (see
        // `scatter_search`'s comment for the rationale).
        let path = format!(
            "/collections/{}/index",
            percent_encode_component(&collection_id)
        );
        let mut local_resp: Option<IndexResponse> = None;
        let mut remote_futures = Vec::new();
        for (shard, items) in shard_items.into_iter().enumerate() {
            if items.is_empty() {
                continue;
            }
            let shard = shard as u32;
            let sub_req = IndexRequest {
                items,
                request_id: req.request_id.clone(),
            };
            if shard == self.local_shard {
                local_resp = Some(
                    self.local_write
                        .index(collection_id.clone(), sub_req)
                        .await?,
                );
            } else {
                remote_futures.push(self.forward_json::<IndexRequest, IndexResponse>(
                    shard,
                    reqwest::Method::POST,
                    &path,
                    Some(sub_req),
                    headers,
                ));
            }
        }
        let remote_resps = try_join_all(remote_futures).await?;

        let mut indexed = 0u32;
        let mut bytes_written = BTreeMap::new();
        let mut shard_lag_ms = 0u64;
        for resp in local_resp.into_iter().chain(remote_resps) {
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
        headers: &HeaderMap,
    ) -> Result<ReplaceDocsResponse> {
        if Self::already_forwarded(headers) {
            self.check_forwarded_map_version(headers)?;
            for doc in &req.docs {
                self.assert_owns(&collection_id, &doc.external_id)?;
            }
            return self.local_write.replace_docs(collection_id, req).await;
        }
        let total = req.docs.len();
        let shard_count = self.shard_map.physical_shard_count() as usize;
        let mut shard_docs: Vec<Vec<ReplaceDocItem>> =
            (0..shard_count).map(|_| Vec::new()).collect();
        let mut shard_positions: Vec<Vec<usize>> = (0..shard_count).map(|_| Vec::new()).collect();
        for (idx, item) in req.docs.into_iter().enumerate() {
            let shard = self
                .shard_map
                .route_document(&collection_id, None, &item.external_id)
                .shard as usize;
            shard_positions[shard].push(idx);
            shard_docs[shard].push(item);
        }

        // #1457 R3: percent-encode the caller-controlled collection id (see
        // `scatter_search`'s comment for the rationale).
        let path = format!(
            "/collections/{}/docs:replace",
            percent_encode_component(&collection_id)
        );
        // #1442 R5: `sent` is the exact number of docs handed to each
        // shard, captured before the request body moves — the response
        // below is validated against it so a short/long response (e.g. a
        // remote pod that dropped part of the batch) is a classified
        // forward error, never a silent `zip`-truncation that leaves a
        // `results[pos]` unassigned.
        let mut local_resp: Option<(u32, usize, ReplaceDocsResponse)> = None;
        let mut remote_shards: Vec<(u32, usize)> = Vec::new();
        let mut remote_futures = Vec::new();
        for (shard, docs) in shard_docs.into_iter().enumerate() {
            if docs.is_empty() {
                continue;
            }
            let shard = shard as u32;
            let sent = docs.len();
            let sub_req = ReplaceDocsRequest { docs };
            if shard == self.local_shard {
                local_resp = Some((
                    shard,
                    sent,
                    self.local_write
                        .replace_docs(collection_id.clone(), sub_req)
                        .await?,
                ));
            } else {
                remote_shards.push((shard, sent));
                remote_futures.push(
                    self.forward_json::<ReplaceDocsRequest, ReplaceDocsResponse>(
                        shard,
                        reqwest::Method::PUT,
                        &path,
                        Some(sub_req),
                        headers,
                    ),
                );
            }
        }
        let remote_resps = try_join_all(remote_futures).await?;

        let mismatch = |shard: u32, sent: usize, got: usize| {
            anyhow::Error::new(ShardForwardRemoteError {
                status: 502,
                message: format!(
                    "shard {shard} replace_docs response has {got} results but {sent} docs were sent"
                ),
            })
        };
        let mut results: Vec<Option<crate::shared_kernel::types::document::ReplaceDocResult>> =
            (0..total).map(|_| None).collect();
        if let Some((shard, sent, resp)) = local_resp {
            if resp.results.len() != sent {
                return Err(mismatch(shard, sent, resp.results.len()));
            }
            for (pos, r) in shard_positions[shard as usize].iter().zip(resp.results) {
                results[*pos] = Some(r);
            }
        }
        for ((shard, sent), resp) in remote_shards.into_iter().zip(remote_resps) {
            if resp.results.len() != sent {
                return Err(mismatch(shard, sent, resp.results.len()));
            }
            for (pos, r) in shard_positions[shard as usize].iter().zip(resp.results) {
                results[*pos] = Some(r);
            }
        }
        // Every position was assigned to exactly one shard above, and every
        // shard's response length was just validated to match the docs
        // sent to it, so every slot is provably `Some` here — but this
        // stays a classified error rather than an `.expect()` panic (R5)
        // as a defensive backstop, not a documented reachable path.
        let mut final_results = Vec::with_capacity(total);
        for r in results {
            final_results.push(r.ok_or_else(|| ShardForwardRemoteError {
                status: 502,
                message: "replace_docs response merge left a position unassigned".to_string(),
            })?);
        }
        Ok(ReplaceDocsResponse {
            results: final_results,
        })
    }

    async fn truncate_docs(&self, collection_id: String, headers: &HeaderMap) -> Result<()> {
        if Self::already_forwarded(headers) {
            // Unlike existing collection lifecycle fan-out, truncate's
            // contract explicitly refuses an unstable map on the forwarded
            // hop.  The sender's version is therefore validated before this
            // physical shard receives its one durable command.
            self.check_forwarded_map_version(headers)?;
            return self.local_write.truncate_docs(collection_id).await;
        }

        let path = format!(
            "/collections/{}/docs:truncate",
            percent_encode_component(&collection_id)
        );
        let remote_futures = (0..self.shard_map.physical_shard_count())
            .filter(|&shard| shard != self.local_shard)
            .map(|shard| self.forward_empty(shard, reqwest::Method::POST, &path, headers));

        // Start local and remote proposals together.  We intentionally wait
        // for every remote result rather than short-circuiting after one
        // error: another shard may have already committed its local truncate,
        // and the frozen contract exposes that mixed window instead of trying
        // an unsafe rollback.
        let (local, remotes) = tokio::join!(
            self.local_write.truncate_docs(collection_id),
            join_all(remote_futures)
        );
        let mut first_error = local.err();
        for remote in remotes {
            if let Err(error) = remote {
                if first_error.is_none() {
                    first_error = Some(error);
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
        headers: &HeaderMap,
    ) -> Result<()> {
        // Keep the routed backend safe for direct callers too.  HTTP already
        // validates before it reaches this point, but routing must never turn
        // an invalid request into a forward or partial shard command.
        validate_batch_unindex_docs_request(&req)?;
        if Self::already_forwarded(headers) {
            self.check_forwarded_map_version(headers)?;
            for external_id in &req.external_ids {
                self.assert_owns(&collection_id, external_id)?;
            }
            return self.local_write.unindex_docs(collection_id, req).await;
        }

        let shard_count = self.shard_map.physical_shard_count() as usize;
        let mut shard_ids: Vec<Vec<String>> = (0..shard_count).map(|_| Vec::new()).collect();
        for external_id in req.external_ids {
            let shard = self
                .shard_map
                .route_document(&collection_id, None, &external_id)
                .shard as usize;
            shard_ids[shard].push(external_id);
        }

        let path = format!(
            "/collections/{}/docs:unindex",
            percent_encode_component(&collection_id)
        );
        // Start every nonempty shard request before awaiting any result.  A
        // failure has no rollback: completed sibling shards retain their
        // atomic local removal, and the caller gets a routed 5xx.
        let mut futures = Vec::new();
        for (shard, external_ids) in shard_ids.into_iter().enumerate() {
            if external_ids.is_empty() {
                continue;
            }
            let shard = shard as u32;
            let collection_id = collection_id.clone();
            let path = path.clone();
            let sub_req = BatchUnindexDocsRequest { external_ids };
            futures.push(async move {
                if shard == self.local_shard {
                    self.local_write.unindex_docs(collection_id, sub_req).await
                } else {
                    self.forward_json_empty(shard, reqwest::Method::POST, &path, sub_req, headers)
                        .await
                }
            });
        }

        let outcomes = join_all(futures).await;
        let mut first_error = None;
        for outcome in outcomes {
            if let Err(error) = outcome {
                if first_error.is_none() {
                    first_error = Some(error);
                }
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
        headers: &HeaderMap,
    ) -> Result<()> {
        if Self::already_forwarded(headers) {
            self.check_forwarded_map_version(headers)?;
            self.assert_owns(&collection_id, &external_id)?;
            return self
                .local_write
                .delete(collection_id, external_id, field)
                .await;
        }
        let route = self
            .shard_map
            .route_document(&collection_id, None, &external_id);
        if route.shard == self.local_shard {
            return self
                .local_write
                .delete(collection_id, external_id, field)
                .await;
        }
        // #1442 R4 / #1457 R3: percent-encode every caller-controlled
        // path/query component — an unencoded `/`, `?`, `#`, or `%` in
        // `collection_id`, `external_id`, or `field` would otherwise be
        // misparsed as URL structure (or a doubly-decoded literal) by the
        // remote pod.
        let mut path = format!(
            "/collections/{}/index/{}",
            percent_encode_component(&collection_id),
            percent_encode_component(&external_id)
        );
        if let Some(f) = &field {
            path.push('?');
            path.push_str("field=");
            path.push_str(&percent_encode_component(f));
        }
        self.forward_empty(route.shard, reqwest::Method::DELETE, &path, headers)
            .await
    }
}
