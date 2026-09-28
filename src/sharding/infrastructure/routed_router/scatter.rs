//! A search with no routing key: a local leg plus one forward per remote shard,
//! merged into one page.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::http::HeaderMap;
use futures::future::join_all;

use crate::api::ShardForwardRemoteError;
use crate::sharding::application::search_fanout::{
    merge_shard_search_responses, search_request_offset,
};
use crate::sharding::infrastructure::routed_router::{percent_encode_component, RoutedRouter};
use crate::storage::StorageError;
use crate::types::{SearchRequest, SearchResponse};

impl RoutedRouter {
    /// Routing-key-less search: local engine direct + one forward per remote
    /// shard, merged through [`merge_shard_search_responses`] exactly like
    /// [`crate::sharding::application::engine_shard_search::EngineShardSearch`]. Known gap vs. that in-process
    /// merger: `sort_value` can only resolve field values from *this* pod's
    /// local engine (`EngineShardSearch` holds every shard's `Engine`
    /// in-process and can resolve any hit; a routed pod only has its own) —
    /// a cross-shard sort-by-field page may rank remote-shard hits by a
    /// missing (`None`) sort value. Score-ranked search (the default) is
    /// unaffected.
    ///
    /// #2489: a physical shard that has never locally registered
    /// `collection_id` answers `CollectionNotFound` — every shard holds
    /// every collection under steady-state operation, but a just-completed
    /// reshard split can strand a receiving shard that migrated zero of a
    /// collection's documents (none of its buckets moved there) without the
    /// collection ever being created on it: the migration pipeline
    /// (`operator::reshard_driver` -> `reshard::snapshot_reshard_batches` /
    /// `snapshot_reshard_prune_chunks` -> `Engine::apply_reshard_batch`) is
    /// purely doc-driven and has no way to propagate a collection's
    /// existence/schema on its own. One shard out of N having nothing to
    /// contribute must not fail the *whole* scatter merge when at least one
    /// other shard actually answers — that shard's real hits would
    /// otherwise be discarded for nothing, exactly the GKE field symptom
    /// (search via the new shard's Service-pinned endpoint returned
    /// `collection not found` even though the collection's sole document
    /// lived, correctly migrated, on the other shard). Only when *every*
    /// participant reports `CollectionNotFound` does this propagate that
    /// error, so a genuinely nonexistent collection still answers 404.
    pub(super) async fn scatter_search(
        &self,
        collection_id: &str,
        req: SearchRequest,
        headers: &HeaderMap,
    ) -> Result<SearchResponse> {
        let start = Instant::now();
        let offset = search_request_offset(&req)?;
        let limit = req.limit as usize;
        let mut shard_req = req.clone();
        shard_req.offset = 0;
        shard_req.cursor = None;
        shard_req.limit = offset.saturating_add(limit).min(u32::MAX as usize) as u32;

        // #1457 R3: percent-encode the caller-controlled collection id —
        // an unencoded `/`, `%`, or space would otherwise be misparsed as
        // URL structure (or land as a doubly-decoded literal) by the
        // remote pod, the same class of bug #1442 R4 fixed for
        // `external_id`/`field`.
        let path = format!(
            "/collections/{}/search",
            percent_encode_component(collection_id)
        );
        let mut remote_futures = Vec::new();
        for shard in 0..self.shard_map.physical_shard_count() {
            if shard == self.local_shard {
                continue;
            }
            let shard_req = shard_req.clone();
            remote_futures.push(self.forward_json::<SearchRequest, SearchResponse>(
                shard,
                reqwest::Method::POST,
                &path,
                Some(shard_req),
                headers,
            ));
        }
        // #2489: `join_all`, not `try_join_all` — a single participant's
        // error (in particular a benign per-shard `CollectionNotFound`,
        // classified below) must not short-circuit before the other
        // shards' real answers are collected. Poll the local blocking task and
        // remote futures together: previously the synchronous local search ran
        // first, so even opening the remote requests waited for one slow sort.
        let engine = Arc::clone(&self.engine);
        let executor = self.search_executor.clone();
        let local_collection = collection_id.to_string();
        let local_search = executor.run(move || engine.search(&local_collection, shard_req));
        let (local_result, remote_results) = tokio::join!(local_search, join_all(remote_futures));

        let mut responses = Vec::with_capacity(1 + remote_results.len());
        let mut not_found_err = None;
        match local_result {
            Ok(resp) => responses.push(resp),
            Err(err) if is_local_collection_not_found(&err) => not_found_err = Some(err),
            Err(err) => return Err(err),
        }
        for result in remote_results {
            match result {
                Ok(resp) => responses.push(resp),
                Err(err) if is_forwarded_collection_not_found(&err) => not_found_err = Some(err),
                Err(err) => return Err(err),
            }
        }
        if responses.is_empty() {
            // Every shard reported `CollectionNotFound` — surface it
            // faithfully instead of manufacturing an empty 200 response for
            // a collection that genuinely does not exist anywhere.
            return Err(not_found_err
                .expect("responses empty implies at least one CollectionNotFound was recorded"));
        }
        if not_found_err.is_some() {
            tracing::warn!(
                collection_id,
                "routed scatter search: at least one shard has no local record of this \
                 collection (likely a just-migrated shard that received zero of its \
                 documents, or a collection created before this pod joined the cluster); \
                 merging the remaining shards' results instead of failing the whole query \
                 (#2489)"
            );
        }

        let engine = self.engine.clone();
        let collection = collection_id.to_string();
        Ok(merge_shard_search_responses(
            &req,
            responses,
            start.elapsed().as_micros() as u64,
            move |hit, field| {
                engine
                    .number_value_for_external_id(&collection, &hit.external_id, field)
                    .ok()
                    .flatten()
            },
        ))
    }
}

/// True when `err` is a local [`StorageError::CollectionNotFound`] — the
/// local half of `scatter_search`'s per-shard not-found tolerance (#2489).
fn is_local_collection_not_found(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<StorageError>(),
        Some(StorageError::CollectionNotFound(_))
    )
}

/// True when `err` is a forwarded shard's 404 — the remote half of
/// `scatter_search`'s per-shard not-found tolerance (#2489). `404` is used
/// exclusively for `StorageError::CollectionNotFound` in `ApiErr`'s mapping
/// (`api.rs`'s `impl From<anyhow::Error> for ApiErr`), so the forwarded
/// status code alone is an unambiguous signal without re-parsing the
/// remote's error message.
fn is_forwarded_collection_not_found(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<ShardForwardRemoteError>(),
        Some(remote) if remote.status == reqwest::StatusCode::NOT_FOUND.as_u16()
    )
}
