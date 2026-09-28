use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::api::WriteBackend;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::sharding::infrastructure::routed_router::{
    drop_outcome_from_status, merge_drop_outcomes, percent_encode_component, RoutedRouter,
    FORWARDED_HEADER, MAP_VERSION_HEADER,
};
use crate::shared_kernel::types::{
    document::{
        BatchUnindexDocsRequest, IndexRequest, IndexResponse, ReplaceDocsRequest,
        ReplaceDocsResponse,
    },
    schema::{CreateCollectionRequest, CreateCollectionResponse},
};
use crate::storage::{DropOutcome, Engine};

#[cfg(test)]
use crate::sharding::infrastructure::routed_router::cursor_offset;

struct DummyWrite;

#[async_trait]
impl WriteBackend for DummyWrite {
    async fn create_collection(
        &self,
        _collection_id: String,
        _req: CreateCollectionRequest,
    ) -> Result<CreateCollectionResponse> {
        unimplemented!("construction-only test double")
    }
    async fn drop_collection(&self, _collection_id: String, _force: bool) -> Result<DropOutcome> {
        unimplemented!("construction-only test double")
    }
    async fn index(&self, _collection_id: String, _req: IndexRequest) -> Result<IndexResponse> {
        unimplemented!("construction-only test double")
    }
    async fn replace_docs(
        &self,
        _collection_id: String,
        _req: ReplaceDocsRequest,
    ) -> Result<ReplaceDocsResponse> {
        unimplemented!("construction-only test double")
    }
    async fn truncate_docs(&self, _collection_id: String) -> Result<()> {
        unimplemented!("construction-only test double")
    }
    async fn unindex_docs(
        &self,
        _collection_id: String,
        _req: BatchUnindexDocsRequest,
    ) -> Result<()> {
        unimplemented!("construction-only test double")
    }
    async fn delete(
        &self,
        _collection_id: String,
        _external_id: String,
        _field: Option<String>,
    ) -> Result<()> {
        unimplemented!("construction-only test double")
    }
    async fn drop_field(&self, _collection_id: String, _field_name: String) -> Result<u32> {
        unimplemented!("construction-only test double")
    }
}

fn shard_map(physical: u32) -> VirtualBucketShardMap {
    VirtualBucketShardMap::balanced(0, 16, physical).unwrap()
}

#[test]
fn new_rejects_mismatched_shard_url_count() {
    let err = RoutedRouter::new(
        Arc::new(Engine::new()),
        Arc::new(DummyWrite),
        shard_map(3),
        0,
        vec!["http://a".into(), "http://b".into()],
    )
    .err()
    .unwrap();
    assert!(err.to_string().contains('3'));
}

#[test]
fn new_rejects_out_of_range_local_shard() {
    let err = RoutedRouter::new(
        Arc::new(Engine::new()),
        Arc::new(DummyWrite),
        shard_map(2),
        2,
        vec!["http://a".into(), "http://b".into()],
    )
    .err()
    .unwrap();
    assert!(err.to_string().contains("out of range"));
}

#[test]
fn new_accepts_valid_topology() {
    let router = RoutedRouter::new(
        Arc::new(Engine::new()),
        Arc::new(DummyWrite),
        shard_map(2),
        0,
        vec![
            "http://search-0.headless".into(),
            "http://search-1.headless".into(),
        ],
    )
    .unwrap();
    assert!(
        router.remotes[0].is_none(),
        "local shard has no remote entry"
    );
    assert!(router.remotes[1].is_some());
}

#[test]
fn already_forwarded_detects_marker_header() {
    let mut headers = HeaderMap::new();
    assert!(!RoutedRouter::already_forwarded(&headers));
    headers.insert(FORWARDED_HEADER, "1".parse().unwrap());
    assert!(RoutedRouter::already_forwarded(&headers));
}

#[test]
fn cursor_offset_decodes_offset_and_defaults_to_zero() {
    assert_eq!(cursor_offset(None), 0);
    assert_eq!(cursor_offset(Some("not-base64!!")), 0);

    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
    let cursor = STANDARD_NO_PAD.encode(r#"{"offset":42}"#);
    assert_eq!(cursor_offset(Some(&cursor)), 42);
}

#[test]
fn percent_encode_component_escapes_reserved_bytes() {
    assert_eq!(percent_encode_component("plain-ID_1.2~3"), "plain-ID_1.2~3");
    assert_eq!(
        percent_encode_component("a/b?c=d&e f"),
        "a%2Fb%3Fc%3Dd%26e%20f"
    );
}

#[test]
fn forwarded_map_version_parses_and_defaults_to_none() {
    let mut headers = HeaderMap::new();
    assert_eq!(RoutedRouter::forwarded_map_version(&headers), None);
    headers.insert(MAP_VERSION_HEADER, "7".parse().unwrap());
    assert_eq!(RoutedRouter::forwarded_map_version(&headers), Some(7));
}

/// #2496: `Physical > Marked > AlreadyMarked > NotFound`, symmetric
/// regardless of argument order — the precedence [`merge_drop_outcomes`]
/// must honor when reducing every shard's [`DropOutcome`] into one
/// caller-facing answer.
#[test]
fn merge_drop_outcomes_precedence() {
    use DropOutcome::*;
    for (a, b, want) in [
        (Physical, NotFound, Physical),
        (NotFound, Physical, Physical),
        (Physical, Marked, Physical),
        (Physical, AlreadyMarked, Physical),
        (Marked, AlreadyMarked, Marked),
        (AlreadyMarked, Marked, Marked),
        (Marked, NotFound, Marked),
        (AlreadyMarked, NotFound, AlreadyMarked),
        (NotFound, NotFound, NotFound),
    ] {
        assert_eq!(
            merge_drop_outcomes(a, b),
            want,
            "merge_drop_outcomes({a:?}, {b:?})"
        );
    }
}

/// #2496: the wire only distinguishes `202`/`204`/`404` — `204`
/// disambiguates to `Physical` vs. `AlreadyMarked` purely from the
/// caller-known `force` flag, since within one fan-out call `force` is
/// uniform across every shard and the two outcomes are mutually
/// exclusive given a single `force` value.
#[test]
fn drop_outcome_from_status_disambiguates_204_by_force() {
    assert_eq!(
        drop_outcome_from_status(reqwest::StatusCode::ACCEPTED, false).unwrap(),
        DropOutcome::Marked
    );
    assert_eq!(
        drop_outcome_from_status(reqwest::StatusCode::NO_CONTENT, false).unwrap(),
        DropOutcome::AlreadyMarked
    );
    assert_eq!(
        drop_outcome_from_status(reqwest::StatusCode::NO_CONTENT, true).unwrap(),
        DropOutcome::Physical
    );
    assert_eq!(
        drop_outcome_from_status(reqwest::StatusCode::NOT_FOUND, true).unwrap(),
        DropOutcome::NotFound
    );
    assert!(drop_outcome_from_status(reqwest::StatusCode::BAD_REQUEST, true).is_err());
}

mod forwarded;
