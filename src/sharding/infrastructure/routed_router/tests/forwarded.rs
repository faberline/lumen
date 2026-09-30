use std::collections::BTreeMap;
use std::sync::Arc;

use axum::http::HeaderMap;

use crate::index::application::engine::Engine;
use crate::sharding::application::ports::routed_backend::RoutedBackend;
use crate::sharding::domain::forward_error::{ShardForwardMisrouted, ShardMapVersionMismatch};
use crate::sharding::infrastructure::routed_router::tests::{shard_map, DummyWrite};
use crate::sharding::infrastructure::routed_router::{
    RoutedRouter, FORWARDED_HEADER, MAP_VERSION_HEADER,
};
use crate::shared_kernel::types::{schema::CreateCollectionRequest, search::SearchRequest};

fn test_router(local_shard: u32) -> RoutedRouter {
    RoutedRouter::new(
        Arc::new(Engine::new()),
        Arc::new(DummyWrite),
        shard_map(2),
        local_shard,
        vec![
            "http://search-0.headless".into(),
            "http://search-1.headless".into(),
        ],
    )
    .unwrap()
}

#[test]
fn check_forwarded_map_version_ok_when_absent_or_matching() {
    let router = test_router(0);
    assert!(router
        .check_forwarded_map_version(&HeaderMap::new())
        .is_ok());

    let mut headers = HeaderMap::new();
    headers.insert(
        MAP_VERSION_HEADER,
        router.shard_map.version().to_string().parse().unwrap(),
    );
    assert!(router.check_forwarded_map_version(&headers).is_ok());
}

#[test]
fn check_forwarded_map_version_rejects_mismatch() {
    let router = test_router(0);
    let mut headers = HeaderMap::new();
    headers.insert(
        MAP_VERSION_HEADER,
        (router.shard_map.version() + 1)
            .to_string()
            .parse()
            .unwrap(),
    );
    let err = router.check_forwarded_map_version(&headers).unwrap_err();
    assert!(err.downcast_ref::<ShardMapVersionMismatch>().is_some());
}

/// #1457 R4 AC4: a keyless (scatter) sub-request forwarded from a peer
/// running a *different* shard-map version must still be answered
/// locally, not rejected with `ShardMapVersionMismatch` — the whole
/// point of exempting scatter sub-requests from the map-version check.
/// A keyed forward under the exact same mismatched-version header must
/// still be rejected, proving the exemption is scoped to keyless
/// requests only, not a blanket bypass.
#[tokio::test]
async fn search_already_forwarded_keyless_ignores_map_version_mismatch() {
    let router = test_router(0);
    let mut fields = BTreeMap::new();
    fields.insert(
        "city".to_string(),
        crate::shared_kernel::types::schema::FieldSpec {
            field_type: crate::shared_kernel::types::schema::FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    router
        .engine
        .create_collection("coll", CreateCollectionRequest { fields })
        .unwrap();

    let mut headers = HeaderMap::new();
    headers.insert(FORWARDED_HEADER, "1".parse().unwrap());
    headers.insert(
        MAP_VERSION_HEADER,
        (router.shard_map.version() + 1)
            .to_string()
            .parse()
            .unwrap(),
    );

    let keyless_req = search_req(None);
    let resp = router
        .search("coll", keyless_req, &headers)
        .await
        .expect("a keyless scatter sub-request must not be rejected on map-version mismatch");
    assert_eq!(resp.hits.len(), 0);

    let mut keyed_req = search_req(None);
    keyed_req.routing_key = Some("some-key".into());
    let err = router
        .search("coll", keyed_req, &headers)
        .await
        .expect_err("a keyed forward must still enforce the map-version check");
    assert!(err.downcast_ref::<ShardMapVersionMismatch>().is_some());
}

/// #1467 R6/AC6: the availability-over-completeness exemption above is
/// paired with an observable signal — a keyless (scatter) sub-request
/// whose sender declared a different map version than this pod's live
/// one must still answer locally (never rejected), but must also
/// increment `lumen_scatter_map_version_mismatches_total` exactly once
/// per mismatched sub-request. A matching-version scatter sub-request
/// must not increment it at all.
#[tokio::test]
async fn search_already_forwarded_keyless_mismatch_increments_scatter_metric() {
    let router = test_router(0);
    let mut fields = BTreeMap::new();
    fields.insert(
        "city".to_string(),
        crate::shared_kernel::types::schema::FieldSpec {
            field_type: crate::shared_kernel::types::schema::FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    router
        .engine
        .create_collection("coll", CreateCollectionRequest { fields })
        .unwrap();

    let before = router
        .engine
        .metrics()
        .scatter_map_version_mismatches_total
        .get();

    // Matching version: no mismatch, counter untouched.
    let mut matching_headers = HeaderMap::new();
    matching_headers.insert(FORWARDED_HEADER, "1".parse().unwrap());
    matching_headers.insert(
        MAP_VERSION_HEADER,
        router.shard_map.version().to_string().parse().unwrap(),
    );
    router
        .search("coll", search_req(None), &matching_headers)
        .await
        .expect("matching-version scatter sub-request must succeed");
    assert_eq!(
        router
            .engine
            .metrics()
            .scatter_map_version_mismatches_total
            .get(),
        before,
        "a matching declared map version must not increment the mismatch counter"
    );

    // Mismatched version: answers locally (per the existing exemption
    // test above) but must now also increment the mismatch counter.
    let mut mismatched_headers = HeaderMap::new();
    mismatched_headers.insert(FORWARDED_HEADER, "1".parse().unwrap());
    mismatched_headers.insert(
        MAP_VERSION_HEADER,
        (router.shard_map.version() + 1)
            .to_string()
            .parse()
            .unwrap(),
    );
    router
        .search("coll", search_req(None), &mismatched_headers)
        .await
        .expect("a keyless scatter sub-request must not be rejected on map-version mismatch");
    assert_eq!(
        router
            .engine
            .metrics()
            .scatter_map_version_mismatches_total
            .get(),
        before + 1,
        "a mismatched declared map version on a keyless scatter sub-request must \
             increment lumen_scatter_map_version_mismatches_total exactly once"
    );
}

fn search_req(sort: Option<Vec<crate::shared_kernel::types::query::SortSpec>>) -> SearchRequest {
    use crate::shared_kernel::types::{
        document::FieldValue,
        query::{QueryNode, TermQuery},
    };
    SearchRequest {
        query: QueryNode::Term(TermQuery {
            field: "city".into(),
            value: FieldValue::String("taipei".into()),
        }),
        limit: 10,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort,
        track_total: true,
        collapse: None,
    }
}

/// #1442 R1: `assert_owns` must reject a bucket this pod does not own —
/// the core of closing the spoofed-forwarded-marker gap.
#[test]
fn assert_owns_rejects_bucket_owned_by_another_shard() {
    let router = test_router(0);
    let remote_id = (0..1000)
        .map(|i| format!("doc-{i}"))
        .find(|id| router.shard_map.route_document("coll", None, id).shard != 0)
        .expect("some id routes to shard 1 out of 2");
    let err = router.assert_owns("coll", &remote_id).unwrap_err();
    assert!(err.downcast_ref::<ShardForwardMisrouted>().is_some());
}

#[test]
fn assert_owns_accepts_locally_owned_bucket() {
    let router = test_router(0);
    let local_id = (0..1000)
        .map(|i| format!("doc-{i}"))
        .find(|id| router.shard_map.route_document("coll", None, id).shard == 0)
        .expect("some id routes to shard 0 out of 2");
    assert!(router.assert_owns("coll", &local_id).is_ok());
}
