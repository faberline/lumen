use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::index::application::engine::Engine;
use crate::sharding::application::ports::routed_backend::RoutedBackend;
use crate::sharding::domain::virtual_bucket_shard_map::VirtualBucketShardMap;
use crate::sharding::infrastructure::routed_router::tests::{shard_map, DummyWrite};
use crate::sharding::infrastructure::routed_router::RoutedRouter;
use crate::shared_kernel::types::{
    document::{FieldValue, IndexItem, IndexRequest},
    query::{QueryNode, TermQuery},
    schema::{CreateCollectionRequest, FieldSpec, FieldType},
    search::{SearchHit, SearchRequest, SearchResponse},
};

type FakeHandler = Box<dyn Fn(&str, SearchRequest) -> (Option<Duration>, Response) + Send + Sync>;

struct FakeShardServer {
    url: String,
    handler: Arc<Mutex<FakeHandler>>,
    server_task: JoinHandle<()>,
}

impl Drop for FakeShardServer {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

async fn fake_search_handler(
    State(handler): State<Arc<Mutex<FakeHandler>>>,
    Path(collection_id): Path<String>,
    Json(req): Json<SearchRequest>,
) -> Response {
    let (delay, resp) = {
        let guard = handler.lock().unwrap();
        guard(&collection_id, req)
    };
    if let Some(dur) = delay {
        tokio::time::sleep(dur).await;
    }
    resp
}

impl FakeShardServer {
    async fn new<F>(f: F) -> Self
    where
        F: Fn(&str, SearchRequest) -> Response + Send + Sync + 'static,
    {
        Self::with_delay(move |col, req| (None, f(col, req))).await
    }

    async fn with_delay<F>(f: F) -> Self
    where
        F: Fn(&str, SearchRequest) -> (Option<Duration>, Response) + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake remote shard ephemeral port");
        let port = listener
            .local_addr()
            .expect("fake listener local addr")
            .port();
        let handler: Arc<Mutex<FakeHandler>> = Arc::new(Mutex::new(Box::new(f)));
        let app = Router::new()
            .route(
                "/collections/{collection_id}/search",
                post(fake_search_handler),
            )
            .with_state(handler.clone());
        let server_task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            url: format!("http://127.0.0.1:{port}"),
            handler,
            server_task,
        }
    }

    fn set_handler<F>(&self, f: F)
    where
        F: Fn(&str, SearchRequest) -> Response + Send + Sync + 'static,
    {
        *self.handler.lock().unwrap() = Box::new(move |col, req| (None, f(col, req)));
    }
}

fn router_with_fake(local_engine: Arc<Engine>, fake_url: &str) -> RoutedRouter {
    RoutedRouter::new(
        local_engine,
        Arc::new(DummyWrite),
        shard_map(2),
        0,
        vec!["http://search-0.headless".into(), fake_url.to_string()],
    )
    .expect("build test RoutedRouter")
}

fn setup_collection(engine: &Engine, collection_id: &str) {
    let mut fields = BTreeMap::new();
    fields.insert(
        "city".to_string(),
        FieldSpec {
            field_type: FieldType::Keyword,
            analyzer: None,
            multi: None,
            dim: None,
            metric: None,
            backend: None,
            quantize: None,
        },
    );
    engine
        .create_collection(collection_id, CreateCollectionRequest { fields })
        .expect("create test collection");
}

fn index_doc(engine: &Engine, collection_id: &str, external_id: &str, city: &str) {
    let req = IndexRequest {
        items: vec![IndexItem {
            external_id: external_id.into(),
            field: "city".into(),
            value: FieldValue::String(city.into()),
            version: None,
        }],
        request_id: None,
    };
    engine
        .index(collection_id, req)
        .expect("index test document");
}

fn search_city_req(city: &str) -> SearchRequest {
    SearchRequest {
        query: QueryNode::Term(TermQuery {
            field: "city".into(),
            value: FieldValue::String(city.into()),
        }),
        limit: 10,
        offset: 0,
        cursor: None,
        routing_key: None,
        sort: None,
        track_total: true,
        collapse: None,
    }
}

/// Topology revision: with the fake answering, the merged page holds the local
/// and remote hits in global score order, and total is the sum of both.
#[tokio::test]
async fn topology_revision_merged_score_order_and_total_sum() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    let fake = FakeShardServer::new(|_col, _req| {
        (
            StatusCode::OK,
            Json(SearchResponse {
                hits: vec![
                    SearchHit {
                        external_id: "doc-remote-high".into(),
                        score: 5.0,
                    },
                    SearchHit {
                        external_id: "doc-remote-low".into(),
                        score: 0.1,
                    },
                ],
                total: 2,
                cursor: None,
                took_ms: 1,
                took_us: 1000,
            }),
        )
            .into_response()
    })
    .await;

    let router = router_with_fake(engine, &fake.url);
    let resp = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("scatter search succeeds");

    assert_eq!(
        resp.total, 3,
        "total is the sum of local (1) and remote (2)"
    );
    assert_eq!(resp.hits.len(), 3);
    assert_eq!(resp.hits[0].external_id, "doc-remote-high");
    assert_eq!(resp.hits[1].external_id, "doc-local");
    assert_eq!(resp.hits[2].external_id, "doc-remote-low");
}

/// Topology revision: a second test changes the router's shard_map to a
/// different map_version and asserts the keyless scatter still succeeds
/// (scatter sub-requests are exempt from the version check, as in forwarded.rs).
#[tokio::test]
async fn topology_revision_different_map_version_scatter_still_succeeds() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    let fake = FakeShardServer::new(|_col, _req| {
        (
            StatusCode::OK,
            Json(SearchResponse {
                hits: vec![SearchHit {
                    external_id: "doc-remote".into(),
                    score: 2.0,
                }],
                total: 1,
                cursor: None,
                took_ms: 1,
                took_us: 1000,
            }),
        )
            .into_response()
    })
    .await;

    let mut router = router_with_fake(engine, &fake.url);
    // Change router's shard_map to a different version (version 7 instead of 0).
    router.shard_map = VirtualBucketShardMap::balanced(7, 16, 2).expect("balanced 2-shard map v7");

    let resp = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("scatter sub-requests are exempt from the map-version check");

    assert_eq!(resp.total, 2);
    assert_eq!(resp.hits.len(), 2);
}

/// Member loss: when the fake answers 404 (CollectionNotFound) and the local
/// engine holds the collection, the page is the local hits only.
#[tokio::test]
async fn member_loss_fake_404_when_local_holds_collection_returns_local_hits() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    let fake = FakeShardServer::new(|_col, _req| {
        (StatusCode::NOT_FOUND, "collection not found").into_response()
    })
    .await;

    let router = router_with_fake(engine, &fake.url);
    let resp = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("scatter search tolerates remote 404 when local collection exists");

    assert_eq!(resp.total, 1);
    assert_eq!(resp.hits.len(), 1);
    assert_eq!(resp.hits[0].external_id, "doc-local");
}

/// Member loss: when the local engine lacks the collection and the fake answers
/// 404, the call returns the not-found error.
#[tokio::test]
async fn member_loss_fake_404_when_local_lacks_collection_returns_not_found() {
    let engine = Arc::new(Engine::new());
    // Do not create collection on local engine.

    let fake = FakeShardServer::new(|_col, _req| {
        (StatusCode::NOT_FOUND, "collection not found").into_response()
    })
    .await;

    let router = router_with_fake(engine, &fake.url);
    let err = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect_err("when every shard reports 404, scatter search surfaces the error");

    let err_str = err.to_string();
    assert!(
        err_str.contains("not found") || err_str.contains("NotFound"),
        "error must be not found: {err_str}"
    );
}

/// Member loss: when the remote URL points at a closed port, scatter_search
/// returns Err and never a partial page.
#[tokio::test]
async fn member_loss_closed_port_returns_err_never_partial_page() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    // Bind and immediately drop listener to reserve a closed local port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let closed_port = listener.local_addr().expect("local addr").port();
    drop(listener);

    let router = router_with_fake(engine, &format!("http://127.0.0.1:{closed_port}"));
    let err = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect_err(
            "closed remote port must fail the whole search, never returning a partial page",
        );

    let err_str = err.to_string();
    assert!(
        !err_str.is_empty(),
        "must return an error when remote port is closed"
    );
}

/// Stale generation: when the fake answers a non-404 error (for example 409
/// or 503 with a body), scatter_search returns Err and does not fall back to
/// a cached or local-only page.
#[tokio::test]
async fn stale_generation_non_404_errors_return_err_no_fallback() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    let fake = FakeShardServer::new(|_col, _req| {
        (StatusCode::CONFLICT, "stale generation 409 conflict").into_response()
    })
    .await;

    let router = router_with_fake(engine.clone(), &fake.url);
    let err_409 = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect_err("409 conflict must fail scatter search without fallback");
    assert!(
        err_409.to_string().contains("409") || err_409.to_string().contains("conflict"),
        "error must reflect 409: {err_409}"
    );

    // Also test 503 Service Unavailable.
    fake.set_handler(|_col, _req| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "shard temporarily unavailable 503",
        )
            .into_response()
    });

    let err_503 = router
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect_err("503 service unavailable must fail scatter search without fallback");
    assert!(
        err_503.to_string().contains("503") || err_503.to_string().contains("unavailable"),
        "error must reflect 503: {err_503}"
    );
}

/// Cross-collection independence: with collections a and b on the same router,
/// a search on a returns no hit of b, and a not-found on b does not change
/// the result of a.
#[tokio::test]
async fn cross_collection_independence() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll-a");
    index_doc(&engine, "coll-a", "doc-local-a", "taipei");
    setup_collection(&engine, "coll-b");
    index_doc(&engine, "coll-b", "doc-local-b", "taipei");

    let fake = FakeShardServer::new(|col, _req| match col {
        "coll-a" => (
            StatusCode::OK,
            Json(SearchResponse {
                hits: vec![SearchHit {
                    external_id: "doc-remote-a".into(),
                    score: 3.0,
                }],
                total: 1,
                cursor: None,
                took_ms: 1,
                took_us: 1000,
            }),
        )
            .into_response(),
        "coll-b" => (StatusCode::NOT_FOUND, "collection b not found on remote").into_response(),
        _ => (StatusCode::NOT_FOUND, "unknown collection").into_response(),
    })
    .await;

    let router = router_with_fake(engine, &fake.url);

    // Search on coll-a
    let resp_a1 = router
        .search("coll-a", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("search on coll-a succeeds");
    assert_eq!(resp_a1.total, 2);
    assert!(
        resp_a1.hits.iter().all(|h| h.external_id.ends_with("-a")),
        "search on coll-a must return no hits from coll-b"
    );

    // Search on coll-b (remote returns 404, local has coll-b)
    let resp_b = router
        .search("coll-b", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("search on coll-b succeeds with local hits");
    assert_eq!(resp_b.total, 1);
    assert_eq!(resp_b.hits[0].external_id, "doc-local-b");

    // Second search on coll-a: verify not-found on coll-b did not change coll-a
    let resp_a2 = router
        .search("coll-a", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("search on coll-a still succeeds");
    assert_eq!(resp_a2.total, 2);
    assert_eq!(resp_a2.hits[0].external_id, "doc-remote-a");
    assert_eq!(resp_a2.hits[1].external_id, "doc-local-a");
}

/// Cancellation: the test aborts the scatter future while the fake delays
/// its answer (tokio::time::timeout or JoinHandle::abort), then runs a second
/// scatter on the same router and asserts it succeeds with the right hits.
#[tokio::test]
async fn cancellation_aborted_future_leaves_router_usable() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    let fake = FakeShardServer::with_delay(|_col, _req| {
        (
            Some(Duration::from_secs(5)),
            (
                StatusCode::OK,
                Json(SearchResponse {
                    hits: vec![SearchHit {
                        external_id: "doc-remote".into(),
                        score: 4.0,
                    }],
                    total: 1,
                    cursor: None,
                    took_ms: 5000,
                    took_us: 5000000,
                }),
            )
                .into_response(),
        )
    })
    .await;

    let router = Arc::new(router_with_fake(engine, &fake.url));

    // Abort the scatter search future via tokio::time::timeout.
    let req = search_city_req("taipei");
    let router_clone = router.clone();
    let aborted = tokio::time::timeout(
        Duration::from_millis(50),
        router_clone.search("coll", req.clone(), &HeaderMap::new()),
    )
    .await;
    assert!(
        aborted.is_err(),
        "first scatter future was aborted by timeout"
    );

    // Set fake server to answer promptly without delay.
    fake.set_handler(|_col, _req| {
        (
            StatusCode::OK,
            Json(SearchResponse {
                hits: vec![SearchHit {
                    external_id: "doc-remote-fast".into(),
                    score: 2.0,
                }],
                total: 1,
                cursor: None,
                took_ms: 1,
                took_us: 1000,
            }),
        )
            .into_response()
    });

    // Run a second scatter search on the SAME router instance.
    let resp = router
        .search("coll", req, &HeaderMap::new())
        .await
        .expect("second scatter on same router succeeds after aborted future");

    assert_eq!(resp.total, 2);
    assert_eq!(resp.hits.len(), 2);
    assert_eq!(resp.hits[0].external_id, "doc-remote-fast");
    assert_eq!(resp.hits[1].external_id, "doc-local");
}

/// Negative control: a test replaces the tolerant CollectionNotFound handling
/// by asserting on a helper, or runs the same scenario with the fake answering
/// 500 instead of 404, and asserts the outcome differs (Err rather than the
/// local-only page). This proves the 404 tolerance test fails when 404 is
/// treated like any other error.
#[tokio::test]
async fn negative_control_500_differs_from_404_tolerance() {
    let engine = Arc::new(Engine::new());
    setup_collection(&engine, "coll");
    index_doc(&engine, "coll", "doc-local", "taipei");

    // 1. With fake answering 404, scatter search tolerates it and returns local-only hits.
    let fake_404 = FakeShardServer::new(|_col, _req| {
        (StatusCode::NOT_FOUND, "collection not found on remote").into_response()
    })
    .await;
    let router_404 = router_with_fake(engine.clone(), &fake_404.url);
    let resp_404 = router_404
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect("404 must be tolerated and return local hits");
    assert_eq!(resp_404.total, 1);
    assert_eq!(resp_404.hits.len(), 1);
    assert_eq!(resp_404.hits[0].external_id, "doc-local");

    // 2. Negative control: with fake answering 500 under the identical setup,
    // scatter search fails with Err rather than returning the local-only page.
    let fake_500 = FakeShardServer::new(|_col, _req| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal server error on remote",
        )
            .into_response()
    })
    .await;
    let router_500 = router_with_fake(engine, &fake_500.url);
    let err_500 = router_500
        .search("coll", search_city_req("taipei"), &HeaderMap::new())
        .await
        .expect_err("500 internal server error must not be tolerated as missing collection");

    assert!(
        err_500.to_string().contains("500") || err_500.to_string().contains("internal"),
        "error must reflect 500 failure: {err_500}"
    );
}
