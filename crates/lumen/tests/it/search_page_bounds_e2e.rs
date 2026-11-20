use std::sync::Arc;

use axum::http::{Method, StatusCode};
use axum_test::TestServer;
use serde_json::{json, Value};

use lumen::api::{router, AppState};
use lumen::storage::Engine;

fn server() -> TestServer {
    let engine = Arc::new(Engine::new());
    TestServer::new(router(AppState::open(engine))).expect("test server")
}

/// Three documents with the same score, inserted out of external-ID order.
async fn seed(server: &TestServer) {
    server
        .put("/collections/ties")
        .json(&json!({ "fields": { "group": { "type": "keyword" } } }))
        .await
        .assert_status_ok();
    server
        .post("/collections/ties/index")
        .json(&json!({
            "items": [
                { "external_id": "zeta", "field": "group", "value": "same" },
                { "external_id": "mid", "field": "group", "value": "same" },
                { "external_id": "alpha", "field": "group", "value": "same" }
            ]
        }))
        .await
        .assert_status_ok();
}

fn term_query(limit: u64, offset: u64) -> Value {
    json!({
        "query": { "term": { "field": "group", "value": "same" } },
        "limit": limit,
        "offset": offset
    })
}

fn hit_ids(response: &Value) -> Vec<&str> {
    response["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["external_id"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn equal_score_hits_come_back_in_external_id_order() {
    let server = server();
    seed(&server).await;
    for _ in 0..2 {
        let response: Value = server
            .post("/collections/ties/search")
            .json(&term_query(10, 0))
            .await
            .json();
        assert_eq!(hit_ids(&response), vec!["alpha", "mid", "zeta"]);
        assert_eq!(response["total"], 3);
    }
}

#[tokio::test]
async fn limit_above_cap_is_rejected_and_cap_is_accepted() {
    let server = server();
    seed(&server).await;

    let rejected = server
        .post("/collections/ties/search")
        .json(&term_query(1001, 0))
        .await;
    rejected.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(rejected.json::<Value>()["error"], "invalid_pagination");

    server
        .post("/collections/ties/search")
        .json(&term_query(1000, 0))
        .await
        .assert_status_ok();
}

#[tokio::test]
async fn offset_above_cap_is_rejected_and_cap_is_accepted() {
    let server = server();
    seed(&server).await;

    let rejected = server
        .post("/collections/ties/search")
        .json(&term_query(10, 10001))
        .await;
    rejected.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(rejected.json::<Value>()["error"], "invalid_pagination");

    let accepted: Value = server
        .post("/collections/ties/search")
        .json(&term_query(10, 10000))
        .await
        .json();
    assert_eq!(accepted["hits"].as_array().unwrap().len(), 0);
    assert_eq!(accepted["total"], 3);
}

#[tokio::test]
async fn query_method_twin_applies_the_same_cap() {
    let server = server();
    seed(&server).await;

    let rejected = server
        .method(Method::from_bytes(b"QUERY").unwrap(), "/collections/ties")
        .json(&term_query(1001, 0))
        .await;
    rejected.assert_status(StatusCode::BAD_REQUEST);
    assert_eq!(rejected.json::<Value>()["error"], "invalid_pagination");
}

#[tokio::test]
async fn search_all_is_not_capped() {
    let server = server();
    seed(&server).await;

    let response: Value = server
        .post("/collections/ties/search:all")
        .json(&json!({ "query": { "term": { "field": "group", "value": "same" } } }))
        .await
        .json();
    assert_eq!(response["external_ids"], json!(["alpha", "mid", "zeta"]));
}
